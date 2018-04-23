__author__ = 'Omri Eival'
__version__ = '0.1.1'

from logging import StreamHandler
from io import BufferedIOBase, BytesIO
from boto3 import Session
from datetime import datetime

import atexit
import signal
import gevent

DEFAULT_CHUNK_SIZE = 5 * 1024 ** 2  # 5 MB
DEFAULT_ROTATION_TIME_SECS = 12 * 60 * 60  # 12 hours
MAX_FILE_SIZE_BYTES = 100 * 1024 ** 2  # 100 MB


class StreamObject:
    """
    Class representation of the s3 object along with all the needed metadata to stream to S3
    """

    def __init__(self, s3_resource, bucket_name, filename):
        self.object = s3_resource.Object(bucket_name, filename)
        self.uploader = self.object.initiate_multipart_upload()
        self.bucket = bucket_name
        try:
            total_bytes = s3_resource.meta.client.head_object(Bucket=self.bucket.name, Key=filename)
        except Exception:
            total_bytes = 0

        self.buffer = BytesIO()
        self.chunk_count = 0
        self.byte_count = total_bytes
        self.parts = []
        self.tasks = {}

    def add_task(self, task_id, task):
        self.tasks[task_id] = task

    def remove_task(self, task_id):
        del self.tasks[task_id]


class S3Streamer(BufferedIOBase):
    """
    The stream interface used by the handler which binds to S3 and utilizes the object class
    """

    def __init__(self, bucket, key_id, secret, key, chunk_size=DEFAULT_CHUNK_SIZE,
                 max_file_log_time=DEFAULT_ROTATION_TIME_SECS, max_file_size_bytes=MAX_FILE_SIZE_BYTES,
                 encoder='utf-8'):

        self.session = Session(key_id, secret)
        self.s3 = self.session.resource('s3')
        self.start_time = int(datetime.utcnow().strftime('%s'))
        self.key = key
        self.chunk_size = chunk_size
        self.max_file_log_time = max_file_log_time
        self.max_file_size_bytes = max_file_size_bytes
        self.current_file_name = "{}_{}".format(key, int(datetime.utcnow().strftime('%s')))
        self.encoder = encoder

        try:
            self.s3.meta.client.head_bucket(Bucket=bucket)
        except Exception:
            raise ValueError('Bucket %s does not exist, or missing permissions' % bucket)

        self._bucket = self.s3.Bucket(bucket)
        self._current_object = self._get_stream_object(self.current_file_name)
        self._rotation_tasks = {}

        self._is_open = True

        BufferedIOBase.__init__(self)

    def add_task(self, task_id, task):
        self._rotation_tasks[task_id] = task

    def remove_task(self, task_id):
        del self._rotation_tasks[task_id]

    def _get_stream_object(self, filename):
        try:
            return StreamObject(self.s3, self._bucket.name, filename)

        except Exception:
            raise RuntimeError('Failed to open new S3 stream object')

    def _rotate_chunk(self, async=True):

        assert self._current_object, "Stream object not found"

        part_num = self._current_object.chunk_count + 1
        part = self._current_object.uploader.Part(part_num)
        buffer = self._current_object.buffer
        self._current_object.buffer = BytesIO()
        buffer.seek(0)
        if async:
            task_id = datetime.utcnow().strftime('%s')
            self._current_object.add_task(task_id,
                                          gevent.spawn(self._upload_part, self._current_object, task_id, part,
                                                       part_num, buffer))
        else:
            upload = part.upload(Body=buffer)
            self._current_object.parts.append({'ETag': upload['ETag'], 'PartNumber': part_num})

        self._current_object.chunk_count += 1

    @staticmethod
    def _upload_part(s3_object, task_id, part, part_num, buffer):
        upload = part.upload(Body=buffer)
        s3_object.parts.append({'ETag': upload['ETag'], 'PartNumber': part_num})
        s3_object.remove_task(task_id)

    def _rotate_file(self):

        if self._current_object.buffer.tell() > 0:
            self._rotate_chunk()

        temp_object = self._current_object
        task_id = datetime.utcnow().strftime('%s')
        self.add_task(task_id,
                      gevent.spawn(self._close_stream, temp_object, callback=self.remove_task, task_id=task_id))
        new_filename = "{}_{}".format(self.key, int(datetime.utcnow().strftime('%s')))
        self.start_time = int(datetime.utcnow().strftime('%s'))
        self._current_object = self._get_stream_object(new_filename)

    @staticmethod
    def _close_stream(stream_object, callback=None, *args, **kwargs):
        gevent.wait(stream_object.tasks.values())
        if stream_object.chunk_count > 0:
            stream_object.uploader.complete(MultipartUpload={'Parts': stream_object.parts})
        else:
            stream_object.uploader.abort()

        if callback and callable(callback):
            callback(*args, **kwargs)

    def close(self, *args, **kwargs):

        if self._current_object.buffer.tell() > 0:
            self._rotate_chunk(async=False)
        gevent.wait(self._rotation_tasks.values())
        self._close_stream(self._current_object)

        self._is_open = False

    @property
    def closed(self):
        return not self._is_open

    @property
    def writable(self, *args, **kwargs):
        return True

    def tell(self, *args, **kwargs):
        return self._current_object.byte_count

    def write(self, *args, **kwargs):
        s = args[0]
        self._current_object.buffer.write(s.encode(self.encoder))
        self._current_object.byte_count = self._current_object.byte_count + len(s)

        if self._current_object.buffer.tell() > self.chunk_size:
            self._rotate_chunk()

        if (self.max_file_size_bytes and self._current_object.byte_count > self.max_file_size_bytes) or (
                self.max_file_log_time and int(
            datetime.utcnow().strftime('%s')) - self.start_time > self.max_file_log_time):
            self._rotate_file()

        return len(s)


class S3Handler(StreamHandler):
    """
    A Logging handler class that streams log records to S3 by chunks
    """

    def __init__(self, filename, bucket, key_id, secret, chunk_size=DEFAULT_CHUNK_SIZE,
                 time_rotation=DEFAULT_ROTATION_TIME_SECS, max_file_size_bytes=MAX_FILE_SIZE_BYTES, encoder='utf-8'):
        """

        :param filename: The name of the S3 object
        :param bucket: The id of the S3 bucket
        :param key_id: Authentication key
        :param secret: Authentication secret
        :param chunk_size: Size of a chunk in the multipart upload in bytes - default 5MB
        :param time_rotation: Interval in seconds to rotate the file by - default 12 hours
        :param max_file_size_bytes: Maximum file size in bytes before rotation - default 100MB
        :param encoder: default utf-8
        """
        self.bucket = bucket
        self.secret = secret
        self.key_id = key_id
        self.stream = S3Streamer(self.bucket, self.key_id, self.secret, filename, chunk_size, time_rotation,
                                 max_file_size_bytes, encoder)

        # Make sure we gracefully clear the buffers and upload the missing parts before existing
        signal.signal(signal.SIGTERM, self.close)
        signal.signal(signal.SIGINT, self.close)
        signal.signal(signal.SIGQUIT, self.close)
        atexit.register(self.close)

        StreamHandler.__init__(self, self.stream)

    def close(self, *args, **kwargs):
        """
        Closes the stream
        """
        self.acquire()
        try:
            if self.stream:
                try:
                    self.flush()
                finally:
                    stream = self.stream
                    self.stream = None
                    if hasattr(stream, "close"):
                        stream.close(*args, **kwargs)
        finally:
            self.release()
