__author__ = 'Omri Eival'

from logging import StreamHandler
from io import BufferedIOBase, BytesIO
from boto3 import Session
from datetime import datetime
from aws_logging_handlers.validation import is_non_empty_string, is_positive_int, is_boolean, bad_type_error, \
    empty_str_err, bad_integer_err, ValidationRule
from aws_logging_handlers.tasks import Task, task_worker

import atexit
import signal
import threading
import queue
import gzip

DEFAULT_CHUNK_SIZE = 5 * 1024 ** 2  # 5 MB
DEFAULT_ROTATION_TIME_SECS = 12 * 60 * 60  # 12 hours
MAX_FILE_SIZE_BYTES = 100 * 1024 ** 2  # 100 MB
MIN_WORKERS_NUM = 2


class StreamObject:
    """
    Class representation of the s3 object along with all the needed metadata to stream to S3
    """

    def __init__(self, s3_resource, bucket_name, filename, buffer_queue):
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
        self.tasks = buffer_queue

    def add_task(self, task):
        self.tasks.put(task)

    def join_tasks(self):
        self.tasks.join()


class S3Streamer(BufferedIOBase):
    """
    The stream interface used by the handler which binds to S3 and utilizes the object class
    """

    _stream_buffer_queue = queue.Queue()
    _rotation_queue = queue.Queue()

    def __init__(self, bucket, key_id, secret, key, chunk_size=DEFAULT_CHUNK_SIZE,
                 max_file_log_time=DEFAULT_ROTATION_TIME_SECS, max_file_size_bytes=MAX_FILE_SIZE_BYTES,
                 encoder='utf-8', workers=2, compress=False):

        self.session = Session(key_id, secret)
        self.s3 = self.session.resource('s3')
        self.start_time = int(datetime.utcnow().strftime('%s'))
        self.key = key.strip('/')
        self.chunk_size = chunk_size
        self.max_file_log_time = max_file_log_time
        self.max_file_size_bytes = max_file_size_bytes
        self.current_file_name = "{}_{}".format(key, int(datetime.utcnow().strftime('%s')))
        if compress:
            self.current_file_name = "{}.gz".format(self.current_file_name)
        self.encoder = encoder

        try:
            self.s3.meta.client.head_bucket(Bucket=bucket)
        except Exception:
            raise ValueError('Bucket %s does not exist, or missing permissions' % bucket)

        self._bucket = self.s3.Bucket(bucket)
        self._current_object = self._get_stream_object(self.current_file_name)
        self.workers = [threading.Thread(target=task_worker, args=(self._rotation_queue,)).start() for _ in
                        range(int(max(workers, MIN_WORKERS_NUM) / 2) + 1)]
        self.stream_bg_workers = [threading.Thread(target=task_worker, args=(self._stream_buffer_queue,)).start() for _
                                  in range(max(int(max(workers, MIN_WORKERS_NUM) / 2), 1))]

        self._is_open = True
        self.compress = compress

        BufferedIOBase.__init__(self)

    def get_filename(self):
        filename = "{}_{}".format(self.key, self.start_time)
        if self.compress:
            return filename
        return "{}.gz".format(filename)

    def add_task(self, task):
        self._rotation_queue.put(task)

    def join_tasks(self):
        self._rotation_queue.join()

    def _get_stream_object(self, filename):
        try:
            return StreamObject(self.s3, self._bucket.name, filename, self._stream_buffer_queue)

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
            self._current_object.add_task(Task(self._upload_part, self._current_object, part, part_num, buffer))
        else:
            self._upload_part(self._current_object, part, part_num, buffer)

        self._current_object.chunk_count += 1

    @staticmethod
    def _upload_part(s3_object, part, part_num, buffer):
        upload = part.upload(Body=buffer)
        s3_object.parts.append({'ETag': upload['ETag'], 'PartNumber': part_num})

    def _rotate_file(self):

        if self._current_object.buffer.tell() > 0:
            self._rotate_chunk()

        temp_object = self._current_object
        self.add_task(Task(self._close_stream, stream_object=temp_object))
        self.start_time = int(datetime.utcnow().strftime('%s'))
        new_filename = self.get_filename()
        self._current_object = self._get_stream_object(new_filename)

    @staticmethod
    def _close_stream(stream_object, callback=None, *args, **kwargs):
        stream_object.join_tasks()
        if stream_object.chunk_count > 0:
            stream_object.uploader.complete(MultipartUpload={'Parts': stream_object.parts})
        else:
            stream_object.uploader.abort()

        if callback and callable(callback):
            callback(*args, **kwargs)

    def close(self, *args, **kwargs):

        if self._current_object.buffer.tell() > 0:
            self._rotate_chunk(async=False)

        self._current_object.join_tasks()
        self.join_tasks()

        # Stop the worker threads
        for _ in range(len(self.workers)):
            self._rotation_queue.put(-1)

        for _ in range(len(self.stream_bg_workers)):
            self._stream_buffer_queue.put(-1)

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
        s = self.compress and gzip.compress(args[0].encode(self.encoder)) or args[0].encode(self.encoder)
        self._current_object.buffer.write(s)
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

    def __init__(self, file_path, bucket, key_id, secret, chunk_size=DEFAULT_CHUNK_SIZE,
                 time_rotation=DEFAULT_ROTATION_TIME_SECS, max_file_size_bytes=MAX_FILE_SIZE_BYTES, encoder='utf-8',
                 max_threads=3, compress=False):
        """

        :param file_path: The path of the S3 object
        :param bucket: The id of the S3 bucket
        :param key_id: Authentication key
        :param secret: Authentication secret
        :param chunk_size: Size of a chunk in the multipart upload in bytes - default 5MB
        :param time_rotation: Interval in seconds to rotate the file by - default 12 hours
        :param max_file_size_bytes: Maximum file size in bytes before rotation - default 100MB
        :param encoder: default utf-8
        :param max_threads: the number of threads that a stream handler would run for file and chunk rotation tasks
        :param compress: Boolean indicating weather to save a compressed gz suffixed file
        """

        args_validation = (
            ValidationRule(file_path, is_non_empty_string, empty_str_err('file_path')),
            ValidationRule(bucket, is_non_empty_string, empty_str_err('bucket')),
            ValidationRule(key_id, is_non_empty_string, empty_str_err('key_id')),
            ValidationRule(secret, is_non_empty_string, empty_str_err('secret')),
            ValidationRule(chunk_size, is_positive_int, bad_integer_err('chunk_size')),
            ValidationRule(time_rotation, is_positive_int, bad_integer_err('time_rotation')),
            ValidationRule(max_file_size_bytes, is_positive_int, bad_integer_err('max_file_size_bytes')),
            ValidationRule(encoder, is_non_empty_string, empty_str_err('encoder')),
            ValidationRule(max_threads, is_positive_int, bad_integer_err('thread_count')),
            ValidationRule(compress, is_boolean, bad_type_error('compress', 'boolean'))
        )

        for rule in args_validation:
            assert rule[1](rule[0]), rule[3]

        self.bucket = bucket
        self.secret = secret
        self.key_id = key_id
        self.stream = S3Streamer(self.bucket, self.key_id, self.secret, file_path, chunk_size, time_rotation,
                                 max_file_size_bytes, encoder, workers=max_threads, compress=compress)

        # Make sure we gracefully clear the buffers and upload the missing parts before exiting
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
