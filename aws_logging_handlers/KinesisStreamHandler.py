__author__ = 'Omri Eival'

from logging import StreamHandler
from io import BufferedIOBase, BytesIO
from boto3 import client
from aws_logging_handlers.validation import is_non_empty_string, is_positive_int, empty_str_err, bad_integer_err, ValidationRule
from aws_logging_handlers.tasks import Task, task_worker

import logging
import atexit
import signal
import threading
import queue

MAX_CHUNK_SIZE = 1 * 1024 ** 2  # 1 MB
DEFAULT_CHUNK_SIZE = int(0.5 * 1024 ** 2)  # 0.5 MB
MIN_WORKERS_NUM = 1


class Task:
    def __init__(self, callable_func, *args, **kwargs):
        assert callable(callable_func), "First argument in task should be callable"
        self.callable_func = callable_func
        self.args = args
        self.kwargs = kwargs


def task_worker(q):
    while True:
        if not q.empty():
            task = q.get()
            if task == -1:
                return
            assert isinstance(task, (Task,)), "task should be of type `Task` only!"
            task.callable_func(*task.args, **task.kwargs)
            q.task_done()


class KinesisStreamer(BufferedIOBase):
    """
    The stream interface used by the handler which binds to Kinesis and utilizes the object class
    """

    _stream_buffer_queue = queue.Queue()

    def __init__(self, key_id, secret, stream_name, region, partition, chunk_size=DEFAULT_CHUNK_SIZE, encoder='utf-8',
                 workers=2):

        self.kinesis = client('kinesis', region_name=region, aws_access_key_id=key_id,
                              aws_secret_access_key=secret)
        self.chunk_size = chunk_size
        self.stream_name = stream_name
        self.region = region
        self.tasks = queue.Queue()
        self.partition = partition
        self.encoder = encoder

        try:
            stream_desc = self.kinesis.describe_stream(StreamName=self.stream_name)
            if stream_desc['StreamDescription']['StreamStatus'] != 'ACTIVE':
                raise AssertionError
        except Exception:
            raise ValueError('Kinesis stream %s does not exist or inactive, or insufficient permissions' % stream_name)

        self.workers = [threading.Thread(target=task_worker, args=(self.tasks,)).start() for _ in
                        range(int(max(workers, MIN_WORKERS_NUM) / 2) + 1)]
        self._stream = BytesIO()

        self._is_open = True

        BufferedIOBase.__init__(self)

    def add_task(self, task):
        self.tasks.put(task)

    def join_tasks(self):
        self.tasks.join()

    def _rotate_chunk(self, async=True):

        assert self._stream, "Stream object not found"

        buffer = self._stream
        self._stream = BytesIO()
        if buffer.tell() > MAX_CHUNK_SIZE:
            # We are limited to a size of 1 MB per stream upload command so we need to enforce it
            chunk_delta = MAX_CHUNK_SIZE - buffer.tell()
            buffer.seek(chunk_delta)
            self._stream.write(buffer.read())
        buffer.seek(0)

        if async:
            self.add_task(Task(self._upload_part, buffer))
        else:
            self._upload_part(buffer)

    def _upload_part(self, buffer):
        try:
            self.kinesis.put_record(
                StreamName=self.stream_name,
                Data=buffer.read(MAX_CHUNK_SIZE).decode(self.encoder),
                PartitionKey=self.partition
            )
        except Exception:
            logging.exception("Failed to stream to AWS Firehose data stream {}".format(self.stream_name))

    def close(self, *args, **kwargs):

        if self._stream.tell() > 0:
            self._rotate_chunk(async=False)

        self.join_tasks()

        # Stop the worker threads
        for _ in range(len(self.workers)):
            self.tasks.put(-1)

        self._is_open = False

    @property
    def closed(self):
        return not self._is_open

    @property
    def writable(self, *args, **kwargs):
        return True

    def tell(self, *args, **kwargs):
        return self._stream.tell()

    def write(self, *args, **kwargs):
        s = args[0]
        self._stream.write(s.encode(self.encoder))

        if self._stream.tell() >= self.chunk_size:
            self._rotate_chunk()

        return len(s)


class KinesisHandler(StreamHandler):
    """
    A Logging handler class that streams log records to AWS Kinesis by chunks
    """

    def __init__(self, key_id, secret, stream_name, region, partition='single', chunk_size=DEFAULT_CHUNK_SIZE,
                 encoder='utf-8', workers=3):
        """

        :param key_id: Authentication key
        :param secret: Authentication secret
        :param stream_name: The name of the kinesis stream
        :param region: The AWS region for the kinesis stream
        :param partition: A partition name in case multiple shards are used
        :param chunk_size: Size of a chunk in the multipart upload in bytes - default 5MB
        :param encoder: default utf-8
        :param workers: the number of threads that a stream handler would run for file and chunk rotation tasks
        """

        args_validation = (
            ValidationRule(key_id, is_non_empty_string, empty_str_err('key_id')),
            ValidationRule(secret, is_non_empty_string, empty_str_err('secret')),
            ValidationRule(stream_name, is_non_empty_string, empty_str_err('stream_name')),
            ValidationRule(region, is_non_empty_string, empty_str_err('region')),
            ValidationRule(partition, is_non_empty_string, empty_str_err('partition')),
            ValidationRule(chunk_size, is_positive_int, bad_integer_err('chunk_size')),
            ValidationRule(encoder, is_non_empty_string, empty_str_err('encoder')),
            ValidationRule(workers, is_positive_int, bad_integer_err('workers')),
        )

        for rule in args_validation:
            assert rule[1](rule[0]), rule[3]

        self.secret = secret
        self.key_id = key_id
        self.stream = KinesisStreamer(self.key_id, self.secret, stream_name, region, partition, chunk_size, encoder,
                                      workers=workers)

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
