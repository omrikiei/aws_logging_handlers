"""
Kinesis Binding Module with logging handler and stream object
"""
__author__ = 'Omri Eival'

from logging import StreamHandler
from io import BufferedIOBase, BytesIO
from boto3 import client
from aws_logging_handlers.validation import is_non_empty_string, is_positive_int, empty_str_err, \
    bad_integer_err, ValidationRule
from aws_logging_handlers.tasks import Task, task_worker, STOP_SIGNAL

import logging
import atexit
import signal
import threading
from queue import Queue

MAX_CHUNK_SIZE = 1 * 1024 ** 2  # 1 MB
DEFAULT_CHUNK_SIZE = int(0.5 * 1024 ** 2)  # 0.5 MB
MIN_WORKERS_NUM = 1


class KinesisStream(BufferedIOBase):
    """
    stream interface used by the handler which binds to Kinesis Firehose and uploads log records
    """

    def __init__(self, stream_name: str, partition_key: str, *, chunk_size: int = DEFAULT_CHUNK_SIZE,
                 encoder: str = 'utf-8', workers: int = 1, **boto_session_kwargs):
        """

        :param stream_name: Name of the Kinesis stream
        :type stream_name: str
        :param partition_key: Kinesis partition key used to group data by shards
        :type partition_key: str
        :param chunk_size: the size of a a chunk of records for rotation threshold (default 524288)
        :type chunk_size: int
        :param encoder: the encoder to be used for log records (default 'utf-8')
        :type encoder: str
        :param workers: the number of background workers that rotate log records (default 1)
        :type workers: int
        :param boto_session_kwargs: additional keyword arguments for the AWS Kinesis Resource
        :type boto_session_kwargs: boto3 resource keyword arguments
        """

        self._client = client('kinesis', **boto_session_kwargs)
        self.chunk_size = chunk_size
        self.stream_name = stream_name
        self.tasks = Queue()
        self.partition_key = partition_key
        self.encoder = encoder

        try:
            stream_desc = self._client.describe_stream(StreamName=self.stream_name)
            if stream_desc['StreamDescription']['StreamStatus'] != 'ACTIVE':
                raise AssertionError
        except Exception:
            raise ValueError('Kinesis stream %s does not exist or inactive, or insufficient permissions' % stream_name)

        self.workers = [threading.Thread(target=task_worker, args=(self.tasks,), daemon=True).start() for _ in
                        range(int(max(workers, MIN_WORKERS_NUM) / 2) + 1)]
        self._stream = BytesIO()

        self._is_open = True

        BufferedIOBase.__init__(self)

    def add_task(self, task):
        """
        Add a new task to the tasks queue
        :param task: a Task object
        :return:
        """
        self.tasks.put(task)

    def join_tasks(self):
        """
        Join tasks in the queue
        :return:
        """
        self.tasks.join()

    def _rotate_chunk(self, run_async=True):
        """
        Send the accumulated records to the stream and clear the buffer
        :param run_async: Indicates whether the rotation should by asynchronous on a different thread
        :type run_async: bool
        :return:
        """

        assert self._stream, "Stream object not found"

        buffer = self._stream
        self._stream = BytesIO()
        if buffer.tell() > MAX_CHUNK_SIZE:
            # We are limited to a size of 1 MB per stream upload command so we need to enforce it
            chunk_delta = MAX_CHUNK_SIZE - buffer.tell()
            buffer.seek(chunk_delta)
            self._stream.write(buffer.read())
        buffer.seek(0)

        if run_async:
            self.add_task(Task(self._upload_part, buffer))
        else:
            self._upload_part(buffer)

    def _upload_part(self, buffer):
        try:
            self._client.put_record(
                StreamName=self.stream_name,
                Data=buffer.read(MAX_CHUNK_SIZE).decode(self.encoder),
                PartitionKey=self.partition_key
            )
        except Exception:
            logging.exception("Failed to stream to AWS Firehose data stream {}".format(self.stream_name))

    def close(self, *args, **kwargs):
        """
        closes the stream for writing and uploads remaining records to Kinesis
        :param args:
        :param kwargs:
        :return:
        """

        if self._stream.tell() > 0:
            self._rotate_chunk(run_async=False)

        self.join_tasks()

        # Stop the worker threads
        for _ in range(len(self.workers)):
            self.tasks.put(STOP_SIGNAL)

        self._is_open = False

    @property
    def closed(self):
        return not self._is_open

    @property
    def writable(self, *args, **kwargs):
        return True

    @property
    def partition_key(self):
        return self._partition_key

    @partition_key.setter
    def partition_key(self, value):
        if value and type(value) is str:
            self._partition_key = value

    def tell(self, *args, **kwargs):
        """
        indication of current size of the stream before rotation
        :param args:
        :param kwargs:
        :return: size of the current stream
        """
        return self._stream.tell()

    def write(self, *args, **kwargs):
        """
        writes a log record to the stream
        :param args:
        :param kwargs:
        :return: size of record that was written
        """
        s = args[0]
        self._stream.write(s.encode(self.encoder))
        return len(s)

    def flush(self):
        """
        flushes the current stream if it exceeds the threshold size
        :return:
        """
        if self._stream.tell() >= self.chunk_size:
            self._rotate_chunk()


class KinesisHandler(StreamHandler):
    """
    A Logging handler class that streams log records to AWS Kinesis
    """

    def __init__(self, stream_name: str, partition_key: str, *, chunk_size: int = DEFAULT_CHUNK_SIZE,
                 encoder: str = 'utf-8', workers: int = 1, **boto_session_kwargs):
        """
        :param stream_name: Name of the Kinesis stream
        :type stream_name: str
        :param partition_key: Kinesis partition key used to group data by shards
        :type partition_key: str
        :param chunk_size: the size of a a chunk of records for rotation threshold (default 524288)
        :type chunk_size: int
        :param encoder: the encoder to be used for log records (default 'utf-8')
        :type encoder: str
        :param workers: the number of background workers that rotate log records (default 1)
        :type workers: int
        :param boto_session_kwargs: additional keyword arguments for the AWS Kinesis Resource
        :type boto_session_kwargs: boto3 resource keyword arguments
        """

        args_validation = (
            ValidationRule(stream_name, is_non_empty_string, empty_str_err('stream_name')),
            ValidationRule(chunk_size, is_positive_int, bad_integer_err('chunk_size')),
            ValidationRule(encoder, is_non_empty_string, empty_str_err('encoder')),
            ValidationRule(workers, is_positive_int, bad_integer_err('workers')),
        )

        for rule in args_validation:
            assert rule[1](rule[0]), rule[3]

        self.stream = KinesisStream(stream_name, partition_key,
                                    chunk_size=chunk_size, encoder=encoder,
                                    workers=workers,
                                    **boto_session_kwargs)

        # Make sure we gracefully clear the buffers and upload the missing parts before exiting
        signal.signal(signal.SIGTERM, self._teardown)
        signal.signal(signal.SIGINT, self._teardown)
        signal.signal(signal.SIGQUIT, self._teardown)
        atexit.register(self.close)

        StreamHandler.__init__(self, self.stream)

    def _teardown(self, _: int, __):
        self.close()

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
