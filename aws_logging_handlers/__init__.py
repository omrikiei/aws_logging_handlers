"""
aws-logging-aws_logging_handlers is an open source package that was written to provide a seamless and abstract way to emit log
records to the cloud. It currently supports AWS S3 and Kinesis Streams services, additional services/vendors will be
evaluated upon community demand.
"""

from aws_logging_handlers.S3 import S3Handler
from aws_logging_handlers.Kinesis import KinesisHandler

__author__ = 'Omri Eival'
__version__ = '2.0.1'
__all__ = [S3Handler, KinesisHandler]