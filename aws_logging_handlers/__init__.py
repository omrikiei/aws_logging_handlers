"""
aws_logging_handlers is an open source package that was written to provide a seamless and abstract way to emit log
records to the cloud.

It currently supports AWS S3 and Kinesis Streams services, additional services/vendors will be
evaluated upon community demand.
"""

import aws_logging_handlers.S3
import aws_logging_handlers.Kinesis

__author__ = 'Omri Eival'
__version__ = '2.0.1'
