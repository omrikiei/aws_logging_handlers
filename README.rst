Logging S3 Handler
==================

A python multithreaded logging handler that streams lines to AWS S3
objects

Getting Started
---------------

Prerequisites
~~~~~~~~~~~~~

Asynchronous multipart uploading relies on the ability to use multiple threads

::

    boto3

Installing
~~~~~~~~~~

Installation using pip

::

    pip install logging-s3-handler

Running the tests
-----------------

::

    To Do

Examples
~~~~~~~~

Stream log records to S3

::

    import logging
    from logging_s3_handler import S3Handler

    KEY_ID="your_aws_auth_key"
    SECRET="your_aws_auth_secret"
    bucket="test_bucket" # The bucket should already exist

    # The log will be rotated to a new object either when an object reaches 5 MB or when 120 seconds pass from the last rotation/initial logging
    handler = S3Handler("test_log", bucket, KEY_ID, SECRET, time_rotation=120, max_file_size_bytes=5*1024**2, workers=3)
    formatter = logging.Formatter('[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger = logging.getLogger('root')
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    for i in range(0, 100000):
        logger.info("test info message")
        logger.warning("test warning message")
        logger.error("test error message")


To be developed
---------------

-  Logging and upload metrics

License
-------

This project is licensed under the MIT License - see the
`LICENSE.md <LICENSE>`__ file for details


