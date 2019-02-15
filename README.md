# AWS Logging Handlers

A python multithreaded logging handler package that streams records to AWS services objects with support for the following AWS services:
* S3
* Kinesis

Has supports for gzip compression(in S3)

## Getting Started

### Prerequisites

Asynchronous multipart uploading relies on the ability to use multiple threads
#### Packages:
```
boto3
```

### Installing

Installation using pip

```
pip install aws-logging-handlers
```

### Examples
Stream log records to S3 and Kinesis
```
import logging
from aws_logging_handlers import S3Handler, KinesisHandler

bucket="test_bucket" # The bucket should already exist

# The log will be rotated to a new object either when an object reaches 5 MB or when 120 seconds pass from the last rotation/initial logging
s3_handler = S3Handler("test_log", bucket, workers=3)
kinesis_handler = KinesisHandler('log_test', 'us-east-1', workers=1)
formatter = logging.Formatter('[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')
s3_handler.setFormatter(formatter)
kinesis_handler.setFormatter(formatter)
logger = logging.getLogger('root')
logger.setLevel(logging.INFO)
logger.addHandler(s3_handler)
logger.addHandler(kinesis_handler)

for i in range(0, 100000):
    logger.info("test info message")
    logger.warning("test warning message")
    logger.error("test error message")
```

## To be developed
* Support for asyncio
* Logging and upload metrics

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE) file for details

