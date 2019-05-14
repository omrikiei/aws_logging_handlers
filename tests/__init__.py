import unittest
import logging
import time

from localstack.services import infra
from localstack.utils.aws import aws_stack
from localstack import config
from aws_logging_handlers.S3 import S3Handler
from aws_logging_handlers.Kinesis import KinesisHandler


class Base(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        infra.start_infra(asynchronous=True, apis=('s3', 'kinesis'))

    @classmethod
    def tearDownClass(cls):
        infra.stop_infra()


class S3Test(Base):

    def setUp(self):
        self.s3_client = aws_stack.connect_to_service('s3')
        self.bucket = "test_log_bucket"

        self.s3_client.create_bucket(Bucket=self.bucket)
        try:
            b_objects = [{'Key': o['Key']} for o in self.s3_client.list_objects(Bucket=self.bucket).get('Contents')]

            self.s3_client.delete_objects(Bucket=self.bucket, Delete={
                'Objects': b_objects
            })
        except:
            pass

    def tearDown(self):
        logging.getLogger('root').removeHandler(self.s3_handler)

    def test_basic_logging_s3(self):
        s3_handler = S3Handler("test_log", self.bucket, time_rotation=120,
                               max_file_size_bytes=5 * 1024 ** 2, compress=False, endpoint_url=config.TEST_S3_URL,
                               verify=False)
        formatter = logging.Formatter('[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')
        s3_handler.setFormatter(formatter)
        s3_handler.setLevel(logging.DEBUG)
        self.s3_handler = s3_handler
        logger = logging.getLogger('root')
        logger.addHandler(s3_handler)

        log_record_num = 10000
        for i in range(0, log_record_num):
            logger.info("test info message")
            logger.warning("test warning message")
            logger.error("test error message")

        bytes_written = s3_handler.stream.tell()
        #s3_handler.close()

        log_records = self.s3_client.list_objects(Bucket=self.bucket)
        assert (sum([s['Size'] for s in log_records['Contents']]) == bytes_written)


class KinesisTest(Base):
    def setUp(self):
        self.kinesis_client = aws_stack.connect_to_service('kinesis')
        self.stream_name = "test_stream"
        self.shard_count = 1
        try:
            self.kinesis_client.create_stream(StreamName=self.stream_name, ShardCount=self.shard_count)
        except Exception:
            pass
        finally:
            time.sleep(2)
            assert (self.stream_name in self.kinesis_client.list_streams()['StreamNames'])

    def tearDown(self):
        logging.getLogger('root').removeHandler(self.k_handler)

    def test_basic_logging_kinesis(self):
        kinesis_handler = KinesisHandler(self.stream_name, 'default', region_name="us-east-1",
                                         chunk_size=100,
                                         endpoint_url=config.TEST_KINESIS_URL, verify=False)
        kinesis_handler.setLevel(logging.DEBUG)
        self.k_handler = kinesis_handler
        logger = logging.getLogger('root')
        logger.addHandler(kinesis_handler)
        stream_desc = self.kinesis_client.describe_stream(StreamName=self.stream_name)
        shard_id = stream_desc['StreamDescription']['Shards'][0]['ShardId']
        shard_iterator = self.kinesis_client.get_shard_iterator(StreamName=self.stream_name, ShardId=shard_id,
                                                                ShardIteratorType='LATEST')
        s_iterator = shard_iterator['ShardIterator']

        log_record_num = 10
        for i in range(0, log_record_num):
            logger.info("test info message")
            logger.warning("test warning message")
            logger.error("test error message")

        kinesis_handler.close()

        log_records = self.kinesis_client.get_records(ShardIterator=s_iterator, Limit=1)

        log_accumulator = 0
        try:
            while 'NextShardIterator' in log_records and log_accumulator < log_record_num*3:
                log_accumulator += sum([len(d['Data'].decode('utf-8').strip().split('\n')) for d in log_records['Records']])
                log_records = self.kinesis_client.get_records(ShardIterator=log_records['NextShardIterator'], Limit=1)
        finally:
            assert(log_accumulator == log_record_num*3)
