from distutils.core import setup
import io, os

def read(fname):
    try:
      return io.open(os.path.join(os.path.dirname(__file__), fname), encoding='utf-8').read()
    except Exception:
      return ""

setup(
    name='aws-logging-handlers',
    packages=['aws_logging_handlers', 'aws_logging_handlers.tasks'],
    version='0.1.6',
    description='Logging handlers to AWS services that support S3 and Kinesis stream logging with multiple threads',
    long_description=read('README.rst'),
    author='Omri Eival',
    author_email='omrieival@gmail.com',
    url='https://github.com/omrikiei/aws_logging_handlers/',
    download_url='https://github.com/omrikiei/aws_logging_handlers/archive/0.1.5.tar.gz',
    keywords=['logging', 's3', 'aws', 'handler', 'amazon', 'stream', 'kinesis', 'firehose'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',

    ],
    install_requires=[
        'boto3'
    ],
)
