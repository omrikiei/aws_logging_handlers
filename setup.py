from setuptools import setup, find_packages
import os


def read(fname):
    try:
        return open(os.path.join(os.path.dirname(__file__), fname), 'r').read()
    except:
        return ""


setup(
    name='aws-logging-handlers',
    packages=find_packages(),
    version='2.0.3',
    description='Logging aws_logging_handlers to AWS services that support S3 and Kinesis stream logging with multiple threads',
    long_description=read('README.rst'),
    author='Omri Eival',
    author_email='omrieival@gmail.com',
    url='https://github.com/omrikiei/aws_logging_handlers/',
    download_url='https://github.com/omrikiei/aws_logging_handlers/archive/2.0.3.tar.gz',
    keywords=['logging', 's3', 'aws', 'handler', 'amazon', 'stream', 'kinesis', 'firehose'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',

    ],
    test_suite='nose.collector',
    tests_require=['nose'],
    install_requires=[
        'boto3'
    ],
)
