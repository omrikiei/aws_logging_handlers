from distutils.core import setup

setup(
    name='logging_s3_handler',
    packages=['logging_s3_handler'],
    version='0.1.1',
    description='A logging handler that streams log records to Aws S3 objects',
    author='Omri Eival',
    author_email='omrieival@gmail.com',
    url='https://github.com/omrikiei/logging_s3_handler/',
    download_url='https://github.com/omrikiei/logging_s3_handler/archive/0.1.tar.gz',
    keywords=['logging', 's3', 'aws', 'handler'],
    classifiers=[],
    install_requires=[
        'boto3',
        'gevent >= 1.2.2'
    ],
)
