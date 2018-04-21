from distutils.core import setup

setup(
    name='logging-s3-handler',
    packages=['logging_s3_handler'],
    version='0.1.1',
    description='A logging handler that streams log records to Aws S3 objects',
    author='Omri Eival',
    author_email='omrieival@gmail.com',
    url='https://github.com/omrikiei/logging_s3_handler/',
    download_url='https://github.com/omrikiei/logging_s3_handler/archive/0.1.tar.gz',
    keywords=['logging', 's3', 'aws', 'handler'],
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
        'boto3',
        'gevent >= 1.2.2'
    ],
)
