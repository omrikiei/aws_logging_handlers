from distutils.core import setup
import io, os

def read(fname):
    return io.open(os.path.join(os.path.dirname(__file__), fname), encoding='utf-8').read()

setup(
    name='logging-s3-handler',
    packages=['logging_s3_handler'],
    version='0.1.3.1',
    description='A multithreaded logging handler that streams log records to Aws S3 objects',
    long_description=read('README.rst'),
    author='Omri Eival',
    author_email='omrieival@gmail.com',
    url='https://github.com/omrikiei/logging_s3_handler/',
    download_url='https://github.com/omrikiei/logging_s3_handler/archive/0.1.3.tar.gz',
    keywords=['logging', 's3', 'aws', 'handler', 'amazon'],
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
