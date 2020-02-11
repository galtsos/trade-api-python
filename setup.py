from glob import glob
from os.path import basename, splitext

import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='galts-trade-api',
    version='0.1.0',
    license='Mozilla Public License 2.0',
    author='Sergey Nevmerzhitsky',
    author_email='sergey.nevmerzhitsky@gmail.com',
    description='A framework to use a trading infrastructure',
    long_description=long_description,
    long_description_content_type='text/markdown',
    keywords=['crypto', 'trading'],
    python_requires='~=3.7',
    install_requires=[
        'aio-pika~=5.0',
        'grpcio~=1.20',
        'protobuf~=3.11',  # Required by grpc_utils
        'structlog~=19.2.0',
        'ujson~=1.35',
    ],
    packages=setuptools.find_packages('src'),
    # https://packaging.python.org/guides/distributing-packages-using-setuptools/#py-modules
    py_modules=[splitext(basename(path))[0] for path in glob('src/*.py')],
    package_dir={'': 'src'},
    include_package_data=True,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Financial and Insurance Industry',
        'License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
    ],
)
