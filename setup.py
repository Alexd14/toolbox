from distutils.core import setup

from setuptools import find_packages

setup(
    name='factor-toolbox',
    version='1.1.4',
    packages=find_packages(),
    license='MIT',
    description='A simple toolbox to aid in the exploration of potential alpha sources.',
    author='Alex',
    author_email='',
    url='https://github.com/Alexd14/factor-toolbox',
    download_url='https://github.com/Alexd14/toolbox/archive/refs/tags/v1.1.4.tar.gz',
    keywords=['factor', 'equity factor'],
    install_requires=[
        'pandas',
        'pandas_market_calendars',
        'tqdm',
        'numpy',
        'duckdb'
    ],
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
)
