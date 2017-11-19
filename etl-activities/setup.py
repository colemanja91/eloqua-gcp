"""
    Setup file
"""

import setuptools

setuptools.setup(
    name='dataflow-etl-eloqua',
    version='0.0.1',
    install_requires=[
        'pyeloqua==0.5.6'
    ],
    packages=setuptools.find_packages(),
)
