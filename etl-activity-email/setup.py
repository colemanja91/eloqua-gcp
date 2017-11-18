""" Basic setup file for grouping dependencies """

import setuptools

setuptools.setup(
    name='etl-elq-activities-all',
    version='0.0.1',
    install_requires=[
        'pyeloqua>=0.5.6'
    ],
    packages=setuptools.find_packages(),
)
