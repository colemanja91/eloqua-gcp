#!/bin/bash

# Upgrade pip and install dependencies

pip install -U pip
pip install --upgrade virtualenv
virtualenv venv
source venv/bin/activate

pip install google-cloud-dataflow
pip install pyeloqua
