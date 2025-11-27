#!/bin/bash

cd /home/marchezi/python_projects/process_embrace_1secMag

conda init

conda activate spaceData

python download-embrace-data.py

conda activate spaceData

python magnetometer-converter.py