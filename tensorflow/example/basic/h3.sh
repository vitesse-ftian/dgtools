#!/bin/bash

python hidden3.py --job_name=ps --task_index=0 & 
python hidden3.py --job_name=worker --task_index=1 & 
python hidden3.py --job_name=worker --task_index=0 & 
