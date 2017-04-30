#!/bin/bash

python softmax.py --job_name=ps --task_index=0 &
python softmax.py --job_name=worker --task_index=1 &
python softmax.py --job_name=worker --task_index=0 &
