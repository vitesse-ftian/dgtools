#!/bin/bash

python softmax.py --job_name=ps --task_index=0 2>/dev/null &
python softmax.py --job_name=worker --task_index=1 2>/dev/null &
python softmax.py --job_name=worker --task_index=0 2>/dev/null &
