#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)"
PHIHOME=$(dirname "$DIR")

export PYTHONPATH=$PYTHONPATH:$PHIHOME/py2

cd $1
shift

python -u phi_main.py $*
