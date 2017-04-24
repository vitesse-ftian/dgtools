#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)"
PHIHOME=$(dirname "$DIR")

source $PHIHOME/scripts/env.sh

cd $1
$PHIHOME/go/bin/phi codegen -input=phi_main.go > phi_gen.go 
go build -o phi_main
