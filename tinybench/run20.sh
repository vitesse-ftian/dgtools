#!/bin/bash
for i in `seq 1 100`;
do
	go test -run=NONO -bench=Foundation -benchtime=0s -timeout 24h ./bench &
done
