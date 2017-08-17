Copyright (c) 2017, VitesseData Inc.  

# TPCH Benchmark

To generate data, run go test.   For example,
```
   BENCH_DIR=$PWD go test -run GenData ./bench
```

Or, for example, 
```
export BENCH_DIR=$PWD
go test -run GenData/Step=dbgen ./bench
```
