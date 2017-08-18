Copyright (c) 2017, VitesseData Inc.  

# TPCH Benchmark

Make sure dg, psql, xdrctl is in $PATH.   To test Greenplum, just
put greenplumn bin before deepgreen bin in $PATH.   
```
export BENCH_DIR=$PWD
```

Test config is in bench.toml file.  

We use golang test.   See golang testing package for how to run a
test or subtest, here we list two examples.
```
	BENCH_DIR=$PWD go test -run GenData ./bench
	go test -run GenData/Step=dbgen ./bench
```

There are a series of tests to run.

# GenData
Generate data.   Costly.   Not timed/measured.
```
go test -run GenData ./bench
```

# Setup
Setup ddl etc.   Cheap.    Not timed/measured.
```
go test -run Setup ./bench
```
