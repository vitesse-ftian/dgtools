Copyright (c) 2017, VitesseData Inc.  

# TPCH Benchmark
Make sure dg, psql, xdrctl is in $PATH.   To test Greenplum, just
put greenplumn bin before deepgreen bin in $PATH.   Test need to 
set up two env variable.   Just source ./env.sh.

You need to have go installed, plus all dependencies.  You need to
have a directory $HOME/go, then the following should do.
```
go get -t ./bench
```
You may see the following error message.  It is OK, just ignore. 
```
go install: no install location for directory /home/centos/p/dgtools/tpch/bench outside GOPATH
	For more details see: 'go help gopath'

```

Test config is in bench.toml file.  We use golang test.  See golang 
testing package for how to run a test or subtest. There are a bunch 
of makefile target in the Makefile.  In general one should
just use make to drive the tests.   The following are details,

# Use Make
To run the benchmark, edit bench.toml, source ./env.sh, then
```
make gendata
make ddl
xdrctl deploy gen/xdrive.toml
xdrctl start gen/xdrive.toml
make load
make run
```

# Notes to Greenplum User
Some deepgreen extensions are not available on opensource Greenplum DB.
You need to edit bench.toml, esp, you should set the following two values.
```
DDL = "mktab-f.sql"
Ext = "GPF"
```

# Manually (instead of using make)
## GenData
Generate data.   Costly.   Not timed/measured.
```
go test -run GenData ./bench
```

## Setup
Setup ddl etc.   Cheap.    Not timed/measured.
```
go test -run Setup ./bench
```

## Load
Loading data.   Benchmarked.   Note that we used -benchtime=0s
to disable golang test b.N loop.   We want to load/bench the 
query only once, regardless how fast/slow etc.  

Also, -run=None basically disable all tests.   Otherwise, golang
test will try to gen data, setup etc again.

```
go test -run=None -bench=Load -benchtime=0s ./bench 
```
