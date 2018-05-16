Copyright (c) 2017, VitesseData Inc.  

# TinyBench 
TinyBench is a tiny benchmark for testing very short transctions that 
do a few insert/update/select.

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

# Use Make
To run the benchmark, edit bench.toml, source ./env.sh, then
```
make db
make fdb
```

make fdb may fail, due to golang binding version mismatch.   Suppose you installed foundationdb 
5.2, then cd to ~/go/src/github.com/apple/foundationdb, and git checkout branch release-5.2, then
you should be good to go.

Also note that make fdb is rather slow -- foundationdb want you to fire up more concurrent clients.
run20 will run 20 clients, and 20 clients can finish in the same time as 1 cli (sometimes even faster).
