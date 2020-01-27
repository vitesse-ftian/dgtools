Copyright (c) 2017, VitesseData Inc.  

# TPCDS Benchmark

Just follow the instruction in tpch/README.   Basically source ./env.sh and 
run
```
make gendata
# start xdrive using xdrctl start gen/xdrive.toml
make load
make run
```

# About TPCDS Queries.
Queries are taking from https://github.com/pivotalguru/TPC-DS, please see the
excellent explaination on what changes they made to original TPCDS query. 
Hint: All costmetic.

We did a few changes as well.
1. Changed from numeric(7,2) to double precision or smallnumber.   We advise 
   all our customer to use smallnumber/number instead of numeric anyway.
2. Made all queries into views so it is much easier to run them.   Cosmetic 
   changes so that create view will not clash on column names (avg1, avg2,...).   
3. A few quries in pivotalguru has two versions(Q 14/23/24/39).   We will use
   version 1.

## Remark on Distribution Key and Partition.
I just used whatever pivotalguru uses.   TPCDS is a very complicated benchmark 
and it is not clear if this is optimal desgin.   But anyway.   Good tests.

