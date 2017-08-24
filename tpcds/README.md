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
1. Changed from numeric(7,2) to double precision.   We advise all our customer to use decimal64 instead of numeric anyway.
2. Made all queries into views so it is much easier to run them.   Cosmetic changes so that create view will not clash
   on column names (avg1, avg2,...).   
3. A few quries in pivotalguru has two queries in it.  Not clear to me if the benchmark will run the two queries, or, they are equivalent queries with minor rewrite.   At this momenet, I treated them as need to run two queries.  I create a view to union all the results of the two queries.

## Remark on Distribution Key and Partition.
I just used whatever pivotalguru uses.   TPCDS is a very complicated benchmark and it is not clear if 
this is optimal desgin.   But anyway.   Good tests.

