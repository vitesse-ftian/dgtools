#!/bin/bash

(cd tpch_2_15_0/dbgen; make)

rm -fr ./data1; mkdir data1
(cd tpch_2_15_0/dbgen/; ./dbgen -f -s 1)
mv tpch_2_15_0/dbgen/*.tbl data1
sed -i 's/|$//' ./data1/*.tbl

rm -fr ./data10; mkdir data10
(cd tpch_2_15_0/dbgen/; ./dbgen -f -s 10)
mv tpch_2_15_0/dbgen/*.tbl data10
sed -i 's/|$//' ./data10/*.tbl
