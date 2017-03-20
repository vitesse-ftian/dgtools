#!/bin/bash

(cd tpch_2_15_0/dbgen; make)
rm -fr ./data; mkdir data
(cd tpch_2_15_0/dbgen/; ./dbgen -f -s 1)
mv tpch_2_15_0/dbgen/*.tbl data
sed -i 's/|$//' ./data/*.tbl

