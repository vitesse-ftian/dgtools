tpch1f:
	createdb tpch1f
	psql -f mktab-f.sql tpch1f
	psql -f mkview-n.sql tpch1f
	psql -f load.sql tpch1f


data: dbgen
	rm -fr ./data; mkdir data
	(cd tpch_2_15_0/dbgen/; ./dbgen -f -s 1)
	mv tpch_2_15_0/dbgen/*.tbl data
	sed -i 's/|$//' ./data/*.tbl

dbgen:
	(cd tpch_2_15_0/dbgen; make)
