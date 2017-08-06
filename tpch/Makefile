tpch1f:
	createdb tpch1f
	psql -f mktab-f.sql tpch1f
	psql -f mkview-n.sql tpch1f
	psql -f load1.sql tpch1f

tpch10f:
	createdb tpch10f
	psql -f mktab-f.sql tpch10f
	psql -f mkview-n.sql tpch10f
	psql -f load10.sql tpch10f

tpch10fao:
	createdb tpch10fao
	psql -f mktab-fao.sql tpch10fao
	psql -f mkview-n.sql tpch10fao
	psql -f load10.sql tpch10fao

start:
	echo "Setup/Deploy xdrive ..." 
	rm -fr /tmp/wetestdata/xdrive
	mkdir -p /tmp/wetestdata/xdrive/data
	xdrctl deploy ./xdrive.toml
	echo "Start xdrive ..."
	xdrctl start ./xdrive.toml
	echo "Sym linking ..." 
	ln -s ${PWD}/data1  /tmp/wetestdata/xdrive/data/tpch1

stop:
	echo "Stop xdrive ..."
	xdrctl stop ./xdrive.toml


