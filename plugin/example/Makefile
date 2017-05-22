start:
	echo "Setup/Deploy xdrive ..." 
	rm -fr /tmp/wetestdata/xdrive
	mkdir -p /tmp/wetestdata/xdrive/data
	xdrctl deploy ./xdrive.toml
	echo "Start xdrive ..."
	xdrctl start ./xdrive.toml
	echo "Copying some file ..."
	cp ./x1.csv /tmp/wetestdata/xdrive/data
	cp ../bin/fsplugin_csv /tmp/wetestdata/xdrive/xdrplugin

stop:
	echo "Stop xdrive ..."
	xdrctl stop ./xdrive.toml

