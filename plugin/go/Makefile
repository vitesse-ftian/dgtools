build:
	(cd src/vitessedata/fsplugin_csv && go get . && go install)
	(cd src/vitessedata/s3plugin_csv && go get . && go install)

test: plugin start fsplugin_csv s3plugin_csv

start:
	echo "Setup/Deploy xdrive ..." 
	rm -fr /tmp/wetestdata/xdrive
	mkdir -p /tmp/wetestdata/xdrive/data
	xdrctl deploy ./xdrive.toml
	echo "Start deploy plugin bits ... " 
	cp bin/* /tmp/wetestdata/xdrive/xdrplugin
	echo "Start xdrive ..."
	xdrctl start ./xdrive.toml
	echo "Copying some file ..."
	cp data/* /tmp/wetestdata/xdrive/data

stop:
	echo "Stop xdrive ..."
	xdrctl stop ./xdrive.toml

fsplugin_csv:
	echo "Running fsplugin test ..."
	go test wetestdata/fsplugin 

s3plugin_csv:
	echo "Running fsplugin test ..."
	go test wetestdata/s3plugin 

wetestdata:
	-psql -c 'drop database wetestdata' template1
	psql -c 'create database wetestdata' template1
	dg setup -all wetestdata
	psql -f xddl.sql wetestdata

cpbin:
	echo "Start deploy plugin bits ... " 
	cp bin/* /tmp/wetestdata/xdrive/xdrplugin
