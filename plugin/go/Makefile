build:
	(cd src/vitessedata/fsplugin_csv && go get . && go install)
	(cd src/vitessedata/s3plugin_csv && go get . && go install)
	(cd src/vitessedata/esplugin_spq && go get . && go install)
	(cd src/vitessedata/hbaseplugin_spq && go get . && go install)
	(cd src/vitessedata/kafkaplugin_spq && go get . && go install)
	(cd src/vitessedata/esload_spq && go get . && go install)
	(cd src/vitessedata/ls_file && go get . && go install)
	rm -rf package ; mkdir package
	(cd bin && tar -zcvf ../package/esplugin.tgz esplugin_spq)
	(cd bin && tar -zcvf ../package/fsplugin.tgz fsplugin_csv)
	(cd bin && tar -zcvf ../package/hbaseplugin.tgz hbaseplugin_spq)
	(cd bin && tar -zcvf ../package/s3plugin.tgz s3plugin_csv)
	(cd bin && tar -zcvf ../package/kafkaplugin.tgz kafkaplugin_spq)
	(cd bin && tar -zcvf ../package/esload.tgz esload_spq)
	(cd bin && tar -zcvf ../package/lsfile.tgz ls_file) 

build_fdb:
	(cd src/vitessedata/fdb_spq && go get . && go install)
	(cd bin && tar -zcvf ../package/fdb.tgz fdb_spq)

start:
	echo "Setup/Deploy xdrive ..." 
	rm -fr /tmp/wetestdata/xdrive
	mkdir -p /tmp/wetestdata/xdrive/data
	xdrctl deploy ./xdrive.toml
	echo "Start xdrive ..."
	xdrctl start ./xdrive.toml
	echo "Copying some file ..."
	cp data/* /tmp/wetestdata/xdrive/data

stop:
	echo "Stop xdrive ..."
	xdrctl stop ./xdrive.toml

testfs: 
	echo "Running fsplugin test ..."
	go test wetestdata/fsplugin 

tests3:
	echo "Running fsplugin test ..."
	go test wetestdata/s3plugin 

testes:
	echo "Running esplugin test ..."
	go test wetestdata/esplugin

wetestdata:
	-psql -c 'drop database wetestdata' template1
	psql -c 'create database wetestdata' template1
	dg setup -all wetestdata
	psql -f xddl.sql wetestdata

cpbin:
	echo "Start deploy plugin bits ... " 
	cp bin/* /tmp/wetestdata/xdrive/plugin

cppkg:
	echo "Copy package to xdrplugin ... " 
	cp -r package /tmp/wetestdata/xdrive/plugin

deploy:
	echo "Start deply plugin bits ... "
	xdrctl deployplugin xdrive.toml package/esplugin.tgz
	xdrctl deployplugin xdrive.toml package/fsplugin.tgz
	xdrctl deployplugin xdrive.toml package/hbaseplugin.tgz
	xdrctl deployplugin xdrive.toml package/s3plugin.tgz
	xdrctl deployplugin xdrive.toml package/kafkaplugin.tgz
	xdrctl deployplugin xdrive.toml package/esload.tgz
	xdrctl deployplugin xdrive.toml package/lsfile.tgz
	#	xdrctl deployplugin xdrive.toml package/fdb.tgz 

