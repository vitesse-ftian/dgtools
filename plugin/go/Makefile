build:
	(cd src/vitessedata/fsplugin_csv && go get . && go install)
#	(cd src/vitessedata/s3plugin_csv && go get . && go install)
#	(cd src/vitessedata/esplugin_spq && go get . && go install)
#	(cd src/vitessedata/hbaseplugin_spq && go get . && go install)
#	(cd src/vitessedata/kafkaplugin_spq && go get . && go install)
	rm -rf package ; mkdir package
#	(cd bin && tar -zcvf ../package/esplugin.tgz esplugin_spq)
	(cd bin && tar -zcvf ../package/fsplugin.tgz fsplugin_csv)
#	(cd bin && tar -zcvf ../package/hbaseplugin.tgz hbaseplugin_spq)
#	(cd bin && tar -zcvf ../package/s3plugin.tgz s3plugin_csv)
#	(cd bin && tar -zcvf ../package/kafkaplugin.tgz kafkaplugin_spq)

start:
	echo "Setup/Deploy xdrive ..." 
	rm -fr /tmp/wetestdata/xdrive
	mkdir -p /tmp/wetestdata/xdrive/data
	xdrctl deploy ./xdrive.toml
	echo "Start deploy plugin bits ... " 
	cp -r package /tmp/wetestdata/xdrive/plugin
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
	cp bin/* /tmp/wetestdata/xdrive/xdrplugin

cppkg:
	echo "Copy package to xdrplugin ... " 
	cp -r package /tmp/wetestdata/xdrive/xdrplugin

deploy:
	echo "Start deply plugin bits ... "
#	xdrctl deployplugin xdrive.toml esplugin
	xdrctl deployplugin xdrive.toml fsplugin
#	xdrctl deployplugin xdrive.toml hbaseplugin
#	xdrctl deployplugin xdrive.toml s3plugin
#	xdrctl deployplugin xdrive.toml kafkaplugin
