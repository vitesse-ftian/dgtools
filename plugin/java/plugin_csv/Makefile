CSVJAR=../jars/commons-csv-1.5.jar
VITESSEJAR=$(GPHOME)/plugin/jars/vitessedata-xdrive2.jar
PROTOBUFJAR=$(GPHOME)/plugin/jars/protobuf-java-3.4.0.jar
PROTOBUFUTILJAR=$(GPHOME)/plugin/jars/protobuf-java-util-3.4.0.jar
CP=$(PROTOBUFJAR):$(PROTOBUFUTILJAR):$(VITESSEJAR):$(CSVJAR):.

all: csv
	mkdir -p package/jars
	(cd classes ; jar xf ../../jars/commons-csv-1.5.jar)
	jar cf package/jars/vitessedata-csv-plugin.jar -C classes .
	(cd package ; tar -zcvf plugin_csv.tgz jars/vitessedata-csv-plugin.jar)

csv:
	mkdir -p classes
	javac -d classes -cp $(CP) ./src/com/vitessedata/xdrive/csv/*.java 

clean:
	rm -fr classes package

cpbin:
	cp package/jars/vitessedata-csv-plugin.jar /tmp/wetestdata/xdrive/plugin/jars
