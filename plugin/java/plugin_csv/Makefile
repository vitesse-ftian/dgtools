CSVJAR=../jars/commons-csv-1.5.jar
VITESSEJAR=$(GPHOME)/plugin/jars/vitessedata-xdrive2.jar
PROTOBUFJAR=$(GPHOME)/plugin/jars/protobuf-java-3.4.0.jar
PROTOBUFUTILJAR=$(GPHOME)/plugin/jars/protobuf-java-util-3.4.0.jar
CP=$(PROTOBUFJAR):$(PROTOBUFUTILJAR):$(VITESSEJAR):$(CSVJAR):.

all: csv
	mkdir -p classes
	(cd classes ; jar xf ../../jars/commons-csv-1.5.jar)
	jar cf vitessedata-csv-plugin.jar -C classes .
	tar -zcvf plugin_csv.tgz vitessedata-csv-plugin.jar

csv:
	mkdir -p classes
	javac -d classes -cp $(CP) ./src/com/vitessedata/xdrive/csv/*.java 

clean:
	rm -fr classes plugin_csv.tgz vitessedata-csv-plugin.jar classes/com/vitessedata/xdrive/csv/*.class

