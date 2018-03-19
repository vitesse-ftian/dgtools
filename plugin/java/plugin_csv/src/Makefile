VITESSEJAR=$(XDRIVE2_HOME)/plugin/jars/vitessedata-xdrive2.jar
PROTOBUFJAR=$(XDRIVE2_HOME)/plugin/jars/protobuf-java-3.4.0.jar
PROTOBUFUTILJAR=$(XDRIVE2_HOME)/plugin/jars/protobuf-java-util-3.4.0.jar
CP=$(PROTOBUFJAR):$(PROTOBUFUTILJAR):$(VITESSEJAR):.

all: csv
	jar cf vitessedata-csv-plugin.jar ./com/vitessedata/xdrive/csv/*.class

csv:
	javac -cp $(CP) ./com/vitessedata/xdrive/csv/*.java 

clean:
	rm -f vitessedata-csv-plugin.jar ./com/vitessedata/xdrive/csv/*.class
