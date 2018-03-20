CSVJAR=../../jars/commons-csv-1.5.jar
VITESSEJAR=$(GPHOME)/plugin/jars/vitessedata-xdrive2.jar
PROTOBUFJAR=$(GPHOME)/plugin/jars/protobuf-java-3.4.0.jar
PROTOBUFUTILJAR=$(GPHOME)/plugin/jars/protobuf-java-util-3.4.0.jar
CP=$(PROTOBUFJAR):$(PROTOBUFUTILJAR):$(VITESSEJAR):$(CSVJAR):.

all: csv
	jar cf vitessedata-csv-plugin.jar ./com/vitessedata/xdrive/csv/*.class

csv:
	javac -cp $(CP) ./com/vitessedata/xdrive/csv/*.java 

clean:
	rm -f vitessedata-csv-plugin.jar ./com/vitessedata/xdrive/csv/*.class
