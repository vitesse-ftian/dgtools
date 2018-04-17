Kafka Xdrive Plugin
==================================

Kafka xdrive plugin can read/write data between deepgreen and kafka.  JSON data format is used as communication protocol between kafka and deepgreen.  Plugin is developed in Golang.

Configurations
--------------
Setting of xdrive.toml
setup the mount point in xdrive.toml as below,

```
# kafka mount point
[[xdrive2.mount]]
name = "kafka"
argv = ["kafkaplugin_spq", "localhost:9092,localhost:9093", "localhost:2181"]

#argv = ["kafkaplugin_spq", "broker1,broker2,broke3", "zkhost"]
# kafka brokerlist is list of host separated with comma, e.g. host1:9092,host1:9093,host2:9092
# zookeeper host, e.g. localhost:2181
```

name - name of mountpoint, choose whatever you like

scheme - "kafkaplugin"

root - specify a list of brokers in kafka

conf - specify the zookeeper host for offset management

Deploy Plugin
-------------
After started the xdrive, you have to deploy the plugin to all xdrive servers.

run 'xdrctl deployplugin' command to deploy plugin to xdrive servers

	% xdrctl deployplugin xdrive.toml kafkaplugin.tgz
 
After deploying plugin to xdrive servers, you are ready to run SQL to transfer data between kafka and deepgreen.
 
Kafka Quick Start
Try out this link to start kafka (https://kafka.apache.org/quickstart)

Create a topic customer with 2 partitions in kafka,

	% bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic customer


Read Operation
--------------
Kafka xdrive plugin use poll method to poll the data from kafka.  We use zookeeper as offset management.  

Firstly, create the read-only external table,
```
DROP EXTERNAL TABLE IF EXISTS customer_kafka_read;
CREATE EXTERNAL TABLE customer_kafka_read ( C_CUSTKEY     INTEGER ,
                             C_NAME        VARCHAR(25) ,
                             C_ADDRESS     VARCHAR(40) ,
                             C_NATIONKEY   INTEGER ,
                             C_PHONE       VARCHAR(15) /*CHAR(15)*/ ,
                             C_ACCTBAL     DOUBLE PRECISION/*DECIMAL(15,2)*/   ,
                             C_MKTSEGMENT  VARCHAR(10) /*CHAR(10)*/ ,
                             C_COMMENT     VARCHAR(117) )
LOCATION ('xdrive://localhost:31416/kafka/customer')
FORMAT 'SPQ';
```

kafka is the mount point name 

customer is the topic in kafka


Write Operation
---------------
```
DROP EXTERNAL TABLE IF EXISTS custome_kafka_write;
CREATE WRITABLE EXTERNAL TABLE customer_kafka_write ( C_CUSTKEY     INTEGER ,
                             C_NAME        VARCHAR(25) ,
                             C_ADDRESS     VARCHAR(40) ,
                             C_NATIONKEY   INTEGER ,
                             C_PHONE       VARCHAR(15) /*CHAR(15)*/,
                             C_ACCTBAL     DOUBLE PRECISION/*DECIMAL(15,2)*/  ,
                             C_MKTSEGMENT  VARCHAR(10) /*CHAR(10)*/,
                             C_COMMENT     VARCHAR(117))
LOCATION ('xdrive://localhost:31416/kafka/customer')
FORMAT 'SPQ';
```
where kafka is the mount point name and customer is the topic in kafka


Use Case
--------
Create two tables customer_source to contain the source data for writing to Kafka and customer_dest table for storing data from Kafka via read-only external table.  

```
DROP TABLE IF EXISTS customer_source;
CREATE TABLE customer_source ( C_CUSTKEY     INTEGER ,
                             C_NAME        VARCHAR(25) ,
                             C_ADDRESS     VARCHAR(40) ,
                             C_NATIONKEY   INTEGER ,
                             C_PHONE       VARCHAR(15) /*CHAR(15)*/,
                             C_ACCTBAL     DOUBLE PRECISION/*DECIMAL(15,2)*/  ,
                             C_MKTSEGMENT  VARCHAR(10) /*CHAR(10)*/,
                             C_COMMENT     VARCHAR(117));

DROP TABLE IF EXISTS customer_dest;
CREATE TABLE customer_dest ( C_CUSTKEY     INTEGER ,
                             C_NAME        VARCHAR(25) ,
                             C_ADDRESS     VARCHAR(40) ,
                             C_NATIONKEY   INTEGER ,
                             C_PHONE       VARCHAR(15) /*CHAR(15)*/,
                             C_ACCTBAL     DOUBLE PRECISION/*DECIMAL(15,2)*/  ,
                             C_MKTSEGMENT  VARCHAR(10) /*CHAR(10)*/,
                             C_COMMENT     VARCHAR(117));
```
 
At this point, you should be able to run a quick query to verify this is working: 

	% psql template1 -c "SELECT * FROM customer_kafka_read"

, which should run but return zero rows of output, since the Kafka topic is empty at this point.

If all is well, you can start a periodic load there. For the purposes of this demo, just run the query to load the table every five seconds: 

```
[gpadmin@mdw ~]$ while $(true) ; do psql template1 -c "INSERT INTO customer_dest SELECT * FROM customer_kafka_read" ; sleep 5 ; done (the output should be INSERT 0 0, showing no data being inserted).
```

In a separate terminal window, also logged into the deepgreen master host, as gpadmin, start the following command so you're able to track the progress of the load from Kafka: 

```
while $(true) ; do psql template1 -c "SELECT COUNT(*) FROM customer_dest" ; sleep 5 ; done
```

In a separate terminal window, start to write data from writable external table customer_kafka_write to Kafka: 

```
INSERT INTO customer_kafka_write SELECT * FROM customer_source; 
```
