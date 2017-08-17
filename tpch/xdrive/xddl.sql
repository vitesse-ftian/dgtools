SET client_min_messages TO WARNING;

DROP SCHEMA xdr1 CASCADE;
CREATE SCHEMA xdr1; 

SET search_path='xdr1'; 

DROP EXTERNAL TABLE IF EXISTS NATION;
CREATE EXTERNAL TABLE NATION  ( N_NATIONKEY  INTEGER, 
                            N_NAME       VARCHAR(25) /*CHAR(25)*/,
                            N_REGIONKEY  INTEGER ,
                            N_COMMENT    VARCHAR(152))
LOCATION ('xdrive://localhost:31416/tpch1/nation.tbl') 
FORMAT 'CSV' (DELIMITER '|') ;

DROP EXTERNAL TABLE IF EXISTS REGION; 
CREATE EXTERNAL TABLE REGION  ( R_REGIONKEY  INTEGER ,
                            R_NAME       VARCHAR(25) /*CHAR(25)*/ ,
                            R_COMMENT    VARCHAR(152))
LOCATION ('xdrive://localhost:31416/tpch1/region.tbl')
FORMAT 'CSV' (DELIMITER '|') ;

DROP EXTERNAL TABLE IF EXISTS PART; 
CREATE EXTERNAL TABLE PART  ( P_PARTKEY     INTEGER ,
                          P_NAME        VARCHAR(55) ,
                          P_MFGR        VARCHAR(25) /*CHAR(25)*/ ,
                          P_BRAND       VARCHAR(10) /*CHAR(10)*/ ,
                          P_TYPE        VARCHAR(25) ,
                          P_SIZE        INTEGER ,
                          P_CONTAINER   VARCHAR(10) /*CHAR(10)*/ ,
                          P_RETAILPRICE DOUBLE PRECISION /*DECIMAL(15,2)*/ ,
                          P_COMMENT     VARCHAR(23))
LOCATION ('xdrive://localhost:31416/tpch1/part.tbl') 
FORMAT 'CSV' (DELIMITER '|') ;

DROP EXTERNAL TABLE IF EXISTS SUPPLIER; 
CREATE EXTERNAL TABLE SUPPLIER ( S_SUPPKEY     INTEGER ,
                             S_NAME        VARCHAR(25) /*CHAR(25)*/ ,
                             S_ADDRESS     VARCHAR(40) ,
                             S_NATIONKEY   INTEGER ,
                             S_PHONE       VARCHAR(15) /*CHAR(15)*/ ,
                             S_ACCTBAL     DOUBLE PRECISION /*DECIMAL(15,2)*/ ,
                             S_COMMENT     VARCHAR(101) )
LOCATION ('xdrive://localhost:31416/tpch1/supplier.tbl') 
FORMAT 'CSV' (DELIMITER '|') ;

DROP EXTERNAL TABLE IF EXISTS PARTSUPP; 
CREATE EXTERNAL TABLE PARTSUPP ( PS_PARTKEY     INTEGER ,
                             PS_SUPPKEY     INTEGER ,
                             PS_AVAILQTY    INTEGER ,
                             PS_SUPPLYCOST  DOUBLE PRECISION /*DECIMAL(15,2)*/  ,
                             PS_COMMENT     VARCHAR(199))
LOCATION ('xdrive://localhost:31416/tpch1/partsupp.tbl')
FORMAT 'CSV' (DELIMITER '|') ;

DROP EXTERNAL TABLE IF EXISTS CUSTOMER; 
CREATE EXTERNAL TABLE CUSTOMER ( C_CUSTKEY     INTEGER ,
                             C_NAME        VARCHAR(25) ,
                             C_ADDRESS     VARCHAR(40) ,
                             C_NATIONKEY   INTEGER ,
                             C_PHONE       VARCHAR(15) /*CHAR(15)*/ ,
                             C_ACCTBAL     DOUBLE PRECISION/*DECIMAL(15,2)*/   ,
                             C_MKTSEGMENT  VARCHAR(10) /*CHAR(10)*/ ,
                             C_COMMENT     VARCHAR(117) )
LOCATION ('xdrive://localhost:31416/tpch1/customer.tbl')
FORMAT 'CSV' (DELIMITER '|') ;

DROP EXTERNAL TABLE IF EXISTS ORDERS; 
CREATE EXTERNAL TABLE ORDERS  ( O_ORDERKEY       INTEGER ,
                           O_CUSTKEY        INTEGER ,
                           O_ORDERSTATUS    VARCHAR(1)/*CHAR(1)*/ ,
                           O_TOTALPRICE     DOUBLE PRECISION /*DECIMAL(15,2)*/ ,
                           O_ORDERDATE      DATE ,
                           O_ORDERPRIORITY  VARCHAR(15) /*CHAR(15)*/ ,  
                           O_CLERK          VARCHAR(15) /*CHAR(15)*/ , 
                           O_SHIPPRIORITY   INTEGER ,
                           O_COMMENT        VARCHAR(79) )
LOCATION ('xdrive://localhost:31416/tpch1/orders.tbl') 
FORMAT 'CSV' (DELIMITER '|') ;

DROP EXTERNAL TABLE IF EXISTS LINEITEM; 
CREATE EXTERNAL TABLE LINEITEM ( L_ORDERKEY    INTEGER ,
                             L_PARTKEY     INTEGER ,
                             L_SUPPKEY     INTEGER ,
                             L_LINENUMBER  INTEGER ,
                             L_QUANTITY    INTEGER /*DECIMAL(15,2)*/ ,
                             L_EXTENDEDPRICE  DOUBLE PRECISION/*DECIMAL(15,2)*/ ,
                             L_DISCOUNT    DOUBLE PRECISION /*DECIMAL(15,2)*/ ,
                             L_TAX         DOUBLE PRECISION /*DECIMAL(15,2)*/ ,
                             L_RETURNFLAG  VARCHAR(1) ,
                             L_LINESTATUS  VARCHAR(1) ,
                             L_SHIPDATE    DATE ,
                             L_COMMITDATE  DATE ,
                             L_RECEIPTDATE DATE ,
                             L_SHIPINSTRUCT VARCHAR(25) /*CHAR(25)*/ ,
                             L_SHIPMODE     VARCHAR(10) /*CHAR(10)*/ ,
                             L_COMMENT      VARCHAR(44) )
LOCATION ('xdrive://localhost:31416/tpch1/lineitem.tbl')
FORMAT 'CSV' (DELIMITER '|') ;


DROP EXTERNAL TABLE IF EXISTS WSPQ_LINITEM;
CREATE WRITABLE EXTERNAL TABLE WSPQ_LINEITEM ( L_ORDERKEY    INTEGER ,
                             L_PARTKEY     INTEGER ,
                             L_SUPPKEY     INTEGER ,
                             L_LINENUMBER  INTEGER ,
                             L_QUANTITY    INTEGER /*DECIMAL(15,2)*/ ,
                             L_EXTENDEDPRICE  DOUBLE PRECISION/*DECIMAL(15,2)*/ ,
                             L_DISCOUNT    DOUBLE PRECISION /*DECIMAL(15,2)*/ ,
                             L_TAX         DOUBLE PRECISION /*DECIMAL(15,2)*/ ,
                             L_RETURNFLAG  VARCHAR(1) ,
                             L_LINESTATUS  VARCHAR(1) ,
                             L_SHIPDATE    DATE ,
                             L_COMMITDATE  DATE ,
                             L_RECEIPTDATE DATE ,
                             L_SHIPINSTRUCT VARCHAR(25) /*CHAR(25)*/ ,
                             L_SHIPMODE     VARCHAR(10) /*CHAR(10)*/ ,
                             L_COMMENT      VARCHAR(44) )
LOCATION ('xdrive://localhost:31416/tpch1/lineitem.#SEGID#.spq')
FORMAT 'SPQ'; 

DROP EXTERNAL TABLE IF EXISTS RSPQ_LINITEM;
CREATE EXTERNAL TABLE RSPQ_LINEITEM ( L_ORDERKEY    INTEGER ,
                             L_PARTKEY     INTEGER ,
                             L_SUPPKEY     INTEGER ,
                             L_LINENUMBER  INTEGER ,
                             L_QUANTITY    INTEGER /*DECIMAL(15,2)*/ ,
                             L_EXTENDEDPRICE  DOUBLE PRECISION/*DECIMAL(15,2)*/ ,
                             L_DISCOUNT    DOUBLE PRECISION /*DECIMAL(15,2)*/ ,
                             L_TAX         DOUBLE PRECISION /*DECIMAL(15,2)*/ ,
                             L_RETURNFLAG  VARCHAR(1) ,
                             L_LINESTATUS  VARCHAR(1) ,
                             L_SHIPDATE    DATE ,
                             L_COMMITDATE  DATE ,
                             L_RECEIPTDATE DATE ,
                             L_SHIPINSTRUCT VARCHAR(25) /*CHAR(25)*/ ,
                             L_SHIPMODE     VARCHAR(10) /*CHAR(10)*/ ,
                             L_COMMENT      VARCHAR(44) )
LOCATION ('xdrive://localhost:31416/tpch1/lineitem.#SEGID#.spq')
FORMAT 'SPQ'; 
