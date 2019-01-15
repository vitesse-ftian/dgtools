SET client_min_messages TO WARNING;

CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL,
                            N_NAME       VARCHAR(25) /*CHAR(25)*/ NOT NULL,
                            N_REGIONKEY  INTEGER NOT NULL,
                            N_COMMENT    VARCHAR(152))
                    WITH (appendonly=true, orientation=row, compresstype='pax'); 

CREATE TABLE REGION  ( R_REGIONKEY  INTEGER NOT NULL,
                            R_NAME       VARCHAR(25) /*CHAR(25)*/ NOT NULL,
                            R_COMMENT    VARCHAR(152))
                    WITH (appendonly=true, orientation=row, compresstype='pax');

CREATE TABLE PART  ( P_PARTKEY     INTEGER NOT NULL,
                          P_NAME        VARCHAR(55) NOT NULL,
                          P_MFGR        VARCHAR(25) /*CHAR(25)*/ NOT NULL,
                          P_BRAND       VARCHAR(10) /*CHAR(10)*/ NOT NULL,
                          P_TYPE        VARCHAR(25) NOT NULL,
                          P_SIZE        INTEGER NOT NULL,
                          P_CONTAINER   VARCHAR(10) /*CHAR(10)*/ NOT NULL,
                          P_RETAILPRICE decimal128 /*DECIMAL(15,2)*/ NOT NULL,
                          P_COMMENT     VARCHAR(23) NOT NULL )
                    WITH (appendonly=true, orientation=row, compresstype='pax');

CREATE TABLE SUPPLIER ( S_SUPPKEY     INTEGER NOT NULL,
                             S_NAME        VARCHAR(25) /*CHAR(25)*/ NOT NULL,
                             S_ADDRESS     VARCHAR(40) NOT NULL,
                             S_NATIONKEY   INTEGER NOT NULL,
                             S_PHONE       VARCHAR(15) /*CHAR(15)*/ NOT NULL,
                             S_ACCTBAL     decimal128 /*DECIMAL(15,2)*/ NOT NULL,
                             S_COMMENT     VARCHAR(101) NOT NULL)
                    WITH (appendonly=true, orientation=row, compresstype='pax');

CREATE TABLE PARTSUPP ( PS_PARTKEY     INTEGER NOT NULL,
                             PS_SUPPKEY     INTEGER NOT NULL,
                             PS_AVAILQTY    INTEGER NOT NULL,
                             PS_SUPPLYCOST  decimal128 /*DECIMAL(15,2)*/  NOT NULL,
                             PS_COMMENT     VARCHAR(199) NOT NULL )
                    WITH (appendonly=true, orientation=row, compresstype='pax');

CREATE TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL,
                             C_NAME        VARCHAR(25) NOT NULL,
                             C_ADDRESS     VARCHAR(40) NOT NULL,
                             C_NATIONKEY   INTEGER NOT NULL,
                             C_PHONE       VARCHAR(15) /*CHAR(15)*/ NOT NULL,
                             C_ACCTBAL     decimal128 /*DECIMAL(15,2)*/   NOT NULL,
                             C_MKTSEGMENT  VARCHAR(10) /*CHAR(10)*/ NOT NULL,
                             C_COMMENT     VARCHAR(117) NOT NULL)
                    WITH (appendonly=true, orientation=row, compresstype='pax');

CREATE TABLE ORDERS  ( O_ORDERKEY       BIGINT NOT NULL,
                           O_CUSTKEY        INTEGER NOT NULL,
                           O_ORDERSTATUS    VARCHAR(1)/*CHAR(1)*/ NOT NULL,
                           O_TOTALPRICE     decimal128 /*DECIMAL(15,2)*/ NOT NULL,
                           O_ORDERDATE      DATE NOT NULL,
                           O_ORDERPRIORITY  VARCHAR(15) /*CHAR(15)*/ NOT NULL,  
                           O_CLERK          VARCHAR(15) /*CHAR(15)*/ NOT NULL, 
                           O_SHIPPRIORITY   INTEGER NOT NULL,
                           O_COMMENT        VARCHAR(79) NOT NULL)
                    WITH (appendonly=true, orientation=row, compresstype='pax');

CREATE TABLE LINEITEM ( L_ORDERKEY    BIGINT NOT NULL,
                             L_PARTKEY     INTEGER NOT NULL,
                             L_SUPPKEY     INTEGER NOT NULL,
                             L_LINENUMBER  INTEGER NOT NULL,
                             L_QUANTITY    INTEGER /*DECIMAL(15,2)*/ NOT NULL,
                             L_EXTENDEDPRICE  decimal128 /*DECIMAL(15,2)*/ NOT NULL,
                             L_DISCOUNT    decimal128 /*DECIMAL(15,2)*/ NOT NULL,
                             L_TAX         decimal128  /*DECIMAL(15,2)*/ NOT NULL,
                             L_RETURNFLAG  VARCHAR(1) NOT NULL,
                             L_LINESTATUS  VARCHAR(1) NOT NULL,
                             L_SHIPDATE    DATE NOT NULL,
                             L_COMMITDATE  DATE NOT NULL,
                             L_RECEIPTDATE DATE NOT NULL,
                             L_SHIPINSTRUCT VARCHAR(25) /*CHAR(25)*/ NOT NULL,
                             L_SHIPMODE     VARCHAR(10) /*CHAR(10)*/ NOT NULL,
                             L_COMMENT      VARCHAR(44) NOT NULL)
                    WITH (appendonly=true, orientation=row, compresstype='pax');