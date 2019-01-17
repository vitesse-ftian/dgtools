SET client_min_messages TO WARNING;

DROP EXTERNAL TABLE if exists fdb_wli;
CREATE WRITABLE EXTERNAL TABLE fdb_wli ( L_ORDERKEY    BIGINT ,
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
                             L_COMMENT      VARCHAR(44) ,
                            __xdr_op int
                        )
                        location ('xdrive://127.0.0.1:50051/fdb/tpch1/lineitem/l_orderkey,l_partkey,l_suppkey,l_linenumber:l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment')
                        format 'spq'
                        distributed by (L_ORDERKEY);


drop external table if exists fdb_rli;
CREATE EXTERNAL TABLE fdb_rli ( L_ORDERKEY    BIGINT ,
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
                             L_COMMENT      VARCHAR(44) 
                        )
                        location ('xdrive://127.0.0.1:50051/fdb/tpch1/lineitem/l_orderkey,l_partkey,l_suppkey,l_linenumber:l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment')
                        format 'spq'
                        distributed by (L_ORDERKEY);

