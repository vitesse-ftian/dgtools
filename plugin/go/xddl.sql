DROP EXTERNAL TABLE IF EXISTS bug2;
CREATE EXTERNAL TABLE bug2
    (
        i int,
        t text
    )
LOCATION ('xdrive://localhost:50051/fs/bug2.csv')
FORMAT 'CSV' (  ESCAPE '$');


DROP EXTERNAL TABLE IF EXISTS bug3;
CREATE EXTERNAL TABLE bug3
    (
        i int,
        t text
    )
LOCATION ('xdrive://localhost:50051/myx/bug2.csv')
FORMAT 'CSV' (  ESCAPE '$');

DROP EXTERNAL TABLE IF EXISTS x1; 
CREATE EXTERNAL TABLE x1
    (
        i int,
        t text
    )
LOCATION ('xdrive://localhost:50051/fs/x1.csv') 
FORMAT 'CSV';

DROP EXTERNAL TABLE IF EXISTS xx1; 
CREATE EXTERNAL TABLE xx1
    (
        i int,
        t text
    )
LOCATION ('xdrive://localhost:50051/myx/x1.csv') 
FORMAT 'CSV';

DROP EXTERNAL TABLE IF EXISTS numbug; 
CREATE EXTERNAL TABLE numbug 
    (
		n numeric
    )
LOCATION ('xdrive://localhost:50051/fs/num.csv') 
FORMAT 'CSV';

DROP EXTERNAL TABLE IF EXISTS numbug2; 
CREATE EXTERNAL TABLE numbug2 
    (
		n numeric
    )
LOCATION ('xdrive://localhost:50051/myx/num.csv') 
FORMAT 'CSV';



DROP EXTERNAL TABLE IF EXISTS xx1exec; 
CREATE EXTERNAL TABLE xx1exec
    (
        i int,
        t text
    )
LOCATION ('xdrive://localhost:50051/myexec/x1.csv') 
FORMAT 'CSV';


DROP EXTERNAL TABLE IF EXISTS xx2; 
CREATE EXTERNAL TABLE xx2
    (
        i int,
        t text
    )
LOCATION ('xdrive://localhost:50051/myx/x?.csv') 
FORMAT 'CSV';

DROP EXTERNAL TABLE IF EXISTS xx2exec; 
CREATE EXTERNAL TABLE xx2exec
    (
        i int,
        t text
    )
LOCATION ('xdrive://localhost:50051/myexec/x?.csv') 
FORMAT 'CSV';


DROP EXTERNAL TABLE IF EXISTS x3; 
CREATE EXTERNAL TABLE x3
    (
        i int,
        t text
    )
LOCATION ('xdrive://localhost:50051/fs/x*.csv') 
FORMAT 'CSV';

DROP EXTERNAL TABLE IF EXISTS xx3; 
CREATE EXTERNAL TABLE xx3
    (
        i int,
        t text
    )
LOCATION ('xdrive://localhost:50051/myx/x*.csv') 
FORMAT 'CSV';

DROP EXTERNAL TABLE IF EXISTS xxw; 
CREATE WRITABLE EXTERNAL TABLE xxw
    (
        i int,
        t text
    )
LOCATION ('xdrive://localhost:50051/myx/x#UUID#.csv') 
FORMAT 'CSV';

DROP EXTERNAL TABLE IF EXISTS xxpw; 
CREATE WRITABLE EXTERNAL TABLE xxpw
    (
        i int,
        t text
    )
LOCATION ('xdrive://localhost:50051/myxpw/x#UUID#.csv') 
FORMAT 'CSV';

DROP EXTERNAL TABLE IF EXISTS s3nation;
CREATE EXTERNAL TABLE s3nation (
    n_nationkey int,
    n_name text,
    n_regionkey int,
    n_comment text)
LOCATION('xdrive://localhost:50051/tpch1fs3/csv/nation.tbl')
FORMAT 'CSV' (delimiter '|');

DROP EXTERNAL TABLE IF EXISTS emptys3nation;
CREATE EXTERNAL TABLE emptys3nation (
    n_nationkey int,
    n_name text,
    n_regionkey int,
    n_comment text)
LOCATION('xdrive://localhost:50051/emptys3/csv/nation.tbl')
FORMAT 'CSV' (delimiter '|');

DROP EXTERNAL TABLE IF EXISTS s3xxw; 
CREATE WRITABLE EXTERNAL TABLE s3xxw
    (
        i int,
        t text
    )
LOCATION ('xdrive://localhost:50051/tpch1fs3/w/x#UUID#.csv') 
FORMAT 'CSV';

DROP EXTERNAL TABLE IF EXISTS s3xxr; 
CREATE EXTERNAL TABLE s3xxr
    (
        i int,
        t text
    )
LOCATION ('xdrive://localhost:50051/tpch1fs3/w/x*.csv') 
FORMAT 'CSV';

DROP EXTERNAL TABLE IF EXISTS esfs;
CREATE EXTERNAL TABLE esfs
	(
	_id text,
	_type text,
	name text,
	age int,
	gender text,
	_routing text
	)
LOCATION ('xdrive://localhost:50051/fs/es.csv')
FORMAT 'SPQ';

DROP EXTERNAL TABLE IF EXISTS esr;
CREATE EXTERNAL TABLE esr
      (
        _id  text,
        _type text,
        name text,
	age int,
	gender text,
	_routing text
        )
LOCATION ('xdrive://localhost:50051/es/')
FORMAT 'SPQ';

DROP EXTERNAL TABLE IF EXISTS esw;
CREATE WRITABLE EXTERNAL TABLE esw
      (
        _id  text,
        _type text,
        name text,
	age int,
	gender text,
	_routing text
        )
LOCATION ('xdrive://localhost:50051/es/')
FORMAT 'SPQ';

DROP EXTERNAL TABLE IF EXISTS estest;
CREATE EXTERNAL TABLE estest
      (
        _id  text,
        _type text,
        _routing text,
        name text,
        age int,
        last_updated bigint
        )
LOCATION ('xdrive://localhost:50051/eslocal/')
FORMAT 'SPQ';

DROP EXTERNAL TABLE IF EXISTS estest_write;
CREATE WRITABLE EXTERNAL TABLE estest_write
      (
        _id  text,
        _type text,
        _routing text,
        name text,
        age int,
        last_updated bigint
        )
LOCATION ('xdrive://localhost:50051/eslocal/')
FORMAT 'SPQ';


DROP EXTERNAL TABLE IF EXISTS hbr;
CREATE EXTERNAL TABLE hbr
(
        _row text,
        _column text,
        _value text,
        _timestamp bigint
)
LOCATION ('xdrive://localhost:50051/hbase/test')
FORMAT 'SPQ';

DROP EXTERNAL TABLE IF EXISTS bugw;
CREATE WRITABLE EXTERNAL TABLE bugw
      (
        _id  text,
        _type text,
        name text,
        age int,
        gender text,
        _routing text
        )
LOCATION ('xdrive://localhost:50051/notexist/bigdata#UUID#.csv')
FORMAT 'CSV';

DROP EXTERNAL TABLE IF EXISTS kafkaw;
CREATE WRITABLE EXTERNAL TABLE kafkaw
(
	id bigint,
	name text,
	age int
)
LOCATION ('xdrive://localhost:50051/kafka/person')
FORMAT 'SPQ';

DROP EXTERNAL TABLE IF EXISTS kafkar;
CREATE EXTERNAL TABLE kafkar
(
        id bigint,
        name text,
        age int
)
LOCATION ('xdrive://localhost:50051/kafka/person')
FORMAT 'SPQ';	

DROP EXTERNAL TABLE IF EXISTS hdfsw;
CREATE WRITABLE EXTERNAL TABLE hdfsw
(
        id bigint,
        name text,
        age int
)
LOCATION ('xdrive://localhost:50051/hdfs/person/data#UUID#.spq')
FORMAT 'SPQ';

DROP EXTERNAL TABLE IF EXISTS hdfsr;
CREATE EXTERNAL TABLE hdfsr
(
        id bigint,
        name text,
        age int
)
LOCATION ('xdrive://localhost:50051/hdfs/person/*')
FORMAT 'SPQ';

