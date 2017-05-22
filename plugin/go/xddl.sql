DROP EXTERNAL TABLE IF EXISTS x1; 
CREATE EXTERNAL TABLE x1
    (
        i int,
        t text
    )
LOCATION ('xdrive://localhost:31416/fs/x1.csv') 
FORMAT 'CSV';

DROP EXTERNAL TABLE IF EXISTS xx1; 
CREATE EXTERNAL TABLE xx1
    (
        i int,
        t text
    )
LOCATION ('xdrive://localhost:31416/myx/x1.csv') 
FORMAT 'CSV';

DROP EXTERNAL TABLE IF EXISTS xx2; 
CREATE EXTERNAL TABLE xx2
    (
        i int,
        t text
    )
LOCATION ('xdrive://localhost:31416/myx/x?.csv') 
FORMAT 'CSV';

DROP EXTERNAL TABLE IF EXISTS x3; 
CREATE EXTERNAL TABLE x3
    (
        i int,
        t text
    )
LOCATION ('xdrive://localhost:31416/fs/x*.csv') 
FORMAT 'CSV';

DROP EXTERNAL TABLE IF EXISTS xx3; 
CREATE EXTERNAL TABLE xx3
    (
        i int,
        t text
    )
LOCATION ('xdrive://localhost:31416/myx/x*.csv') 
FORMAT 'CSV';

DROP EXTERNAL TABLE IF EXISTS xxw; 
CREATE WRITABLE EXTERNAL TABLE xxw
    (
        i int,
        t text
    )
LOCATION ('xdrive://localhost:31416/myx/x#UUID#.csv') 
FORMAT 'CSV';

DROP EXTERNAL TABLE IF EXISTS s3nation;
CREATE EXTERNAL TABLE s3nation (
    n_nationkey int,
    n_name text,
    n_regionkey int,
    n_comment text)
LOCATION('xdrive://localhost:31416/tpch1fs3/csv/nation.tbl')
FORMAT 'CSV' (delimiter '|');

DROP EXTERNAL TABLE IF EXISTS emptys3nation;
CREATE EXTERNAL TABLE emptys3nation (
    n_nationkey int,
    n_name text,
    n_regionkey int,
    n_comment text)
LOCATION('xdrive://localhost:31416/emptys3/csv/nation.tbl')
FORMAT 'CSV' (delimiter '|');

DROP EXTERNAL TABLE IF EXISTS s3xxw; 
CREATE WRITABLE EXTERNAL TABLE s3xxw
    (
        i int,
        t text
    )
LOCATION ('xdrive://localhost:31416/tpch1fs3/w/x#UUID#.csv') 
FORMAT 'CSV';

DROP EXTERNAL TABLE IF EXISTS s3xxr; 
CREATE EXTERNAL TABLE s3xxr
    (
        i int,
        t text
    )
LOCATION ('xdrive://localhost:31416/tpch1fs3/w/x*.csv') 
FORMAT 'CSV';

