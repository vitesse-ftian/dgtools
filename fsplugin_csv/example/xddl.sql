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


