DROP EXTERNAL TABLE IF EXISTS xx1; 
CREATE EXTERNAL TABLE xx1
    (
        i int,
        t text
    )
LOCATION ('xdrive://localhost:50051/myx/x1.csv') 
FORMAT 'CSV';

DROP EXTERNAL TABLE IF EXISTS xx2; 
CREATE EXTERNAL TABLE xx2
    (
        i int,
        t text
    )
LOCATION ('xdrive://localhost:50051/myx/x?.csv') 
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

