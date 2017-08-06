drop external table if exists tsbug; 
create external table tsbug(i int, t text, j int, ts timestamp, ts2 timestamptz) location ('xdrive://localhost:31416/fs/ts.csv') format 'csv';

drop external table if exists myxtsbug; 
create external table myxtsbug(i int, t text, j int, ts timestamp, ts2 timestamptz) location ('xdrive://localhost:31416/myx/ts.csv') format 'csv';

select * from tsbug;
select * from myxtsbug; 
