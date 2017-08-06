drop external table if exists tsok; 
create external table tsok(i int, t text, j int, ts timestamp, ts2 timestamptz) 
location ('xdrive://localhost:31416/fs/ts.csv') 
format 'csv';

drop external table if exists tsbug; 
create external table tsbug(i int, t text, j int, ts timestamp, ts2 timestamptz) 
location ('xdrive://localhost:31416/fs/ts.csv') 
format 'csv' (integer timestamp);

drop external table if exists myxtsok; 
create external table myxtsok(i int, t text, j int, ts timestamp, ts2 timestamptz) 
location ('xdrive://localhost:31416/myx/ts.csv') 
format 'csv';

drop external table if exists myxtsbug; 
create external table myxtsbug(i int, t text, j int, ts timestamp, ts2 timestamptz) 
location ('xdrive://localhost:31416/myx/ts.csv') 
format 'csv' (integer timestamp);

drop external table if exists tsintok;
create external table tsintok(d timestamp) location ('xdrive://localhost:31416/fs/dateint.csv') 
format 'csv' (integer timestamp);

drop external table if exists tsintbug;
create external table tsintbug(d date) location ('xdrive://localhost:31416/fs/dateint.csv') 
format 'csv';


select 'OK';
select * from tsok; 
select * from tsintok;
select * from myxtsok; 

select 'Bug';
select * from tsbug;
select * from tsintbug;
select * from myxtsbug; 
