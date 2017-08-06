drop external table if exists dateok; 
create external table dateok(d date) location ('xdrive://localhost:31416/fs/date.csv') 
format 'csv';

drop external table if exists datebug; 
create external table datebug(d date) location ('xdrive://localhost:31416/fs/date.csv') 
format 'csv' (integer timestamp);

drop external table if exists dateintok;
create external table dateintok(d date) location ('xdrive://localhost:31416/fs/dateint.csv') 
format 'csv' (integer timestamp);

drop external table if exists dateintbug;
create external table dateintbug(d date) location ('xdrive://localhost:31416/fs/dateint.csv') 
format 'csv';

drop external table if exists myxdateok; 
create external table myxdateok(d date) location ('xdrive://localhost:31416/myx/date.csv') 
format 'csv';

drop external table if exists myxdatebug;
create external table myxdatebug(d date) location ('xdrive://localhost:31416/myx/date.csv')
format 'csv' (integer timestamp);

drop external table if exists myxdateintok; 
create external table myxdateintok(d date) location ('xdrive://localhost:31416/myx/dateint.csv') 
format 'csv' (integer timestamp);

drop external table if exists myxdateintbug;
create external table myxdateintbug(d date) location ('xdrive://localhost:31416/myx/dateint.csv') 
format 'csv';

select 'OK';
select * from dateok;
select * from dateintok;
select * from myxdateok;
select * from myxdateintok;

select 'Bug bug ...';
select * from datebug;
select * from dateintbug;
select * from myxdatebug;
select * from myxdateintbug;
