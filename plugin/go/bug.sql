drop external table if exists datebug;
create external table datebug(d date) location ('xdrive://localhost:31416/fs/date.csv') format 'csv';

drop external table if exists dateintbug;
create external table dateintbug(d date) location ('xdrive://localhost:31416/fs/dateint.csv') format 'csv';

drop external table if exists myxdatebug;
create external table myxdatebug(d date) location ('xdrive://localhost:31416/myx/date.csv') format 'csv';

drop external table if exists myxdateintbug;
create external table myxdateintbug(d date) location ('xdrive://localhost:31416/myx/dateint.csv') format 'csv';


select 'Bug bug ...';
select * from datebug;

select 'This is OK, should it?';
select * from dateintbug;

select 'Plugin Bug bug ...';
select * from myxdatebug;

select 'Plugin This is OK, should it?';
select * from myxdateintbug;




