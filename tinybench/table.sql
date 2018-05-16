drop table if exists tinybench;

create table tinybench (ki int, kt varchar(100), vc int, vt text);
create index tinyidx on tinybench(ki, kt);

