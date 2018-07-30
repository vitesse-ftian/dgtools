DROP external table if exists fdbw;
create writable external table fdbw
(
    ki int,
    kt text,
    vc int, 
    vt text,
    __xdr_op int
)
location ('xdrive://127.0.0.1:30031/fdb/wetestdata/tinybench/ki,kt:vc,vt') 
format 'spq';

-- insert into fdbw select i, 'foo' || i, 'barzoo' || i into fdbw;

DROP external table if exists fdbr;
create external table fdbr
(
    ki int,
    kt text,
    vc int,
    vt text
)
location ('xdrive://127.0.0.1:30031/fdb/wetestdata/tinybench/ki,kt:vc,vt') 
format 'spq';
