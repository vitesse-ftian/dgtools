DROP external table if exists fdbw;
create writable external table fdbw
(
    i int,
    t text,
    v text,
    __xdr_op int
)
location ('xdrive://127.0.0.1:50051/fdb/deepgreen/test/i,t:v')
format 'spq'
distributed by (i)
;

-- insert into fdbw select i, 'foo' || i, 'barzoo' || i into fdbw;

DROP external table if exists fdbr;
create external table fdbr
(
    i int,
    t text,
    v text
)
location ('xdrive://127.0.0.1:50051/fdb/deepgreen/test/i,t:v')
format 'spq'
-- distributed by (i)
;

