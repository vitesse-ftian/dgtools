\timing on

\copy customer from 'data/customer.tbl' with csv delimiter '|';
\copy lineitem from 'data/lineitem.tbl' with csv delimiter '|';
\copy nation from 'data/nation.tbl' with csv delimiter '|';
\copy orders from 'data/orders.tbl' with csv delimiter '|';
\copy part from 'data/part.tbl' with csv delimiter '|';
\copy partsupp from 'data/partsupp.tbl' with csv delimiter '|';
\copy region from 'data/region.tbl' with csv delimiter '|';
\copy supplier from 'data/supplier.tbl' with csv delimiter '|';

vacuum analyze;
