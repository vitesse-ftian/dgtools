\timing on

\copy customer from 'data1/customer.tbl' with csv delimiter '|';
\copy lineitem from 'data1/lineitem.tbl' with csv delimiter '|';
\copy nation from 'data1/nation.tbl' with csv delimiter '|';
\copy orders from 'data1/orders.tbl' with csv delimiter '|';
\copy part from 'data1/part.tbl' with csv delimiter '|';
\copy partsupp from 'data1/partsupp.tbl' with csv delimiter '|';
\copy region from 'data1/region.tbl' with csv delimiter '|';
\copy supplier from 'data1/supplier.tbl' with csv delimiter '|';

vacuum analyze;
