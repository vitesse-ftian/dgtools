\timing on

\copy customer from 'data10/customer.tbl' with csv delimiter '|';
\copy lineitem from 'data10/lineitem.tbl' with csv delimiter '|';
\copy nation from 'data10/nation.tbl' with csv delimiter '|';
\copy orders from 'data10/orders.tbl' with csv delimiter '|';
\copy part from 'data10/part.tbl' with csv delimiter '|';
\copy partsupp from 'data10/partsupp.tbl' with csv delimiter '|';
\copy region from 'data10/region.tbl' with csv delimiter '|';
\copy supplier from 'data10/supplier.tbl' with csv delimiter '|';

vacuum analyze;
