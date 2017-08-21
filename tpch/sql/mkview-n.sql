SET client_min_messages TO WARNING;

drop view if exists q0;
create view q0 as
select 'lineitem', count(*) from lineitem
union all
select 'orders', count(*) from orders
union all
select 'customer', count(*) from customer
union all
select 'supplier', count(*) from supplier
union all
select 'partsupp', count(*) from partsupp 
union all
select 'part', count(*) from part 
union all
select 'nation', count(*) from nation 
union all
select 'region', count(*) from region
; 

-- 1
-- using 1461004545 as a seed to the RNG

drop view if exists q1;
create view q1 as
select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice)::numeric(18,2) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount))::numeric(18,2) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax))::numeric(18,2) as sum_charge,
	avg(l_quantity)::numeric(18,2) as avg_qty,
	avg(l_extendedprice)::numeric(18,2) as avg_price,
	avg(l_discount)::numeric(18,2) as avg_disc,
	count(*) as count_order
from
	lineitem
where
	l_shipdate <= date '1998-12-01' - interval '112 day'
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus
;

-- 2
-- using 1461004545 as a seed to the RNG


drop view if exists q2;
create view q2 as
select
	s_acctbal,
	s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
from
	part,
	supplier,
	partsupp,
	nation,
	region
where
	p_partkey = ps_partkey
	and s_suppkey = ps_suppkey
	and p_size = 48
	and p_type like '%TIN'
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'MIDDLE EAST'
	and ps_supplycost = (
		select
			min(ps_supplycost)
		from
			partsupp,
			supplier,
			nation,
			region
		where
			p_partkey = ps_partkey
			and s_suppkey = ps_suppkey
			and s_nationkey = n_nationkey
			and n_regionkey = r_regionkey
			and r_name = 'MIDDLE EAST'
	)
order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey
limit 100
;

-- 3
-- using 1461004545 as a seed to the RNG


drop view if exists q3;
create view q3 as
select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem
where
	c_mktsegment = 'HOUSEHOLD'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < date '1995-03-29'
	and l_shipdate > date '1995-03-29'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate
limit 10
;

-- 4
-- using 1461004545 as a seed to the RNG


drop view if exists q4;
create view q4 as
select
	o_orderpriority,
	count(*) as order_count
from
	orders
where
	o_orderdate >= date '1997-07-01'
	and o_orderdate < date '1997-07-01' + interval '3 month'
	and exists (
		select
			*
		from
			lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
	)
group by
	o_orderpriority
order by
	o_orderpriority
;

-- 5
-- using 1461004545 as a seed to the RNG


drop view if exists q5;
create view q5 as
select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'AMERICA'
	and o_orderdate >= date '1994-01-01'
	and o_orderdate < date '1994-01-01' + interval '1 year'
group by
	n_name
order by
	revenue desc
;

-- 6
-- using 1461004545 as a seed to the RNG


drop view if exists q6;
create view q6 as
select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= date '1994-01-01'
	and l_shipdate < date '1994-01-01' + interval '1 year'
	and l_discount between 0.03 - 0.01 and 0.03 + 0.01
	and l_quantity < 24;

-- 7
-- using 1461004545 as a seed to the RNG


drop view if exists q7;
create view q7 as
select
	supp_nation,
	cust_nation,
	l_year,
	sum(volume)::bigint as revenue
from
	(
		select
			n1.n_name as supp_nation,
			n2.n_name as cust_nation,
			extract(year from l_shipdate) as l_year,
			l_extendedprice * (1 - l_discount) as volume
		from
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2
		where
			s_suppkey = l_suppkey
			and o_orderkey = l_orderkey
			and c_custkey = o_custkey
			and s_nationkey = n1.n_nationkey
			and c_nationkey = n2.n_nationkey
			and (
				(n1.n_name = 'FRANCE' and n2.n_name = 'ARGENTINA')
				or (n1.n_name = 'ARGENTINA' and n2.n_name = 'FRANCE')
			)
			and l_shipdate between date '1995-01-01' and date '1996-12-31'
	) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year
;

-- 8
-- using 1461004545 as a seed to the RNG


drop view if exists q8;
create view q8 as
select
	o_year,
	(sum(case
		when nation = 'ARGENTINA' then volume
		else 0
	end) / sum(volume))::numeric(10,5) as mkt_share
from
	(
		select
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) as volume,
			n2.n_name as nation
		from
			part,
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2,
			region
		where
			p_partkey = l_partkey
			and s_suppkey = l_suppkey
			and l_orderkey = o_orderkey
			and o_custkey = c_custkey
			and c_nationkey = n1.n_nationkey
			and n1.n_regionkey = r_regionkey
			and r_name = 'AMERICA'
			and s_nationkey = n2.n_nationkey
			and o_orderdate between date '1995-01-01' and date '1996-12-31'
			and p_type = 'ECONOMY BURNISHED TIN'
	) as all_nations
group by
	o_year
order by
	o_year
;

-- 9
-- using 1461004545 as a seed to the RNG


drop view if exists q9;
create view q9 as
select
	nation,
	o_year,
	sum(amount)::numeric(18,2) as sum_profit
from
	(
		select
			n_name as nation,
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from
			part,
			supplier,
			lineitem,
			partsupp,
			orders,
			nation
		where
			s_suppkey = l_suppkey
			and ps_suppkey = l_suppkey
			and ps_partkey = l_partkey
			and p_partkey = l_partkey
			and o_orderkey = l_orderkey
			and s_nationkey = n_nationkey
			and p_name like '%pink%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc
;

-- 10
-- using 1461004545 as a seed to the RNG


drop view if exists q10;
create view q10 as
select
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	customer,
	orders,
	lineitem,
	nation
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= date '1993-03-01'
	and o_orderdate < date '1993-03-01' + interval '3 month'
	and l_returnflag = 'R'
	and c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc
limit 20
;

-- 11
-- using 1461004545 as a seed to the RNG


drop view if exists q11;
create view q11 as
select
	ps_partkey,
	sum(ps_supplycost * ps_availqty) as value
from
	partsupp,
	supplier,
	nation
where
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = 'JAPAN'
group by
	ps_partkey having
		sum(ps_supplycost * ps_availqty) > (
			select
				sum(ps_supplycost * ps_availqty) * 0.0001000000
			from
				partsupp,
				supplier,
				nation
			where
				ps_suppkey = s_suppkey
				and s_nationkey = n_nationkey
				and n_name = 'JAPAN'
		)
order by
	value desc
;

-- 12
-- using 1461004545 as a seed to the RNG


drop view if exists q12;
create view q12 as
select
	l_shipmode,
	sum(case
		when o_orderpriority = '1-URGENT'
			or o_orderpriority = '2-HIGH'
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when o_orderpriority <> '1-URGENT'
			and o_orderpriority <> '2-HIGH'
			then 1
		else 0
	end) as low_line_count
from
	orders,
	lineitem
where
	o_orderkey = l_orderkey
	and l_shipmode in ('FOB', 'TRUCK')
	and l_commitdate < l_receiptdate
	and l_shipdate < l_commitdate
	and l_receiptdate >= date '1996-01-01'
	and l_receiptdate < date '1996-01-01' + interval '1 year'
group by
	l_shipmode
order by
	l_shipmode
;

-- 13
-- using 1461004545 as a seed to the RNG


drop view if exists q13;
create view q13 as
select
	c_count,
	count(*) as custdist
from
	(
		select
			c_custkey,
			count(o_orderkey)
		from
			customer left outer join orders on
				c_custkey = o_custkey
				and o_comment not like '%pending%accounts%'
		group by
			c_custkey
	) as c_orders (c_custkey, c_count)
group by
	c_count
order by
	custdist desc,
	c_count desc
;

-- 14
-- using 1461004545 as a seed to the RNG


drop view if exists q14;
create view q14 as
select
	100.00 * sum(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / sum(l_extendedprice * (1 - l_discount))::numeric(18,2) as promo_revenue
from
	lineitem,
	part
where
	l_partkey = p_partkey
	and l_shipdate >= date '1996-04-01'
	and l_shipdate < date '1996-04-01' + interval '1 month';

-- 15
-- using 1461004545 as a seed to the RNG

drop view if exists q15_revenue0 cascade;
create view q15_revenue0 (supplier_no, total_revenue) as
	select
		l_suppkey,
		sum(l_extendedprice * (1 - l_discount))::numeric(18,2)
	from
		lineitem
	where
		l_shipdate >= date '1995-12-01'
		and l_shipdate < date '1995-12-01' + interval '3 month'
	group by
		l_suppkey;


drop view if exists q15;
create view q15 as
select
	s_suppkey,
	s_name,
	s_address,
	s_phone,
	total_revenue
from
	supplier,
	q15_revenue0
where
	s_suppkey = supplier_no
	and total_revenue = (
		select
			max(total_revenue)
		from
			q15_revenue0
	)
order by
	s_suppkey
;


-- 16
-- using 1461004545 as a seed to the RNG


drop view if exists q16;
create view q16 as
select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt
from
	partsupp,
	part
where
	p_partkey = ps_partkey
	and p_brand <> 'Brand#35'
	and p_type not like 'ECONOMY BURNISHED%'
	and p_size in (14, 7, 21, 24, 35, 33, 2, 20)
	and ps_suppkey not in (
		select
			s_suppkey
		from
			supplier
		where
			s_comment like '%Customer%Complaints%'
	)
group by
	p_brand,
	p_type,
	p_size
order by
	supplier_cnt desc,
	p_brand,
	p_type,
	p_size
;

-- 17
-- using 1461004545 as a seed to the RNG


drop view if exists q17;
create view q17 as
select
	sum(l_extendedprice) / 7.0 as avg_yearly
from
	lineitem,
	part
where
	p_partkey = l_partkey
	and p_brand = 'Brand#54'
	and p_container = 'LG BAG'
	and l_quantity < (
		select
			0.2::double precision * avg(l_quantity)
		from
			lineitem
		where
			l_partkey = p_partkey
	);

-- 18
-- using 1461004545 as a seed to the RNG


drop view if exists q18;
create view q18 as
select
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice,
	sum(l_quantity)
from
	customer,
	orders,
	lineitem
where
	o_orderkey in (
		select
			l_orderkey
		from
			lineitem
		group by
			l_orderkey having
				sum(l_quantity) > 314
	)
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey
group by
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice
order by
	o_totalprice desc,
	o_orderdate
limit 100
;

-- 19
-- using 1461004545 as a seed to the RNG


drop view if exists q19;
create view q19 as
select
	sum(l_extendedprice* (1 - l_discount)) as revenue
from
	lineitem,
	part
where
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#23'
		and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		and l_quantity >= 5 and l_quantity <= 5 + 10
		and p_size between 1 and 5
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#15'
		and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		and l_quantity >= 14 and l_quantity <= 14 + 10
		and p_size between 1 and 10
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#44'
		and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
		and l_quantity >= 28 and l_quantity <= 28 + 10
		and p_size between 1 and 15
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	);

-- 20
-- using 1461004545 as a seed to the RNG


drop view if exists q20;
create view q20 as
select
	s_name,
	s_address
from
	supplier,
	nation
where
	s_suppkey in (
		select
			ps_suppkey
		from
			partsupp
		where
			ps_partkey in (
				select
					p_partkey
				from
					part
				where
					p_name like 'lime%'
			)
			and ps_availqty > (
				select
					0.5 * sum(l_quantity)
				from
					lineitem
				where
					l_partkey = ps_partkey
					and l_suppkey = ps_suppkey
					and l_shipdate >= date '1993-01-01'
					and l_shipdate < date '1993-01-01' + interval '1 year'
			)
	)
	and s_nationkey = n_nationkey
	and n_name = 'VIETNAM'
order by s_name
;

-- 21
-- using 1461004545 as a seed to the RNG


drop view if exists q21;
create view q21 as
select
	s_name,
	count(*) as numwait
from
	supplier,
	lineitem l1,
	orders,
	nation
where
	s_suppkey = l1.l_suppkey
	and o_orderkey = l1.l_orderkey
	and o_orderstatus = 'F'
	and l1.l_receiptdate > l1.l_commitdate
	and exists (
		select
			*
		from
			lineitem l2
		where
			l2.l_orderkey = l1.l_orderkey
			and l2.l_suppkey <> l1.l_suppkey
	)
	and not exists (
		select
			*
		from
			lineitem l3
		where
			l3.l_orderkey = l1.l_orderkey
			and l3.l_suppkey <> l1.l_suppkey
			and l3.l_receiptdate > l3.l_commitdate
	)
	and s_nationkey = n_nationkey
	and n_name = 'BRAZIL'
group by
	s_name
order by
	numwait desc,
	s_name
limit 100
;

-- 22
-- using 1461004545 as a seed to the RNG


drop view if exists q22;
create view q22 as
select
	cntrycode,
	count(*) as numcust,
	sum(c_acctbal)::numeric(18,2) as totacctbal
from
	(
		select
			substring(c_phone from 1 for 2) as cntrycode,
			c_acctbal
		from
			customer
		where
			substring(c_phone from 1 for 2) in
				('10', '11', '26', '22', '19', '20', '27')
			and c_acctbal > (
				select
					avg(c_acctbal)
				from
					customer
				where
					c_acctbal > 0.00
					and substring(c_phone from 1 for 2) in
						('10', '11', '26', '22', '19', '20', '27')
			)
			and not exists (
				select
					*
				from
					orders
				where
					o_custkey = c_custkey
			)
	) as custsale
group by
	cntrycode
order by
	cntrycode
;

