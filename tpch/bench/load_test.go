package bench

import (
	"fmt"
	"testing"
)

func BenchmarkLoadData(b *testing.B) {
	conf, err := GetConfig()
	if err != nil {
		b.Errorf("Configuration error: %s", err.Error())
	}

	conn, err := Connect()
	if err != nil {
		b.Errorf("Cannot connect to database %s, error: %s", err.Error())
	}
	defer conn.Disconnect()

	err = conn.Execute("set gp_autostats_mode = 'none'")
	if err != nil {
		b.Errorf("Cannot set guc gp_autostats_mode.  error: %s", err.Error())
	}

	b.Run("Step=nation", func(b *testing.B) {
		err := conn.Execute(fmt.Sprintf(`INSERT INTO NATION SELECT 
			N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT 
			FROM %s.NATION`, conf.Ext))
		if err != nil {
			b.Errorf("Cannot load table nation.  error: %s", err.Error())
		}
	})

	b.Run("Step=region", func(b *testing.B) {
		err := conn.Execute(fmt.Sprintf(`INSERT INTO REGION SELECT 
			R_REGIONKEY, R_NAME, R_COMMENT 
			FROM %s.REGION`, conf.Ext))
		if err != nil {
			b.Errorf("Cannot load table region.  error: %s", err.Error())
		}
	})

	b.Run("Step=part", func(b *testing.B) {
		err := conn.Execute(fmt.Sprintf(`INSERT INTO PART SELECT 
			P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, 
			P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT
			FROM %s.PART`, conf.Ext))
		if err != nil {
			b.Errorf("Cannot load table part.  error: %s", err.Error())
		}
	})

	b.Run("Step=supplier", func(b *testing.B) {
		err := conn.Execute(fmt.Sprintf(`INSERT INTO SUPPLIER SELECT 
			S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE,
			S_ACCTBAL, S_COMMENT
			FROM %s.SUPPLIER`, conf.Ext))
		if err != nil {
			b.Errorf("Cannot load table supplier.  error: %s", err.Error())
		}
	})

	b.Run("Step=partsupp", func(b *testing.B) {
		err := conn.Execute(fmt.Sprintf(`INSERT INTO PARTSUPP SELECT 
			PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT
			FROM %s.PARTSUPP`, conf.Ext))
		if err != nil {
			b.Errorf("Cannot load table partsupp.  error: %s", err.Error())
		}
	})

	b.Run("Step=customer", func(b *testing.B) {
		err := conn.Execute(fmt.Sprintf(`INSERT INTO CUSTOMER SELECT 
			C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, 
			C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT
			FROM %s.CUSTOMER`, conf.Ext))
		if err != nil {
			b.Errorf("Cannot load table customer.  error: %s", err.Error())
		}
	})

	b.Run("Step=orders", func(b *testing.B) {
		err := conn.Execute(fmt.Sprintf(`INSERT INTO ORDERS SELECT 
			O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE,
			O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT
			FROM %s.ORDERS`, conf.Ext))
		if err != nil {
			b.Errorf("Cannot load table orders.  error: %s", err.Error())
		}
	})

	b.Run("Step=lineitem", func(b *testing.B) {
		err := conn.Execute(fmt.Sprintf(`INSERT INTO LINEITEM SELECT 
			 L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER,
			 L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, 
			 L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, 
			 L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, 
			 L_SHIPMODE, L_COMMENT 
			 FROM %s.LINEITEM`, conf.Ext))
		if err != nil {
			b.Errorf("Cannot load table lineitem.  error: %s", err.Error())
		}
	})

	b.Run("Step=analyze", func(b *testing.B) {
		err := conn.Execute("VACUUM ANALYZE")
		if err != nil {
			b.Errorf("Cannot vacuum analyze database. error: %s", err.Error())
		}
	})
}
