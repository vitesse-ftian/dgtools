package bench

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
)

func TestSetup(t *testing.T) {
	conf, err := GetConfig()
	if err != nil {
		t.Fatalf("Configuration error: %s", err.Error())
	}

	segs, err := Segs()
	if err != nil {
		t.Fatalf("Cannot get deepgreen segs, error: %s.", err.Error())
	}

	seghosts := make(map[string]bool)
	for _, seg := range segs {
		seghosts[seg.Addr] = true
	}

	t.Run("Step=mkdirgen", func(t *testing.T) {
		cmd := fmt.Sprintf("mkdir -p %s/gen", Dir())
		err = exec.Command("bash", "-c", cmd).Run()
		if err != nil {
			t.Errorf("Cannot create gen dir.  error: %s", err.Error())
		}
	})

	t.Run("Step=xdrtoml", func(t *testing.T) {
		if conf.Ext != "XDR" {
			return
		}

		tomlf := Dir() + "/gen/xdrive.toml"
		xf, err := os.Create(tomlf)
		if err != nil {
			t.Errorf("Cannot create xdrive.toml file.  error: %s", err.Error())
		}

		fmt.Fprintf(xf, "[xdrive]\n")
		fmt.Fprintf(xf, "dir = \"%s\"\n", conf.Staging)
		fmt.Fprintf(xf, "pluginpath = [\"%s/plugin\"]\n", conf.Staging)
		fmt.Fprintf(xf, "port=27183\n")
		fmt.Fprintf(xf, "host = [")
		prefix := " "
		for k, _ := range seghosts {
			fmt.Fprintf(xf, " %s\"%s\" ", prefix, k)
			prefix = ","
		}
		fmt.Fprintf(xf, " ]\n\n")

		fmt.Fprintf(xf, "[[xdrive.mount]]\n")
		fmt.Fprintf(xf, "name = \"tpch-scale-%d\"\n", conf.Scale)
		fmt.Fprintf(xf, "argv = [\"xdr_fs/xdr_fs\", \"csv\", \"./tpch/scale-%d\"]\n", conf.Scale)

		fmt.Fprintf(xf, "[[xdrive.mount]]\n")
		fmt.Fprintf(xf, "name = \"tpch-spq-%d\"\n", conf.Scale)
		fmt.Fprintf(xf, "argv = [\"xdr_fs/xdr_fs\", \"spq\", \"./tpch/spq-%d\"]\n", conf.Scale)

		xf.Close()

		err = exec.Command("xdrctl", "deploy", tomlf).Run()
		if err != nil {
			t.Errorf("Cannot deploy xdrive. error: %s", err.Error())
		}
	})

	t.Run("Step=db", func(t *testing.T) {
		conn, err := ConnectTemplate1()
		if err != nil {
			t.Errorf("Cannot connect to template1, error: %s", err.Error())
		}
		defer conn.Disconnect()

		conn.Execute(fmt.Sprintf("drop database %s", conf.Db))
		conn.Execute(fmt.Sprintf("create database %s", conf.Db))
	})

	t.Run("Step=ddl", func(t *testing.T) {
		ddlf := fmt.Sprintf("%s/sql/%s", Dir(), conf.DDL)
		cmd, err := PsqlCmd(ddlf)
		if err != nil {
			t.Errorf("Cannot build psql ddl command. error :%s", err.Error())
		}

		err = exec.Command("bash", "-c", cmd).Run()
		if err != nil {
			t.Errorf("Cannot run ddl.   error: %s", err.Error())
		}

		qf := fmt.Sprintf("%s/sql/mkview-n.sql", Dir())
		cmd, err = PsqlCmd(qf)
		if err != nil {
			t.Errorf("Cannot build psql query command. error :%s", err.Error())
		}

		err = exec.Command("bash", "-c", cmd).Run()
		if err != nil {
			t.Errorf("Cannot run query view ddl.   error: %s", err.Error())
		}
	})

	t.Run("Step=extddl", func(t *testing.T) {
		conn, err := Connect()
		if err != nil {
			t.Errorf("Cannot connect to database, error: %s", err.Error())
		}
		defer conn.Disconnect()

		conn.Execute("DROP SCHEMA IF EXISTS XDR CASCADE")
		conn.Execute("DROP SCHEMA IF EXISTS GPF CASCADE")
		conn.Execute("CREATE SCHEMA XDR")
		conn.Execute("CREATE SCHEMA GPF")

		var loc1f func(string) string
		var locallf func(string) string

		if conf.Ext == "XDR" {
			loc1f = func(t string) string {
				// xdrive syntax for nation, region is exactly the same as other tables.   In fact, for a
				// cluster running xdrive as single cluster mode, we must add a * wildcard -- otherwise,
				// if xdrive sees no wildcard, it will enforce the file exists, otherwise, error.
				return fmt.Sprintf("'xdrive://localhost:27183/tpch-scale-%d/seg-#SEGID#/%s.tbl*'", conf.Scale, t)
			}
			locallf = func(t string) string {
				return fmt.Sprintf("'xdrive://localhost:27183/tpch-scale-%d/seg-#SEGID#/%s.tbl*'", conf.Scale, t)
			}
		} else {
			loc1f = func(t string) string {
				return fmt.Sprintf("'gpfdist://%s:22222/tpch/scale-%d/seg-0/%s.tbl'", segs[0].Addr, conf.Scale, t)
			}
			locallf = func(t string) string {
				prefix := ""
				ret := ""
				for h, _ := range seghosts {
					ret = ret + prefix + fmt.Sprintf("'gpfdist://%s:22222/tpch/scale-%d/seg-*/%s.tbl.*'", h, conf.Scale, t)
					prefix = ","
				}
				return ret
			}
		}

		// Create two set of external tables, one for xdrive, one for gpfdist.
		//
		// nation.
		nation := `CREATE EXTERNAL TABLE %s.NATION  ( N_NATIONKEY  INTEGER,
                            N_NAME       VARCHAR(25) /*CHAR(25)*/, 
                            N_REGIONKEY  INTEGER, 
                            N_COMMENT    VARCHAR(152),
							DUMMY TEXT) 
				   LOCATION (%s) 
				   FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(nation, conf.Ext, loc1f("nation")))

		// region
		region := ` CREATE EXTERNAL TABLE %s.REGION  ( R_REGIONKEY  INTEGER, 
                            R_NAME       VARCHAR(25) /*CHAR(25)*/, 
                            R_COMMENT    VARCHAR(152), 
						DUMMY TEXT)
				   LOCATION (%s) 
				   FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(region, conf.Ext, loc1f("region")))

		// part
		part := `CREATE EXTERNAL TABLE %s.PART  ( P_PARTKEY     INTEGER, 
                          P_NAME        VARCHAR(55), 
                          P_MFGR        VARCHAR(25) /*CHAR(25)*/, 
                          P_BRAND       VARCHAR(10) /*CHAR(10)*/, 
                          P_TYPE        VARCHAR(25), 
                          P_SIZE        INTEGER, 
                          P_CONTAINER   VARCHAR(10) /*CHAR(10)*/, 
                          P_RETAILPRICE text /* DOUBLE PRECISION, DECIMAL(15,2)*/, 
                          P_COMMENT     VARCHAR(23), 
						  DUMMY TEXT) 
				   LOCATION (%s) 
				   FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(part, conf.Ext, loc1f("part")))

		// supplier
		supplier := `CREATE EXTERNAL TABLE %s.SUPPLIER ( S_SUPPKEY     INTEGER, 
                             S_NAME        VARCHAR(25) /*CHAR(25)*/, 
                             S_ADDRESS     VARCHAR(40), 
                             S_NATIONKEY   INTEGER, 
                             S_PHONE       VARCHAR(15) /*CHAR(15)*/, 
                             S_ACCTBAL     text /* DOUBLE PRECISION , DECIMAL(15,2)*/, 
                             S_COMMENT     VARCHAR(101), 
							 DUMMY TEXT) 
				   LOCATION (%s) 
				   FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(supplier, conf.Ext, locallf("supplier")))

		partsupp := `CREATE EXTERNAL TABLE %s.PARTSUPP ( PS_PARTKEY     INTEGER, 
                             PS_SUPPKEY     INTEGER, 
                             PS_AVAILQTY    INTEGER,
                             PS_SUPPLYCOST  text /* DOUBLE PRECISION , DECIMAL(15,2)*/, 
                             PS_COMMENT     VARCHAR(199),
							 DUMMY TEXT) 
				   LOCATION (%s) 
				   FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(partsupp, conf.Ext, locallf("partsupp")))

		customer := `CREATE EXTERNAL TABLE %s.CUSTOMER ( C_CUSTKEY     INTEGER, 
                             C_NAME        VARCHAR(25),
                             C_ADDRESS     VARCHAR(40),
                             C_NATIONKEY   INTEGER,
                             C_PHONE       VARCHAR(15) /*CHAR(15)*/,
                             C_ACCTBAL     text /* DOUBLE PRECISION, DECIMAL(15,2)*/, 
                             C_MKTSEGMENT  VARCHAR(10) /*CHAR(10)*/,
                             C_COMMENT     VARCHAR(117),
							 DUMMY TEXT) 
				   LOCATION (%s) 
				   FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(customer, conf.Ext, locallf("customer")))

		orders := `CREATE EXTERNAL TABLE %s.ORDERS  ( O_ORDERKEY       BIGINT, 
                           O_CUSTKEY        INTEGER,
                           O_ORDERSTATUS    VARCHAR(1)/*CHAR(1)*/,
                           O_TOTALPRICE     text /* DOUBLE PRECISION DECIMAL(15,2)*/,
                           O_ORDERDATE      DATE,
                           O_ORDERPRIORITY  VARCHAR(15) /*CHAR(15)*/,
                           O_CLERK          VARCHAR(15) /*CHAR(15)*/,
                           O_SHIPPRIORITY   INTEGER,
                           O_COMMENT        VARCHAR(79), 
						   DUMMY TEXT) 
				   LOCATION (%s) 
				   FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(orders, conf.Ext, locallf("orders")))

		lineitem := `CREATE EXTERNAL TABLE %s.LINEITEM ( L_ORDERKEY BIGINT, 
                             L_PARTKEY     INTEGER,
                             L_SUPPKEY     INTEGER,
                             L_LINENUMBER  INTEGER,
                             L_QUANTITY    INTEGER /*DECIMAL(15,2)*/, 
                             L_EXTENDEDPRICE  text /* DOUBLE PRECISION, or DECIMAL(15,2)*/,
                             L_DISCOUNT    text /* DOUBLE PRECISION, or DECIMAL(15,2)*/,
                             L_TAX         text /* DOUBLE PRECISION, or, DECIMAL(15,2)*/,
                             L_RETURNFLAG  VARCHAR(1),
                             L_LINESTATUS  VARCHAR(1),
                             L_SHIPDATE    DATE,
                             L_COMMITDATE  DATE,
                             L_RECEIPTDATE DATE,
                             L_SHIPINSTRUCT VARCHAR(25) /*CHAR(25)*/,
                             L_SHIPMODE     VARCHAR(10) /*CHAR(10)*/,
                             L_COMMENT      VARCHAR(44),
							 DUMMY TEXT) 
				   LOCATION (%s) 
				   FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(lineitem, conf.Ext, locallf("lineitem")))
	})

	t.Run("Step=spqddl", func(t *testing.T) {
		conn, err := Connect()
		if err != nil {
			t.Errorf("Cannot connect to database, error: %s", err.Error())
		}
		defer conn.Disconnect()

		px := [2]string{"xdrive", "xdrqry"}
		for i := 0; i < 2; i++ {
			Check(t, conn.Execute(fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", px[i])), "drop schema")
			Check(t, conn.Execute(fmt.Sprintf("CREATE SCHEMA %s", px[i])), "create spq schema")
		}

		var locf func(string) string
		locf = func(t string) string {
			return fmt.Sprintf("'xdrive://localhost:27183/tpch-spq-%d/seg-#SEGID#/%s.spq'", conf.Scale, t)
		}

		// Create two set of external tables, one for xdrive, one for gpfdist.
		//
		// nation.
		nation := `CREATE %s EXTERNAL TABLE %s.NATION%s  ( N_NATIONKEY  INTEGER,
                            N_NAME       VARCHAR(25) /*CHAR(25)*/, 
                            N_REGIONKEY  INTEGER, 
                            N_COMMENT    VARCHAR(152))
				   LOCATION (%s) FORMAT '%sSPQ' 
				   DISTRIBUTED BY (N_NATIONKEY)
				   `
		Check(t, conn.Execute(fmt.Sprintf(nation, "WRITABLE", px[0], "_W", locf("nation"), "")), "create nation_w")
		Check(t, conn.Execute(fmt.Sprintf(nation, "", px[0], "", locf("nation"), "")), "create nation")
		Check(t, conn.Execute(fmt.Sprintf(nation, "", px[1], "", locf("nation"), "X")), "create nation")

		// region
		region := ` CREATE %s EXTERNAL TABLE %s.REGION%s  ( R_REGIONKEY  INTEGER, 
                            R_NAME       VARCHAR(25) /*CHAR(25)*/, 
                            R_COMMENT    VARCHAR(152)) 
				   LOCATION (%s) FORMAT '%sSPQ' 
				   DISTRIBUTED BY (R_REGIONKEY)
				   `
		Check(t, conn.Execute(fmt.Sprintf(region, "WRITABLE", px[0], "_W", locf("region"), "")), "create region_w")
		Check(t, conn.Execute(fmt.Sprintf(region, "", px[0], "", locf("region"), "")), "create region")
		Check(t, conn.Execute(fmt.Sprintf(region, "", px[1], "", locf("region"), "X")), "create region")

		// part
		part := `CREATE %s EXTERNAL TABLE %s.PART%s  ( P_PARTKEY     INTEGER, 
                          P_NAME        VARCHAR(55), 
                          P_MFGR        VARCHAR(25) /*CHAR(25)*/, 
                          P_BRAND       VARCHAR(10) /*CHAR(10)*/, 
                          P_TYPE        VARCHAR(25), 
                          P_SIZE        INTEGER, 
                          P_CONTAINER   VARCHAR(10) /*CHAR(10)*/, 
                          P_RETAILPRICE DOUBLE PRECISION /*DECIMAL(15,2)*/, 
                          P_COMMENT     VARCHAR(23))
				   LOCATION (%s) FORMAT '%sSPQ' 
				   DISTRIBUTED BY (P_PARTKEY)
				   `
		Check(t, conn.Execute(fmt.Sprintf(part, "WRITABLE", px[0], "_W", locf("part"), "")), "create part_w")
		Check(t, conn.Execute(fmt.Sprintf(part, "", px[0], "", locf("part"), "")), "create part")
		Check(t, conn.Execute(fmt.Sprintf(part, "", px[1], "", locf("part"), "X")), "create part")

		// supplier
		supplier := `CREATE %s EXTERNAL TABLE %s.SUPPLIER%s ( S_SUPPKEY     INTEGER, 
                             S_NAME        VARCHAR(25) /*CHAR(25)*/, 
                             S_ADDRESS     VARCHAR(40), 
                             S_NATIONKEY   INTEGER, 
                             S_PHONE       VARCHAR(15) /*CHAR(15)*/, 
                             S_ACCTBAL     DOUBLE PRECISION /*DECIMAL(15,2)*/, 
                             S_COMMENT     VARCHAR(101))
				   LOCATION (%s) FORMAT '%sSPQ' 
				   DISTRIBUTED BY (S_SUPPKEY)
				   `
		Check(t, conn.Execute(fmt.Sprintf(supplier, "WRITABLE", px[0], "_W", locf("supplier"), "")), "create supplier_w")
		Check(t, conn.Execute(fmt.Sprintf(supplier, "", px[0], "", locf("supplier"), "")), "create supplier")
		Check(t, conn.Execute(fmt.Sprintf(supplier, "", px[1], "", locf("supplier"), "X")), "create supplier")

		partsupp := `CREATE %s EXTERNAL TABLE %s.PARTSUPP%s ( PS_PARTKEY     INTEGER, 
                             PS_SUPPKEY     INTEGER, 
                             PS_AVAILQTY    INTEGER,
                             PS_SUPPLYCOST  DOUBLE PRECISION /*DECIMAL(15,2)*/, 
                             PS_COMMENT     VARCHAR(199)) 
				   LOCATION (%s) FORMAT '%sSPQ' 
				   DISTRIBUTED BY (PS_PARTKEY)
				   `
		Check(t, conn.Execute(fmt.Sprintf(partsupp, "WRITABLE", px[0], "_W", locf("partsupp"), "")), "create partsupp_w")
		Check(t, conn.Execute(fmt.Sprintf(partsupp, "", px[0], "", locf("partsupp"), "")), "create partsupp")
		Check(t, conn.Execute(fmt.Sprintf(partsupp, "", px[1], "", locf("partsupp"), "X")), "create partsupp")

		customer := `CREATE %s EXTERNAL TABLE %s.CUSTOMER%s ( C_CUSTKEY     INTEGER, 
                             C_NAME        VARCHAR(25),
                             C_ADDRESS     VARCHAR(40),
                             C_NATIONKEY   INTEGER,
                             C_PHONE       VARCHAR(15) /*CHAR(15)*/,
                             C_ACCTBAL     DOUBLE PRECISION/*DECIMAL(15,2)*/, 
                             C_MKTSEGMENT  VARCHAR(10) /*CHAR(10)*/,
                             C_COMMENT     VARCHAR(117)) 
				   LOCATION (%s) FORMAT '%sSPQ' 
				   DISTRIBUTED BY (C_CUSTKEY)
				   `
		Check(t, conn.Execute(fmt.Sprintf(customer, "WRITABLE", px[0], "_W", locf("customer"), "")), "create customer_w")
		Check(t, conn.Execute(fmt.Sprintf(customer, "", px[0], "", locf("customer"), "")), "create customer")
		Check(t, conn.Execute(fmt.Sprintf(customer, "", px[1], "", locf("customer"), "X")), "create customer")

		orders := `CREATE %s EXTERNAL TABLE %s.ORDERS%s  ( O_ORDERKEY       BIGINT, 
                           O_CUSTKEY        INTEGER,
                           O_ORDERSTATUS    VARCHAR(1)/*CHAR(1)*/,
                           O_TOTALPRICE     DOUBLE PRECISION /*DECIMAL(15,2)*/,
                           O_ORDERDATE      DATE,
                           O_ORDERPRIORITY  VARCHAR(15) /*CHAR(15)*/,
                           O_CLERK          VARCHAR(15) /*CHAR(15)*/,
                           O_SHIPPRIORITY   INTEGER,
                           O_COMMENT        VARCHAR(79)) 
				   LOCATION (%s) FORMAT '%sSPQ' 
				   DISTRIBUTED BY (O_ORDERKEY)
				   `
		Check(t, conn.Execute(fmt.Sprintf(orders, "WRITABLE", px[0], "_W", locf("orders"), "")), "create orders_w")
		Check(t, conn.Execute(fmt.Sprintf(orders, "", px[0], "", locf("orders"), "")), "create orders")
		Check(t, conn.Execute(fmt.Sprintf(orders, "", px[1], "", locf("orders"), "X")), "create orders")

		lineitem := `CREATE %s EXTERNAL TABLE %s.LINEITEM%s ( L_ORDERKEY BIGINT, 
                             L_PARTKEY     INTEGER,
                             L_SUPPKEY     INTEGER,
                             L_LINENUMBER  INTEGER,
                             L_QUANTITY    INTEGER /*DECIMAL(15,2)*/, 
                             L_EXTENDEDPRICE  DOUBLE PRECISION/*DECIMAL(15,2)*/,
                             L_DISCOUNT    DOUBLE PRECISION /*DECIMAL(15,2)*/,
                             L_TAX         DOUBLE PRECISION /*DECIMAL(15,2)*/,
                             L_RETURNFLAG  VARCHAR(1),
                             L_LINESTATUS  VARCHAR(1),
                             L_SHIPDATE    DATE,
                             L_COMMITDATE  DATE,
                             L_RECEIPTDATE DATE,
                             L_SHIPINSTRUCT VARCHAR(25) /*CHAR(25)*/,
                             L_SHIPMODE     VARCHAR(10) /*CHAR(10)*/,
                             L_COMMENT      VARCHAR(44)) 
				   LOCATION (%s) FORMAT '%sSPQ' 
				   DISTRIBUTED BY (L_ORDERKEY)
				   `
		Check(t, conn.Execute(fmt.Sprintf(lineitem, "WRITABLE", px[0], "_W", locf("lineitem"), "")), "create lineitem_w")
		Check(t, conn.Execute(fmt.Sprintf(lineitem, "", px[0], "", locf("lineitem"), "")), "create lineitem")
		Check(t, conn.Execute(fmt.Sprintf(lineitem, "", px[1], "", locf("lineitem"), "X")), "create lineitem")
	})

	t.Run("Step=spqview", func(t *testing.T) {
		px := [2]string{"spq", "xq"}
		for i := 0; i < 2; i++ {
			qf := fmt.Sprintf("%s/sql/mkview-%s.sql", Dir(), px[i])
			cmd, err := PsqlCmd(qf)
			if err != nil {
				t.Errorf("Cannot build psql query command. error :%s", err.Error())
			}

			err = exec.Command("bash", "-c", cmd).Run()
			if err != nil {
				t.Errorf("Cannot run spq query view ddl.   error: %s", err.Error())
			}
		}
	})
}
