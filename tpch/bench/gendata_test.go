package bench

import (
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/vitesse-ftian/dggo/vitessedata/ssh"
)

func TestGenData(t *testing.T) {
	conf, err := GetConfig()
	if err != nil {
		t.Errorf("Configuration error: %s", err.Error())
	}

	segs, err := Segs()
	if err != nil {
		t.Errorf("Cannot get deepgreen segs, error: %s.", err.Error())
	}

	seghosts := make(map[string]bool)
	for _, seg := range segs {
		seghosts[seg.Addr] = true
	}

	t.Run("Step=make", func(t *testing.T) {
		cmd := fmt.Sprintf("cd %s/tpch_2_15_0/dbgen && make", Dir())
		err := exec.Command("bash", "-c", cmd).Run()
		if err != nil {
			t.Errorf("Cannot make dbgen.  error: %s", err.Error())
		}

		cmd = fmt.Sprintf("mkdir -p %s/gen", Dir())
		err = exec.Command("bash", "-c", cmd).Run()
		if err != nil {
			t.Errorf("Cannot create gen dir.  error: %s", err.Error())
		}
	})

	t.Run("Step=dbgen", func(t *testing.T) {
		// We deliberately run dbgen one section by one section, because
		// we only assume there is enough disk space for one section.
		nseg := len(segs)
		for _, seg := range segs {
			t.Logf("DB Seg %d, Addr %s.", seg.Id, seg.Addr)
			cmd := fmt.Sprintf("cd %s/tpch_2_15_0/dbgen && ./dbgen -s %d -C %d -S %d", Dir(), conf.Scale, nseg, seg.Id+1)

			err := exec.Command("bash", "-c", cmd).Run()
			if err != nil {
				t.Errorf("Cannot call dbgen.")
			}

			seghost := []string{seg.Addr}
			datadir := fmt.Sprintf("%s/tpch/scale-%d/seg-%d", conf.Staging, conf.Scale, seg.Id)
			cmd = fmt.Sprintf("rm -fr %s; mkdir -p %s", datadir, datadir)
			if ssh.ExecAnyError(ssh.ExecCmdOn("", seghost, cmd)) != nil {
				t.Errorf("Cannot prepare staging data dir %s on host %s.", datadir, seg.Addr)
			}

			if seg.Id == 0 {
				cmd = fmt.Sprintf("cd %s/tpch_2_15_0/dbgen && scp *tbl* %s:%s/", Dir(), seghost, datadir)
			} else {
				cmd = fmt.Sprintf("cd %s/tpch_2_15_0/dbgen && scp *tbl.* %s:%s/", Dir(), seghost, datadir)
			}

			err = exec.Command("bash", "-c", cmd).Run()
			if err != nil {
				t.Errorf("Cannot scp dbgen.")
			}

			cmd = fmt.Sprintf("cd %s/tpch_2_15_0/dbgen && rm -f *tbl*", Dir())
			err = exec.Command("bash", "-c", cmd).Run()
			if err != nil {
				t.Errorf("Cannot clean dbgen.")
			}
		}
	})

	t.Run("Step=xdrtoml", func(t *testing.T) {
		tomlf := Dir() + "/gen/xdrive.toml"
		xf, err := os.Create(tomlf)
		if err != nil {
			t.Errorf("Cannot create xdrive.toml file.  error: %s", err.Error())
		}

		fmt.Fprintf(xf, "[xdrive]\n")
		fmt.Fprintf(xf, "dir = \"%s\"\n", conf.Staging)
		fmt.Fprintf(xf, "host = [")
		prefix := " "
		for k, _ := range seghosts {
			fmt.Fprintf(xf, " %s\"%s:31416\" ", prefix, k)
			prefix = ","
		}
		fmt.Fprintf(xf, " ]\n\n")

		fmt.Fprintf(xf, "[[xdrive.mount]]\n")
		fmt.Fprintf(xf, "name = \"tpch-scale-%d\"\n", conf.Scale)
		fmt.Fprintf(xf, "scheme = \"nfs\"\n")
		fmt.Fprintf(xf, "root = \"./tpch/scale-%d\"\n", conf.Scale)
		fmt.Fprintf(xf, "conf = \"\"\n")

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
	})

	t.Run("Step=extddl", func(t *testing.T) {
		conn, err := Connect()
		if err != nil {
			t.Errorf("Cannot connect to database %s, error: %s", err.Error())
		}
		defer conn.Disconnect()

		conn.Execute("DROP SCHEMA IF EXISTS XDR CASCADE")
		conn.Execute("DROP SCHEMA IF EXISTS GPF CASCADE")
		conn.Execute("CREATE SCHEMA XDR")
		conn.Execute("CREATE SCHEMA GPF")

		xdr1f := func(t string) string {
			return fmt.Sprintf("xdrive://localhost:31416/tpch-scale-%d/seg-1/%s.tbl", conf.Scale, t)
		}
		xdrallf := func(t string) string {
			return fmt.Sprintf("xdrive://localhost:31416/tpch-scale-%d/seg-#SEGID#/%s.tbl.*", conf.Scale, t)
		}
		gpf1f := func(t string) string {
			return fmt.Sprintf("gpfdist://%s:22222/tpch/scale-%d/seg-1/%s.tbl", segs[0].Addr, conf.Scale, t)
		}
		gpfallf := func(t string) string {
			prefix := ""
			ret := ""
			for h, _ := range seghosts {
				ret = ret + prefix + fmt.Sprintf("gpfdist://%s:22222/tpch/scale-%d/seg-*/%s.tbl.*", h, conf.Scale, t)
			}
			return ret
		}

		// Create two set of external tables, one for xdrive, one for gpfdist.
		//
		// nation.
		nation := `CREATE EXTERNAL TABLE %s.NATION  ( N_NATIONKEY  INTEGER,
                            N_NAME       VARCHAR(25) /*CHAR(25)*/, 
                            N_REGIONKEY  INTEGER, 
                            N_COMMENT    VARCHAR(152),
							DUMMY TEXT) 
				   LOCATION ('%s') 
				   FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(nation, "XDR", xdr1f("nation")))
		conn.Execute(fmt.Sprintf(nation, "GPF", gpf1f("nation")))

		// region
		region := ` CREATE EXTERNAL TABLE %s.REGION  ( R_REGIONKEY  INTEGER, 
                            R_NAME       VARCHAR(25) /*CHAR(25)*/, 
                            R_COMMENT    VARCHAR(152), 
						DUMMY TEXT)
				   LOCATION ('%s') 
				   FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(region, "XDR", xdr1f("region")))
		conn.Execute(fmt.Sprintf(region, "GPF", gpf1f("region")))

		// part
		part := `CREATE EXTERNAL TABLE %s.PART  ( P_PARTKEY     INTEGER, 
                          P_NAME        VARCHAR(55), 
                          P_MFGR        VARCHAR(25) /*CHAR(25)*/, 
                          P_BRAND       VARCHAR(10) /*CHAR(10)*/, 
                          P_TYPE        VARCHAR(25), 
                          P_SIZE        INTEGER, 
                          P_CONTAINER   VARCHAR(10) /*CHAR(10)*/, 
                          P_RETAILPRICE DOUBLE PRECISION /*DECIMAL(15,2)*/, 
                          P_COMMENT     VARCHAR(23), 
						  DUMMY TEXT) 
				   LOCATION ('%s') 
				   FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(part, "XDR", xdrallf("part")))
		conn.Execute(fmt.Sprintf(part, "GPF", gpfallf("part")))

		// supplier
		supplier := `CREATE EXTERNAL TABLE %s.SUPPLIER ( S_SUPPKEY     INTEGER, 
                             S_NAME        VARCHAR(25) /*CHAR(25)*/, 
                             S_ADDRESS     VARCHAR(40), 
                             S_NATIONKEY   INTEGER, 
                             S_PHONE       VARCHAR(15) /*CHAR(15)*/, 
                             S_ACCTBAL     DOUBLE PRECISION /*DECIMAL(15,2)*/, 
                             S_COMMENT     VARCHAR(101), 
							 DUMMY TEXT) 
				   LOCATION ('%s') 
				   FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(supplier, "XDR", xdrallf("supplier")))
		conn.Execute(fmt.Sprintf(supplier, "GPF", gpfallf("supplier")))

		partsupp := `CREATE EXTERNAL TABLE %s.PARTSUPP ( PS_PARTKEY     INTEGER, 
                             PS_SUPPKEY     INTEGER, 
                             PS_AVAILQTY    INTEGER,
                             PS_SUPPLYCOST  DOUBLE PRECISION /*DECIMAL(15,2)*/, 
                             PS_COMMENT     VARCHAR(199),
							 DUMMY TEXT) 
				   LOCATION ('%s') 
				   FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(partsupp, "XDR", xdrallf("partsupp")))
		conn.Execute(fmt.Sprintf(partsupp, "GPF", gpfallf("partsupp")))

		customer := `CREATE EXTERNAL TABLE %s.CUSTOMER ( C_CUSTKEY     INTEGER, 
                             C_NAME        VARCHAR(25),
                             C_ADDRESS     VARCHAR(40),
                             C_NATIONKEY   INTEGER,
                             C_PHONE       VARCHAR(15) /*CHAR(15)*/,
                             C_ACCTBAL     DOUBLE PRECISION/*DECIMAL(15,2)*/, 
                             C_MKTSEGMENT  VARCHAR(10) /*CHAR(10)*/,
                             C_COMMENT     VARCHAR(117),
							 DUMMY TEXT) 
				   LOCATION ('%s') 
				   FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(customer, "XDR", xdrallf("customer")))
		conn.Execute(fmt.Sprintf(customer, "GPF", gpfallf("customer")))

		orders := `CREATE EXTERNAL TABLE %s.ORDERS  ( O_ORDERKEY       INTEGER, 
                           O_CUSTKEY        INTEGER,
                           O_ORDERSTATUS    VARCHAR(1)/*CHAR(1)*/,
                           O_TOTALPRICE     DOUBLE PRECISION /*DECIMAL(15,2)*/,
                           O_ORDERDATE      DATE,
                           O_ORDERPRIORITY  VARCHAR(15) /*CHAR(15)*/,
                           O_CLERK          VARCHAR(15) /*CHAR(15)*/,
                           O_SHIPPRIORITY   INTEGER,
                           O_COMMENT        VARCHAR(79), 
						   DUMMY TEXT) 
				   LOCATION ('%s') 
				   FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(orders, "XDR", xdrallf("orders")))
		conn.Execute(fmt.Sprintf(orders, "GPF", gpfallf("orders")))

		lineitem := `CREATE EXTERNAL TABLE %s.LINEITEM ( L_ORDERKEY,
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
                             L_COMMENT      VARCHAR(44),
							 DUMMY TEXT) 
				   LOCATION ('%s') 
				   FORMAT 'CSV' (DELIMITER '|') 
				   `
		conn.Execute(fmt.Sprintf(lineitem, "XDR", xdrallf("lineitem")))
		conn.Execute(fmt.Sprintf(lineitem, "GPF", gpfallf("lineitem")))
	})
}
