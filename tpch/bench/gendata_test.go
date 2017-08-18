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
		seghosts := make(map[string]bool)
		for _, seg := range segs {
			seghosts[seg.Addr] = true
		}

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

		// Create two set of external tables, one for xdrive, one for gpfdist.

	})
}
