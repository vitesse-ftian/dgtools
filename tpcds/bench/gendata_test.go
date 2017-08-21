package bench

import (
	"fmt"
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
		cmd := fmt.Sprintf("cd %s/tpcds_tools/tools && make", Dir())
		err := exec.Command("bash", "-c", cmd).Run()
		if err != nil {
			t.Errorf("Cannot make dsdgen.  error: %s", err.Error())
		}

		cmd = fmt.Sprintf("mkdir -p %s/gen", Dir())
		err = exec.Command("bash", "-c", cmd).Run()
		if err != nil {
			t.Errorf("Cannot create gen dir.  error: %s", err.Error())
		}
	})

	t.Run("Step=dsdgen", func(t *testing.T) {
		// dsdgen is run parallely on each seg,
		nseg := len(segs)
		hosts := make([]string, nseg)
		cmds := make([]string, nseg)

		for i, seg := range segs {
			hosts[i] = seg.Addr
			datadir := fmt.Sprintf("%s/tpcds/scale-%d/seg-%d", conf.Staging, conf.Scale, seg.Id)
			cmds[i] = fmt.Sprintf("rm -fr %s; mkdir -p %s", datadir, datadir)
		}

		if ssh.ExecAnyError(ssh.ExecOn(hosts, cmds)) != nil {
			t.Errorf("Cannot prepare staging data dir.")
		}

		for _, seg := range segs {
			datadir := fmt.Sprintf("%s/tpcds/scale-%d/seg-%d", conf.Staging, conf.Scale, seg.Id)
			cmd := fmt.Sprintf("cd %s; scp -r tpcds_tools %s:%s/", Dir(), seg.Addr, datadir)
			err := exec.Command("bash", "-c", cmd).Run()
			if err != nil {
				t.Errorf("Cannot scp tpcds_tools.")
			}
		}

		for i, seg := range segs {
			datadir := fmt.Sprintf("%s/tpcds/scale-%d/seg-%d", conf.Staging, conf.Scale, seg.Id)
			// nation and region table (*tbl files).  should only be copied to dest dir once.
			// other tables are generated in parts (*tbl.*).   Note the .
			cmds[i] = fmt.Sprintf("cd %s/tpcds_tools/tools && rm -f *.dat && ./dsdgen -scale %d -parallel %d -child %d -terminate n && mv *.dat ../..", datadir, conf.Scale, nseg, seg.Id+1)
		}

		if ssh.ExecAnyError(ssh.ExecOn(hosts, cmds)) != nil {
			t.Errorf("Cannot generate data.")
		}
	})
}
