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
		// dbgen is run parallely on each seg,
		nseg := len(segs)
		hosts := make([]string, nseg)
		cmds := make([]string, nseg)

		for i, seg := range segs {
			hosts[i] = seg.Addr
			datadir := fmt.Sprintf("%s/tpch/scale-%d/seg-%d", conf.Staging, conf.Scale, seg.Id)
			cmds[i] = fmt.Sprintf("rm -fr %s; mkdir -p %s", datadir, datadir)
		}

		if ssh.ExecAnyError(ssh.ExecOn(hosts, cmds)) != nil {
			t.Errorf("Cannot prepare staging data dir.")
		}

		for _, seg := range segs {
			datadir := fmt.Sprintf("%s/tpch/scale-%d/seg-%d", conf.Staging, conf.Scale, seg.Id)
			cmd := fmt.Sprintf("cd %s; scp -r tpch_2_15_0 %s:%s/", Dir(), seg.Addr, datadir)
			err := exec.Command("bash", "-c", cmd).Run()
			if err != nil {
				t.Errorf("Cannot scp tpch_2_15_0.")
			}
		}

		for i, seg := range segs {
			datadir := fmt.Sprintf("%s/tpch/scale-%d/seg-%d", conf.Staging, conf.Scale, seg.Id)
			// nation and region table (*tbl files).  should only be copied to dest dir once.
			// other tables are generated in parts (*tbl.*).   Note the .
			if seg.Id == 0 {
				cmds[i] = fmt.Sprintf("cd %s/tpch_2_15_0/dbgen && ./dbgen -s %d -C %d -S %d && mv *tbl* ../..", datadir, conf.Scale, nseg, seg.Id+1)
			} else {
				cmds[i] = fmt.Sprintf("cd %s/tpch_2_15_0/dbgen && ./dbgen -s %d -C %d -S %d && mv *tbl.* ../..", datadir, conf.Scale, nseg, seg.Id+1)
			}
		}

		if ssh.ExecAnyError(ssh.ExecOn(hosts, cmds)) != nil {
			t.Errorf("Cannot generate data.")
		}
	})
}
