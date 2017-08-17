package bench

import (
	"fmt"
	"os/exec"
	"testing"

	"github.com/vitesse-ftian/dggo/vitessedata/ssh"
)

func TestGenData(t *testing.T) {
	t.Run("Step=make", func(t *testing.T) {
		cmd := fmt.Sprintf("cd %s/tpch_2_15_0/dbgen && make", Dir())
		err := exec.Command("bash", "-c", cmd).Run()
		if err != nil {
			t.Errorf("Cannot make dbgen.")
		}
	})

	t.Run("Step=dbgen", func(t *testing.T) {
		conf, err := GetConfig()
		if err != nil {
			t.Errorf("Configuration error: %s", err.Error())
		}

		segs, err := Segs()
		if err != nil {
			t.Errorf("Cannot get deepgreen segs, error: %s.", err.Error())
		}

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
			datadir := fmt.Sprintf("%s/seg-%d", conf.Staging, seg.Id)
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
}
