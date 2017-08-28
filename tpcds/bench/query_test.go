package bench

import (
	"fmt"
	"testing"

	"github.com/vitesse-ftian/dggo/vitessedata/xtable"
)

func runQ(b *testing.B, conn *xtable.Deepgreen, n int) {
	err := conn.Execute(fmt.Sprintf("select * from tpcds.q%d", n))
	if err != nil {
		b.Errorf("Cannot run query %d.  error: %s", n, err.Error())
	}
}

func BenchmarkQuery(b *testing.B) {
	conf, err := GetConfig()
	if err != nil {
		b.Errorf("Configuration error: %s", err.Error())
	}

	conn, err := Connect()
	if err != nil {
		b.Errorf("Cannot connect to database %s, error: %s", err.Error())
	}
	defer conn.Disconnect()

	// Gucs.
	if conf.Orca != 0 {
		err = conn.Execute("set optimizer = on")
		if err != nil {
			b.Errorf("Cannot set guc optimizer = on.   error: %s", err.Error())
		}
	}

	if conf.Vitesse == 0 {
		err = conn.Execute("set vitesse.enable = 0")
		if err != nil {
			b.Errorf("Cannot set guc vitesse.enable to 0.  error: %s", err.Error())
		}
	}

	if conf.StatementMem != 0 {
		err = conn.Execute(fmt.Sprintf("set statement_mem = %d", conf.StatementMem))
		if err != nil {
			b.Errorf("Cannot set statement_mem = %d, error :%s", conf.StatementMem, err.Error())
		}
	}

	for i := 0; i <= 99; i++ {
		runid := fmt.Sprintf("Step=q%d", i)
		b.Run(runid, func(b *testing.B) {
			runQ(b, conn, i)
		})
	}
}
