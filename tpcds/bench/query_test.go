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

	// Gucs.   Only set if not default
	for _, guc := range conf.Gucs {
		err = conn.Execute(guc)
		if err != nil {
			b.Errorf("Cannot set guc %s.  error: %s", guc, err.Error())
		}
	}

	for i := 0; i <= 99; i++ {
		runid := fmt.Sprintf("Step=q%d", i)
		b.Run(runid, func(b *testing.B) {
			runQ(b, conn, i)
		})
	}
}
