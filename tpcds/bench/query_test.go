package bench

import (
	"fmt"
	"testing"

	"github.com/vitesse-ftian/dggo/vitessedata/xtable"
)

func runQ(b *testing.B, conn *xtable.Deepgreen, n int) {
	err := conn.Execute(fmt.Sprintf("select * from q%d", n))
	if err != nil {
		b.Errorf("Cannot run query %d.  error: %s", n, err.Error())
	}
}

func BenchmarkQuery(b *testing.B) {
	// conf, err := GetConfig()
	// if err != nil {
	//		b.Errorf("Configuration error: %s", err.Error())
	//}

	conn, err := Connect()
	if err != nil {
		b.Errorf("Cannot connect to database %s, error: %s", err.Error())
	}
	defer conn.Disconnect()

	// Gucs.
	// err = conn.Execute("set gp_autostats_mode = 'none'")
	// if err != nil {
	//	b.Errorf("Cannot set guc gp_autostats_mode.  error: %s", err.Error())
	// }
	for i := 0; i <= 99; i++ {
		runid := fmt.Sprintf("Step=q%d", i)
		b.Run(runid, func(b *testing.B) {
			runQ(b, conn, i)
		})
	}
}
