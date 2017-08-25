package plugin

import (
	"fmt"
	"github.com/vitesse-ftian/dggo/vitessedata/xtable"
	"testing"
)

func checkError(t *testing.T, err error) {
	if err != nil {
		t.Error(err.Error())
	}
}

func checkCond(t *testing.T, cond bool, msg string) {
	if !cond {
		t.Error(msg)
	}
}

func countRows(t *testing.T, dg *xtable.Deepgreen, sql string) int {
	rows, err := dg.Conn.Query(sql)
	checkError(t, err)
	defer rows.Close()

	cnt := 0

	for rows.Next() {
		cnt += 1
	}
	return cnt
}

func TestPlugin(t *testing.T) {
	dg := xtable.Deepgreen{
		Host: "localhost",
		Port: "5432",
		Db:   "wetestdata",
	}
	err := dg.Connect()
	if err != nil {
		t.Error(err.Error())
		panic("Cannot open database connection.")
	}
	defer dg.Disconnect()

	t.Run("xtables", func(t *testing.T) {
		cnt := countRows(t, &dg, "select * from esfs")
		checkCond(t, cnt == 5, fmt.Sprintf("esfs %d bad result", cnt))

		cnt = countRows(t, &dg, "select * from esr")
		checkCond(t, cnt == 5, fmt.Sprintf("esr %d bad result", cnt))

		cnt = countRows(t, &dg, "select * from esr where dg_utils.xdrive_query($$q=female$$)")
		checkCond(t, cnt == 2, fmt.Sprintf("esr female %d bad result", cnt))

		cnt = countRows(t, &dg, "select * from esr where dg_utils.xdrive_query($$routing=gold$$)")
		checkCond(t, cnt == 2, fmt.Sprintf("esr routing %d bad result", cnt))

		err = dg.Execute("insert into esw select * from esfs")
		checkError(t, err)

	})
}
