package plugin

import (
	"fmt"
	"testing"
	"vitessedata/xtable"
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
		cnt := countRows(t, &dg, "select * from x1")
		checkCond(t, cnt == 404, fmt.Sprintf("x1 %d bad result", cnt))

		cnt = countRows(t, &dg, "select * from xx1")
		checkCond(t, cnt == 404, fmt.Sprintf("xx1 %d bad result", cnt))

		cnt = countRows(t, &dg, "select * from xx2")
		checkCond(t, cnt == 404, fmt.Sprintf("xx2 %d bad result", cnt))

		x3cnt := countRows(t, &dg, "select * from x3")
		xx3cnt := countRows(t, &dg, "select * from xx3")
		checkCond(t, x3cnt == xx3cnt, fmt.Sprintf("x3 %d != xx3 %d bad result", x3cnt, xx3cnt))

		err = dg.Execute("insert into xxw select * from xx1")
		checkError(t, err)

		cnt = countRows(t, &dg, "select * from xx2")
		checkCond(t, cnt == 404, fmt.Sprintf("xx2 %d bad result", cnt))

		cnt = countRows(t, &dg, "select * from x3")
		checkCond(t, cnt == x3cnt+404, fmt.Sprintf("x3 %d + 404 != %d", x3cnt, cnt))

		cnt = countRows(t, &dg, "select * from xx3")
		checkCond(t, cnt == xx3cnt+404, fmt.Sprintf("xx3 %d + 404 != %d", xx3cnt, cnt))
	})
}
