package s3

import (
	"fmt"
	"os"
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

	checkCond(t, rows != nil, "Bad rows, nil.")
	for rows.Next() {
		cnt += 1
	}
	return cnt
}

func TestS3N(t *testing.T) {
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

	t.Run("Step=show", func(t *testing.T) {
		s3n, err := xtable.MakeXTable(&dg, "s3nation")
		if err != nil {
			t.Fatal("Cannot create xtable s3n")
		}
		err = s3n.RenderAll(os.Stdout)
		if err != nil {
			t.Error("Cannot read s3n")
		}

		es3n, err := xtable.MakeXTable(&dg, "emptys3nation")
		if err != nil {
			t.Fatal("Cannot create xtable emptys3n")
		}
		err = es3n.RenderAll(os.Stdout)
		if err == nil {
			t.Error("Empty s3 read should fail.")
		}
	})

	t.Run("Step=write", func(t *testing.T) {
		cnt := countRows(t, &dg, "select * from s3xxr")

		err := dg.Execute("insert into s3xxw select * from xx1")
		checkError(t, err)

		cnt2 := countRows(t, &dg, "select * from s3xxr")
		checkCond(t, cnt2 == cnt+404, fmt.Sprintf("s3xxr %d bad result", cnt))
	})
}
