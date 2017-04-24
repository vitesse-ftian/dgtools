package xtable

import (
	"os"
	"testing"
)

func TestSelectTable(t *testing.T) {
	dg := Deepgreen{
		Host: "localhost",
		Port: "5432",
		Db:   "tpch1f",
	}

	err := dg.Connect()
	if err != nil {
		t.Error(err.Error())
		panic("Cannot open connection to database tpch1f: " + err.Error())
	}
	defer dg.Disconnect()

	t.Run("nation", func(t *testing.T) {
		nation, err := MakeXTable(&dg, "nation")
		if err != nil {
			t.Fatal("Cannot open connection to tpch1f ", err)
		}

		q, err := MakeXTableSql(&dg, "select '##', * from #0# where #0.2# = 1", []XTable{nation})
		if err != nil {
			t.Fatal("Cannot create select table ", err)
		}

		err = q.RenderAll(os.Stdout)
		if err != nil {
			t.Fatal("Cannot render ", err)
		}
	})
}
