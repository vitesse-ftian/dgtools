package xtable

import (
	"fmt"
	"os"
	"testing"
)

func TestSqlTable(t *testing.T) {
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
			t.Error(err.Error())
		}
		err = nation.RenderAll(os.Stdout)
		if err != nil {
			t.Error(err.Error())
		}
	})

	t.Run("part", func(t *testing.T) {
		part, err := MakeXTable(&dg, "part")
		if err != nil {
			t.Error(err.Error())
		}
		err = part.RenderN(os.Stdout, 10)
		if err != nil {
			t.Error(err.Error())
		}
	})

	t.Run("q1", func(t *testing.T) {
		q1, err := MakeXTable(&dg, "q1")
		if err != nil {
			t.Error(err.Error())
		}
		err = q1.RenderAll(os.Stdout)
		if err != nil {
			t.Error(err.Error())
		}
	})

	t.Run("badtable", func(t *testing.T) {
		badt, err := MakeXTable(&dg, "table_not_exist")
		if badt != nil || err == nil {
			t.Error("Bad sql should be caught by explain")
		} else {
			fmt.Println("Expected error: " + err.Error())
		}
	})
}
