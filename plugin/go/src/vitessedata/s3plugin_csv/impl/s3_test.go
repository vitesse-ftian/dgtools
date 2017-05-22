package impl

import (
	"fmt"
	"testing"
)

func TestListDir(t *testing.T) {
	t.Run("Bkt=list", func(t *testing.T) {
		var sb S3Bkt
		sb.Connect("us-west-1", "tpch1f")
		tbls, err := sb.ListDir(pathSimplePrefix(""))
		if err != nil {
			t.Error(err.Error())
		}

		fmt.Printf("---- ListDir csv -----------\n")
		for _, t := range tbls {
			fmt.Printf("\t%s %d\n", t.Name, t.Size)
		}

		tbls, err = sb.ListDir(pathSimplePrefix("//w/x*.csv"))
		if err != nil {
			t.Error(err.Error())
		}

		fmt.Printf("---- ListDir //w/x*.csv -----------\n")
		for _, t := range tbls {
			fmt.Printf("\t%s %d\n", t.Name, t.Size)
		}
	})

	t.Run("Bkt=tpch1f", func(t *testing.T) {
		var sb S3Bkt
		sb.Connect("us-west-1", "tpch1f")
		tbls, err := sb.ListDir(pathSimplePrefix("csv"))
		if err != nil {
			t.Error(err.Error())
		}

		fmt.Printf("---- ListDir csv -----------\n")
		for _, t := range tbls {
			fmt.Printf("\t%s %d\n", t.Name, t.Size)
		}

		tbls, err = sb.ListDir("/csv")
		if err != nil {
			t.Error(err.Error())
		}

		fmt.Printf("---- ListDir /csv -----------\n")
		for _, t := range tbls {
			fmt.Printf("\t%s %d\n", t.Name, t.Size)
		}

		tbls, err = sb.ListDir(pathSimplePrefix("csv/"))
		if err != nil {
			t.Error(err.Error())
		}

		fmt.Printf("---- ListDir csv/ -----------\n")
		for _, t := range tbls {
			fmt.Printf("\t%s %d\n", t.Name, t.Size)
		}

		tbls, err = sb.ListDir(pathSimplePrefix("/c*v/*"))
		if err != nil {
			t.Error(err.Error())
		}

		fmt.Printf("---- ListDir /csv/ -----------\n")
		for _, t := range tbls {
			fmt.Printf("\t%s %d\n", t.Name, t.Size)
		}

		tbls, err = sb.ListDir(pathSimplePrefix("/emptydir"))
		if err != nil {
			t.Error(err.Error())
		}

		fmt.Printf("---- ListDir /emptydir/ -----------\n")
		for _, t := range tbls {
			fmt.Printf("\t%s %d\n", t.Name, t.Size)
		}

		tbls, err = sb.ListDir("")
		if err != nil {
			t.Error(err.Error())
		}

		fmt.Printf("---- ListDir () -----------\n")
		for _, t := range tbls {
			fmt.Printf("\t%s %d\n", t.Name, t.Size)
		}

		tbls, err = sb.ListDir(pathSimplePrefix("/notexist"))
		if err != nil {
			t.Error(err.Error())
		}

		fmt.Printf("---- ListDir notexist -----------\n")
		for _, t := range tbls {
			fmt.Printf("\t%s %d\n", t.Name, t.Size)
		}
	})

	t.Run("Bkt=wetestdataempty", func(t *testing.T) {
		var sb S3Bkt
		sb.Connect("us-west-1", "wetestdataempty")

		tbls, err := sb.ListDir(pathSimplePrefix("/emptydir"))
		if err != nil {
			t.Error(err.Error())
		}

		fmt.Printf("---- ListDir wetestdataempty /emptydir/ -----------\n")
		for _, t := range tbls {
			fmt.Printf("\t%s %d\n", t.Name, t.Size)
		}

		tbls, err = sb.ListDir(pathSimplePrefix(""))
		if err != nil {
			t.Error(err.Error())
		}

		fmt.Printf("---- ListDir wetestdataempty () -----------\n")
		for _, t := range tbls {
			fmt.Printf("\t%s %d\n", t.Name, t.Size)
		}
	})

	t.Run("Bkt=badbkt", func(t *testing.T) {
		var sb S3Bkt
		sb.Connect("us-west-1", "badbkt")

		tbls, err := sb.ListDir("emptydir")
		if err == nil {
			t.Error("Should have failed.")
			fmt.Printf("---- ListDir badbkt /emptydir/ -----------\n")
			for _, t := range tbls {
				fmt.Printf("\t%s %d\n", t.Name, t.Size)
			}
		}

		tbls, err = sb.ListDir("")
		if err == nil {
			t.Error("Should have failed.")
			fmt.Printf("---- ListDir badbkt () -----------\n")
			for _, t := range tbls {
				fmt.Printf("\t%s %d\n", t.Name, t.Size)
			}
		}
	})

	t.Run("Bkt=badregion", func(t *testing.T) {
		var sb S3Bkt
		sb.Connect("xxx-us-west-1", "badbkt")

		tbls, err := sb.ListDir("emptydir")
		if err == nil {
			t.Error("Should have failed.")
			fmt.Printf("---- ListDir badbkt /emptydir/ -----------\n")
			for _, t := range tbls {
				fmt.Printf("\t%s %d\n", t.Name, t.Size)
			}
		}

		tbls, err = sb.ListDir("")
		if err == nil {
			t.Error("Should have failed.")
			fmt.Printf("---- ListDir badbkt () -----------\n")
			for _, t := range tbls {
				fmt.Printf("\t%s %d\n", t.Name, t.Size)
			}
		}
	})
}
