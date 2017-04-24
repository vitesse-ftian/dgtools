package codegen

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

func Command(args []string) {
	// phi codegen -input=phirun.go
	flagset := flag.NewFlagSet("codegen", flag.ExitOnError)
	input := flagset.String("input", "", "src file")

	if err := flagset.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "Usage: %s codegen\n", os.Args[0])
		flagset.PrintDefaults()
		os.Exit(1)
	}

	// args := flagset.Args()
	if strings.HasSuffix(*input, "go") {
		genGo(*input)
	} else {
		panic("Unknown src code language. Src file: " + *input)
	}
}
