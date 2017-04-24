package main

import (
	"fmt"
	"os"
	"vitessedata/phi/codegen"
)

var usage string = `
Usage: phi <command> [<flags>] [<args>]

Commands:
	codegen		Generate boilerplate code for phirun.  Internal use only.
`

func main() {
	if len(os.Args) == 1 {
		panic(usage)
	}

	switch os.Args[1] {
	case "codegen":
		codegen.Command(os.Args[2:])
	default:
		panic(fmt.Sprintf("Error: %s is not a valid command.\n", os.Args[1]))
	}
}
