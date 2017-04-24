package xtable

import (
	"log"
	"os"
)

func PanicIf(v bool, msg string, args ...interface{}) {
	if v {
		log.Panicf(msg, args...)
	}
}

func FatalIf(v bool, msg string, args ...interface{}) {
	if v {
		log.Fatalf(msg, args...)
	}
}

func FatalErr(err error, msg string, args ...interface{}) {
	if err != nil {
		log.Fatalf(msg+": error "+err.Error(), args...)
	}
}

var g_verbose = false

func SetVerbose(v bool) {
	g_verbose = v
}

func LogIf(v bool, msg string, args ...interface{}) {
	if g_verbose && v {
		log.Printf(msg, args...)
	}
}

func LogErr(err error, msg string, args ...interface{}) {
	if g_verbose && err != nil {
		log.Printf(msg+": error "+err.Error()+"\n", args...)
	}
}

func Log(msg string, args ...interface{}) {
	if g_verbose {
		log.Printf(msg, args...)
	}
}

var logOutput *os.File

func SetOutputFile(fn string) {
	var err error
	if logOutput != nil {
		logOutput.Close()
	}

	logOutput, err = os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Error when open log output file %s", fn)
	}

	log.SetOutput(logOutput)
}
