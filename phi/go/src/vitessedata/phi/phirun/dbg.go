package phirun

import (
	"log"
	"os"
	"path/filepath"
)

// plugin is launched by xdrive.   The following code provides some simple debugging
// aid that logs some message.  Real impl should consider a better logging mechanism
var g_dbgLog *os.File

func init() {
	if true {
		logdir := filepath.Dir(os.Args[0])
		logfn := logdir + "/errlog.log"
		var err error

		// Tricky!  Must declare err.  If not declare err and use :=, then
		// we will shadow g_dbgLog ...
		g_dbgLog, err = os.OpenFile(logfn, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("Cannot open log file for debugging outputs.")
		}

		log.SetOutput(g_dbgLog)
		log.Printf("Switch log output to %s, g_dbgLog is %v\n", logfn, g_dbgLog)
	}
}

func Log(msg string, args ...interface{}) {
	if g_dbgLog != nil {
		log.Printf(msg, args...)
	}
}

func LogIfErr(err error, msg string, args ...interface{}) {
	if err != nil && g_dbgLog != nil {
		log.Printf(msg+": error "+err.Error()+"\n", args...)
	}
}
