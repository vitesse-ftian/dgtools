package plugin

import (
	"fmt"
	"log"
	"os"
	"time"
	//	"syscall"
)

// plugin is launched by xdrive.   The following code provides some simple debugging
// aid that logs some message.  Real impl should consider a better logging mechanism
var g_dbgLog *os.File

func StartDbgLogWithPrefix(prefix string) {
	var err error
	fn := fmt.Sprintf("%s-%s.log", prefix, time.Now().Local().Format("2006-01-02"))
	//	fn := fmt.Sprintf("%s-%d.log", prefix, os.Getpid())
	g_dbgLog, err = os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Cannot open log file for debugging outputs.")
	}
	/*
		err = syscall.Dup2(int(g_dbgLog.Fd()), int(os.Stderr.Fd()))
		if err != nil {
			log.Fatalf("Failed to redirect stderr to file: %v", err)
		}
	*/
	log.SetOutput(g_dbgLog)
	log.Printf("Switch log output to %s\n", fn)
	log.Printf("PID=%d\n", os.Getpid())
}

func StartDbgLog() {
	//
	// Change/comment this line, for debugging.
	//
	if true {
		var err error
		fn := fmt.Sprintf("/tmp/xdrive_fsplugin.%d.out", os.Getpid())
		g_dbgLog, err = os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("Cannot open log file for debugging outputs.")
		}
		log.SetOutput(g_dbgLog)
		log.Printf("Switch log output to %s\n", fn)
	}
}

func StopDbgLog() {
	if g_dbgLog != nil {
		log.Printf("Stop log.\n")
		g_dbgLog.Close()
	}
	g_dbgLog = nil
}

func DbgLog(msg string, args ...interface{}) {
	if g_dbgLog != nil {
		log.Printf(msg, args...)
	}
}

func DbgLogIfErr(err error, msg string, args ...interface{}) {
	if err != nil && g_dbgLog != nil {
		log.Printf(msg+": error "+err.Error()+"\n", args...)
	}
}

func FatalIfErr(err error, msg string, args ...interface{}) {
	// fatal: is FATAL, regardless g_dbgLog, we die.
	if err != nil {
		log.Fatalf(msg+": error "+err.Error()+"\n", args...)
	}
}

func FatalIf(shouldDie bool, msg string, args ...interface{}) {
	if shouldDie {
		log.Fatalf(msg, args...)
	}
}
