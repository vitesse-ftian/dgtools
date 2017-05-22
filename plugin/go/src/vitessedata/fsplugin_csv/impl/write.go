package impl

import (
	"fmt"
	"os"
	"vitessedata/plugin"
	"vitessedata/plugin/csvhandler"
)

// DoWrite services xdrive write request.  It read a sequence of PluginWriteRequest
// from stdin and write to file system.
func DoWrite() error {
	//
	// Open destination file for write, we will create a .part file
	// and rename it if succeed, remove if fail.  Another good reason of this
	// renaming is that if there is an external table using a glob like *.csv,
	// we will not read partial files.   Because writable external table
	// will open exactly one file, this trick actually gives a very poor man's
	// transaction (all or nothing) semantics.
	//
	rinfo := plugin.RInfo()
	tmpfn := rinfo.Rpath + ".part"
	wf, err := os.Create(tmpfn)
	if err != nil {
		plugin.ReplyWriteError(-2, "Cannot open file to write: "+rinfo.Rpath)
		return fmt.Errorf("Cannot open file to write.")
	}

	err = csvhandler.WritePart(wf)
	if err == nil {
		// Success!
		plugin.DbgLog("OK.  Close writer, then rename %s -> %s.", tmpfn, rinfo.Rpath)
		os.Rename(tmpfn, rinfo.Rpath)
		plugin.ReplyWriteError(0, "")
		return nil
	} else {
		plugin.DbgLog("Failed.   Close writer, then remove %s.", tmpfn)
		os.Remove(tmpfn)
		plugin.ReplyWriteError(-1, err.Error())
		return err
	}
}
