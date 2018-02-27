package impl

import (
	"fmt"
	"os"
	"encoding/csv"
	"vitessedata/plugin"
	"vitessedata/plugin/csvhandler"
	"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
)

var wreq xdrive.WriteRequest
var ncol int = 0
var cols []xdrive.XCol
var coldesc []xdrive.ColumnDesc
var nextcol int
var wf *os.File
var tmpfn, path string
var csvwriter *csv.Writer

func WriteRequest(req xdrive.WriteRequest) error {
	wreq = req
	ncol = len(wreq.Columndesc)
	cols = make([]xdrive.XCol, ncol)
	coldesc = make([]xdrive.ColumnDesc, ncol)
	nextcol = 0

	//
	// Open destination file for write, we will create a .part file
	// and rename it if succeed, remove if fail.  Another good reason of this
	// renaming is that if there is an external table using a glob like *.csv,
	// we will not read partial files.   Because writable external table
	// will open exactly one file, this trick actually gives a very poor man's
	// transaction (all or nothing) semantics.
	//
	
	path, err := plugin.WritePath(req)
	if err != nil {
		plugin.DbgLogIfErr(err, "write path failed")
		return err
	}

	tmpfn = path + ".part"
	wf, err = os.Create(tmpfn)
	if err != nil {
		plugin.DbgLogIfErr(err, "Cannot open file to write: "+path)
		return fmt.Errorf("Cannot open file to write.")
	}

	csvwriter = csv.NewWriter(wf)

	return nil
}



func DoWriteEnd() error {

	if nextcol == 0 {
		if wf != nil {
			wf.Close()
		}
		plugin.DbgLog("OK.  Close writer, then rename %s -> %s.", tmpfn, path)
		os.Rename(tmpfn, path)
		return nil
	} else {
		if wf != nil {
			wf.Close()
		}
		plugin.DbgLog("Failed.   Close writer, then remove %s.", tmpfn)
		os.Remove(tmpfn)
		return fmt.Errorf("End in the middle of stream")
	}

	return nil
}

// DoWrite services xdrive write request.  It read a sequence of PluginWriteRequest
// from stdin and write to file system.
func DoWrite(col xdrive.XCol) error {
	cols[nextcol] = col
	nextcol++
	if nextcol == ncol {
		err := csvhandler.WritePart(wreq, csvwriter, cols)
		if err != nil {
			return fmt.Errorf("Write Part failed")
		}
		nextcol = 0
	}

	return nil
}
