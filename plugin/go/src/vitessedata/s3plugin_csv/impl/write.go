package impl

import (
	"io"
	"encoding/csv"
	"fmt"
	"vitessedata/plugin"
	"vitessedata/plugin/csvhandler"
	"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
)


var wreq xdrive.WriteRequest
var ncol int = 0
var cols []xdrive.XCol
var coldesc []xdrive.ColumnDesc
var nextcol int
var wf io.ReadWriteCloser
var fnpath string
var csvwriter *csv.Writer

func WriteRequest(req xdrive.WriteRequest, rootpath, bucket, region string) error {
	wreq = req
	ncol = len(wreq.Columndesc)
	cols = make([]xdrive.XCol, ncol)
        coldesc = make([]xdrive.ColumnDesc, ncol)
        nextcol = 0


// DoWrite services xdrive write request.  It read a sequence of PluginWriteRequest
// from stdin and write to file system.

	path, err := plugin.WritePath(req, rootpath)
	fnpath = path
	if err != nil {
		plugin.DbgLogIfErr(err, "write path failed")
		return err
	}

	var sb S3Bkt
	sb.Connect(region, bucket)

	wf, err = sb.ObjectWriter(fnpath)
	if err != nil {
		return fmt.Errorf("Cannot open file to write: " + fnpath)
	}

	csvwriter = csv.NewWriter(wf)
	return nil
}


func DoWriteEnd() error {
        if nextcol == 0 {
                if wf != nil {
                        wf.Close()
                }
                plugin.DbgLog("OK.  Close writer,%s.", fnpath)
                return nil
	} else {
                if wf != nil {
                        wf.Close()
                }
                plugin.DbgLog("Failed.   Close writer, %s", fnpath)
                return fmt.Errorf("End in the middle of stream")
        }



}

func DoWrite(col xdrive.XCol) error {
        cols[nextcol] = col
        nextcol++
        if nextcol == ncol {
		err := csvhandler.WritePart(wreq, csvwriter, cols)
		if err != nil {
			return err
		}
		nextcol = 0

	} 

	return nil
}
