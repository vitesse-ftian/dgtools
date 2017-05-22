package impl

import (
	"fmt"
	"vitessedata/plugin"
	"vitessedata/plugin/csvhandler"
)

// DoWrite services xdrive write request.  It read a sequence of PluginWriteRequest
// from stdin and write to file system.
func DoWrite() error {
	rinfo := plugin.RInfo()
	var sb S3Bkt
	sb.ConnectUsingRInfo()

	wf, err := sb.ObjectWriter(rinfo.Rpath)
	if err != nil {
		plugin.ReplyWriteError(-2, "Cannot open file to write: "+rinfo.Rpath)
		return fmt.Errorf("Cannot open file to write.")
	}

	err = csvhandler.WritePart(wf)
	if err == nil {
		plugin.ReplyWriteError(0, "")
		return nil
	} else {
		plugin.ReplyWriteError(-1, err.Error())
		return err
	}
}
