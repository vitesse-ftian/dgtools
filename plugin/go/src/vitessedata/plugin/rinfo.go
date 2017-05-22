package plugin

import (
	"fmt"
	"vitessedata/proto/xdrive"
)

var rinfo xdrive.RmgrInfo

// Reply an error to xdrive server.   ec=0 means OK.
func replyError(ec int32, msg string) {
	var r xdrive.PluginDataReply
	r.Errcode = ec
	r.Errmsg = msg
	DelimWrite(&r)
}

func ReadRInfo() error {
	err := DelimRead(&rinfo)
	if err != nil {
		return err
	}

	DbgLog("Rinfo: %v\n", rinfo)
	//
	// Check plugin type and format.  Strictly speaking this is not necessary
	// because xdriver server promises valid values.
	//
	if rinfo.Scheme != "fsplugin" || rinfo.Format != "csv" {
		DbgLog("Invalid Rinfo %v\n", rinfo)
		replyError(-1, "rmgr info invalid")
		return fmt.Errorf("Invalid rmgr")
	}
	return nil
}

func PluginOp() string {
	return rinfo.Pluginop
}

func RInfo() *xdrive.RmgrInfo {
	return &rinfo
}
