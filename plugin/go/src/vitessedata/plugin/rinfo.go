package plugin

import (
	"vitessedata/proto/xdrive"
)

var rinfo xdrive.RmgrInfo

// Reply an error to xdrive server.   ec=0 means OK.
func ReplyError(ec int32, msg string) {
	var r xdrive.PluginDataReply
	r.Errcode = ec
	r.Errmsg = msg
	DelimWrite(&r)
}

// Reply an error to xdrive server.   ec=0 means OK.
func ReplyWriteError(ec int32, msg string) {
	var r xdrive.PluginWriteReply
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
	return nil
}

func PluginOp() string {
	return rinfo.Pluginop
}

func RInfo() *xdrive.RmgrInfo {
	return &rinfo
}
