package plugin

import (
	"fmt"
	"github.com/satori/go.uuid"
	"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
	"strconv"
	"strings"
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

func WritePath() (string, error) {
	str := rinfo.Rpath
	str = strings.Replace(str, "#SEGCOUNT#", strconv.Itoa(int(rinfo.FragCnt)), -1)
	path := strings.Replace(str, "#SEGID#", strconv.Itoa(int(rinfo.FragId)), -1)
	path = strings.Replace(path, "#UUID#", fmt.Sprintf("%s", uuid.NewV4()), -1)

	if path == str {
		return path, fmt.Errorf("No #SEGID# or #UUID# substitution in write request.")
	}
	return path, nil
}
