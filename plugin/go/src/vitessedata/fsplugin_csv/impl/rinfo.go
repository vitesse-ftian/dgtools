package impl

import (
	"fmt"
	"vitessedata/proto/xdrive"
)

var rinfo xdrive.RmgrInfo

func ReadRInfo() error {
	err := delim_read(&rinfo)
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
		readError(-1, "rmgr info invalid")
		return fmt.Errorf("Invalid rmgr")
	}
	return nil
}

func PluginOp() string {
	return rinfo.Pluginop
}
