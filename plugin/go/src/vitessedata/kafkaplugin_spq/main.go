/*
XDrive Plugin es_csv customery storage/format interface for XDrive.

A plugin for amazon elastic search, therefore, called es_csv.
*/

package main

import (
	"fmt"
	"vitessedata/plugin"
	"vitessedata/kafkaplugin_csv/impl"
)

func main() {

	plugin.StartDbgLogWithPrefix("/tmp/xdrive_kafkaplugin")
	defer plugin.StopDbgLog()

	// The first message from xdrive will always be an RmgrInfo.  Scheme can pass configurations
	// to plugin via RmgrInfo.Conf, which reads from xdrive.toml file.
	plugin.DbgLog("Starting read rinfo ...\n")
	err := plugin.ReadRInfo()
	plugin.DbgLogIfErr(err, "Cannot read rinfo message from server.")
	plugin.DbgLog("Serving %s\n", plugin.PluginOp())

	switch plugin.PluginOp() {
	case "read":
		err = impl.DoRead()
	case "sample":
		err = impl.DoSample()
	case "size_meta":
		err = impl.DoSizeMeta()
	case "write":
		err = impl.DoWrite()
	default:
		err = fmt.Errorf("Bad command from rinfo %s", plugin.PluginOp())
	}

	plugin.DbgLogIfErr(err, "Error!!!")
}
