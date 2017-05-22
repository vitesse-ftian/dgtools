/*
XDrive Plugin implements customery storage/format interface for XDrive.

The binary should be called schema_format.  This example implements a fsplugin for
csv format, therefore, the binary is fsplugin_csv.   XDriver server will launch this
program, and send requests to stdin, and read reply from stdout.
*/

package main

import (
	"fmt"
	"vitessedata/fsplugin_csv/xdrive"
)

func main() {
	xdrive.StartDbgLog()
	defer xdrive.StopDbgLog()

	// The first message from xdrive will always be an RmgrInfo.  Scheme can pass configurations
	// to plugin via RmgrInfo.Conf, which reads from xdrive.toml file.
	xdrive.DbgLog("Starting read rinfo ...\n")
	err := xdrive.ReadRInfo()
	xdrive.DbgLogIfErr(err, "Cannot read rinfo message from server.")
	xdrive.DbgLog("Serving %s\n", xdrive.PluginOp())

	switch xdrive.PluginOp() {
	case "read":
		err = xdrive.DoRead()
	case "sample":
		err = xdrive.DoSample()
	case "size_meta":
		err = xdrive.DoSizeMeta()
	case "write":
		err = xdrive.DoWrite()
	default:
		err = fmt.Errorf("Bad command from rinfo %s", xdrive.PluginOp())
	}

	xdrive.DbgLogIfErr(err, "Error!!!")
}
