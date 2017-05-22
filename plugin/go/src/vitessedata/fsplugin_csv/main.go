/*
XDrive Plugin implements customery storage/format interface for XDrive.

The binary should be called schema_format.  This example implements a fsplugin for
csv format, therefore, the binary is fsplugin_csv.   XDriver server will launch this
program, and send requests to stdin, and read reply from stdout.
*/

package main

import (
	"fmt"
	"vitessedata/fsplugin_csv/impl"
)

func main() {
	impl.StartDbgLog()
	defer impl.StopDbgLog()

	// The first message from xdrive will always be an RmgrInfo.  Scheme can pass configurations
	// to plugin via RmgrInfo.Conf, which reads from xdrive.toml file.
	impl.DbgLog("Starting read rinfo ...\n")
	err := impl.ReadRInfo()
	impl.DbgLogIfErr(err, "Cannot read rinfo message from server.")
	impl.DbgLog("Serving %s\n", impl.PluginOp())

	switch impl.PluginOp() {
	case "read":
		err = impl.DoRead()
	case "sample":
		err = impl.DoSample()
	case "size_meta":
		err = impl.DoSizeMeta()
	case "write":
		err = impl.DoWrite()
	default:
		err = fmt.Errorf("Bad command from rinfo %s", impl.PluginOp())
	}

	impl.DbgLogIfErr(err, "Error!!!")
}
