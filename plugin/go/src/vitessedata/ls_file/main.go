/*
XDrive Plugin fsplugin_csv customery storage/format interface for XDrive.

List a dir recursively and return all path, file name, and content (base64 encoded.)
*/

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"vitessedata/ls_file/impl"
	"vitessedata/plugin"
)

func main() {
	plugin.StartDbgLog()
	defer plugin.StopDbgLog()

	// check argc
	if len(os.Args) != 2 {
		plugin.DbgLog("Wrong arguments.  Usage: fsplugin_csv rootpath")
		os.Exit(1)
	}

	rootpath := os.Args[1]
	plugin.DbgLog("rootpath = %s", rootpath)
	if !filepath.IsAbs(rootpath) {
		plugin.DbgLog("rootpath must be an absolute path %s", rootpath)
		os.Exit(1)
	}

	err := plugin.OpenXdriveIO()
	if err != nil {
		plugin.DbgLogIfErr(err, "open xdrive IO failed")
		os.Exit(1)
	}

	opspec, err := plugin.GetOpSpec()
	if err != nil {
		plugin.DbgLogIfErr(err, "read request failed.")
		os.Exit(1)
	}

	// The first message from xdrive will always be an RmgrInfo.  Scheme can pass configurations
	// to plugin via RmgrInfo.Conf, which reads from xdrive.toml file.
	plugin.DbgLog("Starting read op spec...\n")
	switch opspec.GetOp() {
	case "read":
		// OpStatus set flag to 1 to enable XCol protocol
		err := plugin.ReplyOpStatus(0, "", plugin.OPSTATUS_FLAG_XCOL)
		if err != nil {
			plugin.DbgLogIfErr(err, "write op status failed")
			return
		}

		rreq, err := plugin.GetReadRequest()
		if err != nil {
			plugin.DbgLogIfErr(err, "read request failed")
			return
		}

		err = impl.DoRead(rreq, rootpath)
		if err == nil {
			plugin.DbgLog("Done reading!\n")
		}
	default:
		err = fmt.Errorf("Bad command from opspec %s", opspec.GetOp())
	}

	plugin.DbgLogIfErr(err, "Error!!!")
}
