/*
XDrive Plugin fsplugin_csv customery storage/format interface for XDrive.

The binary should be called schema_format.  This example implements a fsplugin for
csv format, therefore, the binary is fsplugin_csv.   XDriver server will launch this
program, and send requests to stdin, and read reply from stdout.
*/

package main

import (
	"os"
	"fmt"
	"path/filepath"
	"vitessedata/fsplugin_csv/impl"
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
	if ! filepath.IsAbs(rootpath) {
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
	case "sample":
		err := plugin.ReplyOpStatus(0, "", plugin.OPSTATUS_FLAG_XCOL)
		if err != nil {
			plugin.DbgLogIfErr(err, "write op status failed")
			return
		}

                req, err := plugin.GetSampleRequest()
                if err != nil {
                        plugin.DbgLogIfErr(err, "read request failed")
                        return
                }
		err = impl.DoSample(req)
	case "size_meta":
		err := plugin.ReplyOpStatus(0, "", plugin.OPSTATUS_FLAG_XCOL)
		if err != nil {
			plugin.DbgLogIfErr(err, "write op status failed")
			return
		}

                req, err := plugin.GetSizeMetaRequest()
                if err != nil {
                        plugin.DbgLogIfErr(err, "read request failed")
                        return
                }
		err = impl.DoSizeMeta(req)
	case "write":
		err := plugin.ReplyOpStatus(0, "", plugin.OPSTATUS_FLAG_XCOL)
		if err != nil {
			plugin.DbgLogIfErr(err, "write op status failed")
			return
		}

                wreq, err := plugin.GetWriteRequest()
                if err != nil {
                        plugin.DbgLogIfErr(err, "read request failed")
                        return
                }

		err = impl.WriteRequest(wreq, rootpath)
		if err != nil {
			plugin.DbgLogIfErr(err, "impl write request failed")
			return
		}

		err = plugin.WriteReply(0, "")
                if err != nil {
                        plugin.DbgLogIfErr(err, "write reply failed")
                        return
                }

		done := false
		for ; !done ; {
			col, err := plugin.GetXCol()
			if err != nil {
				plugin.DbgLogIfErr(err, "xcol read failed")
				return
			}

			if (col.GetNrow() == 0) {
				err = impl.DoWriteEnd();
				done = true
			} else {
				err = impl.DoWrite(col)
			}
			
			if (err != nil) {
				plugin.WriteReply(-1, err.Error())

			} else {
				plugin.WriteReply(0, "")
			}
		}
	default:
		err = fmt.Errorf("Bad command from opspec %s", opspec.GetOp())
	}

	plugin.DbgLogIfErr(err, "Error!!!")
}
