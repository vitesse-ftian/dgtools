/*
XDrive Plugin hbaseplugin_spq for XDrive2.

*/

package main

import (
	"os"
	"fmt"
	"vitessedata/plugin"
	"vitessedata/hbaseplugin_spq/impl"
)

func main() {

	plugin.StartDbgLogWithPrefix("/tmp/xdrive_hbaseplugin")
	defer plugin.StopDbgLog()

	// check args
	if len(os.Args) != 5 {
		plugin.DbgLog("Wrong arguments.  Usage: hbaseplugin_spq hbasehost user field_separator token_separator")
		os.Exit(1)
	}
	
	hbasehost := os.Args[1]
	user := os.Args[2]
	field_separator := os.Args[3]
	token_separator := os.Args[4]

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
	plugin.DbgLog("Starting read op spec ...\n")
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

                err = impl.DoRead(rreq, hbasehost, user, field_separator, token_separator)
	case "sample":
                err := plugin.ReplyOpStatus(-1, "sample not supported", plugin.OPSTATUS_FLAG_XCOL)
                if err != nil {
                        plugin.DbgLogIfErr(err, "write op status failed")
                        return
                }
	case "size_meta":
                err := plugin.ReplyOpStatus(-1, "size_meta not supported", plugin.OPSTATUS_FLAG_XCOL)
                if err != nil {
                        plugin.DbgLogIfErr(err, "write op status failed")
                        return
                }
	case "write":
                err := plugin.ReplyOpStatus(-1, "write not supported", plugin.OPSTATUS_FLAG_XCOL)
                if err != nil {
                        plugin.DbgLogIfErr(err, "write op status failed")
                        return
                }

	default:
		err = fmt.Errorf("Bad command from opspec %s", opspec.GetOp())
	}

	plugin.DbgLogIfErr(err, "Error!!!")
}
