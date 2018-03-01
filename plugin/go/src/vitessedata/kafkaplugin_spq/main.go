/*
XDrive Plugin es_csv customery storage/format interface for XDrive.

A plugin for amazon elastic search, therefore, called es_csv.
*/

package main

import (
	"os"
	"fmt"
	"vitessedata/plugin"
	"vitessedata/kafkaplugin_spq/impl"
)

func main() {

	plugin.StartDbgLogWithPrefix("/tmp/xdrive_kafkaplugin")
	defer plugin.StopDbgLog()

	// check args
	if len(os.Args) != 3 {
		plugin.DbgLog("Wrong arguments.  Usage: kafkaplugin_spq kafkahost zookeeper")
		os.Exit(1)
	}
	
	brokerList := os.Args[1]
	zkhost := os.Args[2]

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

                err = impl.DoRead(rreq, brokerList, zkhost)
	case "sample":
                err := plugin.ReplyOpStatus(-1, "sample not supported", plugin.OPSTATUS_FLAG_XCOL)
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
                err := plugin.ReplyOpStatus(-1, "size_meta not supported", plugin.OPSTATUS_FLAG_XCOL)
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

                err = impl.WriteRequest(wreq, brokerList, zkhost)
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
