/*
XDrive Plugin es_csv customery storage/format interface for XDrive.

A plugin for amazon elastic search, therefore, called es_csv.
*/

package main

import (
	"os"
	"fmt"
	"strconv"
	"vitessedata/plugin"
	"vitessedata/esplugin_spq/impl"
)

func main() {

	plugin.StartDbgLogWithPrefix("/tmp/xdrive_esplugin")
	defer plugin.StopDbgLog()

	var es_url, indexname, aws_access_id, aws_access_key string
	var nshards int
	var err error

	// check args
	if len(os.Args) == 4 {
		es_url = os.Args[1]
		indexname = os.Args[2]
		nshards, err = strconv.Atoi(os.Args[3])
		if err != nil {
			plugin.DbgLogIfErr(err, "nshards should be an integer")
			os.Exit(1)
		}
	} else if len(os.Args) == 6 { 
		es_url = os.Args[1]
		indexname = os.Args[2]
		nshards, err = strconv.Atoi(os.Args[3])
		if err != nil {
			plugin.DbgLogIfErr(err, "nshards should be an integer")
			os.Exit(1)
		}
		aws_access_id = os.Args[4]
		aws_access_key = os.Args[5]
	} else {
		plugin.DbgLog("Wrong arguments.  Usage: esplugin_spq es_url index nshards [aws_access_key_id] [aws_secret_access_key]")
		os.Exit(1)
	}
	
	err = plugin.OpenXdriveIO()
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
		
                err = impl.DoRead(rreq, es_url, indexname, nshards, aws_access_id, aws_access_key)
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
                err = impl.DoSample(req, es_url, indexname, nshards, aws_access_id, aws_access_key)
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


                err = impl.WriteRequest(wreq, es_url, indexname, nshards, aws_access_id, aws_access_key)
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
