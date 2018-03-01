/*
XDrive Plugin s3plugin_csv customery storage/format interface for XDrive.
*/

package main

import (
	"os"
	"fmt"
	"path/filepath"
	"vitessedata/s3plugin_csv/impl"
	"vitessedata/plugin"
)

func main() {
	plugin.StartDbgLog()
	defer plugin.StopDbgLog()

	// check argc
	if len(os.Args) != 4 {
		plugin.DbgLog("Wrong arguments.  Usage: s3plugin_csv rootpath bucket region")
		os.Exit(1)
	}
	rootpath := os.Args[1]
	bucket := os.Args[2]
	region := os.Args[3]

	plugin.DbgLog("rootpath = %s", rootpath)
	plugin.DbgLog("bucket = %s", bucket)
	plugin.DbgLog("region = %s", region)

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
		
		err = impl.DoRead(rreq, rootpath, bucket, region)
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
		err = impl.DoSample(req, rootpath, bucket, region)
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
		err = impl.DoSizeMeta(req, rootpath, bucket, region)
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

		err = impl.WriteRequest(wreq, rootpath, bucket, region)
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
