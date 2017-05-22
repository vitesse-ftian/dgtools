package impl

import (
	"encoding/csv"
	"fmt"
	"os"
	"vitessedata/proto/xdrive"
)

// Reply an error to xdrive server.   ec=0 means OK.
func writeError(ec int32, msg string) {
	var r xdrive.PluginWriteReply
	r.Errcode = ec
	r.Errmsg = msg
	delim_write(&r)
}

// DoWrite services xdrive write request.  It read a sequence of PluginWriteRequest
// from stdin and write to file system.
func DoWrite() error {
	//
	// Open destination file for write, we will create a .part file
	// and rename it if succeed, remove if fail.  Another good reason of this
	// renaming is that if there is an external table using a glob like *.csv,
	// we will not read partial files.   Because writable external table
	// will open exactly one file, this trick actually gives a very poor man's
	// transaction (all or nothing) semantics.
	//
	tmpfn := rinfo.Rpath + ".part"
	wf, err := os.Create(tmpfn)
	if err != nil {
		writeError(-2, "Cannot open file to write: "+rinfo.Rpath)
		return fmt.Errorf("Cannot ope file to write.")
	}

	err = writePart(wf)
	if err == nil {
		// Success!
		DbgLog("OK.  Close writer, then rename %s -> %s.", tmpfn, rinfo.Rpath)
		wf.Close()
		os.Rename(tmpfn, rinfo.Rpath)

		writeError(0, "")
		return nil
	} else {
		DbgLog("Failed.   Close writer, then remove %s.", tmpfn)
		wf.Close()
		os.Remove(tmpfn)

		writeError(-1, err.Error())
		return err
	}
}

func writePart(wf *os.File) error {
	defer wf.Close()
	w := csv.NewWriter(wf)

	for {
		var req xdrive.PluginWriteRequest
		delim_read(&req)

		if req.Rowset == nil {
			DbgLog("Done writing!")
			return nil
		}

		// TODO: Configure csv writer with CSVSpec.
		ncol := len(req.Rowset.Columns)
		nrow := req.Rowset.Columns[0].Nrow
		rec := make([][]string, nrow)

		for row := int32(0); row < nrow; row++ {
			rec[row] = make([]string, ncol)
		}

		for col := 0; col < ncol; col++ {
			switch {
			case req.Rowset.Columns[col].Sdata != nil:
				for row := int32(0); row < nrow; row++ {
					if req.Rowset.Columns[col].Nullmap[row] {
						rec[row][col] = ""
					} else {
						rec[row][col] = req.Rowset.Columns[col].Sdata[row]
					}
				}

			case req.Rowset.Columns[col].I32Data != nil:
				for row := int32(0); row < nrow; row++ {
					if req.Rowset.Columns[col].Nullmap[row] {
						rec[row][col] = ""
					} else {
						rec[row][col] = fmt.Sprintf("%d", req.Rowset.Columns[col].I32Data[row])
					}
				}

			case req.Rowset.Columns[col].I64Data != nil:
				for row := int32(0); row < nrow; row++ {
					if req.Rowset.Columns[col].Nullmap[row] {
						rec[row][col] = ""
					} else {
						rec[row][col] = fmt.Sprintf("%d", req.Rowset.Columns[col].I64Data[row])
					}
				}

			case req.Rowset.Columns[col].F32Data != nil:
				for row := int32(0); row < nrow; row++ {
					if req.Rowset.Columns[col].Nullmap[row] {
						rec[row][col] = ""
					} else {
						rec[row][col] = fmt.Sprintf("%f", req.Rowset.Columns[col].F32Data[row])
					}
				}

			case req.Rowset.Columns[col].F64Data != nil:
				for row := int32(0); row < nrow; row++ {
					if req.Rowset.Columns[col].Nullmap[row] {
						rec[row][col] = ""
					} else {
						rec[row][col] = fmt.Sprintf("%f", req.Rowset.Columns[col].F64Data[row])
					}
				}

			default:
				writeError(-10, "Rowset with no data")
				return fmt.Errorf("Rowset with no data")
			}
		}

		w.WriteAll(rec)
	}

	return nil
}
