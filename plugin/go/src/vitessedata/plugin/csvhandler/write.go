package csvhandler

import (
	"encoding/csv"
	"fmt"
	"io"
	"vitessedata/plugin"
	"vitessedata/proto/xdrive"
)

func WritePart(wf io.WriteCloser) error {
	defer wf.Close()
	w := csv.NewWriter(wf)

	for {
		var req xdrive.PluginWriteRequest
		plugin.DelimRead(&req)

		if req.Rowset == nil {
			plugin.DbgLog("Done writing!")
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
				return fmt.Errorf("Rowset with no data")
			}
		}

		w.WriteAll(rec)
	}

	return nil
}
