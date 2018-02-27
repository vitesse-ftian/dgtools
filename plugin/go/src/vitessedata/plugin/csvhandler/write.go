package csvhandler

import (
	"encoding/csv"
	"fmt"
	"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
	"vitessedata/plugin"
)


func WritePart(req xdrive.WriteRequest, w *csv.Writer, Columns []xdrive.XCol) error {

	// TODO: Configure csv writer with CSVSpec.
	ncol := len(Columns)
	if ncol == 0 {
		plugin.DbgLog("Done writing!")
		return nil
	}
	
	nrow := Columns[0].Nrow
	rec := make([][]string, nrow)
	
	for row := int32(0); row < nrow; row++ {
		rec[row] = make([]string, ncol)
	}
	
	for col := 0; col < ncol; col++ {
		switch {
		case Columns[col].Sdata != nil:
			for row := int32(0); row < nrow; row++ {
				if Columns[col].Nullmap[row] {
					rec[row][col] = ""
				} else {
					rec[row][col] = Columns[col].Sdata[row]
				}
			}

		case Columns[col].I32Data != nil:
			for row := int32(0); row < nrow; row++ {
				if Columns[col].Nullmap[row] {
					rec[row][col] = ""
				} else {
					rec[row][col] = fmt.Sprintf("%d", Columns[col].I32Data[row])
				}
			}

		case Columns[col].I64Data != nil:
			for row := int32(0); row < nrow; row++ {
				if Columns[col].Nullmap[row] {
					rec[row][col] = ""
				} else {
					rec[row][col] = fmt.Sprintf("%d", Columns[col].I64Data[row])
				}
			}

		case Columns[col].F32Data != nil:
			for row := int32(0); row < nrow; row++ {
				if Columns[col].Nullmap[row] {
					rec[row][col] = ""
				} else {
					rec[row][col] = fmt.Sprintf("%f", Columns[col].F32Data[row])
				}
			}
			
		case Columns[col].F64Data != nil:
			for row := int32(0); row < nrow; row++ {
				if Columns[col].Nullmap[row] {
					rec[row][col] = ""
				} else {
					rec[row][col] = fmt.Sprintf("%f", Columns[col].F64Data[row])
				}
			}
			
		default:
			return fmt.Errorf("Rowset with no data")
		}
	}
	
	w.WriteAll(rec)
	return nil
}
