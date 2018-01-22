package impl

import (
	"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
        "fmt"
        //"io"
        "vitessedata/plugin"
	//"github.com/tsuna/gohbase"                                                                                       
        "github.com/tsuna/gohbase/hrpc"                                                                                  
)

/*
  Hbase table definition matching to DG

  Row []byte, Column []byte, Timestamp uint64, Value []byte

  query format:

  {"COLUMNS" : ["cf:a", "cf:b"], "FILTERS": [{"PrefixFilter": ["row2"]}, {"QualifierFilter" : [">=", "binary:xyz"] } , { "TimestampsFilter": [123, 456]}],
   "LIMIT" : 5, "STARTROW": "row1", "ENDROW": "rowN", "TIMERANGE" : [123, 456]}

"((PrefixFilter("row2") AND (QualifierFilter (>=, 'binary:xyz'))) AND (TimestampsFilter (123, 456))"}

*/

var HBASETABLEDESC = [4]string {"_row", "_column", "_timestamp", "_value"}
const MAXROW = 100

type HBWriter struct {
	fspec *xdrive.FileSpec
	ncol    int
	colid   []int
	typ     []int32
	collist []string
	colmap  map[string]int
	RowCnt  int32
	rowidx  int32
	dataReply xdrive.PluginDataReply
}


func (h *HBWriter) Init(fspec *xdrive.FileSpec, coldesc []*xdrive.ColumnDesc, projcollist []string) {
	h.rowidx = 0
	h.RowCnt = 0
	h.fspec = fspec
	if len(projcollist) == 0 {
		// collist empty means we will proj all.
		h.collist = make([]string, len(coldesc))
		for j, d := range coldesc {
			h.collist[j] = d.Name
		}
	} else {
		h.collist = make([]string, len(projcollist))
		copy(h.collist, projcollist)
	}
	
	h.ncol = len(h.collist)
	h.colid = make([]int, h.ncol)
	h.typ = make([]int32, h.ncol)
	h.colmap = make(map[string]int)

        for i, c := range h.collist {
                for j, d := range coldesc {
                        if c == d.Name {
                                h.colid[i] = j
                                h.typ[i] = d.Type
                                break
                        }
                }
        }

	for i, c := range h.collist {
		h.colmap[c] = i
	}


	h.dataReply.Rowset = new(xdrive.XRowSet)
	h.dataReply.Rowset.Columns = make([]*xdrive.XCol, h.ncol)
	
	for col := 0 ; col < h.ncol ; col++ {
		xcol := new(xdrive.XCol)
		h.dataReply.Rowset.Columns[col] = xcol
		xcol.Colname = h.collist[col]
		xcol.Nrow = int32(MAXROW)
		xcol.Nullmap = make([]bool, xcol.Nrow)

		switch xdrive.SpqType(h.typ[col]) {
		case xdrive.SpqType_BOOL, xdrive.SpqType_INT16, xdrive.SpqType_INT32, xdrive.SpqType_DATE, xdrive.SpqType_TIME_MILLIS:
			plugin.DbgLog("Col %d Building I32Data size %d\n", col, xcol.Nrow)
			xcol.I32Data = make([]int32, xcol.Nrow)
			for i := 0 ; i < int(xcol.Nrow) ; i++ {
				xcol.I32Data[i] = 0
				xcol.Nullmap[i] = true
			}
			
		case xdrive.SpqType_INT64, xdrive.SpqType_TIMESTAMP_MILLIS, xdrive.SpqType_TIME_MICROS, xdrive.SpqType_TIMESTAMP_MICROS:
			plugin.DbgLog("Col %d Buiding I64Data size %d\n", col, xcol.Nrow)
			xcol.I64Data = make([]int64, xcol.Nrow)
			for i := 0 ; i < int(xcol.Nrow) ; i++ {
				xcol.I64Data[i] = 0
				xcol.Nullmap[i] = true
			}
		case xdrive.SpqType_FLOAT:
			plugin.DbgLog("Col %d Buiding F32Data size %d\n", col, xcol.Nrow)
			// These types are encoded as float32 in xcol                                                                                              
			xcol.F32Data = make([]float32, xcol.Nrow)
			for i := 0 ; i < int(xcol.Nrow) ; i++ {
				xcol.F32Data[i] = 0
				xcol.Nullmap[i] = true
			}
		case xdrive.SpqType_DOUBLE:
			plugin.DbgLog("Col %d Buiding F64Data size %d\n", col, xcol.Nrow)
			// These types are encoded as float64 in xcol                                                                                              
			xcol.F64Data = make([]float64, xcol.Nrow)
			for i := 0 ; i < int(xcol.Nrow) ; i++ {
				xcol.F64Data[i] = 0
				xcol.Nullmap[i] = true
			}
		default:
			plugin.DbgLog("Buiding SData size %d\n", xcol.Nrow)
			xcol.Sdata = make([]string, xcol.Nrow)
			for i := 0 ; i < int(xcol.Nrow) ; i++ {
				xcol.Sdata[i] = ""
				xcol.Nullmap[i] = true
			}
		}
			
	}
}

func (h *HBWriter) resetData() {

	h.rowidx = 0
	for col := 0 ; col < h.ncol ; col++ {
		xcol := h.dataReply.Rowset.Columns[col]
		switch xdrive.SpqType(h.typ[col]) {
		case xdrive.SpqType_BOOL, xdrive.SpqType_INT16, xdrive.SpqType_INT32, xdrive.SpqType_DATE, xdrive.SpqType_TIME_MILLIS:
			for i := 0 ; i < int(xcol.Nrow) ; i++ {
				xcol.I32Data[i] = 0
				xcol.Nullmap[i] = true
			}
		case xdrive.SpqType_INT64, xdrive.SpqType_TIMESTAMP_MILLIS, xdrive.SpqType_TIME_MICROS, xdrive.SpqType_TIMESTAMP_MICROS:
                        for i := 0 ; i < int(xcol.Nrow) ; i++ {
                                xcol.I64Data[i] = 0
                                xcol.Nullmap[i] = true
                        }
                case xdrive.SpqType_FLOAT:
                        // These types are encoded as float32 in xcol                                                                                                             
                        for i := 0 ; i < int(xcol.Nrow) ; i++ {
                                xcol.F32Data[i] = 0
                                xcol.Nullmap[i] = true
                        }
                case xdrive.SpqType_DOUBLE:
                        // These types are encoded as float64 in xcol                                                                                                             
                        for i := 0 ; i < int(xcol.Nrow) ; i++ {
                                xcol.F64Data[i] = 0
                                xcol.Nullmap[i] = true
                        }
                default:
                        for i := 0 ; i < int(xcol.Nrow) ; i++ {
                                xcol.Sdata[i] = ""
                                xcol.Nullmap[i] = true
                        }
                }

	}

}


func (h *HBWriter) Write(r *hrpc.Result) {

	for _, c := range r.Cells {



		col, ok := h.colmap["_row"]
		if ok {
			xcol := h.dataReply.Rowset.Columns[col]
			xcol.Sdata[h.rowidx] = string(c.Row)
			xcol.Nullmap[h.rowidx] = false
		}

		col, ok = h.colmap["_column"]
		if ok {
			xcol := h.dataReply.Rowset.Columns[col]
			xcol.Sdata[h.rowidx] = fmt.Sprintf("%s:%s", string(c.Family), string(c.Qualifier))
			xcol.Nullmap[h.rowidx] = false
		}

/*
		col, ok = h.colmap["_qualifier"]
		if ok {
			xcol := h.dataReply.Rowset.Columns[col]
			xcol.Sdata[h.rowidx] = string(c.Qualifier)
			xcol.Nullmap[h.rowidx] = false
		}
*/

		col, ok = h.colmap["_timestamp"]
		if ok {
			xcol := h.dataReply.Rowset.Columns[col]
			xcol.I64Data[h.rowidx] = int64(*c.Timestamp)
			xcol.Nullmap[h.rowidx] = false
		}

		col, ok = h.colmap["_value"]
		if ok {
			xcol := h.dataReply.Rowset.Columns[col]
			xcol.Sdata[h.rowidx] = string(c.Value)
			xcol.Nullmap[h.rowidx] = false
		}

		plugin.DbgLog("%s %s %s %s\n", string(c.Row), string(c.Family),
			string(c.Qualifier), string(c.Value))		
		

		h.rowidx++

		if h.rowidx == MAXROW {
			h.flush()
		}

			
	}


}

func (h *HBWriter) flush() error {
	// write to plugin
	if h.rowidx > 0 && h.rowidx < MAXROW {
		// shrink the result
		for col := 0 ; col < h.ncol ; col++ {
			xcol := h.dataReply.Rowset.Columns[col]
			if h.rowidx < h.dataReply.Rowset.Columns[col].Nrow {
				xcol.Nrow = h.rowidx
				xcol.Nullmap = xcol.Nullmap[:h.rowidx]
				if xcol.Sdata != nil {
					xcol.Sdata = xcol.Sdata[:h.rowidx]
				}
				if xcol.F32Data != nil {
					xcol.F32Data = xcol.F32Data[:h.rowidx]
				}
				if xcol.I32Data != nil {
					xcol.I32Data = xcol.I32Data[:h.rowidx]
				}
				if xcol.F64Data != nil {
					xcol.F64Data = xcol.F64Data[:h.rowidx]
				}
				if xcol.I64Data != nil {
					xcol.I64Data = xcol.I64Data[:h.rowidx]
				}
			}
		}

	}

	
	if h.rowidx > 0 {
		err := plugin.DelimWrite(&h.dataReply)
		if err != nil {
			return err
		}
		
		h.RowCnt += h.rowidx
		// reset the h.rowidx = 0 and do resetData
		h.resetData()
	}

	return nil
}

func (h *HBWriter) Close() error {
	return h.flush()
}
