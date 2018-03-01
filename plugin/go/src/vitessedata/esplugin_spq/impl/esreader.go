package impl

import (
	"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
	"github.com/buger/jsonparser"
	"vitessedata/plugin"
//	"io"
	"strconv"
)

type ESReader struct {
	fspec *xdrive.FileSpec

	ncol    int
	colid   []int
	typ     []int32
	collist []string

	RowCnt  int

	esresultpath [][]string
	eshitpath [][]string
}

func (h *ESReader) Init(fspec *xdrive.FileSpec, coldesc []*xdrive.ColumnDesc, projcollist []string) {
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

	for i, c := range h.collist {
		for j, d := range coldesc {
			if c == d.Name {
				h.colid[i] = j
				h.typ[i] = d.Type
				break
			}
		}
	}

	// initialize es path
	h.esresultpath =[][]string {
		[]string{TimedOutField},
                []string{TookField},
                []string{ShardsField},
		[]string{HitsField},
        }
/*
	h.eshitpath = [][]string {
		[]string{"_index"},
		[]string{"_type"},
		[]string{"_id"},
		[]string{"_score"},
	}
*/

	for _, c := range h.collist {
		if c == IdField || c == IndexField || c == TypeField || c == ScoreField || c == RoutingField {
			h.eshitpath = append(h.eshitpath, []string{c})
		} else {
			h.eshitpath = append(h.eshitpath, []string{SourceField, c})
		}
		plugin.DbgLog("col %s", c)
	}

	plugin.DbgLog("%v", h.eshitpath)
}

func (h* ESReader) process(json []byte, default_size int) error {

	var rowidx int32 = 0
	var coldatareply []xdrive.XColDataReply

	var keyhandler = func(col int, value []byte, vt jsonparser.ValueType, err error) {

		xcol := coldatareply[col].Data
		xcol.Nullmap[rowidx] = false
		
		switch xdrive.SpqType(h.typ[col]) {
		case xdrive.SpqType_BOOL, xdrive.SpqType_INT16, xdrive.SpqType_INT32, xdrive.SpqType_DATE, xdrive.SpqType_TIME_MILLIS:

			iv, err := strconv.Atoi(string(value))
			if err != nil {
				plugin.DataReply(-100, "Invalid int data " + string(value))
				return
			}
			xcol.I32Data[rowidx] = int32(iv)


		case xdrive.SpqType_INT64, xdrive.SpqType_TIMESTAMP_MILLIS, xdrive.SpqType_TIME_MICROS, xdrive.SpqType_TIMESTAMP_MICROS:
			iv64, err := strconv.ParseInt(string(value), 0, 64)
			if err != nil {
				plugin.DataReply(-100, "Invalid int64 data " + string(value))
				return
			}
			xcol.I64Data[rowidx] = iv64

		case xdrive.SpqType_FLOAT:

			fv, err := strconv.ParseFloat(string(value), 32)
			if err != nil {
				plugin.DataReply(-100, "Invliad float data " + string(value))
				return
			}
			xcol.F32Data[rowidx] = float32(fv)

		case xdrive.SpqType_DOUBLE:
			
			xcol.F64Data[rowidx], err = strconv.ParseFloat(string(value), 64)
			if err != nil {
				plugin.DataReply(-100, "Invalid float64 data " + string(value))
				return
			}
			
		default:
			xcol.Sdata[rowidx] = string(value)

		}
			
	}

	var arrhandler = func(value[] byte, dataType jsonparser.ValueType, offset int, err error) {
		jsonparser.EachKey(value, keyhandler, h.eshitpath...)
		rowidx++
	}

	jsonparser.EachKey(json, func(idx int, value []byte, vt jsonparser.ValueType, err error) {

		if idx == 0 {
			// timed_out
			timed_out, err2 := jsonparser.ParseBoolean(value)

			if err2 != nil {
				plugin.DbgLog("ES: Failed to get the timed_out. %v", err2)
				return
			}
			if timed_out {
				plugin.DataReply(-100, "Connection timed out")
				return
			}
		} else if idx == 1 {
			// took

		} else if idx == 2 {
			// _shards
		
		} else if idx == 3 {
			// hits
			
			total, err2 := jsonparser.GetInt(value, TotalField) 
			if err2 != nil {
				plugin.DbgLog("ES: Failed to get total count. %v", err2)
				return
			}

			plugin.DbgLog("ES: Total number of row = %d, size = %d", total, default_size)
			// initialize the rows
			if default_size < int(total) {
				h.RowCnt = default_size
			} else {
				h.RowCnt = int(total)
			}

			// Build reply message. Errcode initialized to 0, which is what we want.
			coldatareply = make([]xdrive.XColDataReply, h.ncol)

			for col := 0 ; col < h.ncol ; col++ {
				xcol := new(xdrive.XCol)
				coldatareply[col].Data = xcol
				xcol.Colname = h.collist[col]
				xcol.Nrow = int32(h.RowCnt)
				xcol.Nullmap = make([]bool, xcol.Nrow)


				switch xdrive.SpqType(h.typ[col]) {
				case xdrive.SpqType_BOOL, xdrive.SpqType_INT16, xdrive.SpqType_INT32, xdrive.SpqType_DATE, xdrive.SpqType_TIME_MILLIS:
					plugin.DbgLog("Col %d Buiding I32Data size %d\n", col, xcol.Nrow)
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
			

			rowidx = 0
			// parse the hits array
			jsonparser.ArrayEach(value, arrhandler, HitsField)
		}
	}, h.esresultpath...)

	// total may not be equal to numbe of row if param size is specified in the URI request
	for col := 0 ; col < h.ncol ; col++ {
		xcol := coldatareply[col].Data
		if rowidx < xcol.Nrow {
			xcol.Nrow = rowidx
			xcol.Nullmap = xcol.Nullmap[:rowidx]
			if xcol.Sdata != nil {
				xcol.Sdata = xcol.Sdata[:rowidx]
			} 
			if xcol.F32Data != nil {
				xcol.F32Data = xcol.F32Data[:rowidx]
			}
			if xcol.I32Data != nil {
				xcol.I32Data = xcol.I32Data[:rowidx]
			}
			if xcol.F64Data != nil {
				xcol.F64Data = xcol.F64Data[:rowidx]
			}
			if xcol.I64Data != nil {
				xcol.I64Data = xcol.I64Data[:rowidx]
			}
		}
		err := plugin.ReplyXColData(coldatareply[col])
		if err != nil {
			plugin.DbgLogIfErr(err, "write data column failed")
			return err
		}
	}

	plugin.DbgLog("Done Building Rowset, %d rows, %d cols", rowidx, h.ncol)
        return nil
}
