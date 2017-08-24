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
		[]string{"timed_out"},
                []string{"took"},
                []string{"_shards"},
		[]string{"hits"},
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
		if c == "_id" || c == "_type" || c == "_index" || c == "_score" || c == "_routing" {
			h.eshitpath = append(h.eshitpath, []string{c})
		} else {
			h.eshitpath = append(h.eshitpath, []string{"_source", c})
		}
		plugin.DbgLog("col %s", c)
	}

	plugin.DbgLog("%v", h.eshitpath)
}

func (h* ESReader) process(json []byte) error {

	var rowidx int = 0
	var dataReply xdrive.PluginDataReply

	var keyhandler = func(col int, value []byte, vt jsonparser.ValueType, err error) {

		xcol := dataReply.Rowset.Columns[col]
		xcol.Nullmap[rowidx] = false
		
		switch xdrive.SpqType(h.typ[col]) {
		case xdrive.SpqType_BOOL, xdrive.SpqType_INT16, xdrive.SpqType_INT32, xdrive.SpqType_DATE, xdrive.SpqType_TIME_MILLIS:

			iv, err := strconv.Atoi(string(value))
			if err != nil {
				plugin.ReplyError(-100, "Invalid int data " + string(value))
				return
			}
			xcol.I32Data[rowidx] = int32(iv)


		case xdrive.SpqType_INT64, xdrive.SpqType_TIMESTAMP_MILLIS, xdrive.SpqType_TIME_MICROS, xdrive.SpqType_TIMESTAMP_MICROS:
			iv64, err := strconv.ParseInt(string(value), 0, 64)
			if err != nil {
				plugin.ReplyError(-100, "Invalid int64 data " + string(value))
				return
			}
			xcol.I64Data[rowidx] = iv64

		case xdrive.SpqType_FLOAT:

			fv, err := strconv.ParseFloat(string(value), 32)
			if err != nil {
				plugin.ReplyError(-100, "Invliad float data " + string(value))
				return
			}
			xcol.F32Data[rowidx] = float32(fv)

		case xdrive.SpqType_DOUBLE:
			
			xcol.F64Data[rowidx], err = strconv.ParseFloat(string(value), 64)
			if err != nil {
				plugin.ReplyError(-100, "Invalid float64 data " + string(value))
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
		
		// hits
		if idx == 3 {
			total, err2 := jsonparser.GetInt(value, "total") 
			if err2 != nil {
				plugin.DbgLog("ES: Failed to get total count. %v", err2)
				return
			}

			plugin.DbgLog("ES: Total number of row = %d", total)
			// initialize the rows
			h.RowCnt = int(total)

			// Build reply message. Errcode initialized to 0, which is what we want.
			dataReply.Rowset = new(xdrive.XRowSet)
			dataReply.Rowset.Columns = make([]*xdrive.XCol, h.ncol)

			for col := 0 ; col < h.ncol ; col++ {
				xcol := new(xdrive.XCol)
				dataReply.Rowset.Columns[col] = xcol
				xcol.Colname = h.collist[col]
				xcol.Nrow = int32(total)
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
			jsonparser.ArrayEach(value, arrhandler, "hits")
		}
	}, h.esresultpath...)



	plugin.DbgLog("Done Building Rowset, %d rows, %d cols", rowidx, h.ncol)
        err := plugin.DelimWrite(&dataReply)
        return err
}
