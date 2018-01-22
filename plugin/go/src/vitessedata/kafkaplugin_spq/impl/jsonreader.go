package impl

import (
        "github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
        "github.com/buger/jsonparser"
        "vitessedata/plugin"
//      "io"
        "strconv"
)

type JsonReader struct {
	fspec *xdrive.FileSpec
	ncol int
	colid []int
	typ   []int32
	collist []string
	RowCnt int

	jsonpath [][]string
}

func (js *JsonReader) Init(fspec *xdrive.FileSpec, coldesc[]*xdrive.ColumnDesc, projcollist []string) {

	js.RowCnt = 0
	js.fspec = fspec
	if len(projcollist) == 0 {
		// collist empty means we will proj all.
		js.collist = make([]string, len(coldesc))
		for j, d := range coldesc {
			js.collist[j] = d.Name
		}
	} else {
		js.collist = make([]string, len(projcollist))
		copy(js.collist, projcollist)
	}

	js.ncol = len(js.collist)
	js.colid = make([]int, js.ncol)
	js.typ = make([]int32, js.ncol)

	for i, c := range js.collist {
		for j, d := range coldesc {
			if c == d.Name {
				js.colid[i] = j
				js.typ[i] = d.Type
				break
			}
		}
	}

	for _, c := range js.collist {
		js.jsonpath = append(js.jsonpath, []string{c})
	}

}

func (js *JsonReader) processAll(records [][]byte) error {
	
	var rowidx int32 = 0
	var dataReply xdrive.PluginDataReply

	var keyhandler = func(col int, value []byte, vt jsonparser.ValueType, err error) {
                xcol := dataReply.Rowset.Columns[col]
                xcol.Nullmap[rowidx] = false
                
                switch xdrive.SpqType(js.typ[col]) {
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

	// build reply message
	js.RowCnt = len(records)
	dataReply.Rowset = new(xdrive.XRowSet)
	dataReply.Rowset.Columns = make([]*xdrive.XCol, js.ncol)
	
	for col := 0 ; col < js.ncol ; col++ {
		xcol := new(xdrive.XCol)
		dataReply.Rowset.Columns[col] = xcol
		xcol.Colname = js.collist[col]
		xcol.Nrow = int32(js.RowCnt)
		xcol.Nullmap = make([]bool, xcol.Nrow)


		switch xdrive.SpqType(js.typ[col]) {
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
		
	for _, r := range records {
		jsonparser.EachKey(r, keyhandler, js.jsonpath...)
		rowidx++
	}


	err := plugin.DelimWrite(&dataReply)
	return err
}
