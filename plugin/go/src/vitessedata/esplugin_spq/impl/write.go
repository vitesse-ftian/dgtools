package impl

import (
        "encoding/json"
        "fmt"
        "github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
        //"io"
        "vitessedata/plugin"
	"bytes"
	"github.com/buger/jsonparser"
)

var wreq xdrive.WriteRequest
var ncol int = 0
var cols []xdrive.XCol
var nextcol int
var es ESClient

// DoWrite services xdrive write request.  It read a sequence of PluginWriteRequest
// from stdin and write to file system.
func WriteRequest(req xdrive.WriteRequest, es_url, indexname string, nshards int, aws_access_id, aws_access_key string) error {

	wreq = req
	ncol = len(wreq.Columndesc)
	cols = make([]xdrive.XCol, ncol)
	nextcol = 0
	es.Init(es_url, indexname, nshards, aws_access_id, aws_access_key)

	return nil
}

func DoWriteEnd() error {
	if nextcol == 0 {
		return nil
	} else {
		return fmt.Errorf("End in the middle of stream")
	}
	return nil
}


func DoWrite(col xdrive.XCol) error {

	cols[nextcol] = col
	nextcol++
	if nextcol == ncol {
		
		nrow := cols[0].Nrow
		coldesc := wreq.Columndesc

		plugin.DbgLog("nrow = %d", nrow)
		var buf bytes.Buffer

		for row := int32(0) ; row < nrow ; row++ {

			action := make(map[string]interface{})
			meta := make(map[string]interface{})
			//meta["_index"] = es.Index   // if index is provided by URI, no need to add this
			action["index"] = meta
			source := make(map[string]interface{})
			
			for col := 0; col < ncol ; col++ {
				colname := coldesc[col].Name

				switch {
				case cols[col].Sdata != nil:
					if colname == "_index" || colname == "_type" || colname == "_id" || colname =="_routing" {
						// add to meta 
						if cols[col].Nullmap[row] {
							meta[colname] = ""
						} else {
							meta[colname] = cols[col].Sdata[row]
						}
					} else {
						if cols[col].Nullmap[row] {
							source[colname] = ""
						} else {
							source[colname] = cols[col].Sdata[row]
						}
					}
						
				case cols[col].I32Data != nil:
					if ! cols[col].Nullmap[row] {
						source[colname] = cols[col].I32Data[row]
					}
					
				case cols[col].I64Data != nil:
					if ! cols[col].Nullmap[row] {
						source[colname] = cols[col].I64Data[row]
					}
				case cols[col].F32Data != nil:
					if ! cols[col].Nullmap[row] {
                                                source[colname] = cols[col].F32Data[row]
                                        }
				case cols[col].F64Data != nil:
					if ! cols[col].Nullmap[row] {
						source[colname] = cols[col].F64Data[row]
                                        }
				default:
					return fmt.Errorf("rowset with no data")
				}
			}

			a, _ := json.Marshal(action)
			s, _ := json.Marshal(source)
			
			buf.Write(a)
			buf.Write([]byte("\n"))
			buf.Write(s)
			buf.Write([]byte("\n"))
		}

		buf.Write([]byte("\n"))

		plugin.DbgLog(buf.String())

		// bulk write to elastic search

		result, err := es.Bulk(es.Index, "", &buf)
		plugin.DbgLog(string(result))
		if err != nil {
			return err
		}

		var esbulkrespath = [][]string {
			[]string{TookField},
			[]string{ErrorsField},
			[]string{ItemsField},
		}

		bulkerrors := false

		jsonparser.EachKey(result, func(idx int, value []byte, vt jsonparser.ValueType, err error) {

			if idx == 0 {
				// took

			} else if idx == 1 {
				// errors
				var err error

				bulkerrors, err = jsonparser.ParseBoolean(value)
				if err != nil {
					plugin.DbgLog("Parse _bulk result errors failed. %v", err)
					return
				}
				
				if bulkerrors {
					plugin.DbgLog("Bulk operations has errors. " + string(result))
					return
				}
			} else if idx == 2 {
				// items

			}
			
		}, esbulkrespath...)

		if bulkerrors {
			return fmt.Errorf("bulk operation error")
		}

	}

	return nil

}
