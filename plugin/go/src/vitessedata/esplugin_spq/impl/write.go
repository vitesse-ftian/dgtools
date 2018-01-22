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


// DoWrite services xdrive write request.  It read a sequence of PluginWriteRequest
// from stdin and write to file system.
func DoWrite() error {

	var es ESClient
	es.CreateUsingRinfo()

	for {
		var req xdrive.PluginWriteRequest
		plugin.DelimRead(&req)

		if req.Rowset == nil {
			plugin.DbgLog("Done writing")
			plugin.ReplyWriteError(0, "")
			return nil
		}

		ncol := len(req.Rowset.Columns)
		if ncol == 0 {
			plugin.DbgLog("Done writing")
			plugin.ReplyWriteError(0, "")
			return nil
		}
		
		nrow := req.Rowset.Columns[0].Nrow
		coldesc := req.Columndesc

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
				case req.Rowset.Columns[col].Sdata != nil:
					if colname == "_index" || colname == "_type" || colname == "_id" || colname =="_routing" {
						// add to meta 
						if req.Rowset.Columns[col].Nullmap[row] {
							meta[colname] = ""
						} else {
							meta[colname] = req.Rowset.Columns[col].Sdata[row]
						}
					} else {
						if req.Rowset.Columns[col].Nullmap[row] {
							source[colname] = ""
						} else {
							source[colname] = req.Rowset.Columns[col].Sdata[row]
						}
					}
						
				case req.Rowset.Columns[col].I32Data != nil:
					if ! req.Rowset.Columns[col].Nullmap[row] {
						source[colname] = req.Rowset.Columns[col].I32Data[row]
					}
					
				case req.Rowset.Columns[col].I64Data != nil:
					if ! req.Rowset.Columns[col].Nullmap[row] {
						source[colname] = req.Rowset.Columns[col].I64Data[row]
					}
				case req.Rowset.Columns[col].F32Data != nil:
					if ! req.Rowset.Columns[col].Nullmap[row] {
                                                source[colname] = req.Rowset.Columns[col].F32Data[row]
                                        }
				case req.Rowset.Columns[col].F64Data != nil:
					if ! req.Rowset.Columns[col].Nullmap[row] {
						source[colname] = req.Rowset.Columns[col].F64Data[row]
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
			plugin.ReplyWriteError(-2, err.Error())
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
					plugin.ReplyWriteError(-2, err.Error())
					return
				}
				
				if bulkerrors {
					plugin.ReplyWriteError(-100, "Bulk operations has errors. " + string(result))
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
