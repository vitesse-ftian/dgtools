package impl

import (
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
	"vitessedata/plugin"
)

type wreqColMap struct {
	wreq    xdrive.WriteRequest
	dirpath []string
	keys    []string
	vals    []string

	cols   []xdrive.XCol
	colisk []bool
	colpos []int

	nextcol int
}

var ctxt *fdbctxt
var cm wreqColMap

func (cm *wreqColMap) init(req xdrive.WriteRequest) error {
	var err error
	cm.wreq = req
	cm.dirpath, cm.keys, cm.vals, err = decodeReqPath(req.Filespec.Path)
	if err != nil {
		return err
	}
	plugin.DbgLog("Writer: decode req, cm.dirpath %v, keys %v, vals %v.\n", cm.dirpath, cm.keys, cm.vals)

	cm.cols = make([]xdrive.XCol, len(req.Columndesc))
	cm.colisk = make([]bool, len(req.Columndesc))
	cm.colpos = make([]int, len(req.Columndesc))

	for idx, c := range req.Columndesc {
		if c.Name == "__xdr_op" {
			cm.colisk[idx] = false
			cm.colpos[idx] = -1
		} else {
			for idxk, kn := range cm.keys {
				if kn == c.Name {
					cm.colisk[idx] = true
					cm.colpos[idx] = idxk
					break
				}
			}

			for idxv, vn := range cm.vals {
				if vn == c.Name {
					cm.colisk[idx] = false
					cm.colpos[idx] = idxv
					break
				}
			}
		}
	}

	return nil
}

func WriteRequest(req xdrive.WriteRequest) error {
	err := cm.init(req)
	if err != nil {
		return err
	}

	ctxt = opendb(cm.dirpath)
	return nil
}

func DoWriteEnd() error {
	if cm.nextcol != 0 {
		return fmt.Errorf("End in the middle of stream")
	}
	return nil
}

func DoWrite(col xdrive.XCol) error {
	cm.cols[cm.nextcol] = col
	cm.nextcol++
	if cm.nextcol == len(cm.cols) {
		nrow := cm.cols[0].Nrow
		keys := make([][]interface{}, nrow)
		vals := make([][]interface{}, nrow)
		opdel := make([]bool, nrow)
		for r := int32(0); r < nrow; r++ {
			keys[r] = make([]interface{}, len(cm.keys))
			vals[r] = make([]interface{}, len(cm.vals))
			opdel[r] = false
		}

		for c := 0; c < len(cm.cols); c++ {
			if cm.colpos[c] < 0 {
				// __xdr_op
				for r := int32(0); r < nrow; r++ {
					if !cm.cols[c].Nullmap[r] && cm.cols[c].I32Data[r] < 0 {
						opdel[r] = true
					}
				}
				continue
			}

			cpos := cm.colpos[c]

			switch {
			case cm.cols[c].Sdata != nil:
				if cm.colisk[c] {
					for r := int32(0); r < nrow; r++ {
						if cm.cols[c].Nullmap[r] {
							keys[r][cpos] = nil
						} else {
							keys[r][cpos] = cm.cols[c].Sdata[r]
						}
					}
				} else {
					for r := int32(0); r < nrow; r++ {
						if cm.cols[c].Nullmap[r] {
							vals[r][cpos] = nil
						} else {
							vals[r][cpos] = cm.cols[c].Sdata[r]
						}
					}
				}

				// According to fdb doc, TupleElement can be int64, or int.
				// using int is inherently bad idea, and they does not support
				// int32, so, just convert int32 to int64.
			case cm.cols[c].I32Data != nil:
				if cm.colisk[c] {
					for r := int32(0); r < nrow; r++ {
						if cm.cols[c].Nullmap[r] {
							keys[r][cpos] = nil
						} else {
							keys[r][cpos] = int64(cm.cols[c].I32Data[r])
						}
					}
				} else {
					for r := int32(0); r < nrow; r++ {
						if cm.cols[c].Nullmap[r] {
							vals[r][cpos] = nil
						} else {
							vals[r][cpos] = int64(cm.cols[c].I32Data[r])
						}
					}
				}

			case cm.cols[c].I64Data != nil:
				if cm.colisk[c] {
					for r := int32(0); r < nrow; r++ {
						if cm.cols[c].Nullmap[r] {
							keys[r][cpos] = nil
						} else {
							keys[r][cpos] = cm.cols[c].I64Data[r]
						}
					}
				} else {
					for r := int32(0); r < nrow; r++ {
						if cm.cols[c].Nullmap[r] {
							vals[r][cpos] = nil
						} else {
							vals[r][cpos] = cm.cols[c].I64Data[r]
						}
					}
				}

			case cm.cols[c].F32Data != nil:
				if cm.colisk[c] {
					for r := int32(0); r < nrow; r++ {
						if cm.cols[c].Nullmap[r] {
							keys[r][cpos] = nil
						} else {
							keys[r][cpos] = cm.cols[c].F32Data[r]
						}
					}
				} else {
					for r := int32(0); r < nrow; r++ {
						if cm.cols[c].Nullmap[r] {
							vals[r][cpos] = nil
						} else {
							vals[r][cpos] = cm.cols[c].F32Data[r]
						}
					}
				}

			case cm.cols[c].F64Data != nil:
				if cm.colisk[c] {
					for r := int32(0); r < nrow; r++ {
						if cm.cols[c].Nullmap[r] {
							keys[r][cpos] = nil
						} else {
							keys[r][cpos] = cm.cols[c].F64Data[r]
						}
					}
				} else {
					for r := int32(0); r < nrow; r++ {
						if cm.cols[c].Nullmap[r] {
							vals[r][cpos] = nil
						} else {
							vals[r][cpos] = cm.cols[c].F64Data[r]
						}
					}
				}

			default:
				return fmt.Errorf("Rowset with no data")
			}
		}

		_, err := ctxt.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			for r := int32(0); r < nrow; r++ {
				if !opdel[r] {
					plugin.DbgLog("Insert: keys %v, values %v", keys, vals)
					ctxt.ins(tr, buildTuple(keys[r]), buildTuple(vals[r]))
				} else {
					plugin.DbgLog("Del: keys %v.", keys)
					ctxt.del(tr, buildTuple(keys[r]))
				}
			}
			return nil, nil
		})

		if err != nil {
			return fmt.Errorf("Write Part failed")
		}
		cm.nextcol = 0
	}
	return nil
}
