package impl

import (
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
	"strconv"
	"vitessedata/plugin"
)

type rreqColMap struct {
	rreq    xdrive.ReadRequest
	dirpath []string
	keys    []string
	keyt    []int32
	vals    []string
	valt    []int32

	proj   []string
	colisk []bool
	colpos []int

	kta, ktz tuple.Tuple
}

func args2ifv(t int32, s string) (interface{}, error) {
	switch xdrive.SpqType(t) {
	case xdrive.SpqType_BOOL, xdrive.SpqType_INT16, xdrive.SpqType_INT32,
		xdrive.SpqType_DATE, xdrive.SpqType_TIME_MILLIS,
		xdrive.SpqType_INT64,
		xdrive.SpqType_TIME_MICROS,
		xdrive.SpqType_TIMESTAMP_MILLIS, xdrive.SpqType_TIMESTAMP_MICROS,
		xdrive.SpqType_TIMESTAMPTZ_MILLIS, xdrive.SpqType_TIMESTAMPTZ_MICROS:
		iv, err := strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
		return iv, nil

	case xdrive.SpqType_FLOAT:
		fv, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return nil, err
		}
		return float32(fv), nil

	case xdrive.SpqType_DOUBLE:
		fv, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, err
		}
		return fv, nil

	default:
		return s, nil
	}
}

func (cm *rreqColMap) init(req xdrive.ReadRequest) error {
	var err error

	if req.FragCnt <= 0 || req.FragId < 0 || req.FragId >= req.FragCnt {
		return fmt.Errorf("Invalid read request")
	}

	cm.rreq = req
	cm.dirpath, cm.keys, cm.vals, err = decodeReqPath(req.Filespec.Path)
	if err != nil {
		return err
	}

	cm.keyt = make([]int32, len(cm.keys))
	cm.valt = make([]int32, len(cm.vals))
	for idx, k := range cm.keys {
		for _, cd := range req.Columndesc {
			if k == cd.Name {
				cm.keyt[idx] = cd.Type
				break
			}
		}
	}

	for idx, v := range cm.vals {
		for _, cd := range req.Columndesc {
			if v == cd.Name {
				cm.valt[idx] = cd.Type
				break
			}
		}
	}

	if len(req.Columnlist) == 0 {
		// empty proj list, means we will proj all according to columndesec.
		cm.proj = make([]string, len(req.Columndesc))
		for j, d := range req.Columndesc {
			cm.proj[j] = d.Name
		}
	} else {
		cm.proj = req.Columnlist
	}

	// plugin.DbgLog("FDB tup desc: key cols %v, val cols %v.", cm.keys, cm.vals)
	plugin.DbgLog("Read: will project %v.", cm.proj)

	cm.colisk = make([]bool, len(cm.proj))
	cm.colpos = make([]int, len(cm.proj))

	for idx, c := range cm.proj {
		found := false
		for idxk, kn := range cm.keys {
			if kn == c {
				cm.colisk[idx] = true
				cm.colpos[idx] = idxk
				found = true
				break
			}
		}

		for idxv, vn := range cm.vals {
			if vn == c {
				cm.colisk[idx] = false
				cm.colpos[idx] = idxv
				found = true
				break
			}
		}

		if !found {
			plugin.DbgLog("Column %s cannot be found in fdb tuple desc!", c)
			return fmt.Errorf("Column %s cannot be found.", c)
		}
	}

	// build filter, we only handle = that can form a key prefix.
	for kidx, kcol := range cm.keys {
		found := ""
		for _, f := range req.Filter {
			if f.Column == kcol {
				ifv, err := args2ifv(cm.keyt[kidx], f.Args[0])
				if err != nil {
					return err
				}

				if f.Op == "=" || f.Op == "==" {
					if found == "" {
						found = ""
						cm.kta = append(cm.kta, ifv)
						cm.ktz = append(cm.ktz, ifv)
						break
					}
				}

				if f.Op == "<" || f.Op == "<=" {
					if found == "" {
						found = "<"
					} else if found == ">" {
						found = "<>"
					} else {
						continue
					}
					cm.ktz = append(cm.ktz, ifv)
				}

				if f.Op == ">" || f.Op == ">=" {
					if found == "" {
						found = ">"
					} else if found == "<" {
						found = "<>"
					} else {
						continue
					}
					cm.kta = append(cm.kta, ifv)
				}
			}
		}

		if found != "=" {
			break
		}
	}

	plugin.DbgLog("FDB read: use filter [%v, %v].", cm.kta, cm.ktz)
	return nil
}

func DoRead(req xdrive.ReadRequest) error {
	var cm rreqColMap
	var ctxt *fdbctxt

	err := cm.init(req)
	if err != nil {
		return err
	}

	ctxt = opendb(cm.dirpath)

	// TODO: If prefixKey is a full key, compute hash and go to that bkt
	// directly.
	for i := int32(0); i < 256; i++ {
		if i%req.FragCnt != req.FragId {
			continue
		}

		bkt := byte(i)
		kr := ctxt.buildRange(bkt, cm.kta, cm.ktz)

		// plugin.DbgLog("Scanning bkt %d, with [%v, %v]", bkt, cm.kta, cm.ktz)
		kra := kr.Begin
		krz := kr.End
		// plugin.DbgLog("Scanning bkt %d, with range [%v, %v]", bkt, kra, krz)
		ncol := len(cm.proj)

		for kra != nil {
			coldatareply := make([]xdrive.XColDataReply, ncol)
			rcnt := int32(0)

			// plugin.DbgLog("FDB read, key range kra %v.", kra)
			for col := 0; col < ncol; col++ {
				xcol := new(xdrive.XCol)
				coldatareply[col].Data = xcol
				xcol.Colname = cm.proj[col]
				xcol.Nrow = 1024
				xcol.Nullmap = make([]bool, xcol.Nrow)

				var colt int32
				if cm.colisk[col] {
					colt = cm.keyt[cm.colpos[col]]
				} else {
					colt = cm.valt[cm.colpos[col]]
				}

				switch xdrive.SpqType(colt) {
				case xdrive.SpqType_BOOL, xdrive.SpqType_INT16, xdrive.SpqType_INT32,
					xdrive.SpqType_DATE, xdrive.SpqType_TIME_MILLIS:
					xcol.I32Data = make([]int32, xcol.Nrow)

				case xdrive.SpqType_INT64,
					xdrive.SpqType_TIME_MICROS,
					xdrive.SpqType_TIMESTAMP_MILLIS, xdrive.SpqType_TIMESTAMP_MICROS,
					xdrive.SpqType_TIMESTAMPTZ_MILLIS, xdrive.SpqType_TIMESTAMPTZ_MICROS:
					xcol.I64Data = make([]int64, xcol.Nrow)

				case xdrive.SpqType_FLOAT:
					xcol.F32Data = make([]float32, xcol.Nrow)

				case xdrive.SpqType_DOUBLE:
					xcol.F64Data = make([]float64, xcol.Nrow)

				default:
					xcol.Sdata = make([]string, xcol.Nrow)
				}
			}

			_, err := ctxt.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
				kvrr := tr.GetRange(fdb.KeyRange{kra, krz}, fdb.RangeOptions{})
				kvri := kvrr.Iterator()
				var ktup, vtup tuple.Tuple
				var terr error

				for kvri.Advance() {
					kv := kvri.MustGet()
					ktup, vtup, terr = ctxt.parseKeyValue(bkt, kv)
					if terr != nil {
						return nil, terr
					}

					for col := 0; col < ncol; col++ {
						var iv interface{}
						if cm.colisk[col] {
							iv = ktup[cm.colpos[col]]
						} else {
							iv = vtup[cm.colpos[col]]
						}

						if iv == nil {
							coldatareply[col].Data.Nullmap[rcnt] = true
							continue
						} else {
							coldatareply[col].Data.Nullmap[rcnt] = false
						}

						if coldatareply[col].Data.I32Data != nil {
							i64v := iv.(int64)
							coldatareply[col].Data.I32Data[rcnt] = int32(i64v)
						} else if coldatareply[col].Data.I64Data != nil {
							i64v := iv.(int64)
							coldatareply[col].Data.I64Data[rcnt] = i64v
						} else if coldatareply[col].Data.F32Data != nil {
							f32v := iv.(float32)
							coldatareply[col].Data.F32Data[rcnt] = f32v
						} else if coldatareply[col].Data.F64Data != nil {
							f64v := iv.(float64)
							coldatareply[col].Data.F64Data[rcnt] = f64v
						} else {
							sv := iv.(string)
							coldatareply[col].Data.Sdata[rcnt] = sv
						}
					}

					rcnt += 1
					if rcnt == 1024 {
						break
					}
				}

				if rcnt == 1024 {
					if kvri.Advance() {
						kv := kvri.MustGet()
						kra = kv.Key
						// plugin.DbgLog("FDB transact found %d rows, advace kra to %s.", rcnt, kra)
					} else {
						// plugin.DbgLog("FDB transact found %d rows, reset kra ...", rcnt)
						kra = nil
					}
				} else {
					kra = nil
				}

				return nil, nil
			})

			// plugin.DbgLog("Reader return data, rcnt %d, col %d", rcnt, ncol)
			if rcnt > 0 {
				for col := 0; col < ncol; col++ {
					if rcnt < 1024 {
						coldatareply[col].Data.Nrow = rcnt
						coldatareply[col].Data.Nullmap = coldatareply[col].Data.Nullmap[:rcnt]
						if coldatareply[col].Data.I32Data != nil {
							coldatareply[col].Data.I32Data = coldatareply[col].Data.I32Data[:rcnt]
						} else if coldatareply[col].Data.I64Data != nil {
							coldatareply[col].Data.I64Data = coldatareply[col].Data.I64Data[:rcnt]
						} else if coldatareply[col].Data.F32Data != nil {
							coldatareply[col].Data.F32Data = coldatareply[col].Data.F32Data[:rcnt]
						} else if coldatareply[col].Data.F64Data != nil {
							coldatareply[col].Data.F64Data = coldatareply[col].Data.F64Data[:rcnt]
						} else {
							coldatareply[col].Data.Sdata = coldatareply[col].Data.Sdata[:rcnt]
						}
					}

					err = plugin.ReplyXColData(coldatareply[col])
					if err != nil {
						plugin.DbgLogIfErr(err, "write data column failed.")
						return err
					}
				}
			}
		}
	}

	// Done!   Fill in an empty reply, indicating end of stream.
	var col xdrive.XColDataReply
	err = plugin.ReplyXColData(col)
	//err = plugin.DataReply(0, "")
	if err != nil {
		plugin.DbgLogIfErr(err, "DataReply failed.")
		return err
	}
	return nil
}
