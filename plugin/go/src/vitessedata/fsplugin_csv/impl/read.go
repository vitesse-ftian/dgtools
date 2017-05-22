package impl

import (
	"encoding/csv"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"strconv"
	"vitessedata/plugin"
	"vitessedata/proto/xdrive"
)

// DoRead servies XDrive read requests.   It read a ReadRequest from stdin and reply
// a sequence of PluginDataReply to stdout.   It should end the data stream with a
// trivial (Errcode == 0, but there is no data) message.
func DoRead() error {
	var req xdrive.ReadRequest
	err := plugin.DelimRead(&req)
	if err != nil {
		plugin.DbgLogIfErr(err, "Delim read rinfo failed.")
		return err
	}

	// Check/validate frag info.  Again, not necessary, as xdriver server should always
	// fill in good value.
	if req.FragCnt <= 0 || req.FragId < 0 || req.FragId >= req.FragCnt {
		plugin.DbgLog("Invalid read req %v", req)
		plugin.ReplyError(-3, fmt.Sprintf("Read request frag (%d, %d) is not valid.", req.FragId, req.FragCnt))
		return fmt.Errorf("Invalid read request")
	}

	// Glob:
	rinfo := plugin.RInfo()
	flist, err := filepath.Glob(rinfo.Rpath)
	if err != nil {
		plugin.DbgLogIfErr(err, "Glob failed.  Rinfo %v", *rinfo)
		plugin.ReplyError(-2, "rmgr glob failed: "+err.Error())
		return err
	}

	// There are many different ways to implement FragId/FragCnt.   Here we use filename.
	// All data within one file go to one fragid.  We determine which files this call
	// should serve.  Any deterministic scheme should work.  We use hash mod.
	// One may, for example choos to impl fragid/fragcnt by hashing (or round robin) each
	// row.  For CSV file, that is not really efficient because it will parse the file many
	// times in different plugin processes (but it does parallelize the task ...)
	myflist := []string{}
	for _, f := range flist {
		h := fnv.New32a()
		h.Write([]byte(f))
		hv := int32(h.Sum32())

		tmp := hv % req.FragCnt
		if tmp < 0 {
			tmp += req.FragCnt
		}

		if req.FragId == tmp {
			plugin.DbgLog("Frag: file %s hash to %d, match frag (%d, %d)", f, hv, req.FragId, req.FragCnt)
			myflist = append(myflist, f)
		} else {
			plugin.DbgLog("Frag: file %s hash to %d, does not match frag (%d, %d)", f, hv, req.FragId, req.FragCnt)
		}
	}

	// Find out the col id and type, for projection.
	ncol := len(req.Columnlist)
	colid := make([]int, ncol)
	typ := make([]int32, ncol)
	for i, c := range req.Columnlist {
		for j, d := range req.Columndesc {
			if c == d.Name {
				colid[i] = j
				typ[i] = d.Type
				break
			}
		}
		// XDrive servers promise that colid and typ will be filled correctly.
		// Here you can assert on typ[i] is a valid value, etc.
	}

	//
	// Filters: The read request may contain some filters.  Plugin is free to choose to implement
	// these filters, or, just ignore them.  It can also choose to do best-effort filtering, filter
	// out some but allow false positives.  Later XDrive server and/or DeepGreen will apply filter
	// again.  Obviously, false negative, will loose data, so, must not happen.
	//
	// Filters are most useful when the scheme has some index methods -- for example, bloom filter
	// or min/max on a block.   If there is no index, applying filter here early (compared to apply
	// filter in xdrive server) probably will have little performance benefit -- data transfer from
	// plugin to xdrive is local (stdin/stdout), main cost is protobuf marshal/unmarshal.  The filter
	// evalution in xdirve side is heavily optimized and probably is faster than the plugin code.
	//
	// We just ignore the filters because csv does not have index capability.
	//
	// NOTE: The most important filter use case is actually a filter called "QUERY" filter.   In
	// Deepgreen, user can issue a query on an xdrive external table t, for example,
	//
	// select * from t WHERE DG_UTILS.XDRIVE_QUERY('foo bar zoo')
	//
	// For such a query, the plugin will receive a "QUERY" filter 'foo bar zoo'.  It is up for the
	// plugin to interprete/execute this query filter.   This can be used to implement arbitrary
	// query push down to another database (postgres/mysql/elasticsearch etc etc...)
	//

	// Now process each file.
	for _, f := range myflist {
		file, err := os.Open(f)
		if err != nil {
			plugin.DbgLogIfErr(err, "Open csv file %s failed.", f)
			plugin.ReplyError(-10, "Cannot open file "+f)
			return err
		}

		r := csv.NewReader(file)

		// CSV options, in file spec, we just do Comma.
		r.Comma = rune(req.Filespec.Csvspec.Delimiter[0])

		// If we need to process huge CSV files, we should read line by line.  Lazy.
		records, err := r.ReadAll()
		if err != nil {
			plugin.DbgLogIfErr(err, "Parse csv file %s failed.", f)
			plugin.ReplyError(-20, "CSV file "+f+" has invalid data")
			return err
		}

		// Empty file.  This is fine -- however, we do not want to send a data reply
		// with no data, because xdrive will interprete this as end of stream.
		if len(records) == 0 {
			continue
		}

		// Build reply message.   Errcode initialized to 0, which is what we want.
		var dataReply xdrive.PluginDataReply
		// dataReply.Errcode = 0
		dataReply.Rowset = new(xdrive.XRowSet)
		dataReply.Rowset.Columns = make([]*xdrive.XCol, ncol)

		plugin.DbgLog("Building Rowset, %d rows, %d cols", len(records), ncol)

		for col := 0; col < ncol; col++ {
			xcol := new(xdrive.XCol)
			dataReply.Rowset.Columns[col] = xcol
			xcol.Colname = req.Columnlist[col]
			xcol.Nrow = int32(len(records))
			xcol.Nullmap = make([]bool, xcol.Nrow)

			switch xdrive.SpqType(typ[col]) {
			case xdrive.SpqType_BOOL, xdrive.SpqType_INT16, xdrive.SpqType_INT32, xdrive.SpqType_DATE, xdrive.SpqType_TIME_MILLIS:
				//
				// These types are encoded as int32 in xcol.   For csv data that use a different encoding,
				// for example, BOOL as t/f, this is the place to implement parser.
				//
				plugin.DbgLog("Col %d Buiding I32Data size %d\n", col, xcol.Nrow)
				xcol.I32Data = make([]int32, xcol.Nrow)
				for idx, rec := range records {
					val := rec[col]
					if val == "" {
						// Trivial null, for better null handling, need to deal with the nullstr in csvspec.
						xcol.Nullmap[idx] = true
						xcol.I32Data[idx] = 0
					} else {
						xcol.Nullmap[idx] = false
						iv, err := strconv.Atoi(val)
						if err != nil {
							plugin.ReplyError(-100, "Invalid int data "+val)
							return err
						}
						xcol.I32Data[idx] = int32(iv)
					}
				}

			case xdrive.SpqType_INT64, xdrive.SpqType_TIMESTAMP_MILLIS, xdrive.SpqType_TIME_MICROS, xdrive.SpqType_TIMESTAMP_MICROS:
				// These types are encoded as int64 in xcol
				plugin.DbgLog("Col %d Buiding I64Data size %d\n", col, xcol.Nrow)
				xcol.I64Data = make([]int64, xcol.Nrow)
				for idx, rec := range records {
					val := rec[col]
					if val == "" {
						// Trivial null, for better null handling, need to deal with the nullstr in csvspec.
						xcol.Nullmap[idx] = true
						xcol.I64Data[idx] = 0
					} else {
						xcol.Nullmap[idx] = false
						xcol.I64Data[idx], err = strconv.ParseInt(val, 0, 64)
						if err != nil {
							plugin.ReplyError(-100, "Invalid int data "+val)
							return err
						}
					}
				}

			case xdrive.SpqType_FLOAT:
				plugin.DbgLog("Col %d Buiding F32Data size %d\n", col, xcol.Nrow)
				// These types are encoded as float32 in xcol
				xcol.F32Data = make([]float32, xcol.Nrow)
				for idx, rec := range records {
					val := rec[col]
					if val == "" {
						// Trivial null, for better null handling, need to deal with the nullstr in csvspec.
						xcol.Nullmap[idx] = true
						xcol.F32Data[idx] = 0
					} else {
						xcol.Nullmap[idx] = false
						fv, err := strconv.ParseFloat(val, 32)
						if err != nil {
							plugin.ReplyError(-100, "Invalid float data "+val)
							return err
						}
						xcol.F32Data[idx] = float32(fv)
					}
				}

			case xdrive.SpqType_DOUBLE:
				plugin.DbgLog("Col %d Buiding F64Data size %d\n", col, xcol.Nrow)
				// These types are encoded as float64 in xcol
				xcol.F64Data = make([]float64, xcol.Nrow)
				for idx, rec := range records {
					val := rec[col]
					if val == "" {
						// Trivial null, for better null handling, need to deal with the nullstr in csvspec.
						xcol.Nullmap[idx] = true
						xcol.F64Data[idx] = 0
					} else {
						xcol.Nullmap[idx] = false
						xcol.F64Data[idx], err = strconv.ParseFloat(val, 64)
						if err != nil {
							plugin.ReplyError(-100, "Invalid float data "+val)
							return err
						}
					}
				}

			// case SpqType_CSTR, SpqType_JSON:
			default:
				//
				// Handle default type as string.  In fact, we do not need to do ANY of the above.
				// We can always pass data as string in XCol, and xdrive side will do proper parsing.
				//
				plugin.DbgLog("Buiding SData size %d\n", xcol.Nrow)
				xcol.Sdata = make([]string, xcol.Nrow)
				for idx, rec := range records {
					val := rec[col]
					if val == "" {
						// Trivial null, for better null handling, need to deal with the nullstr in csvspec.
						xcol.Nullmap[idx] = true
						xcol.Sdata[idx] = ""
					} else {
						xcol.Nullmap[idx] = false
						xcol.Sdata[idx] = val
					}
				}
			}
		}

		plugin.DbgLog("Done Building Rowset, %d rows, %d cols", len(records), ncol)
		err = plugin.DelimWrite(&dataReply)
		if err != nil {
			plugin.ReplyError(-100, "Write data reply failed")
			return err
		}
	}

	// Done!   Fill in an empty reply, indicating end of stream.
	plugin.ReplyError(0, "")
	return nil
}
