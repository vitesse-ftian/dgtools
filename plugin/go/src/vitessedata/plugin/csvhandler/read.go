package csvhandler

import (
	"encoding/csv"
	"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
	"io"
	"strconv"
	"vitessedata/plugin"
)

type CsvReader struct {
	fspec *xdrive.FileSpec

	ncol    int
	colid   []int
	typ     []int32
	collist []string

	RowCnt int
}

func (h *CsvReader) Init(fspec *xdrive.FileSpec, coldesc []*xdrive.ColumnDesc, projcollist []string) {
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
}

func (h *CsvReader) ProcessEachFile(file io.ReadCloser) error {
	// We take ownership of file, and we will close.
	defer file.Close()
	r := csv.NewReader(file)

	// CSV options, in file spec. For now, we just do Comma.
	r.Comma = rune(h.fspec.Csvspec.Delimiter[0])

	// If we need to process huge CSV files, we should read line by line.  Lazy.
	records, err := r.ReadAll()
	if err != nil {
		return err
	}

	// Empty file.  This is fine.
	if len(records) == 0 {
		return nil
	}
	// Trivial stats ...
	h.RowCnt += len(records)

	// Build reply message.   Errcode initialized to 0, which is what we want.
	var dataReply xdrive.PluginDataReply
	// dataReply.Errcode = 0
	dataReply.Rowset = new(xdrive.XRowSet)
	dataReply.Rowset.Columns = make([]*xdrive.XCol, h.ncol)

	plugin.DbgLog("Building Rowset, %d rows, %d cols", len(records), h.ncol)

	for col := 0; col < h.ncol; col++ {
		xcol := new(xdrive.XCol)
		dataReply.Rowset.Columns[col] = xcol
		xcol.Colname = h.collist[col]
		xcol.Nrow = int32(len(records))
		xcol.Nullmap = make([]bool, xcol.Nrow)

		switch xdrive.SpqType(h.typ[col]) {
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

	plugin.DbgLog("Done Building Rowset, %d rows, %d cols", len(records), h.ncol)
	err = plugin.DelimWrite(&dataReply)
	return err
}
