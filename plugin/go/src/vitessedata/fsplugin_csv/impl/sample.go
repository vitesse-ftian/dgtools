package impl

import (
	"encoding/csv"
	"hash/fnv"
	"os"
	"path/filepath"
	"vitessedata/plugin"
	"vitessedata/proto/xdrive"
)

//
// Sample is very similar to read, and they should share lots of code.   Here we
// will just copy/paste code and highlight some of the differences.
//
// NOTE: The followin impl. of sampling, is not a sample at all.  We just take
// the first Nrow data and return it.   For real impl, please refer to a good
// alogirthm, like reservoir sampling.
//
func DoSample() error {
	var req xdrive.SampleRequest
	err := plugin.DelimRead(&req)
	if err != nil {
		return err
	}

	// Glob:
	rinfo := plugin.RInfo()
	flist, err := filepath.Glob(rinfo.Rpath)
	if err != nil {
		readError(-2, "rmgr glob failed: "+err.Error())
		return err
	}

	//
	// Filter flist, to look for files that this call should serve.  Any deterministic
	// scheme should work, we use hash mod.
	//
	// NOTE: This filtering method should be the same as the one in read.
	//
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
			myflist = append(myflist, f)
		}
	}

	// count so far.
	cnt := int32(0)
	ncol := len(req.Columndesc)

	//
	// NOTE: Sample does not have a columnlist, as it will read all columns, in order.
	//
	for _, f := range myflist {
		if cnt >= req.Nrow {
			// Got enough.
			break
		}

		file, err := os.Open(f)
		if err != nil {
			readError(-10, "Cannot open file "+f)
			return err
		}

		r := csv.NewReader(file)

		// CSV options, in file spec, we just do Comma.
		r.Comma = rune(req.Filespec.Csvspec.Delimiter[0])

		// If we need to process huge CSV files, we should read line by line.  Lazy.
		records, err := r.ReadAll()
		if err != nil {
			readError(-20, "CSV file "+f+" has invalid data")
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

		for col := 0; col < ncol; col++ {
			xcol := new(xdrive.XCol)
			dataReply.Rowset.Columns[col] = xcol
			xcol.Nrow = int32(len(records))
			xcol.Nullmap = make([]bool, xcol.Nrow)

			//
			// One can pack proper data types into XCol.   But here, we don't bother
			// just use string.  XDrive will take care of parsing.
			//
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

		cnt += int32(len(records))
	}

	// Done!   Fill in an empty reply, indicating end of stream.
	readError(0, "")
	return nil
}
