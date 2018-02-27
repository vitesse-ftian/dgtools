package impl

import (
	"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
	"hash/fnv"
	"os"
	"path/filepath"
	"vitessedata/plugin"
	"vitessedata/plugin/csvhandler"
)

//
// Sample is very similar to read, and they should share lots of code.   Here we
// will just copy/paste code and highlight some of the differences.
//
// NOTE: The followin impl. of sampling, is not a sample at all.  We just take
// the first Nrow data and return it.   For real impl, please refer to a good
// alogirthm, like reservoir sampling.
//
func DoSample(req xdrive.SampleRequest) error {

	// Glob:
	path := req.Filespec.Path
	flist, err := filepath.Glob(path)
	if err != nil {
		plugin.DataReply(-2, "rmgr glob failed: "+err.Error())
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

	var csvh csvhandler.CsvReader
	csvh.Init(req.Filespec, req.Columndesc, nil)

	//
	// NOTE: Sample does not have a columnlist, as it will read all columns, in order.
	//
	for _, f := range myflist {
		if csvh.RowCnt >= int(req.Nrow) {
			// Got enough.
			break
		}

		file, err := os.Open(f)
		if err != nil {
			plugin.DataReply(-10, "Cannot open file "+f)
			return err
		}
		err = csvh.ProcessEachFile(file)
		if err != nil {
			plugin.DataReply(-20, "CSV file "+f+" has invalid data")
			return err
		}
	}

	// Done!   Fill in an empty reply, indicating end of stream.
	plugin.DataReply(0, "")
	return nil
}
