package impl

import (
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"vitessedata/plugin"
	"vitessedata/plugin/csvhandler"
	"vitessedata/proto/xdrive"
)

// DoRead servies XDrive read requests.   It read a ReadRequest from stdin and reply
// a sequence of PluginDataReply to stdout.   It should end the data stream with a
// trivial (Errcode == 0, but there is no data) message.
func DoRead() error {
	var req xdrive.ReadRequest
	err := plugin.DelimRead(&req)
	if err != nil {
		plugin.DbgLogIfErr(err, "Delim read req failed.")
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

	plugin.DbgLog("fsplugin: path %s, frag (%d, %d) globed %v", rinfo.Rpath, req.FragId, req.FragCnt, myflist)

	// Csv Handler.
	var csvh csvhandler.CsvReader
	csvh.Init(req.Filespec, req.Columndesc, req.Columnlist)

	// Now process each file.
	for _, f := range myflist {
		file, err := os.Open(f)
		if err != nil {
			plugin.DbgLogIfErr(err, "Open csv file %s failed.", f)
			plugin.ReplyError(-10, "Cannot open file "+f)
			return err
		}

		// csvh will close.
		err = csvh.ProcessEachFile(file)
		if err != nil {
			plugin.DbgLogIfErr(err, "Parse csv file %s failed.", f)
			plugin.ReplyError(-20, "CSV file "+f+" has invalid data")
			return err
		}
	}

	// Done!   Fill in an empty reply, indicating end of stream.
	plugin.ReplyError(0, "")
	return nil
}
