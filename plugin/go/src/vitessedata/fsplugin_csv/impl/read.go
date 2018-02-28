package impl

import (
	"fmt"
	"strings"
	"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
	"hash/fnv"
	"os"
	"path/filepath"
	"time"
	"vitessedata/plugin"
	"vitessedata/plugin/csvhandler"
)

func inject_fault(fault string) error {
	switch fault {
	case "sleep":
		time.Sleep(1 * time.Hour)
		return nil
	case "crash":
		plugin.FatalIf(true, "Fault inj crash.")
		return nil
	case "garble":
		fmt.Printf("Garbage out!")
		return nil
	case "error":
		return fmt.Errorf("Fault inj error.")
	default:
		return fmt.Errorf("Fault inj unknown.")
	}
}

// DoRead servies XDrive read requests.   It read a ReadRequest from stdin and reply
// a sequence of PluginDataReply to stdout.   It should end the data stream with a
// trivial (Errcode == 0, but there is no data) message.
func DoRead(req xdrive.ReadRequest, rootpath string) error {

	// Check/validate frag info.  Again, not necessary, as xdriver server should always
	// fill in good value.
	if req.FragCnt <= 0 || req.FragId < 0 || req.FragId >= req.FragCnt {
		plugin.DbgLog("Invalid read req %v", req)
		plugin.DataReply(-3, fmt.Sprintf("Read request frag (%d, %d) is not valid.", req.FragId, req.FragCnt))
		return fmt.Errorf("Invalid read request")
	}

	//
	// Filter:
	// req may contains a list of Filters that got pushed down from XDrive server.
	// As per plugin protocol, plugin can ignore all of them if they choose to be
	// lazy.  See comments in csvhandler.go.
	//
	// All filters are derived from SQL (where clause).  There is a special kind of
	// filter called "QUERY", which allow users to send any query to plugin.  Here as
	// an example, we implement a poorman's fault injection.
	//
	var fault string
	for _, f := range req.Filter {
		// f cannot be nil
		if f.Op == "QUERY" {
			fault = f.Args[0]
		}
	}

	if fault != "" {
		err := inject_fault(fault)
		if err != nil {
			return err
		}
	}

	// Glob:
        idx := strings.Index(req.Filespec.Path[1:], "/")
	path := req.Filespec.Path[idx+1:]
	path = filepath.Join(rootpath, path)
	plugin.DbgLog("path %s", path)
	flist, err := filepath.Glob(path)
	if err != nil {
		plugin.DbgLogIfErr(err, "Glob failed.  %s", path)
		plugin.DataReply(-2, "rmgr glob failed: "+err.Error())
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

	plugin.DbgLog("fsplugin: path %s, frag (%d, %d) globed %v", path, req.FragId, req.FragCnt, myflist)

	// Csv Handler.
	var csvh csvhandler.CsvReader
	csvh.Init(req.Filespec, req.Columndesc, req.Columnlist)

	// Now process each file.
	for _, f := range myflist {
		file, err := os.Open(f)
		if err != nil {
			plugin.DbgLogIfErr(err, "Open csv file %s failed.", f)
			plugin.DataReply(-10, "Cannot open file "+f)
			return err
		}

		// csvh will close.
		err = csvh.ProcessEachFile(file)
		if err != nil {
			plugin.DbgLogIfErr(err, "Parse csv file %s failed.", f)
			plugin.DataReply(-20, "CSV file "+f+" has invalid data")
			return err
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
