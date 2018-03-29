package impl

import (
	"encoding/base64"
	"fmt"
	"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
	"hash/fnv"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
	"vitessedata/plugin"
)

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
	// filter called "QUERY", which allow users to send any query to plugin.
	//
	// var query string
	// for _, f := range req.Filter {
	// f cannot be nil
	//		if f.Op == "QUERY" {
	//			query = f.Args[0]
	//		}
	//	}

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

	// Return each file.
	plugin.DbgLog("fsplugin: path %s, frag (%d, %d) globed %v", path, req.FragId, req.FragCnt, myflist)

	// ls_file columns:
	//	dir,
	//	basename, size, mode, modtime, isdir,  as returned by lstat
	//	content_base64
	//
	needContent := false
	projcollist := req.Columnlist
	if len(projcollist) == 0 {
		projcollist = make([]string, len(req.Columndesc))
		for j, d := range req.Columndesc {
			projcollist[j] = d.Name
		}
	}

	for _, c := range projcollist {
		if c == "content_base64" {
			needContent = true
		}
	}

	// Now process each file.
	for _, f := range myflist {
		fi, err := os.Lstat(f)
		if err != nil {
			return err
		}

		content := ""
		contentNull := true
		if needContent && !fi.IsDir() {
			dat, err := ioutil.ReadFile(f)
			if err == nil {
				content = base64.StdEncoding.EncodeToString(dat)
				contentNull = false
			}
		}

		for col := 0; col < len(projcollist); col++ {
			xcol := new(xdrive.XCol)
			xcol.Colname = projcollist[col]
			xcol.Nrow = 1
			xcol.Nullmap = make([]bool, 1)
			xcol.Nullmap[0] = false

			switch xcol.Colname {
			case "dir":
				xcol.Sdata = make([]string, 1)
				xcol.Sdata[0] = filepath.Dir(f)
			case "basename":
				xcol.Sdata = make([]string, 1)
				xcol.Sdata[0] = fi.Name()
			case "size":
				// BUG BUG: Deepgreen server has a bug, it messed up col type.
				// only str is safe.   Fix that!   And we will reenable this
				// int64 thing.
				// xcol.I64Data = make([]int64, 1)
				// xcol.I64Data[0] = fi.Size()
				xcol.Sdata = make([]string, 1)
				xcol.Sdata[0] = fmt.Sprintf("%d", fi.Size())
			case "mode":
				xcol.Sdata = make([]string, 1)
				xcol.Sdata[0] = fi.Mode().String()
			case "modtime":
				xcol.Sdata = make([]string, 1)
				xcol.Sdata[0] = fi.ModTime().Format(time.RFC1123Z)
			case "isdir":
				// BUG BUG
				// xcol.I32Data = make([]int32, 1)
				xcol.Sdata = make([]string, 1)
				if fi.IsDir() {
					// xcol.I32Data[0] = 1
					xcol.Sdata[0] = "1"
				} else {
					// xcol.I32Data[0] = 0
					xcol.Sdata[0] = "0"
				}
			case "content_base64":
				xcol.Sdata = make([]string, 1)
				if contentNull {
					xcol.Nullmap[0] = true
				} else {
					xcol.Sdata[0] = content
				}
			}

			var colreply xdrive.XColDataReply
			colreply.Data = xcol
			err = plugin.ReplyXColData(colreply)
			if err != nil {
				plugin.DbgLogIfErr(err, "Write data col failed.")
				return err
			}
		}
	}

	// Done, write an end.
	var col xdrive.XColDataReply
	err = plugin.ReplyXColData(col)
	if err != nil {
		plugin.DbgLogIfErr(err, "Write data end failed.")
		return err
	}
	return nil
}
