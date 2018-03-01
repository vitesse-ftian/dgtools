package impl

import (
	"fmt"
	"strings"
	"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
	"hash/fnv"
	"path/filepath"
	"vitessedata/plugin"
	"vitessedata/plugin/csvhandler"
)

func buildS3Flist(sb *S3Bkt, path string, fragid int32, fragcnt int32) ([]S3Item, error) {
	prefix := pathSimplePrefix(path)
	items, err := sb.ListDir(prefix)
	if err != nil {
		return nil, err
	}

	myflist := make([]S3Item, 0)
	patt := path
	for i, r := range path {
		if r == '/' {
			continue
		} else {
			patt = path[i:]
			break
		}
	}

	plugin.DbgLog("S3 Listdir path: %s, prefix :%s, pattern :%s", path, prefix, patt)
	for _, item := range items {
		if item.Size > 0 && !item.IsDir() {
			ok, err := filepath.Match(patt, item.Name)
			if err != nil {
				return nil, err
			}

			if ok {
				h := fnv.New32a()
				h.Write([]byte(item.Name))
				hv := int32(h.Sum32())

				tmp := hv % fragcnt
				if tmp < 0 {
					tmp += fragcnt
				}

				if fragid == tmp {
					plugin.DbgLog("Frag: file %s hash to %d, match frag (%d, %d)", item.Name, hv, fragid, fragcnt)
					myflist = append(myflist, item)
				} else {
					plugin.DbgLog("Frag: file %s hash to %d, does not match frag (%d, %d)", item.Name, hv, fragid, fragcnt)
				}
			} else {
				plugin.DbgLog("Item %s does not match pattern %s", item.Name, patt)
			}
		} else {
			plugin.DbgLog("Item %s does is not a file, pattern %s.", item.Name, patt)
		}
	}
	return myflist, nil
}

// DoRead servies XDrive read requests.   It read a ReadRequest from stdin and reply
// a sequence of PluginDataReply to stdout.   It should end the data stream with a
// trivial (Errcode == 0, but there is no data) message.
func DoRead(req xdrive.ReadRequest, rootpath, bucket, region string) error {

	// Check/validate frag info.  Again, not necessary, as xdriver server should always
	// fill in good value.
	if req.FragCnt <= 0 || req.FragId < 0 || req.FragId >= req.FragCnt {
		plugin.DbgLog("Invalid read req %v", req)
		plugin.DataReply(-3, fmt.Sprintf("Read request frag (%d, %d) is not valid.", req.FragId, req.FragCnt))
		return fmt.Errorf("Invalid read request")
	}

	// Init s3 bkt
	var sb S3Bkt
	sb.Connect(region, bucket)

	// process path
	idx := strings.Index(req.Filespec.Path[1:], "/")
	path := filepath.Join(rootpath, req.Filespec.Path[idx+1:])
	plugin.DbgLog("filepath = %s", path)

	myflist, err := buildS3Flist(&sb, path, req.FragId, req.FragCnt)
	if err != nil {
		plugin.DbgLogIfErr(err, "S3 listdir failed.  %s", path)
		plugin.DataReply(-2, "listdir failed: "+err.Error())
		return err
	} else {
		plugin.DbgLog("Fragid %d, FragCnt %d, will process files %v", req.FragId, req.FragCnt, myflist)
	}

	// csvhandler.
	var csvh csvhandler.CsvReader
	csvh.Init(req.Filespec, req.Columndesc, req.Columnlist)

	// Now process each file.
	for _, f := range myflist {
		file, err := sb.GetObject(f.Name)
		if err != nil {
			plugin.DbgLogIfErr(err, "Open csv file %s failed.", f.Name)
			plugin.DataReply(-10, "Cannot open file "+f.Name)
			return err
		}

		// csvh will close file.
		err = csvh.ProcessEachFile(file)
		if err != nil {
			plugin.DbgLogIfErr(err, "Parse csv file %s failed.", f)
			plugin.DataReply(-20, "CSV file "+f.Name+" has invalid data")
			return err
		}
	}

	// Done!   Fill in an empty reply, indicating end of stream.
	plugin.DataReply(0, "")
	return nil
}
