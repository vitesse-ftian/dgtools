package impl

import (
	"strings"
	"path/filepath"
	"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
	"vitessedata/plugin"
	"vitessedata/plugin/csvhandler"
)

func DoSample(req xdrive.SampleRequest, rootpath, bucket, region string) error {

	// Init s3 bkt
	var sb S3Bkt
	sb.Connect(region, bucket)

	// process path
        idx := strings.Index(req.Filespec.Path[1:], "/")
        path := filepath.Join(rootpath, req.Filespec.Path[idx+1:])
        plugin.DbgLog("filepath = %s", path)

	myflist, err := buildS3Flist(&sb, path, req.FragId, req.FragCnt)
	if err != nil {
		plugin.DbgLogIfErr(err, "S3 listdir failed.  Path %s", path)
		plugin.DataReply(-2, "listdir failed: "+err.Error())
		return err
	}

	// csvhandler.
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

		file, err := sb.GetObject(f.Name)
		if err != nil {
			plugin.DataReply(-10, "Cannot open file "+f.Name)
			return err
		}
		err = csvh.ProcessEachFile(file)
		if err != nil {
			plugin.DataReply(-20, "CSV file "+f.Name+" has invalid data")
			return err
		}
	}

	// Done!   Fill in an empty reply, indicating end of stream.
	plugin.DataReply(0, "")
	return nil
}
