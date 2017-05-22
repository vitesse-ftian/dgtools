package impl

import (
	"vitessedata/plugin"
	"vitessedata/plugin/csvhandler"
	"vitessedata/proto/xdrive"
)

func DoSample() error {
	var req xdrive.SampleRequest
	err := plugin.DelimRead(&req)
	if err != nil {
		return err
	}

	// Init s3 bkt
	var sb S3Bkt
	sb.ConnectUsingRInfo()

	// process path
	rinfo := plugin.RInfo()
	myflist, err := buildS3Flist(&sb, rinfo.Rpath, req.FragId, req.FragCnt)
	if err != nil {
		plugin.DbgLogIfErr(err, "S3 listdir failed.  Rinfo %v", *rinfo)
		plugin.ReplyError(-2, "listdir failed: "+err.Error())
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
			plugin.ReplyError(-10, "Cannot open file "+f.Name)
			return err
		}
		err = csvh.ProcessEachFile(file)
		if err != nil {
			plugin.ReplyError(-20, "CSV file "+f.Name+" has invalid data")
			return err
		}
	}

	// Done!   Fill in an empty reply, indicating end of stream.
	plugin.ReplyError(0, "")
	return nil
}
