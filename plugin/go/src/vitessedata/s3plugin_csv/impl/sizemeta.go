package impl

import (
	"vitessedata/plugin"
	"vitessedata/proto/xdrive"
)

func DoSizeMeta() error {
	var req xdrive.SizeMetaRequest
	err := plugin.DelimRead(&req)
	if err != nil {
		return err
	}

	// Init s3 bkt
	var sb S3Bkt
	sb.ConnectUsingRInfo()

	// process path
	rinfo := plugin.RInfo()
	// fragid = 0, fragcnt = 1 will return everything (no mod)
	myflist, err := buildS3Flist(&sb, rinfo.Rpath, 0, 1)
	if err != nil {
		return err
	}

	sz := int64(0)
	for _, item := range myflist {
		sz += item.Size
	}

	var r xdrive.PluginSizeMetaReply
	r.Sizemeta = new(xdrive.SizeMetaReply)
	r.Sizemeta.Nrow = sz / 100
	r.Sizemeta.Nbyte = sz
	plugin.DelimWrite(&r)
	return nil
}
