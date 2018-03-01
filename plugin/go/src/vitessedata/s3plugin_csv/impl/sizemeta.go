package impl

import (
	"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
	"vitessedata/plugin"
	"strings"
	"path/filepath"
)

func DoSizeMeta(req xdrive.SizeMetaRequest, rootpath, bucket, region string) error {

	// Init s3 bkt
	var sb S3Bkt
	sb.Connect(region, bucket)

	// process path
	idx := strings.Index(req.Filespec.Path[1:], "/")
	path := filepath.Join(rootpath, req.Filespec.Path[idx+1:])
	plugin.DbgLog("filepath = %s", path)
	// fragid = 0, fragcnt = 1 will return everything (no mod)
	myflist, err := buildS3Flist(&sb, path, 0, 1)
	if err != nil {
		return err
	}

	sz := int64(0)
	for _, item := range myflist {
		sz += item.Size
	}

	return plugin.SizeMetaReply(sz/100, sz);
}
