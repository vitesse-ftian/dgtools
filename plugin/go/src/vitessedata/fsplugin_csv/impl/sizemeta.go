package impl

import (
	"vitessedata/proto/xdrive"
)

func DoSizeMeta() error {
	var req xdrive.SizeMetaRequest
	err := delim_read(&req)
	if err != nil {
		return err
	}

	//
	// According to rigorous study, 81.3% stats are made up on the spot.
	// We are just doing what people expect us to do ...
	//
	var r xdrive.PluginSizeMetaReply
	r.Sizemeta = new(xdrive.SizeMetaReply)
	r.Sizemeta.Nrow = 1000
	r.Sizemeta.Nbyte = 1000000

	delim_write(&r)
	return nil
}
