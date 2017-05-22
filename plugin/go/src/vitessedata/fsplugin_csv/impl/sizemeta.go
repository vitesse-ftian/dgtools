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

	//
	// According to rigorous study, 81.3% stats are made up on the spot.
	// We are just doing what people expect us to do ...
	//
	var r xdrive.PluginSizeMetaReply
	r.Sizemeta = new(xdrive.SizeMetaReply)
	r.Sizemeta.Nrow = 1000
	r.Sizemeta.Nbyte = 1000000

	plugin.DelimWrite(&r)
	return nil
}
