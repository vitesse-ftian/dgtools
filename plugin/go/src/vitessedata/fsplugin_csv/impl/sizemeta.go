package impl

import (
	"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
	"vitessedata/plugin"
)

func DoSizeMeta(req xdrive.SizeMetaRequest) error {

	//
	// According to rigorous study, 81.3% stats are made up on the spot.
	// We are just doing what people expect us to do ...
	//
	plugin.SizeMetaReply(100, 1000000)
	return nil
}
