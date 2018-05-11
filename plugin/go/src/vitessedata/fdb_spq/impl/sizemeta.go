package impl

import (
	"fmt"
	"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
	"vitessedata/plugin"
)

func DoSizeMeta(req xdrive.SizeMetaRequest) error {
	// TODO: Get real stats from LocalityGetBoundaries.
	plugin.SizeMetaReply(100, 1000000)
	return nil
}

func DoSample(req xdrive.SampleRequest) error {
	return fmt.Errorf("Sample NYI")
}
