package impl

import (
	"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
	"fmt"
	"io"
	"vitessedata/plugin"
//        "github.com/tsuna/gohbase"
//        "github.com/tsuna/gohbase/hrpc"
//        "github.com/tsuna/gohbase/filter"
//        "github.com/tsuna/gohbase/region"
)

func DoRead() error {
	var req xdrive.ReadRequest
	err := plugin.DelimRead(&req)
	if err != nil {
		plugin.DbgLogIfErr(err, "Delim read req failed.")
		return err
	}

	if req.FragCnt <= 0 || req.FragId < 0 || req.FragId >= req.FragCnt {
		plugin.DbgLog("Invalid read req %v", req)
		plugin.ReplyError(-3, fmt.Sprintf("Read request frag (%d, %d) is not valid.", req.FragId, req.FragCnt))
		return fmt.Errorf("Invalid read request")
	}


	var hbase HBClient
	hbase.CreateUsingRinfo()

	regions := hbase.GetRegions(req.FragId, req.FragCnt)

	// families
	
	// filter

	var writer HBWriter
	writer.Init(req.Filespec, req.Columndesc, req.Columnlist)


	for _, rg := range regions {
		scanner, err := hbase.Scan(rg, nil, nil)
		if err != nil {
			fmt.Errorf("Scan failed. %v", err)
			return err
		}

		for {
			r, err := scanner.Next()
			if err != nil {
				if err == io.EOF {
					break
				} else {
					fmt.Errorf("scan next failed. %s", err)
					return err
				}
			}


			writer.Write(r)
			/*
			for _, e := range r.Cells {
				plugin.DbgLog("%s %s %s %s\n", string(e.Row), string(e.Family),
					string(e.Qualifier), string(e.Value))
			}
*/
		}
	}

	writer.Close()
	plugin.ReplyError(0, "")
	return nil
}
