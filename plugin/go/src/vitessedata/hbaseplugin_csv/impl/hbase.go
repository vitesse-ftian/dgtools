package impl

import (
        //"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
        "github.com/tsuna/gohbase"
        "github.com/tsuna/gohbase/hrpc"
        "github.com/tsuna/gohbase/filter"
        "github.com/tsuna/gohbase/region"
	"io"
	//"os"
	"vitessedata/plugin"
	"fmt"
	"context"
	"strings"
)

type HBClient struct {
	host string
	table string
	user string
	metaregions []hrpc.RegionInfo
	client gohbase.Client

}


func (hb *HBClient) CreateUsingRinfo() {

	rinfo := plugin.RInfo()
	ss := strings.Split(rinfo.Rpath, "/")
	hb.host = ss[0]
	hb.table = ss[1]
	
	conf := rinfo.GetConf()
	for _, kv := range conf.GetKv() {
		if kv.GetKey() == "user" {
			hb.user = kv.GetValue()
		}
	}

	plugin.DbgLog("host: '%s', table: '%s', user: '%s'", hb.host, hb.table, hb.user)
	
	hb.client = gohbase.NewClient(hb.host, gohbase.EffectiveUser(hb.user))	
	hb.getmetaregions()
}


func (hb *HBClient) Create(host, user, table string) {
	hb.host = host
	hb.table = table
	hb.user = user
	hb.client = gohbase.NewClient(hb.host, gohbase.EffectiveUser(user))
	hb.getmetaregions()
}

func (hb *HBClient) Close() {
	hb.client.Close()
}

func (hb *HBClient) getmetaregions() {
	
	filterstr := fmt.Sprintf("%s,", hb.table)

	f := filter.NewPrefixFilter([]byte(filterstr))
	family := map[string][]string{"info": []string{"regioninfo", "server"}}

	plugin.DbgLog(filterstr)

	scanreq, _ := hrpc.NewScanStr(context.Background(), "hbase:meta", hrpc.Families(family), hrpc.Filters(f))
	scanner := hb.client.Scan(scanreq)

	for {
		r, err := scanner.Next()

		if err == io.EOF {
			break
		}
		region, _, err := region.ParseRegionInfo(r)
		if err != nil {
			plugin.DbgLog("Error %v", err)
			return
		}

		hb.metaregions = append(hb.metaregions, region)
	}

}

func (hb *HBClient) GetRegions(fragid, fragcnt int32) []hrpc.RegionInfo {
	
	var regions [] hrpc.RegionInfo

	for _ , r := range hb.metaregions {
		id := r.ID()
		rem := id % uint64(fragcnt)
		if rem == uint64(fragid) {
			regions = append(regions, r)
		}
	}
	return regions
}

func (hb *HBClient) Scan(region hrpc.RegionInfo, families map[string][]string, pfilter filter.Filter) (hrpc.Scanner, error) {
	
	table := region.Table()
	startkey := region.StartKey()
	endkey := region.StopKey()
	
	scanRequest, err := hrpc.NewScanRange(context.Background(), table, startkey, endkey, hrpc.Families(families), hrpc.Filters(pfilter))
	if err != nil {
		return nil, err
	}
	
	scanner := hb.client.Scan(scanRequest)
	return scanner, nil
}
