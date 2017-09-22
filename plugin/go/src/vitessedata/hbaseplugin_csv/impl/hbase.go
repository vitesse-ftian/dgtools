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
)

type HBClient struct {
	host string
	table string
	user string
	metaregions []hrpc.RegionInfo
	client gohbase.Client

}


func (hb *HBClient) CreateUsingRinfo(table string) {

	rinfo := plugin.RInfo()
	hb.host = rinfo.Rpath
	hb.table = table
	
	conf := rinfo.GetConf()
	for _, kv := range conf.GetKv() {
		if kv.GetKey() == "user" {
			hb.user = kv.GetValue()
		}
	}
	
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
	scanreq, _ := hrpc.NewScanStr(context.Background(), "hbase:meta", hrpc.Families(family), hrpc.Filters(f))
	scanner := hb.client.Scan(scanreq)
	
	for {
		r, err := scanner.Next()
		if err == io.EOF {
			break
		}
		region, _, err := region.ParseRegionInfo(r)
		if err != nil {
			fmt.Print(err)
			return
		}
		//fmt.Printf("addr = %s, region value = %s\n", addr, region.String())

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
