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
	"bytes"
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

func (hb *HBClient) Scan(region hrpc.RegionInfo, startrow []byte, endrow []byte, families map[string][]string, pfilter filter.Filter) (hrpc.Scanner, error) {
	
	table := region.Table()
	startkey := region.StartKey()
	endkey := region.StopKey()
	

	if len(startkey) > 0 {
		if  bytes.Compare(startkey, startrow) < 0 && bytes.Compare(startrow, endkey) < 0 {
			startkey = startrow
		}
	} else {
		startkey = startrow
	}

	if len(endkey) > 0 {
		if  bytes.Compare(startkey, endrow) < 0 && bytes.Compare(endrow, endkey) < 0 {
			endkey = endrow
		}
	} else {
		endkey = endrow
	}

	scanRequest, err := hrpc.NewScanRange(context.Background(), table, startkey, endkey, hrpc.Families(families), hrpc.Filters(pfilter))
	if err != nil {
		return nil, err
	}
	
	scanner := hb.client.Scan(scanRequest)
	return scanner, nil
}

func (hb *HBClient) NewFilter(filtername string, param string) (filter.Filter, error) {
	switch filtername {
	case "ColumnCountGetFilter":
		plugin.DbgLog("ColumnCountGetFilter")
		//return filter.NewColumnCountGetFilter(limit int32)
	case "ColumnPaginationFilter":
		plugin.DbgLog("ColumnPaginationFilter")
		//return filter.NewColumnPaginationFilter(limit offset int32, columnoffset []byte)
	case "ColumnPrefixFilter":
		plugin.DbgLog("ColumnPrefixFilter")
		return filter.NewColumnPrefixFilter([]byte(param)), nil
	case "ColumnRangeFilter":
		plugin.DbgLog("ColumnRangeFilter")
		//return filter.NewColumnRangeFilter(mincol, maxcol []byte, false, false)
	case "CompareFilter":
		plugin.DbgLog("CompareFilter")
		//return filter.NewCompareFilter(compareOp, comparetor)
	case "DependentColumnFilter":
		plugin.DbgLog("DependentColumnFilter")
		//return filter.NewDependentColumnFilter(comparefilter, cf, cq, true)
	case "FamilyFilter":
		plugin.DbgLog("FamilyFilter")
		//return filter.NewFamilyFilter(comprefilter)
	case "FirstKeyOnlyFilter":
		plugin.DbgLog("FirstKeyOnlyFilter")
		return filter.NewFirstKeyOnlyFilter(), nil
	case "FirstKeyValueMatchingQualifiersFilter":
		plugin.DbgLog("FirstKeyValueMatchingQualifierFilter")
		//return filter.NewFirstKeyValueMatchingQualifiersFilter(qualifiers[][]byte)
	case "InclusiveStopFilter":
		plugin.DbgLog("InclusiveStopFilter")
		return filter.NewInclusiveStopFilter([]byte(param)), nil
	case "KeyOnlyFilter":
		plugin.DbgLog("KeyOnlyFilter")
		return filter.NewKeyOnlyFilter(true), nil
	case "MultipleColumnPrefixFilter":
		plugin.DbgLog("MultipleColumnPrefixFilter")
		//return filter.NewMultipleColumnPrefixFilter(sortedPrefixes)
	case "NewPageFilter":
		plugin.DbgLog("PageFilter")
		//return filter.NewPageFilter(pageSize)
	case "PrefixFilter":
		plugin.DbgLog("PrefixFilter")
		return filter.NewPrefixFilter([]byte(param)), nil
	case "QualifierFilter":
		plugin.DbgLog("QualifierFilter")
		//return filter.NewQualifierFilter(compareFilter)
	case "RandomRowFilter":
		plugin.DbgLog("RandomRowFilter")
		//return filter.NewRandomRowFilter(chance)
	case "RowFilter":
		plugin.DbgLog("RowFilter")
		//return filter.NewRowFilter(compareFilter)
	case "SingleColumnValueFilter":
		plugin.DbgLog("SingleColumnValueFilter")
		//return filter.NewSingleColumnValueFilter
	case "SingleColumnValueExcludeFilter":
		plugin.DbgLog("SingleColumnValueExcludeFilter")
		//return filter.NewColumnValueExcludeFilter
	case "SkipFilter":
		plugin.DbgLog("SkipFilter")
		//return filter.NewSkipFilter(skippingFilter)
	case "TimstampsFilter":
		plugin.DbgLog("TimestampsFilter")
		//return filter.newTimestampsFilter(timestamps)
	case "ValueFilter":
		plugin.DbgLog("ValueFilter")
		//return filter.NewValueFilter(compareFilter)
	case "WhileMatchFilter":
		plugin.DbgLog("WhileMatchFilter")
		//return filter.NewWhileMatchFilter(matchingFilter)
	case "AllFilter":
		plugin.DbgLog("AllFilter")
		//return filter.NewAllFilter()
	case "RowRange":
		plugin.DbgLog("RowRange")
		//return filter.NewRowRange(startrow, stoprow, false, false)
	case "MultiRowRangeFilter":
		plugin.DbgLog("MultiRowRangeFilter")
		//return filter.NewMultiRowRangeFilter(rowRangeList)
	}

	
	return nil, nil

}
