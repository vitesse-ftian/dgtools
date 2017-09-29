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
	"strconv"
	"errors"
)

var FIELD_SEPARATOR string = ","
var TOKEN_SEPARATOR string = "&"

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
		} else if kv.GetKey() == "field_separator" {
			FIELD_SEPARATOR = kv.GetValue()
		} else if kv.GetKey() == "token_separator" {
			TOKEN_SEPARATOR = kv.GetValue()
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
	regionstart := region.StartKey()
	regionend := region.StopKey()

	if len(regionstart) > 0 && len(regionend) > 0 && len(startrow) > 0 && len(endrow) > 0 {
		if bytes.Compare(endrow, regionstart) < 0 || bytes.Compare(regionend, startrow) < 0 {
			// out of range and ignore the region
			return nil, nil
		} else {
			if  bytes.Compare(regionstart, startrow) < 0 && bytes.Compare(startrow, regionend) < 0 {
				regionstart = startrow
			}
			if  bytes.Compare(regionstart, endrow) < 0 && bytes.Compare(endrow, regionend) < 0 {
				regionend = endrow
			}
		}			

	} else {

		if len(startrow) > 0 {
			if len(regionstart) > 0 {
				if  bytes.Compare(regionstart, startrow) < 0 && bytes.Compare(startrow, regionend) < 0 {
					regionstart = startrow
				}
			} else {
				regionstart = startrow
			}
		}
		
		if len(endrow) > 0 {
			if len(regionend) > 0 {
				if  bytes.Compare(regionstart, endrow) < 0 && bytes.Compare(endrow, regionend) < 0 {
					regionend = endrow
				}
			} else {
				regionend = endrow
			}
		}
	}
	
	scanRequest, err := hrpc.NewScanRange(context.Background(), table, regionstart, regionend, hrpc.Families(families), hrpc.Filters(pfilter))
	if err != nil {
		return nil, err
	}
	
	scanner := hb.client.Scan(scanRequest)
	return scanner, nil
}

func (hb *HBClient) newColumnCountGetFilter(param string) (filter.Filter, error) {
	var err error
	var limit int
	plugin.DbgLog("ColumnCountGetFilter")
	if limit, err = strconv.Atoi(param) ; err != nil {
		plugin.DbgLog("ColunCountGetFilter.  Invalid argument.  Integer is required. " + param)
		return nil, errors.New("ColumnCountGetFilter. Invalid argument.  Integer is required.")
	}
	return filter.NewColumnCountGetFilter(int32(limit)), nil
}

func (hb *HBClient) newColumnPaginationFilter(param string) (filter.Filter, error) {
	var err error
	var pp []string
	var limit, offset int
	plugin.DbgLog("ColumnPaginationFilter")
	if pp = strings.SplitN(param, FIELD_SEPARATOR, 3) ; len(pp) != 3 {
		plugin.DbgLog("ColumnPaginationFilter. Invalid argument. ColumnPaginationFilter=limit,offset,columnOffset. " + param)
		return nil, errors.New("ColumnPaginationFilter: Invalid argument. " + param)
	}
	if limit, err = strconv.Atoi(pp[0]) ; err != nil {
		return nil, errors.New("ColumnPaginationFilter: Invalid argument limit is an integer. e.g. limit,offset,columnOffset")
	}
	
	if offset, err = strconv.Atoi(pp[1]) ; err != nil {
		return nil, errors.New("ColumnPaginationFilter: Invalid argument. offset is an integer. e.g. limit,offset,columnOffset")
	}
	return filter.NewColumnPaginationFilter(int32(limit), int32(offset), []byte(pp[2])), nil
	
}

// mincol, maxcol []byte, bool, bool
func (hb *HBClient) newColumnRangeFilter(param string) (filter.Filter, error) {
	var err error
	var pp[]string
	var minColumnInclusive, maxColumnInclusive bool

	if pp = strings.SplitN(param, FIELD_SEPARATOR, 4) ; len(pp) != 4 {
		return nil, errors.New("ColumnRangeFilter: Invalid argument.  minColumn, maxColumn, minColumnInclusive, maxColumnExclusive")
	}

	if minColumnInclusive, err = strconv.ParseBool(pp[2]) ; err != nil {
		return nil, errors.New("ColumnRangeFilter: Invalid argument.  minColumnInclusive is boolean. Expect []byte,[]byte,bool,bool")
	}
	if maxColumnInclusive, err = strconv.ParseBool(pp[3]) ; err != nil {
		return nil, errors.New("ColumnRangeFilter: Invalid argument.  maxColumnInclusive is boolean. Expect []byte,[]byte,bool,bool")
	}
	
	return filter.NewColumnRangeFilter([]byte(pp[0]), []byte(pp[1]), minColumnInclusive, maxColumnInclusive), nil
}


func (hb *HBClient) getCompareOp(op string) (filter.CompareType, error) {
	switch op {
	case "lt":
		return filter.Less, nil
	case "le":
		return filter.LessOrEqual, nil
	case "eq":
		return filter.Equal, nil
	case "ne":
		return filter.NotEqual, nil
	case "ge":
		return filter.GreaterOrEqual, nil
	case "gt":
		return  filter.Greater, nil
	case "no":
		return filter.NoOp, nil
	}

	return filter.NoOp, errors.New("Invalid CompareOp")
}

func (hb *HBClient) getBitCompareOp(op string) (filter.BitComparatorBitwiseOp, error) {
	
	switch (op) {
	case "and":
		return filter.BitComparatorAND, nil
	case "or":
		return filter.BitComparatorOR, nil
	case "xor":
		return filter.BitComparatorXOR, nil
	}

	return 0, errors.New("Invalid Bitwise Comparator. " + op)
}

func (hb *HBClient) newCompareFilter(param string) (*filter.CompareFilter, error) {

	compareType, comparator, err := hb.newCompareComparator(param)
	
	if err != nil {
		return nil, err
	}

	return filter.NewCompareFilter(compareType, comparator), nil
}


func (hb *HBClient) newCompareComparator(param string) (filter.CompareType, filter.Comparator, error) {

	var compareType filter.CompareType
	var bitwiseop filter.BitComparatorBitwiseOp
	var err error
	var pp []string
	var key []byte

	idx := strings.Index(param, FIELD_SEPARATOR)
	if idx == -1 {
		return filter.NoOp, nil, errors.New("CompareFilter: Invalid parameters. " + param)
	}
	typ := param[:idx]

	switch typ {
	case "binary", "long", "binaryprefix":
		pp = strings.SplitN(param, FIELD_SEPARATOR, 3)
		if len(pp) != 3 {
			return filter.NoOp, nil, errors.New("CompareFilter: Invalid parameters. "  + param)
		}
	
		compareType, err = hb.getCompareOp(pp[1])
		if err != nil {
			return filter.NoOp, nil, errors.New("Invalid compare type " + pp[1] + ". e.g. lt, le, eq, gt, ge")
		}

		key = []byte(pp[2])


		switch typ {
		case "binary":
			return compareType, filter.NewBinaryComparator(filter.NewByteArrayComparable(key)), nil
		case "long":		
			return compareType, filter.NewLongComparator(filter.NewByteArrayComparable(key)), nil
		case "binaryprefix":
			return compareType, filter.NewBinaryPrefixComparator(filter.NewByteArrayComparable(key)), nil
		}
	case "bit":
		pp := strings.SplitN(param, FIELD_SEPARATOR, 4)
		if len(pp) != 4 {
			return filter.NoOp, nil, errors.New("CompareFilter: Invalid parameters. "  + param)
		}

		compareType, err = hb.getCompareOp(pp[1])
		if err != nil {
                        return filter.NoOp, nil, errors.New("Invalid compare type " + pp[1] + ". e.g. lt, le, eq, gt, ge")
                }


		bitwiseop, err = hb.getBitCompareOp(pp[2])
		if err != nil {
			return filter.NoOp, nil, errors.New("BitCompareFilter: Invalid compare type " + pp[2] + ". e.g. and, or, xor")
		}

		key = []byte(pp[3])
		return compareType, filter.NewBitComparator(bitwiseop, filter.NewByteArrayComparable(key)), nil
	case "substring":
		// substring, regex
		pp := strings.SplitN(param, FIELD_SEPARATOR, 3)
		if len(pp) != 3 {
			return filter.NoOp, nil, errors.New("CompareFilter: Invalid parameters. "  + param)
		}

                compareType, err = hb.getCompareOp(pp[1])
		if err != nil {
			return filter.NoOp, nil, errors.New("Invalid compare type " + pp[1] + ". e.g. lt, le, eq, gt, ge")
                }

		return compareType, filter.NewSubstringComparator(pp[2]), nil		
	case "regex":
		return filter.NoOp, nil, errors.New("Regex CompareFilter not supported yet.")	

	} 
	
	return filter.NoOp, nil, errors.New("CompareFilter not supported yet. " + typ)
}		



// comparefilter, cf, cq, bool
func (hb *HBClient) newDependentColumnFilter(param string) (filter.Filter, error) {

        idx := strings.Index(param, FIELD_SEPARATOR)
        if idx == -1 {
                return nil, errors.New("DependentColumnFilter: Invalid parameter. " + param)
        }
        cc := strings.SplitN(param[:idx], ":", 2)
        if len(cc) != 2 {
                return nil, errors.New("DependentColumnFilter: invalid column name. " + param[:idx])
        }

        filterp := param[idx+1:]

        comparefilter, err := hb.newCompareFilter(filterp)
        if err != nil {
                return nil, err
        }

        return filter.NewDependentColumnFilter(comparefilter, []byte(cc[0]), []byte(cc[1]), false), nil
}


// comparefilter
func (hb *HBClient) newFamilyFilter(param string) (filter.Filter, error) {
	cf, err := hb.newCompareFilter(param)
	if err != nil {
		return nil, err
	}
	return filter.NewFamilyFilter(cf), nil
}

// qualifiers[][]byte
func (hb *HBClient) newFirstKeyValueMatchingQualifiersFilter(param string) (filter.Filter, error) {
	pp := strings.Split(param, FIELD_SEPARATOR)
	qualifiers := make([][]byte, len(pp))
	for i, ppp := range pp {
		qualifiers[i] = []byte(ppp)
	}

	return filter.NewFirstKeyValueMatchingQualifiersFilter(qualifiers), nil
}

// bool
func (hb *HBClient) newKeyOnlyFilter(param string) (filter.Filter, error) {

	yesno, err := strconv.ParseBool(param)
	if err != nil {
		return nil, errors.New("KeyOnlyFilter: invalid argument.  Bool is required")
	}
	
	return filter.NewKeyOnlyFilter(yesno), nil
}


// [][]byte sortedPrefixes
func (hb *HBClient) newMultipleColumnPrefixFilter(param string) (filter.Filter, error) {
	pp := strings.Split(param, FIELD_SEPARATOR)
	prefixes := make([][]byte, len(pp))
	for i, ppp := range pp {
		prefixes[i] = []byte(ppp)
	}

	return filter.NewMultipleColumnPrefixFilter(prefixes), nil
}

// pageSize int64
func (hb *HBClient) newPageFilter(param string) (filter.Filter, error) {

	pagesize, err := strconv.ParseInt(param, 10, 64)
	if err != nil {
		plugin.DbgLog("PageFilter.  Invalid page size. " + param)
		return nil, err
	}
	return filter.NewPageFilter(pagesize), nil

}

// comparefilter
func (hb *HBClient) newQualifierFilter(param string) (filter.Filter, error) {
	cf, err := hb.newCompareFilter(param)
	if err != nil {
		return nil, err
	}
	return filter.NewQualifierFilter(cf), nil
}

// chance float32
func (hb *HBClient) newRandomRowFilter(param string) (filter.Filter, error) {
	chance, err := strconv.ParseFloat(param, 32)
	if err != nil {
		return nil, errors.New("RandomRowFilter: Invalid argument. float is required.")
	}
	return filter.NewRandomRowFilter(float32(chance)), nil
}


// comparefilter
func (hb *HBClient) newRowFilter(param string) (filter.Filter, error) {
	cf, err := hb.newCompareFilter(param)
	if err != nil {
		return nil, err
	}
	return filter.NewRowFilter(cf), nil
}

// columnFamily, columnQualifier [] byte, compareOp, comparatorObj, filterIfMissing, latestVersionOnly
func (hb *HBClient) newSingleColumnValueFilter(param string) (*filter.SingleColumnValueFilter, error) {
	

	idx := strings.Index(param, FIELD_SEPARATOR)
	if idx == -1 {
		return nil, errors.New("SingleColumnValueFilter: Invalid parameter. " + param)
	}
	cc := strings.SplitN(param[:idx], ":", 2)
	if len(cc) != 2 {
		return nil, errors.New("SingleColumnValueFilter: invalid column name. " + param[:idx])
	}

	filterp := param[idx+1:]
	
	compareType, comparator, err := hb.newCompareComparator(filterp)
	if err != nil {
		return nil, err
	}
	
	return filter.NewSingleColumnValueFilter([]byte(cc[0]), []byte(cc[1]), compareType, comparator, true, true), nil
}


// columnFamily, columnQualifier [] byte, compareOp, comparatorObj, filterIfMissing, latestVersionOnly
func (hb *HBClient) newSingleColumnValueExcludeFilter(param string) (filter.Filter, error) {
	

	f, err := hb.newSingleColumnValueFilter(param)
	if err != nil {
		return nil, err
	}
	return filter.NewSingleColumnValueExcludeFilter(f), nil
}


// filter
func (hb *HBClient) newSkipFilter(param string) (filter.Filter, error) {

	return nil, errors.New("SkipFilter not supported yet.")
}

// timestamps []int64
func (hb *HBClient) newTimestampsFilter(param string) (filter.Filter, error) {
	pp := strings.Split(param, FIELD_SEPARATOR)
	tsarray := make([]int64, len(pp))
	for i, ppp := range pp {
		ts, err := strconv.ParseInt(ppp, 10, 64)
		if err != nil {
			return nil, errors.New("TimestampsFilter: invalid argument. int array is requested")
		}
		tsarray[i] = ts
	}

	return filter.NewTimestampsFilter(tsarray), nil
	//return nil, errors.New("TimestampsFilter not supported yet.")
}

// compareFilter
func (hb *HBClient) newValueFilter(param string) (filter.Filter, error) {
	cf, err := hb.newCompareFilter(param)
	if err != nil {
		return nil, err
	}
	return filter.NewValueFilter(cf), nil
}

// matchingFilter
func (hb *HBClient) newWhileMatchFilter(param string) (filter.Filter, error) {

	return nil, errors.New("WhileMatchFilter not supported yet.")
}

// startRow, stopRow []byte, startRowInclusive, stopRowInclusive bool
func (hb *HBClient) NewRowRange(param string) (*filter.RowRange, error) {
	pp := strings.SplitN(param, FIELD_SEPARATOR, 4)
	if len(pp) != 4 {
		return nil, errors.New("RowRange: invalid argument. startrow,stoprow,bool,bool")
	}

	startrow := []byte(pp[0])
	stoprow := []byte(pp[1])
	startRowInclusive, err := strconv.ParseBool(pp[2])
	if err != nil {
		return nil, errors.New("RowRange: startRowInclusive bool expected")
	}
	stopRowInclusive, err := strconv.ParseBool(pp[3])
	if err != nil {
		return nil, errors.New("RowRange: stopRowInclusive bool expected")
	}	
	
	return filter.NewRowRange(startrow, stoprow, startRowInclusive, stopRowInclusive), nil
}

//rowRangeList[]*RowRange
func (hb *HBClient) newMultiRowRangeFilter(param string) (filter.Filter, error) {

	return nil, errors.New("MultiRowRangeFilter not supported yet.")
}

// filter[]byte, second []byte
func (hb *HBClient) newBytesBytesPair(param string) (filter.Filter, error) {

	return nil, errors.New("BytesBytesPair not supported yet.")
}

// pairs []*BytesBytesPair
func (hb *HBClient) newFuzzyRowFilter(param string) (filter.Filter, error) {

	return nil, errors.New("FuzzyRowFilter not supported yet.")
}


func (hb *HBClient) NewFilter(filtername string, param string) (filter.Filter, error) {

	switch filtername {
	case "ColumnCountGetFilter":
		return hb.newColumnCountGetFilter(param)
	case "ColumnPaginationFilter":
		return hb.newColumnPaginationFilter(param)
	case "ColumnPrefixFilter":
		plugin.DbgLog("ColumnPrefixFilter")
		return filter.NewColumnPrefixFilter([]byte(param)), nil
	case "ColumnRangeFilter":
		plugin.DbgLog("ColumnRangeFilter")
		return hb.newColumnRangeFilter(param)
	case "DependentColumnFilter":
		plugin.DbgLog("DependentColumnFilter")
		return hb.newDependentColumnFilter(param)
	case "FamilyFilter":
		plugin.DbgLog("FamilyFilter")
		return hb.newFamilyFilter(param)
	case "FirstKeyOnlyFilter":
		plugin.DbgLog("FirstKeyOnlyFilter")
		return filter.NewFirstKeyOnlyFilter(), nil
	case "FirstKeyValueMatchingQualifiersFilter":
		plugin.DbgLog("FirstKeyValueMatchingQualifierFilter")
		return hb.newFirstKeyValueMatchingQualifiersFilter(param)
	case "InclusiveStopFilter":
		plugin.DbgLog("InclusiveStopFilter")
		return filter.NewInclusiveStopFilter([]byte(param)), nil
	case "KeyOnlyFilter":
		plugin.DbgLog("KeyOnlyFilter")
		return hb.newKeyOnlyFilter(param)
	case "MultipleColumnPrefixFilter":
		plugin.DbgLog("MultipleColumnPrefixFilter")
		return hb.newMultipleColumnPrefixFilter(param)
	case "PageFilter":
		plugin.DbgLog("PageFilter")
		return hb.newPageFilter(param)
	case "PrefixFilter":
		plugin.DbgLog("PrefixFilter")
		return filter.NewPrefixFilter([]byte(param)), nil
	case "QualifierFilter":
		plugin.DbgLog("QualifierFilter")
		return hb.newQualifierFilter(param)
	case "RandomRowFilter":
		plugin.DbgLog("RandomRowFilter")
		return hb.newRandomRowFilter(param)
	case "RowFilter":
		plugin.DbgLog("RowFilter")
		return hb.newRowFilter(param)
	case "SingleColumnValueFilter":
		plugin.DbgLog("SingleColumnValueFilter")
		return hb.newSingleColumnValueFilter(param)
	case "SingleColumnValueExcludeFilter":
		plugin.DbgLog("SingleColumnValueExcludeFilter")
		return hb.newSingleColumnValueExcludeFilter(param)
	case "SkipFilter":
		plugin.DbgLog("SkipFilter")
		return hb.newSkipFilter(param)
	case "TimestampsFilter":
		plugin.DbgLog("TimestampsFilter")
		return hb.newTimestampsFilter(param)
	case "ValueFilter":
		plugin.DbgLog("ValueFilter")
		return hb.newValueFilter(param)
	case "WhileMatchFilter":
		plugin.DbgLog("WhileMatchFilter")
		return hb.newWhileMatchFilter(param)
	case "MultiRowRangeFilter":
		plugin.DbgLog("MultiRowRangeFilter")
		return hb.newMultiRowRangeFilter(param)
	case "BytesBytesPair":
		plugin.DbgLog("BytesBytesPair")
		return hb.newBytesBytesPair(param)
	case "FuzzyRowFilter":
		plugin.DbgLog("FuzzyRowFilter")
		return hb.newFuzzyRowFilter(param)
	}

	
	return nil, errors.New("Filter not supported. " + filtername)

}
