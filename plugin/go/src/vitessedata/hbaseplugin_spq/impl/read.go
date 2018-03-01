package impl

import (
	"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
	"fmt"
	"io"
	"strconv"
	"strings"
	"errors"
	"vitessedata/plugin"
//        "github.com/tsuna/gohbase"
        "github.com/tsuna/gohbase/hrpc"
        "github.com/tsuna/gohbase/filter"
//        "github.com/tsuna/gohbase/region"
)

func DoRead(req xdrive.ReadRequest, hbasehost, user, field_separator, token_separator string) error {
	var err error

	if req.FragCnt <= 0 || req.FragId < 0 || req.FragId >= req.FragCnt {
		plugin.DbgLog("Invalid read req %v", req)
		plugin.DataReply(-3, fmt.Sprintf("Read request frag (%d, %d) is not valid.", req.FragId, req.FragCnt))
		return fmt.Errorf("Invalid read request")
	}

	idx := strings.LastIndex(req.Filespec.Path, "/")
	table := req.Filespec.Path[idx+1:]

	var hbase HBClient
	hbase.Init(hbasehost, user, table, field_separator, token_separator)

	regions := hbase.GetRegions(req.FragId, req.FragCnt)

	/*
	{"COLUMNS" : ["cf:a", "cf:b"], "FILTERS": [{"PrefixFilter": ["row2"]}, {"QualifierFilter" : [">=", "binary:xyz"] } , { "TimestampsFilter": [123, 456]}],
	  "LIMIT" : 5, "STARTROW": "row1", "ENDROW": "rowN", "TIMERANGE" : [123, 456]}

        column=cf:a&column=cf:b&limit=5&startrow=row1&endrow=rowN&timerange=starttime,endtime&prefixfilter=row2&qualifierfilter=ge,binary:xyz&timestampsfilter=123,456
        */
	families := make(map[string][]string)
	filters := filter.NewList(filter.MustPassAll)
	//filters := filter.NewList(filter.MustPassOne)
	filtercnt := 0

	var rowrangelist []*filter.RowRange
	var srow, erow []byte
	var stime, etime int64
	var query string
	var limit, offset int
	
	for _, f := range req.Filter {
		// f cannot be nil
		if f.Op == "QUERY" {
			query = f.Args[0]
		}
	}

	if query != "" {
		p := strings.Split(query, TOKEN_SEPARATOR)
		for _, pp := range p {
			plugin.DbgLog(pp)
			ppp := strings.SplitN(pp, "=", 2)
			if len(ppp) == 2 {
				switch ppp[0] {
				case "column":
					cc := strings.SplitN(ppp[1], ":", 2)
					if len(cc) != 2 {
						plugin.DataReply(-100, "Invalid column. Family + Qualifier. " + ppp[1])
						return fmt.Errorf("Invalid column. Family + Qualifier. " + ppp[1])
					}
					a := families[cc[0]]
					families[cc[0]] = append(a, cc[1])
					plugin.DbgLog("families %v ", families)
				case "offset":
					offset, err = strconv.Atoi(ppp[1])
					if err != nil {
						plugin.DataReply(-100, "Invalid offset datatype.  integer is required. " + ppp[1])
						return err
					}
					plugin.DbgLog("offset = %d", offset)
				case "limit":
					limit, err = strconv.Atoi(ppp[1])
					if err != nil {
						plugin.DataReply(-100, "Invalid limit datatype.  integer is required. " + ppp[1])
						return err
					}
					plugin.DbgLog("limit = %d", limit)
				case "startrow":
					srow = []byte(ppp[1])
					plugin.DbgLog("startrow = %s", string(srow))
				case "stoprow":
					erow = []byte(ppp[1])
					plugin.DbgLog("stoprow = %s", string(erow))
				case "timerange":
					tt := strings.SplitN(ppp[1], FIELD_SEPARATOR, 2)
					if len(tt) != 2 {
						plugin.DataReply(-100, "Invalid timerange. format: starttime,endtime. " + ppp[1])
						return errors.New("Invalid timerange")
					}
					stime, err = strconv.ParseInt(tt[0], 10, 64)
					if err != nil {
						plugin.DataReply(-100, "Invalid timerange. starttime invalid. " + ppp[1])
						return err
					}
					etime, err = strconv.ParseInt(tt[1], 10, 64)
					if err != nil {
						plugin.DataReply(-100, "Invalid timerange. endtime invalid. " + ppp[1])
						return err
					}						
					plugin.DbgLog("stime = %d, etime = %d", stime, etime)
				default:
					if strings.HasSuffix(ppp[0], "Filter") {
						plugin.DbgLog("filter %s = %s", ppp[0], ppp[1])
						
						if ppp[0] == "RowRangeFilter" {
							rowrange, err := hbase.NewRowRange(ppp[1])
							if err != nil {
								plugin.DataReply(-100, err.Error())
								return err
							}
							rowrangelist = append(rowrangelist, rowrange)
						} else {

							filter, err := hbase.NewFilter(ppp[0], ppp[1])
							if err != nil {
								plugin.DataReply(-100, err.Error())
								return err
							}
							if filter != nil {
								filters.AddFilters(filter)
								filtercnt++
							}
						}
					} else {
						plugin.DataReply(-100, "Invalid argument. " + ppp[0] + ": " + ppp[1])
						return errors.New("Invalid argument " + ppp[0] + ": " + ppp[1])
					}
				}
			}
		}
	}

	// families
	plugin.DbgLog("Families %v", families)
	// filter
	// rowrange
	if len(rowrangelist)  > 0 {
		filters.AddFilters(filter.NewMultiRowRangeFilter(rowrangelist))
		filtercnt++
	}
		

	plugin.DbgLog("Filters %v", filters)

	var writer HBWriter
	writer.Init(req.Filespec, req.Columndesc, req.Columnlist)


	for _, rg := range regions {
		var scanner hrpc.Scanner
		if filtercnt == 0  {
			scanner, err = hbase.Scan(rg, srow, erow, families, nil, uint64(stime), uint64(etime), uint32(limit), uint32(offset))
		} else {
			scanner, err = hbase.Scan(rg, srow, erow, families, filters, uint64(stime), uint64(etime), uint32(limit), uint32(offset))
		}

		if err != nil {
			plugin.DataReply(-100, err.Error())
			return err
		}

		if scanner == nil {
			// skip this region
			plugin.DbgLog("Scanner skipped. Skip this region")
			continue
		}

		for {
			r, err := scanner.Next()
			if err != nil {
				if err == io.EOF {
					break
				} else {
					plugin.DbgLog("scan next failed. %v", err)
					plugin.DataReply(-100, err.Error())
					return err
				}
			}
			writer.Write(r)
		}
	}

	err = writer.Close()
	if err != nil {
		plugin.DataReply(-100, err.Error())
		return err
	}
	plugin.DataReply(0, "")
	return nil
}
