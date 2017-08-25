package impl

import (
	"fmt"
	"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
	//"os"
	//"time"
	"strings"
	"vitessedata/plugin"
)

// the reference URI search protocol can be found on the below link.
// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-uri-request.html
//
// DoSample servies XDrive read requests.   It read a ReadRequest from stdin and reply
// a sequence of PluginDataReply to stdout.   It should end the data stream with a
// trivial (Errcode == 0, but there is no data) message.
func DoSample() error {
	var req xdrive.ReadRequest
	err := plugin.DelimRead(&req)
	if err != nil {
		plugin.DbgLogIfErr(err, "Delim read req failed.")
		return err
	}

	// Check/validate frag info.  Again, not necessary, as xdriver server should always
	// fill in good value.
	if req.FragCnt <= 0 || req.FragId < 0 || req.FragId >= req.FragCnt {
		plugin.DbgLog("Invalid read req %v", req)
		plugin.ReplyError(-3, fmt.Sprintf("Read request frag (%d, %d) is not valid.", req.FragId, req.FragCnt))
		return fmt.Errorf("Invalid read request")
	}

	var es ESClient
	es.CreateUsingRinfo()

	shards := es.GetShards(req.FragId, req.FragCnt)

	if len(shards) == 0 {
		// return 0 record
		return nil
	}
	preference := es.GetPreferenceShards(shards)
	plugin.DbgLog("shards preference: %s", preference)	
	
	params := make(map[string]string)
	params["preference"] = preference
	params["timeout"] = "30000"
	//
	// Filter:
	// req may contains a list of Filters that got pushed down from XDrive server.
	// As per plugin protocol, plugin can ignore all of them if they choose to be
	// lazy.  See comments in csvhandler.go.
	//
	// All filters are derived from SQL (where clause).  There is a special kind of
	// filter called "QUERY", which allow users to send any query to plugin.  Here as
	// an example, we implement a poorman's fault injection.
	//
	var query, _type string
	for _, f := range req.Filter {
		// f cannot be nil
		if f.Op == "QUERY" {
			query= f.Args[0]
			p := strings.Split(query, "&")

			for _, pp := range p {
				plugin.DbgLog(pp)
				ppp := strings.SplitN(pp, "=", 2)
				if len(ppp) == 2 {
					params[ppp[0]] = ppp[1]
				}
			}

		} else if f.Column == "_type" && f.Op == "==" {
			_type = f.Args[0]
		} else {
			params[f.Column] = f.Args[0]
		}
	}


	body, err := es.Search(es.Index, _type, params)
	if err != nil {
		plugin.DbgLogIfErr(err, "ElasticSearch failed. Error %v", err)
		plugin.ReplyError(-2, "elasticSearch access failed: " + err.Error())
		return err
	}
/*
	plugin.DbgLog("type=%s",  _type)

	body := []byte(`{"timed_out":false, "took":1, "_shards":{}, "hits":{ "total":1, "hits":[{"_id":"1", "_type":"online", "_score":"1", "_routing":"vip", "_source":{ "age":12, "gender":"female", "name":"eric"}}]}}`)
*/
	plugin.DbgLog(string(body))


	var reader ESReader
	reader.Init(req.Filespec, req.Columndesc, req.Columnlist)

	err = reader.process(body)
	if err != nil {
		plugin.DbgLogIfErr(err, "Parse Json result failed.")
		plugin.ReplyError(-20, "JSON result has invalid data")
		return err
	}

	// Done!   Fill in an empty reply, indicating end of stream.
	plugin.ReplyError(0, "")
	return nil
}