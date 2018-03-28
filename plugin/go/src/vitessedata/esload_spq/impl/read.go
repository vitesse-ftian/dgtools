package impl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
	//"os"
	//"time"
	"strings"
//	"strconv"
	"vitessedata/plugin"
)

// the reference URI search protocol can be found on the below link.
// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-uri-request.html
//
// DoRead servies XDrive read requests.   It read a ReadRequest from stdin and reply
// a sequence of PluginDataReply to stdout.   It should end the data stream with a
// trivial (Errcode == 0, but there is no data) message.
func DoRead(req xdrive.ReadRequest, es_url, indexname string, nshards int, aws_access_id, aws_access_key string) error {
	var err error
	var es ESClient

	es.Init(es_url, indexname, nshards, aws_access_id, aws_access_key)
	// Check/validate frag info.  Again, not necessary, as xdriver server should always
	// fill in good value.
	if req.FragCnt <= 0 || req.FragId < 0 || req.FragId >= req.FragCnt {
		plugin.DbgLog("Invalid read req %v", req)
		plugin.DataReply(-3, fmt.Sprintf("Read request frag (%d, %d) is not valid.", req.FragId, req.FragCnt))
		return fmt.Errorf("Invalid read request")
	}


/*
	var _type string
	default_size := 10
	default_timeout := "30s"
*/

	params := make(map[string]string)
	params["scroll"] = "1m"

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
/*
	for _, f := range req.Filter {
		// f cannot be nil
		if f.Op == "QUERY" {
			p := strings.Split(f.Args[0], "&")

			for _, pp := range p {
				plugin.DbgLog(pp)
				ppp := strings.SplitN(pp, "=", 2)
				if len(ppp) == 2 {
					switch ppp[0] {
					case "_type":
						_type = ppp[1]
					case "size":
						default_size, err = strconv.Atoi(ppp[1])
						if err != nil {
							plugin.DataReply(-100, "Invalid size " + ppp[1])
							return err
						}
						params[ppp[0]] = ppp[1]
					default:
						params[ppp[0]] = ppp[1]
					}
				}
			}

		} 
	}
*/
	var buf bytes.Buffer
	
	default_size := 1024
	searchbody := make(map[string]interface{})
	slice := make(map[string]interface{})
	slice["id"] = req.FragId
	slice["max"] = req.FragCnt
	searchbody["slice"] = slice
	searchbody["size"] = default_size

	s, _ := json.Marshal(searchbody)
	buf.Write(s)

	_, body, err := es.Scroll(es.Index, params, &buf)
	if err != nil {
		plugin.DbgLogIfErr(err, "ElasticSearch failed. Error %v", err)
		plugin.DataReply(-2, "elasticSearch access failed: " + err.Error())
		return err
	}
/*
	plugin.DbgLog("type=%s",  _type)

	body := []byte(`{"timed_out":false, "took":1, "_shards":{}, "hits":{ "total":1, "hits":[{"_id":"1", "_type":"online", "_score":"1", "_routing":"vip", "_source":{ "age":12, "gender":"female", "name":"eric"}}]}}`)
*/
	//plugin.DbgLog(string(body))
	
	var reader ESReader
	reader.Init(req.Filespec, req.Columndesc, req.Columnlist)

	scroll_id, nrow, err := reader.process(body, default_size)
	if err != nil {
		plugin.DbgLogIfErr(err, "Parse Json result failed.")
		plugin.DataReply(-20, "JSON result has invalid data")
		return err
	}

	for ; nrow > 0 ; {
	        searchbody := make(map[string]interface{})
		searchbody["scroll_id"] = scroll_id
		searchbody["scroll"] = "1m"

	  	var buf bytes.Buffer
		s, _ := json.Marshal(searchbody)
		buf.Write(s)

		status, body, err := es.Scroll("", nil, &buf)
		if err != nil {
                	plugin.DbgLogIfErr(err, "ElasticSearch failed. Error %v", err)
                	plugin.DataReply(-2, "elasticSearch access failed: " + err.Error())
                	return err
        	}

		if ! strings.HasPrefix(status, "200 ") {
			plugin.DbgLog("ElasticSearch Error: " + status)
			break
		}

		//plugin.DbgLog(string(body))
		scroll_id, nrow, err = reader.process(body, default_size)
        	if err != nil {
                	plugin.DbgLogIfErr(err, "Parse Json result failed.")
                	plugin.DataReply(-20, "JSON result has invalid data")
                	return err
        	}

	}
	// Done!   Fill in an empty reply, indicating end of stream.
	plugin.DataReply(0, "")
	return nil
}
