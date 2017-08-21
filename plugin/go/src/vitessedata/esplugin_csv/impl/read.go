package impl

import (
	"fmt"
	"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
	//"os"
	//"time"
	"vitessedata/plugin"
)


func GetPreferenceShards(shards []int) string {
	if len(shards) == 0 {
		return ""
	}
	s := "_shards:"
	for i := 0 ; i < len(shards) ; i++ {
		s = fmt.Sprintf("%s%d", s, shards[i])
		if i < len(shards) - 1 {
			s = fmt.Sprintf("%s,", s)
		}
	}
	return s
}

// DoRead servies XDrive read requests.   It read a ReadRequest from stdin and reply
// a sequence of PluginDataReply to stdout.   It should end the data stream with a
// trivial (Errcode == 0, but there is no data) message.
func DoRead() error {
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
	preference := GetPreferenceShards(shards)
	plugin.DbgLog("shards preference: %s", preference)	

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
	var query string
	for _, f := range req.Filter {
		// f cannot be nil
		if f.Op == "QUERY" {
			query= f.Args[0]
		}
	}

	body, err := es.Search(es.Index, "", preference, query)
	if err != nil {
		plugin.DbgLogIfErr(err, "ElasticSearch failed. Error %v", err)
		plugin.ReplyError(-2, "elasticSearch access failed: " + err.Error())
		return err
	}

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
