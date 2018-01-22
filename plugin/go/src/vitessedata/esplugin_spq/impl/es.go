package impl

import (
	"github.com/smartystreets/go-aws-auth"
	"net/http"
	"net/url"
	"fmt"
	"io/ioutil"
	"bytes"
	"strconv"
	"vitessedata/plugin"
	"errors"
)

type ESClient struct {
	Index string
	NShards int
	Uri string
	AccessKeyID string
	SecretAccessKey string
	SecurityToken string
}


const IndexField string = "_index"
const TypeField string = "_type"
const SourceField string = "_source"
const RoutingField string = "_routing"
const ScoreField string = "_score"
const TimedOutField string = "timed_out"
const HitsField string = "hits"
const ShardsField string = "_shards"
const TookField string = "took"
const TotalField string = "total"
const IdField string = "_id"
const ErrorsField string = "errors"
const ItemsField string = "items"
const IndexActionField string = "index"
const UpdateActionField string = "update"
const CreateActionField string = "create"
const DeleteActionField string = "delete"


func (es *ESClient) CreateUsingRinfo() {
	
	rinfo := plugin.RInfo()
	es.Uri = rinfo.Rpath
	
	conf := rinfo.GetConf()
	for _, kv := range conf.GetKv() {
		if kv.GetKey() == "index" {
			es.Index = kv.GetValue()
		} else if kv.GetKey() == "nshards" {
			es.NShards, _ = strconv.Atoi(kv.GetValue())
		} else if kv.GetKey() == "access_key_id" {
			es.AccessKeyID = kv.GetValue()
		} else if kv.GetKey() == "secret_access_key" {
			es.SecretAccessKey = kv.GetValue()
		} else if kv.GetKey() == "security_token" {
			es.SecurityToken = kv.GetValue()
		}
	}	

	plugin.DbgLog("URI = %s", es.Uri)
	plugin.DbgLog("Index = %s", es.Index)
	plugin.DbgLog("Shard = %d", es.NShards)

	plugin.FatalIf(es.Index == "" || es.NShards == 0, "ES requires index, nshards, access_key_id, secret_access_key and security_token")
	
}



func (es *ESClient) GetPreferenceShards(shards []int) string {
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


func (es *ESClient) GetShards(fragid, fragcnt int32) []int {
	var shards [] int

	for i := 0 ; i < es.NShards; i++  {

		ii := i %  int(fragcnt)
		if int(fragid) == ii {
			shards = append(shards, i)
		}
	}
	
	return shards
}

func (es *ESClient) makeURL(action string, index string, _type string, params map[string]string) (string, awsauth.Credentials) {
	var endpoint, path string

	cred := awsauth.Credentials{ AccessKeyID: es.AccessKeyID,
			SecretAccessKey: es.SecretAccessKey, }
		//SecurityToken: es.SecurityToken, }

	if (action == "search") {
		endpoint = "/_search"
	} else if (action == "count") {
		endpoint = "/_count"
	} else if (action == "bulk") {
		endpoint = "/_bulk"
	}

	if (_type == "") {
		path = fmt.Sprintf("/%s%s", index, endpoint)	
	} else {
		path = fmt.Sprintf("/%s/%s%s", index, _type, endpoint)	
	}

	data := url.Values{}

	if params != nil {
		for k, v := range params {
			data.Add(k, v)
		}
	}

	u, _ := url.ParseRequestURI(es.Uri)
	u.Path = path
	u.RawQuery = data.Encode()
	urlStr := fmt.Sprintf("%v", u)

	plugin.DbgLog("Request URL = %s", urlStr)

	return urlStr, cred

}

func (es *ESClient) Search(index string, _type string, params map[string]string) ([] byte, error) {

	urlStr, cred := es.makeURL("search", index, _type, params)
	client := new(http.Client)
	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		plugin.DbgLog(err.Error())
		return nil, err
	}
	awsauth.Sign4(req, cred)
	resp, err := client.Do(req)
       	if err != nil {
		plugin.DbgLog(err.Error())
                return nil, err
        }
        defer resp.Body.Close()

	plugin.DbgLog("HTTP request:", urlStr)
        plugin.DbgLog("response Status:", resp.Status)
        plugin.DbgLog("response Headers:", resp.Header)
        body, err := ioutil.ReadAll(resp.Body)
        plugin.DbgLog("response Body:", string(body))
	if err != nil {
		return nil, err
	}
		
	if resp.StatusCode != 200 {
		return nil, errors.New(string(body))
	}

	return body, nil
}

func (es *ESClient) Count(index string, _type string, params map[string]string) ([] byte, error) {

	urlStr, cred := es.makeURL("count", index, _type, params)
	client := new(http.Client)
	req, err := http.NewRequest("GET", urlStr, nil)
	awsauth.Sign4(req, cred)
	resp, err := client.Do(req)
       	if err != nil {
                return nil, err
        }
        defer resp.Body.Close()

        plugin.DbgLog("response Status:", resp.Status)
        plugin.DbgLog("response Headers:", resp.Header)
        body, err2 := ioutil.ReadAll(resp.Body)
        plugin.DbgLog("response Body:", string(body))

	return body, err2
}

func (es *ESClient) Bulk(index string, _type string, json *bytes.Buffer) ([] byte, error) {

	urlStr, cred := es.makeURL("bulk", index, _type, nil)
	client := new(http.Client)
	req, err := http.NewRequest("POST", urlStr, json)
	req.Header.Set("Content-Type", "application/x-ndjson")
	awsauth.Sign4(req, cred)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	plugin.DbgLog("response Status:", resp.Status)
	plugin.DbgLog("response Headers:", resp.Header)
	body, err2 := ioutil.ReadAll(resp.Body)
	plugin.DbgLog("response Body:", string(body))


	return body, err2
} 
