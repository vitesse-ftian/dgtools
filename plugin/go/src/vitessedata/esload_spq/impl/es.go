package impl

import (
	"github.com/smartystreets/go-aws-auth"
	"net/http"
	"net/url"
	"fmt"
	"io/ioutil"
	"bytes"
	"vitessedata/plugin"
	"errors"
	"strings"
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
const ScrollIdField string = "_scroll_id"


func (es *ESClient) Init(es_url, index string, nshards int, aws_access_id, aws_access_key string) {
	
	es.Uri = es_url
	es.Index = index
	es.NShards = nshards
	es.AccessKeyID = aws_access_id
	es.SecretAccessKey = aws_access_key

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

	if (index == "") {
		path = fmt.Sprintf("%s/scroll", endpoint)
	} else {
		if (_type == "") {
			path = fmt.Sprintf("/%s%s", index, endpoint)	
		} else {
			path = fmt.Sprintf("/%s/%s%s", index, _type, endpoint)	
		}
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

func (es *ESClient) Scroll(index string, param map[string]string, json *bytes.Buffer) ([] byte, error) {

	urlStr, cred := es.makeURL("search", index, "", param)
	plugin.DbgLog("request URL:", urlStr)
	plugin.DbgLog("request JSON:", json)
	client := new(http.Client)
	req, err := http.NewRequest("POST",urlStr, json)
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	awsauth.Sign4(req, cred)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	
	defer resp.Body.Close()

	plugin.DbgLog("response Status:", resp.Status)
        plugin.DbgLog("response Headers:", resp.Header)
        body, err2 := ioutil.ReadAll(resp.Body)
        //plugin.DbgLog("response Body:", string(body))

	if err2 != nil {
		return body, err2;
	}

	if ! strings.HasPrefix(resp.Status, "200") {
		return body, errors.New(string(body))
	}

        return body, err2

}
