package main

import (
        "fmt"
        //"vitessedata/plugin"
        //"vitessedata/esplugin_csv/impl"
	"./impl"
	"github.com/buger/jsonparser"
)



func main() {
/*
esrespath := [][]string {
		[]string{"took"},
		[]string{"_shards"},
		[]string{"hits", "hits"},
}
*/
eshitpath := [][]string {
		[]string{"_index"},
		[]string{"_type"},
		[]string{"_id"},
		[]string{"_score"},
		[]string{"_source", "name"},
		[]string{"_source", "email"},
}

	var esrespath [][]string
	esrespath = [][]string {
		[]string{"took"},
		[]string{"_shards"},
		[]string{"hits"},
	}

//	esrespath = append(esrespath, []string{"took"}, []string{"_shards"}, []string{"hits", "hits"})

	fmt.Println(len(esrespath))
	for k := 0; k < len(esrespath) ;  k++ {
		fmt.Println(esrespath[k][0])
	}

	es := impl.ESClient{"customer", 5, "http://localhost:9200", "access_key_id", "access_key", "token"}
	//u, err := es.Search("index", "type", "_shards:0", "user apple*")

	params := make(map[string]string)
	params["size"] = "3"
	params["from"] = "0"
	params["preference"] = "_shards:0,2"

	u, err := es.Search("customer", "", params, "")
	if err != nil {
		panic(err)
	}

	//fmt.Println(string(u))

/*
	if took, err :=jsonparser.GetInt(u, "took") ; err == nil {
		fmt.Printf("took: %d\n", took)
	}

	jsonparser.ArrayEach(u, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {

		name, err := jsonparser.GetString(value, "_source", "name")
		fmt.Println(name, dataType, offset)

		v, t, o, e := jsonparser.Get(value, "_source", "name")
		fmt.Println(v, t, o, e)
		if t == jsonparser.String {
			fmt.Println("String")
		}
	}, "hits", "hits")

*/
	rowcnt := 0 

	var objhandler = func(key []byte, value[]byte, dataType jsonparser.ValueType, offset int) error {
		fmt.Println(string(key), string(value))

		return nil
	}

	var keyhandler = func(idx int, value []byte, vt jsonparser.ValueType, err error) {
		fmt.Println(idx, string(value))
		if idx == 4 {
			jsonparser.ObjectEach(value, objhandler)
		}  
	}

	var arrhandler = func(value[]byte, dataType jsonparser.ValueType, offset int, err error){
		fmt.Println("offset = ", offset)
		jsonparser.EachKey(value, keyhandler, eshitpath...)
		fmt.Printf("row finished...%d\n", rowcnt)
		rowcnt++
	}

	jsonparser.EachKey(u, func(idx int, value []byte, vt jsonparser.ValueType, err error) {
		fmt.Println(idx,string( value))

		if idx == 2 {
                        total, err := jsonparser.GetInt(value, "total")
			if err != nil {
                                fmt.Printf("ES: Failed to get total count. %v", err)
                                return
                        }
			fmt.Printf("Total number of rows = %d\n", total)
                        jsonparser.ArrayEach(value, arrhandler, "hits")
			
			//jsonparser.ArrayEach(value, arrhandler)
		}
	}, esrespath...)


	fmt.Println("Shards start...")
	fragcnt := 3
	for i := 0 ; i < fragcnt ; i++ {
		shards := es.GetShards(int32(i), int32(fragcnt))
		pref := es.GetPreferenceShards(shards)
		fmt.Print("shards for fragid ", i, ":")
		for j := 0 ; j < len(shards) ; j++ {
			fmt.Print(shards[j], ",")
		}
		fmt.Println()
		fmt.Println(pref)
	
	}

/*

	cu, err := es.Count("customer", "", "", "Mini*")
	fmt.Println(string(cu))

	var jsonStr  = []byte(`{"index": { "_index" : "customer", "_type" : "external", "_id" : "100"}
{ "name" : "John John"}
`)
	bu := es.Bulk("index", "type", jsonStr)
	fmt.Println(bu)
*/

}
