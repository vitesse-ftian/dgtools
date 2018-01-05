package impl

import (
	"encoding/json"
	"fmt"
	"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
        "vitessedata/plugin"
        "bytes"
	"strings"
	"time"
//        "github.com/buger/jsonparser"
	"github.com/Shopify/sarama"
)

const (
	tSleepSeconds = 5    // Time to pause, in seconds, between batches
	batchSize     = 5000 // Number of rows per batch
)

// DoWrite services xdrive write request.  It read a sequence of PluginWriteRequest
// from stdin and write to file system.
func DoWrite() error {

        rinfo := plugin.RInfo()
	ss := strings.Split(rinfo.Rpath, "/")
	brokerList := ss[0]
	topic := ss[1]

	plugin.DbgLog("brokerlist = %s\n", brokerList)
	plugin.DbgLog("topic = %s\n", topic) 

	/*
        conf := rinfo.GetConf()
        for _, kv := range conf.GetKv() {
                if kv.GetKey() == "brokerList" {
                        brokerList = kv.GetValue()
                }
        }
*/

        plugin.FatalIf(topic == "" || brokerList == "", "Kafka requires topic and brokerList")


	partitionerConstructor := sarama.NewRandomPartitioner
	
	var keyEncoder, valueEncoder sarama.Encoder
	
	config := sarama.NewConfig()
	config.Producer.Partitioner = partitionerConstructor
	// See https://github.com/Shopify/sarama/issues/816
	config.Producer.Return.Successes = true
	//config.Producer.Flush.Messages = 5000
	producer, err := sarama.NewSyncProducer(strings.Split(brokerList, ","), config)
	if err != nil {
		return err
	}
	defer producer.Close()

	nLines := 1
	for {
		var req xdrive.PluginWriteRequest
		plugin.DelimRead(&req)
		
		if req.Rowset == nil {
			plugin.DbgLog("Done writing")
			plugin.ReplyWriteError(0, "")
			return nil
		}

		ncol := len(req.Rowset.Columns)
		if ncol == 0 {
			plugin.DbgLog("Done writing")
			plugin.ReplyWriteError(0, "")
			return nil
		}

		nrow := req.Rowset.Columns[0].Nrow
		coldesc := req.Columndesc

		plugin.DbgLog("nrow = %d", nrow)
		var buf bytes.Buffer

		for row := int32(0) ; row < nrow ; row++ {
			nLines++
			source := make(map[string]interface{})
			
			for col := 0 ; col < ncol ; col++ {
				colname := coldesc[col].Name

				switch {
				case req.Rowset.Columns[col].Sdata != nil:
					if ! req.Rowset.Columns[col].Nullmap[row] {
						source[colname] = req.Rowset.Columns[col].Sdata[row]
					}					
				case req.Rowset.Columns[col].I32Data != nil:
					if ! req.Rowset.Columns[col].Nullmap[row] {
						source[colname] = req.Rowset.Columns[col].I32Data[row]
					}                                        
                                case req.Rowset.Columns[col].I64Data != nil:
                                        if ! req.Rowset.Columns[col].Nullmap[row] {
                                                source[colname] = req.Rowset.Columns[col].I64Data[row]
                                        }
                                case req.Rowset.Columns[col].F32Data != nil:
                                        if ! req.Rowset.Columns[col].Nullmap[row] {
                                                source[colname] = req.Rowset.Columns[col].F32Data[row]
                                        }
                                case req.Rowset.Columns[col].F64Data != nil:
                                        if ! req.Rowset.Columns[col].Nullmap[row] {
                                                source[colname] = req.Rowset.Columns[col].F64Data[row]
                                        }
                                default:
                                        return fmt.Errorf("rowset with no data")
                                }
                        }

			s, _ := json.Marshal(source)
			buf.Write(s)

			//plugin.DbgLog(buf.String())
			// write to kafka

			if nLines % batchSize == 0 {
				time.Sleep(tSleepSeconds * time.Second)
			}

			valueEncoder = sarama.StringEncoder(buf.String())
			_, _, err := producer.SendMessage(&sarama.ProducerMessage{
				Topic: topic,
				Key: keyEncoder,
				Value: valueEncoder,
			})
			if err != nil {
				return err
			}	

			plugin.DbgLog("wrote %d rows done...", nrow)
		}
	}

	plugin.DbgLog("Total number of rows = %d", nLines)
	return nil
}	

		
