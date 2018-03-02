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
	batchSize     = 500 // Number of rows per batch
)

var wreq xdrive.WriteRequest
var ncol int = 0
var cols[]xdrive.XCol
//var coldesc []xdrive.ColumnDesc
var nextcol int
var producer sarama.SyncProducer
var topic string
var nLines int = 0

func WriteRequest(req xdrive.WriteRequest, brokerList string, zkhost string) error {

	wreq = req
	ncol = len(wreq.Columndesc)
	cols = make([]xdrive.XCol, ncol)
	//coldesc = make([]xdrive.ColumnDesc, ncol)
	nextcol = 0

	idx := strings.LastIndex(req.Filespec.Path, "/")
	topic = req.Filespec.Path[idx+1:]
	plugin.DbgLog("path = %s, topic = %s", req.Filespec.Path, topic)


        partitionerConstructor := sarama.NewRandomPartitioner

        config := sarama.NewConfig()
        config.Producer.Partitioner = partitionerConstructor
        // See https://github.com/Shopify/sarama/issues/816                                                                          
        config.Producer.Return.Successes = true
        //config.Producer.Flush.Messages = 5000         

	var err error
	producer, err = sarama.NewSyncProducer(strings.Split(brokerList, ","), config)
        if err != nil {
                return err
        }

	return nil
}

func DoWriteEnd() error {
	if producer != nil {
		producer.Close()
	}
	if nextcol == 0 {
		plugin.DbgLog("Total number of rows = %d", nLines)
		plugin.DbgLog("OK. Close producer.")
		return nil
	} else {
		plugin.DbgLog("Failed. Close producer.")
		return fmt.Errorf("End in the middle of stream")
	}
	return nil
}


// DoWrite services xdrive write request.  It read a sequence of PluginWriteRequest
// from stdin and write to file system.
func DoWrite(col xdrive.XCol) error {
	var keyEncoder, valueEncoder sarama.Encoder

	cols[nextcol] = col
	nextcol++
	if nextcol == ncol {

		nrow := cols[0].Nrow
		coldesc := wreq.Columndesc
		
		plugin.DbgLog("nrow = %d", nrow)
		
		for row := int32(0) ; row < nrow ; row++ {
			nLines++
			source := make(map[string]interface{})
			
			var buf bytes.Buffer
			for col := 0 ; col < ncol ; col++ {
				colname := coldesc[col].Name
				
				switch {
				case cols[col].Sdata != nil:
					if ! cols[col].Nullmap[row] {
						source[colname] = cols[col].Sdata[row]
					}					
				case cols[col].I32Data != nil:
					if ! cols[col].Nullmap[row] {
						source[colname] = cols[col].I32Data[row]
					}                                        
                                case cols[col].I64Data != nil:
                                        if ! cols[col].Nullmap[row] {
                                                source[colname] = cols[col].I64Data[row]
                                        }
                                case cols[col].F32Data != nil:
                                        if ! cols[col].Nullmap[row] {
                                                source[colname] = cols[col].F32Data[row]
                                        }
                                case cols[col].F64Data != nil:
                                        if ! cols[col].Nullmap[row] {
                                                source[colname] = cols[col].F64Data[row]
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
				plugin.DbgLogIfErr(err, "Producer send message failed")
				return err
			}	
		}
		plugin.DbgLog("wrote %d rows done...", nrow)

		nextcol = 0
	}

	return nil
}	

		
