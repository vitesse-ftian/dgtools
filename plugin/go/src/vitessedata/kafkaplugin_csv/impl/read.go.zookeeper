package impl

import (
	"fmt"
	"strings"
	"time"
	"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
	"vitessedata/plugin"
//	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/wvanbergen/kazoo-go"
)

const (
	waitMilliseconds = 1000
	consumerGroupName = "deepgreen"
)


func DoRead() error {
	var topic, zkString string

	rinfo := plugin.RInfo()
	ss := strings.Split(rinfo.Rpath, "/")
	//brokerList := ss[0]
	topic = ss[1]

	conf := rinfo.GetConf()
	for _, kv := range conf.GetKv() {
		if kv.GetKey() == "zookeeper" {
			zkString = kv.GetValue()
		}
	}

	plugin.FatalIf(topic == "" || zkString == "", "Kafka requires topic and zookeeper")
	
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


//	zkPeers := strings.Split(zkString, ",")
	
	config := consumergroup.NewConfig()
//	config.Offsets.ResetOffsets = true
//	config.Offsets.Initial = sarama.OffsetNewest

	var zkPeers []string
	zkPeers, config.Zookeeper.Chroot = kazoo.ParseConnectionString(zkString)

	consumer, consumerErr := consumergroup.JoinConsumerGroup(
		consumerGroupName,
		[]string{topic},
		zkPeers,
		config)
	
	if consumerErr != nil {
		plugin.DbgLog("Failed to join consumer group")
		plugin.ReplyError(-4, "join consumer group error")
		return consumerErr
	}


	defer func() {
		if closeErr := consumer.Close() ; closeErr != nil {
			plugin.DbgLogIfErr(closeErr, "consumer close failed")
		}
	}()

	tStart := time.Now()

	var js JsonReader
	js.Init(req.Filespec, req.Columndesc, req.Columnlist)

	var messages [][]byte
	running := true
	for ; running ; {
		select {
		case err := <- consumer.Errors():
			plugin.DbgLogIfErr(err, "consumer error")
			plugin.ReplyError(-20, "Consumer Error")
			return err
		case msg := <- consumer.Messages():
			tStart = time.Now()

			//plugin.DbgLog("message received...")
			//plugin.DbgLog(string(msg.Value))
			messages = append(messages, msg.Value)
			consumer.CommitUpto(msg)

			if len(messages) == 1000 {
				err = js.processAll(messages)
				if err != nil {
					plugin.DbgLogIfErr(err, "failed to write to deepgreen")
					plugin.ReplyError(-20, "Failed to write to deepgreen")
					return err
				}
				plugin.DbgLog("%d rows read", len(messages))
				messages = nil
				consumer.FlushOffsets()
			}
			
		default:
			elapsed := time.Since(tStart)
			if elapsed > waitMilliseconds*time.Millisecond {
				plugin.DbgLog("plugin timed out")
				running = false
			}
		}
	}

	if len(messages) > 0 {
		err = js.processAll(messages)
		if err != nil {
			plugin.DbgLogIfErr(err, "failed to write to deepgreen")
			plugin.ReplyError(-20, "Failed to write to deepgreen")
			return err
		}
		consumer.FlushOffsets()
		plugin.DbgLog("%d rows read", len(messages))
	}

	plugin.ReplyError(0, "")
	return nil
}
