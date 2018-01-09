package impl

import (
	"fmt"
	"strings"
	"time"
	"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
	"vitessedata/plugin"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	//"github.com/wvanbergen/kafka/consumergroup"
	//"github.com/wvanbergen/kazoo-go"
)

const (
	waitMilliseconds = 5000
	consumerGroupName = "deepgreen"
)


func DoRead() error {
	var topic, zkString string

	rinfo := plugin.RInfo()
	ss := strings.Split(rinfo.Rpath, "/")
	brokerList := ss[0]
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
	
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	plugin.DbgLog("Cluster consumer start...")
	consumer, err := cluster.NewConsumer(strings.Split(brokerList, ","),
		consumerGroupName,
		[]string{topic},
		config)

	if err != nil {
		plugin.DbgLogIfErr(err, "Failed to start consumer")
		plugin.ReplyError(-4, "Failed to start consumer")
		return err
	}

	defer consumer.Close()


	// consume errors
	go func() {
		for err := range consumer.Errors() {
			plugin.DbgLog("Error: %s\n", err.Error())
		}
	}()

	// consume notifications
	go func() {
		for ntf := range consumer.Notifications() {
			plugin.DbgLog("Rebalanced: %+v\n", ntf)
		}
	}()
	
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(waitMilliseconds * time.Millisecond)
		timeout <- true
	}()


	//tStart := time.Now()
	
	var js JsonReader
	js.Init(req.Filespec, req.Columndesc, req.Columnlist)

	var messages [][]byte
	firstmsg := true

	running := true
	for ; running ; {
		select {
		case msg, ok := <- consumer.Messages():
			if ok {
				//tStart = time.Now()
				if firstmsg {
					plugin.DbgLog("message received...")
					firstmsg = false
				}
				//plugin.DbgLog(string(msg.Value))
				messages = append(messages, msg.Value)
				consumer.MarkOffset(msg, "")
				
				if len(messages) == 1000 {
					plugin.DbgLog("%d messages write to db", len(messages))
					err = js.processAll(messages)
					if err != nil {
						plugin.DbgLogIfErr(err, "failed to write to deepgreen")
						plugin.ReplyError(-20, "Failed to write to deepgreen")
						return err
					}
					plugin.DbgLog("%d rows read", len(messages))
					messages = nil
					//consumer.FlushOffsets()
				}
			}

			case <- timeout:
			plugin.DbgLog("plugin timed out")
			running = false
		}
	}

	if len(messages) > 0 {
		plugin.DbgLog("%d messages write to db", len(messages))
		err = js.processAll(messages)
		if err != nil {
			plugin.DbgLogIfErr(err, "failed to write to deepgreen")
			plugin.ReplyError(-20, "Failed to write to deepgreen")
			return err
		}
//		consumer.FlushOffsets()
		plugin.DbgLog("%d rows read", len(messages))
	}

	plugin.ReplyError(0, "")
	return nil
}
