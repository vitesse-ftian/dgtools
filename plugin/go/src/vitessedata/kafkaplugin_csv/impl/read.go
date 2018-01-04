package impl

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"
	"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
	"vitessedata/plugin"
	//"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
)

const (
	waitMilliseconds = 250
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


	zkPeers := strings.Split(zkString, ",")
	
	config := consumergroup.NewConfig()
	
	consumer, consumerErr := consumergroup.JoinConsumerGroup(
		consumerGroupName,
		[]string{topic},
		zkPeers,
		config)
	
	if consumerErr != nil {
		return consumerErr
	}


	defer func() {
		if closeErr := consumer.Close() ; closeErr != nil {
			plugin.DbgLogIfErr(closeErr, "consumer close failed")
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	
	doneCh := make(chan struct{})
	tStart := time.Now()
	go func() {

		for {
			select {
			case err := <- consumer.Errors():
				plugin.DbgLogIfErr(err, "consumer error")
			case msg := <- consumer.Messages():
				plugin.DbgLog(string(msg.Value))
				consumer.CommitUpto(msg)
			case <- signals:
				doneCh <- struct{}{}
			default:
				elapsed := time.Since(tStart)
				if elapsed > waitMilliseconds*time.Millisecond {
					signals <- os.Interrupt
				}
			}
		}
	}()
	
	<- doneCh
	consumer.FlushOffsets()

	plugin.ReplyError(0, "")
	return nil
}
