package phirun

import (
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	"os"
	"vitessedata/phi/proto/xdrive"
)

func delim32_read(pb proto.Message) error {
	var msgsz int32
	err := binary.Read(os.Stdin, binary.LittleEndian, &msgsz)
	if err != nil {
		return err
	}

	Log("Delim32_read get a message, size %d.\n", msgsz)

	buf := make([]byte, msgsz)
	rsz, err := os.Stdin.Read(buf)

	if int32(rsz) != msgsz {
		// don't check err, because EOF is a real error here.
		return fmt.Errorf("delim read short read msg")
	}

	err = proto.Unmarshal(buf, pb)
	return err
}

func delim32_write(pb proto.Message) error {
	msg, err := proto.Marshal(pb)
	if err != nil {
		return err
	}

	msgsz := int32(len(msg))
	Log("Delim32_write write a message, size %d.\n", msgsz)

	err = binary.Write(os.Stdout, binary.LittleEndian, &msgsz)
	if err != nil {
		return err
	}

	if msgsz > 0 {
		wsz, err := os.Stdout.Write(msg)
		if err != nil || int32(wsz) != msgsz {
			return fmt.Errorf("delim write short write msg")
		}
	}

	return nil
}

func ReadXMsg() (*xdrive.XMsg, error) {
	var msg xdrive.XMsg
	err := delim32_read(&msg)
	return &msg, err
}

func WriteXMsg(msg *xdrive.XMsg) error {
	return delim32_write(msg)
}
