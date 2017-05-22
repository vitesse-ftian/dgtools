package impl

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	"os"
)

//
// XDrive and plugin commincate via so called delim stream.  Each message starts with
// an unsigned uvarint32 for message length (uvarint32 encoding as defined in protobuf,
// notice, it is unsigned).   Then, a protobuf message.
//
// NOTE: that the message size can be 0 -- in fact, the trivial message of last
// read op, is size 0.  Must handle correctly.
//

// golang binary.ReadUvarint requires a byte reader.
var stdin *bufio.Reader = bufio.NewReader(os.Stdin)

func delim_read(pb proto.Message) error {
	DbgLog("Delim read ... ")
	msgsz, err := binary.ReadUvarint(stdin)
	if err != nil {
		DbgLogIfErr(err, "Delim read error, msgsz is %d", msgsz)
		return err
	}

	DbgLog("Delim read msg %d bytes ... ", msgsz)
	buf := make([]byte, msgsz)
	rsz, err := stdin.Read(buf)

	if uint64(rsz) != msgsz {
		DbgLogIfErr(err, "Delim read data error, msgsz is %d", msgsz)
		// don't check err, because EOF is a real error here.
		return fmt.Errorf("delim read short read msg")
	}

	err = proto.Unmarshal(buf, pb)
	DbgLogIfErr(err, "Unmarshal error")
	return err
}

// NOTE: we do not wrap os.Stdout with a bufio -- actually, better not, because we
// want to push message over the wire instead of buffering it.
func delim_write(pb proto.Message) error {
	msg, err := proto.Marshal(pb)
	if err != nil {
		DbgLogIfErr(err, "Marshal error.")
		return err
	}

	msgsz := len(msg)
	DbgLog("Delim write %d bytes ... ", msgsz)

	szbuf := make([]byte, 20)
	szsz := binary.PutUvarint(szbuf, uint64(msgsz))
	DbgLog("Delim write %d bytes, szsz %d.", msgsz, szsz)
	wsz, err := os.Stdout.Write(szbuf[:szsz])
	if wsz != szsz {
		DbgLog("Delim write msg sz %d short write (%d)", szsz, wsz)
		return fmt.Errorf("delim write short write msg sz")
	}

	if msgsz > 0 {
		wsz, err = os.Stdout.Write(msg)
		if wsz != msgsz {
			DbgLog("Delim write msg %d bytes short write (%d)", msgsz, wsz)
			return fmt.Errorf("delim write short write msg")
		}
	}

	return nil
}
