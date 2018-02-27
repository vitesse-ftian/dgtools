package plugin

import (
        "fmt"
	"github.com/satori/go.uuid"
	"github.com/vitesse-ftian/dggo/vitessedata/proto/xdrive"
	"strconv"
	"strings"
	"os"
)

var g_xdrfile *os.File

func XdriveFd() (int, error) {
	fd := os.Getenv("XDRIVE_FD");
	return strconv.Atoi(fd);
}

func OpenXdriveIO() error {
	fdstr := os.Getenv("XDRIVE_FD");
	fd, err := strconv.Atoi(fdstr);
	if err != nil {
		return err
	}
	g_xdrfile = os.NewFile(uintptr(fd), "pipe")
	return nil
}

func ReplyOpStatus(errcode int32, errmsg string) error {
	var status xdrive.OpStatus
	status.Errcode = errcode
	status.Errmsg = errmsg
	return xdrive.ProtostreamWrite(g_xdrfile, &status)
}

func DataReply(errcode int32, errmsg string) error {
	var reply xdrive.DataReply
	reply.Errcode = errcode
	reply.Errmsg = errmsg
	return xdrive.ProtostreamWrite(g_xdrfile, &reply)
}


func WriteReply(errcode int32, errmsg string) error {
	var reply xdrive.WriteReply
	reply.Errcode = errcode
	reply.Errmsg = errmsg
	return xdrive.ProtostreamWrite(g_xdrfile, &reply)
}

func WritePath(req xdrive.WriteRequest) (string, error) {
        str := req.Filespec.Path
        str = strings.Replace(str, "#SEGCOUNT#", strconv.Itoa(int(req.FragCnt)), -1)
        path := strings.Replace(str, "#SEGID#", strconv.Itoa(int(req.FragId)), -1)
        path = strings.Replace(path, "#UUID#", fmt.Sprintf("%s", uuid.NewV4()), -1)

        if path == str {
                return path, fmt.Errorf("No #SEGID# or #UUID# substitution in write request.")
        }
        return path, nil
}

func ReplyXColData(coldatareply xdrive.XColDataReply) error {
	err := xdrive.ProtostreamWrite(g_xdrfile, &coldatareply)
	return err;
}

func GetOpSpec() (xdrive.OpSpec, error) {
	var opspec xdrive.OpSpec;
	err := xdrive.ProtostreamRead(g_xdrfile, &opspec);
	return opspec, err;
}

func GetReadRequest() (xdrive.ReadRequest, error) {
	var rreq xdrive.ReadRequest;
	err := xdrive.ProtostreamRead(g_xdrfile,&rreq);
	return rreq, err;
}


func GetSampleRequest() (xdrive.SampleRequest, error) {
        var sreq xdrive.SampleRequest;
        err := xdrive.ProtostreamRead(g_xdrfile,&sreq);
	return sreq, err;
}


func GetSizeMetaRequest() (xdrive.SizeMetaRequest, error) {
        var req xdrive.SizeMetaRequest;
        err := xdrive.ProtostreamRead(g_xdrfile,&req);
        return req, err;
}



func GetWriteRequest() (xdrive.WriteRequest, error) {
        var req xdrive.WriteRequest;
        err := xdrive.ProtostreamRead(g_xdrfile,&req);
        return req, err;
}

func SizeMetaReply(nrow int64, nbyte int64) error {
	var r xdrive.SizeMetaReply
	r.Nrow = nrow
	r.Nbyte = nbyte
	err := xdrive.ProtostreamWrite(g_xdrfile, &r)
	return err
}

func GetXCol() (xdrive.XCol, error) {
	var col xdrive.XCol
	err := xdrive.ProtostreamRead(g_xdrfile, &col)
	return col, err
}

