package impl

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"vitessedata/plugin"
)

type S3Bkt struct {
	region string
	bkt    string
	svc    *s3.S3
}

func (sb *S3Bkt) Connect(reg string, bkt string) {
	sb.region = reg
	sb.bkt = bkt

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(reg),
	})
	plugin.FatalIfErr(err, "Cannot connect to AWS region %s", reg)
	sb.svc = s3.New(sess)
}

func (sb *S3Bkt) ConnectUsingRInfo() {
	var region, bkt string
	rinfo := plugin.RInfo()
	conf := rinfo.GetConf()
	for _, kv := range conf.GetKv() {
		if kv.GetKey() == "region" {
			region = kv.GetValue()
		}
		if kv.GetKey() == "bucket" {
			bkt = kv.GetValue()
		}
	}
	plugin.FatalIf(region == "" || bkt == "", "S3 request requires region and bkt config.")
	sb.Connect(region, bkt)
}

type S3Item struct {
	Name string
	Size int64
}

func (i *S3Item) IsDir() bool {
	return pathIsDir(i.Name)
}

func (sb *S3Bkt) ListDir(prefix string) ([]S3Item, error) {
	reply, err := sb.svc.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(sb.bkt),
		Prefix: aws.String(prefix),
	})

	if err != nil {
		plugin.DbgLogIfErr(err, "S3 ListObjects error")
		return nil, err
	}

	ret := make([]S3Item, 0)
	for _, item := range reply.Contents {
		ret = append(ret, S3Item{*item.Key, *item.Size})
	}
	return ret, nil
}

func (sb *S3Bkt) GetObject(path string) (io.ReadCloser, error) {
	resp, err := sb.svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(sb.bkt),
		Key:    aws.String(path),
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// objWriter is a io.ReadWriterCloser
type ioRet struct {
	n   int
	err error
}

type objWriter struct {
	key   string
	ch    chan []byte
	retCh chan ioRet
}

func (w *objWriter) Read(p []byte) (n int, err error) {
	plugin.DbgLog("w %s: Try to read %d bytes.", w.key, len(p))
	w.ch <- p
	r := <-w.retCh
	plugin.DbgLog("w %s: Read got %d bytes, err %v.", w.key, r.n, r.err)
	return r.n, r.err
}

func (w *objWriter) Write(p []byte) (n int, err error) {
	plugin.DbgLog("w %s: Try to write %d bytes.", w.key, len(p))
	pos := 0
	have := len(p)
	for have > 0 {
		buf := <-w.ch
		want := len(buf)
		ret := ioRet{want, nil}
		if ret.n > have {
			ret.n = have
		}

		plugin.DbgLog("w %s: Writer got a buffer of %d bytes, will copy %d bytes.", w.key, want, ret.n)
		copy(buf, p[pos:pos+ret.n])
		pos += ret.n
		have = len(p) - pos
		w.retCh <- ret
	}
	return pos, nil
}

func (w *objWriter) Close() error {
	plugin.DbgLog("w %s: Close!  Get a buf, but send EOF.", w.key)
	<-w.ch
	ret := ioRet{0, io.EOF}
	w.retCh <- ret

	n := <-w.ch
	plugin.FatalIf(n != nil, "Sync bug!")
	return nil
}

func objWriteWorker(sb *S3Bkt, objw *objWriter) {
	uploader := s3manager.NewUploaderWithClient(sb.svc)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(sb.bkt),
		Key:    aws.String(objw.key),
		Body:   objw,
	})

	// Send ch a nil, indicating uploader finished.
	if err == nil {
		plugin.DbgLog("S3 update done!   Signalling.")
		objw.ch <- nil
	}
	plugin.FatalIfErr(err, "Upload to AWS failed.")
}

func (sb *S3Bkt) ObjectWriter(path string) (io.ReadWriteCloser, error) {
	// sanity check.
	prefix := pathSimplePrefix(path)
	chkpath := ""
	for i, r := range path {
		if r == '/' {
			continue
		} else {
			chkpath = path[i:]
			break
		}
	}

	if prefix != chkpath {
		plugin.DbgLog("Write S3 object to %s:%s, chkpath failed %s.", sb.bkt, prefix, chkpath)
		return nil, fmt.Errorf("Bad S3 Object Key: %s, checkpath %s", path, chkpath)
	}
	plugin.DbgLog("Write S3 object to %s:%s.", sb.bkt, prefix)

	var objw objWriter
	objw.key = prefix
	objw.ch = make(chan []byte)
	objw.retCh = make(chan ioRet)
	go objWriteWorker(sb, &objw)
	return &objw, nil
}
