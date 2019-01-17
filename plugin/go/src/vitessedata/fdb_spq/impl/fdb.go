package impl

import (
	"flag"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/satori/go.uuid"
	"strconv"
	"strings"
	"vitessedata/plugin"
)

type fdbctxt struct {
	db          fdb.Database
	dir         directory.DirectorySubspace
	clusterFile string
}

func opendb(path []string) *fdbctxt {
	var ctxt fdbctxt

	cf := flag.String("clusterfile", "", "fdb cluster file.")
	flag.Parse()
	ctxt.clusterFile = *cf
	plugin.DbgLog("Opening database with cf %s.", *cf)

	fdb.MustAPIVersion(600)

	if ctxt.clusterFile != "" {
		// For now, fdb only support one database "DB"
		ctxt.db = fdb.MustOpen(ctxt.clusterFile, []byte("DB"))
	} else {
		ctxt.db = fdb.MustOpenDefault()
	}

	var err error
	ctxt.dir, err = directory.CreateOrOpen(ctxt.db, path, nil)
	if err != nil {
		panic(err)
	}
	return &ctxt
}

func buildTuple(vs []interface{}) tuple.Tuple {
	tup := make([]tuple.TupleElement, len(vs))
	for idx, v := range vs {
		tup[idx] = v.(tuple.TupleElement)
	}
	return tup
}

func (ctxt *fdbctxt) buildKey(t tuple.Tuple) fdb.Key {
	if t == nil || len(t) == 0 {
		return ctxt.dir.FDBKey()
	}
	return ctxt.dir.Pack(t)
}

func (ctxt *fdbctxt) parseKeyValue(kv fdb.KeyValue) (tuple.Tuple, tuple.Tuple, error) {
	kt, err := ctxt.dir.Unpack(kv.Key)
	if err != nil {
		return nil, nil, err
	}

	vt, err := tuple.Unpack(kv.Value)
	if err != nil {
		return nil, nil, err
	}

	return kt, vt, nil
}

func (ctxt *fdbctxt) buildRange(ta, tz tuple.Tuple) fdb.KeyRange {
	ka := ctxt.buildKey(ta)
	kra, _ := fdb.PrefixRange(ka)
	kz := ctxt.buildKey(tz)
	krz, _ := fdb.PrefixRange(kz)
	return fdb.KeyRange{kra.Begin, krz.End}
}

func (ctxt *fdbctxt) ins(tr fdb.Transaction, kt, vt tuple.Tuple) {
	k := ctxt.buildKey(kt)
	v := vt.Pack()
	tr.Set(k, v)
}

func (ctxt *fdbctxt) del(tr fdb.Transaction, kt tuple.Tuple) {
	k := ctxt.buildKey(kt)
	tr.Clear(k)
}

func (ctxt *fdbctxt) get(tr fdb.Transaction, kt tuple.Tuple) (tuple.Tuple, error) {
	k := ctxt.buildKey(kt)
	ba := tr.Get(k).MustGet()
	return tuple.Unpack(ba)
}

func uuidv4() string {
	u, _ := uuid.NewV4()
	return fmt.Sprintf("%s", u)
}

func decodeReqPath(reqpath string, segid, segcnt int32) ([]string, []string, []string, error) {
	// path from request should be format mountpoint/dir/dir/key1,key2:val1,val2,val3
	// remove mount point, then return path splited by "/"
	path := strings.Replace(reqpath, "#SEGCOUNT#", strconv.Itoa(int(segcnt)), -1)
	path = strings.Replace(path, "#SEGID#", strconv.Itoa(int(segid)), -1)
	path = strings.Replace(path, "#UUID#", fmt.Sprintf("%s", uuidv4()), -1)

	idx := strings.Index(path[1:], "/")
	strs := strings.Split(path[idx+1:], "/")

	if len(strs) < 2 {
		return nil, nil, nil, fmt.Errorf("FDB path %v is not a valid format.", path)
	}

	dirpath := strs[1 : len(strs)-1]
	plugin.DbgLog("Dir path: \n")
	for ith, dp := range dirpath {
		plugin.DbgLog("Dir path %d |%s|.\n", ith, dp)
	}

	kvstrs := strings.Split(strs[len(strs)-1], ":")
	if len(kvstrs) != 2 {
		return nil, nil, nil, fmt.Errorf("FDB path %v is not a valid format.", path)
	}
	return dirpath, strings.Split(kvstrs[0], ","), strings.Split(kvstrs[1], ","), nil
}
