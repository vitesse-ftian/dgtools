package impl

import (
	"flag"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"hash/crc32"
	"strings"
	"vitessedata/plugin"
)

type fdbctxt struct {
	db          fdb.Database
	dir         directory.DirectorySubspace
	subs        [256]subspace.Subspace
	clusterFile string
	nhk         int
}

func opendb(path []string) *fdbctxt {
	var ctxt fdbctxt

	cf := flag.String("clusterfile", "", "fdb cluster file.")
	nhk := flag.Int("nh", 1, "number of hash bucket column")
	flag.Parse()
	ctxt.clusterFile = *cf
	ctxt.nhk = *nhk
	plugin.DbgLog("Opening database with cf %s, nhk %d.", *cf, *nhk)

	fdb.MustAPIVersion(510)

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

	for i := 0; i < 256; i++ {
		ctxt.subs[i] = ctxt.dir.Sub([]byte{byte(i)})
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

func (ctxt *fdbctxt) buildKey(t tuple.Tuple) (fdb.Key, byte) {
	nhk := ctxt.nhk
	if nhk == 0 {
		nhk = len(t)
	}

	kb := t[:nhk].Pack()
	bkt := byte(crc32.ChecksumIEEE(kb))
	key := ctxt.subs[bkt].Pack(t)

	// plugin.DbgLog("Build key: %v -> %v, at bkt %d.", t, key, bkt)
	return key, bkt
}

func (ctxt *fdbctxt) buildBktKey(bkt byte, t tuple.Tuple) fdb.Key {
	if t == nil || len(t) == 0 {
		return ctxt.subs[bkt].FDBKey()
	}
	return ctxt.subs[bkt].Pack(t)
}

func (ctxt *fdbctxt) parseKeyValue(bkt byte, kv fdb.KeyValue) (tuple.Tuple, tuple.Tuple, error) {
	kt, err := ctxt.subs[bkt].Unpack(kv.Key)
	if err != nil {
		return nil, nil, err
	}

	vt, err := tuple.Unpack(kv.Value)
	if err != nil {
		return nil, nil, err
	}

	return kt, vt, nil
}

func (ctxt *fdbctxt) buildRange(bkt byte, ta, tz tuple.Tuple) fdb.KeyRange {
	ka := ctxt.buildBktKey(bkt, ta)
	kra, _ := fdb.PrefixRange(ka)
	kz := ctxt.buildBktKey(bkt, tz)
	krz, _ := fdb.PrefixRange(kz)
	return fdb.KeyRange{kra.Begin, krz.End}
}

func (ctxt *fdbctxt) ins(tr fdb.Transaction, kt, vt tuple.Tuple) {
	k, _ := ctxt.buildKey(kt)
	v := vt.Pack()
	tr.Set(k, v)
}

func (ctxt *fdbctxt) del(tr fdb.Transaction, kt tuple.Tuple) {
	k, _ := ctxt.buildKey(kt)
	tr.Clear(k)
}

func (ctxt *fdbctxt) get(tr fdb.Transaction, kt tuple.Tuple) (tuple.Tuple, error) {
	k, _ := ctxt.buildKey(kt)
	ba := tr.Get(k).MustGet()
	return tuple.Unpack(ba)
}

func decodeReqPath(path string) ([]string, []string, []string, error) {
	// path from request should be format mountpoint/dir/dir/key1,key2:val1,val2,val3
	// remove mount point, then return path splited by "/"
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
