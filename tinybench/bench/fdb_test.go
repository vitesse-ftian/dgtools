package bench

import (
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"hash/crc32"
	"testing"
)

type fdbCtxt struct {
	conf        *Config
	db          fdb.Database
	dir         directory.DirectorySubspace
	subs        [256]subspace.Subspace
	clusterFile string
	nhk         int
}

func opendb(b *testing.B) (*fdbCtxt, error) {
	conf, err := GetConfig()
	if err != nil {
		panic(err)
	}

	var ctxt fdbCtxt
	ctxt.conf = conf
	b.Logf("Open database ...")
	fdb.MustAPIVersion(510)
	ctxt.db = fdb.MustOpenDefault()

	b.Logf("Open directory %s.", conf.Db)
	ctxt.dir, err = directory.CreateOrOpen(ctxt.db, []string{conf.Db, conf.Table}, nil)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 256; i++ {
		b.Logf("Creating subspace %d.", i)
		ctxt.subs[i] = ctxt.dir.Sub([]byte{byte(i)})
	}
	return &ctxt, nil
}

func (ctxt *fdbCtxt) buildTuple(t *Tup) (tuple.Tuple, tuple.Tuple) {
	kt := make([]tuple.TupleElement, 2)
	vt := make([]tuple.TupleElement, 2)
	kt[0] = t.ki
	kt[1] = t.kt
	vt[0] = t.vc
	vt[1] = t.vt
	return kt, vt
}

func (ctxt *fdbCtxt) buildKey(kt tuple.Tuple) (fdb.Key, byte) {
	kb := kt.Pack()
	bkt := byte(crc32.ChecksumIEEE(kb))
	key := ctxt.subs[bkt].Pack(kt)
	return key, bkt
}

func (ctxt *fdbCtxt) oneOp(tr fdb.Transaction, b *testing.B) error {
	tup, w := RandomTup(ctxt.conf.KiMax, ctxt.conf.WPercent)
	ktup, vtup := ctxt.buildTuple(tup)
	k, _ := ctxt.buildKey(ktup)

	ba := tr.Get(k).MustGet()

	if w {
		if ba != nil {
			rtup, _ := tuple.Unpack(ba)
			vc := rtup[0].(int64)
			rtup[0] = vc + 1
			v := rtup.Pack()
			tr.Set(k, v)
		} else {
			v := vtup.Pack()
			tr.Set(k, v)
		}
	}
	return nil
}

func BenchmarkFoundation(b *testing.B) {
	fmt.Printf("Benchmarking foundation db ... \n")
	ctxt, err := opendb(b)
	if err != nil {
		b.Errorf("Cannot connect to FDB.  error %s.", err.Error())
	}
	fmt.Printf("Opened database. \n")

	b.Run("Step=tx", func(b *testing.B) {
		nloop := ctxt.conf.NOp / ctxt.conf.OpPerTx
		nok := 0
		nabrt := 0
		fmt.Printf("Running %d transactions.\n", nloop)
		for i := 0; i < nloop; i++ {
			// fmt.Printf("Running %d transactions, #%d.\n", nloop, i)
			_, err = ctxt.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
				for j := 0; j < ctxt.conf.OpPerTx; j++ {
					err = ctxt.oneOp(tr, b)
					if err != nil {
						panic(err)
					}
				}
				return nil, nil
			})

			if err == nil {
				nok += 1
			} else {
				nabrt += 1
			}
		}

		b.Logf("Run %d Transactions, %d Commited, %d Aborted.", nloop, nok, nabrt)
	})
}
