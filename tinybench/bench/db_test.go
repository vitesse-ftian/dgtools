package bench

import (
	"database/sql"
	"fmt"
	"github.com/vitesse-ftian/dggo/vitessedata/xtable"
	"strconv"
	"testing"
)

type dbCtxt struct {
	dg  *xtable.Deepgreen
	sel *sql.Stmt
	ins *sql.Stmt
	upd *sql.Stmt
}

func ConnectDb() (*dbCtxt, error) {
	var db dbCtxt
	conf, err := GetConfig()
	if err != nil {
		panic(err)
	}

	db.dg = &xtable.Deepgreen{
		Host: conf.DGHost,
		Port: strconv.Itoa(conf.DGPort),
		Db:   conf.Db,
	}

	err = db.dg.Connect()
	if err != nil {
		panic(err)
	}

	db.sel, err = db.dg.Conn.Prepare("select vc, vt from tinybench where ki = $1 and kt = $2")
	if err != nil {
		panic(err)
	}

	db.ins, err = db.dg.Conn.Prepare("insert into tinybench values ($1, $2, $3, $4)")
	if err != nil {
		panic(err)
	}

	db.upd, err = db.dg.Conn.Prepare("update tinybench set vc = vc + 1 where ki = $1 and kt = $2")
	if err != nil {
		panic(err)
	}
	return &db, nil
}

func (db *dbCtxt) Disconnect() {
	if db.upd != nil {
		db.upd.Close()
	}

	if db.ins != nil {
		db.ins.Close()
	}

	if db.sel != nil {
		db.sel.Close()
	}

	if db.dg != nil {
		db.dg.Disconnect()
	}
}

func oneOp(conf *Config, db *dbCtxt, b *testing.B) error {
	tup, w := RandomTup(conf.KiMax, conf.WPercent)
	rows, err := db.sel.Query(tup.ki, tup.kt)
	found := false
	if err != nil {
		return err
	}

	for rows.Next() {
		var rtup Tup
		rows.Scan(&rtup.vc, &rtup.vt)
		if tup.vt != rtup.vt {
			b.Errorf("Read bad value, %s", rtup.vt)
			rows.Close()
			return fmt.Errorf("Read bad value, %s", rtup.vt)
		}
		found = true
	}
	rows.Close()

	if w {
		if found {
			_, err = db.upd.Exec(tup.ki, tup.kt)
			if err != nil {
				return err
			}
		} else {
			_, err = db.ins.Exec(tup.ki, tup.kt, tup.vc, tup.vt)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func BenchmarkDB(b *testing.B) {
	conf, err := GetConfig()
	if err != nil {
		b.Errorf("Configuration error %s", err.Error())
	}

	db, err := ConnectDb()
	if err != nil {
		b.Errorf("Cannot connect to DB or prep stmt.  error %s.", err.Error())
	}
	defer db.Disconnect()

	err = db.dg.Execute("set vitesse.threshold = 1000;")
	if err != nil {
		b.Errorf("Cannot set threshold guc to 1000, error %s.", err.Error())
	}

	b.Run("Step=tx", func(b *testing.B) {
		nloop := conf.NOp / conf.OpPerTx
		nok := 0
		nabrt := 0
		for i := 0; i < nloop; i++ {
			tx, err := db.dg.Conn.Begin()
			for j := 0; j < conf.OpPerTx; j++ {
				err = oneOp(conf, db, b)
				if err != nil {
					return
				}
			}
			err = tx.Commit()
			if err == nil {
				nok += 1
			} else {
				nabrt += 1
			}
		}

		b.Logf("Run %d Transactions, %d Commited, %d Aborted.", nloop, nok, nabrt)
	})
}
