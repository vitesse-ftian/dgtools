package xtable

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	// _ "github.com/jackc/pgx/stdlib"
)

type Deepgreen struct {
	Host string
	Port string
	User string
	Db   string

	Conn *sql.DB

	typMap map[int]string
	idgen  int64
}

func (dg *Deepgreen) nextTmpName() string {
	ret := fmt.Sprintf("XTTMP_%d", dg.idgen)
	dg.idgen++
	return ret
}

func (dg *Deepgreen) Connect() error {
	if dg.Conn != nil {
		return fmt.Errorf("Deepgreen already connected.")
	}

	var err error
	var connstr string
	if dg.User == "" {
		connstr = fmt.Sprintf("postgres://%s:%s/%s?sslmode=disable", dg.Host, dg.Port, dg.Db)
	} else {
		connstr = fmt.Sprintf("postgres://%s@%s:%s/%s?sslmode=disable", dg.User, dg.Host, dg.Port, dg.Db)
	}
	dg.Conn, err = sql.Open("postgres", connstr)
	if err != nil {
		return err
	}

	rows, err := dg.Conn.Query("select oid, typname from pg_type")
	if err != nil {
		dg.Conn.Close()
		return fmt.Errorf("Cannot retrieve types")
	}
	defer rows.Close()

	dg.typMap = make(map[int]string)
	for rows.Next() {
		var oid int
		var typname string
		rows.Scan(&oid, &typname)
		dg.typMap[oid] = typname
	}

	return nil
}

func (dg *Deepgreen) Disconnect() {
	if dg.Conn != nil {
		dg.Conn.Close()
		dg.Conn = nil
	}
}

func (dg *Deepgreen) Execute(sql string) error {
	_, err := dg.Conn.Exec(sql)
	return err
}
