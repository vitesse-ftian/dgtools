package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/jackc/pgx/stdlib"
	"vitessedata/dgza/dgza"
)

var dgHost = flag.String("h", "127.0.0.1", "DeepGreen host")
var dgPort = flag.String("p", "5432", "DeepGreen port")
var dgUser = flag.String("u", "", "DeepGreen DGZA User")

func main() {
	flag.Parse()

	cmd := flag.Args()[0]
	connstr := fmt.Sprintf("postgres://%s@%s:%s/template1", *dgUser, *dgHost, *dgPort)

	db, err := sql.Open("pgx", connstr)
	if err != nil {
		panic("Cannot open connection to DeepGreen database.")
	}
	defer db.Close()

	if cmd == "ping" {
		dgza.Ping(db)
	} else if cmd == "activity" {
		dgza.GetSegStatus(db)
		dgza.GetActivity(db)
	} else if cmd == "storage" {
		dgza.GetMasterStorage(db)
		dgza.GetSegStorage(db)
	} else {
		panic("Unknown command: " + cmd)
	}
}
