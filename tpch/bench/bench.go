package bench

import (
	"fmt"
	"os"
	"strconv"

	"github.com/BurntSushi/toml"
	"github.com/vitesse-ftian/dggo/vitessedata/ssh"
	"github.com/vitesse-ftian/dggo/vitessedata/xtable"
)

func Dir() string {
	return os.Getenv("TPCH_BENCH_DIR")
}

type Config struct {
	DGHost  string
	DGPort  int
	Staging string
	Db      string
	Scale   int
	DDL     string
	Ext     string
	Orca    int
	Vitesse int
	StatementMem int
}

type tomlConfig struct {
	TPCH Config
}

var configOk bool
var benchConfig tomlConfig

func GetConfig() (*Config, error) {
	if !configOk {
		dir := Dir()
		fn := dir + "/bench.toml"

		if _, err := toml.DecodeFile(fn, &benchConfig); err != nil {
			return nil, err
		}
		configOk = true
	}
	return &benchConfig.TPCH, nil
}

func ConnectTemplate1() (*xtable.Deepgreen, error) {
	conf, err := GetConfig()
	if err != nil {
		return nil, err
	}

	dg := xtable.Deepgreen{
		Host: conf.DGHost,
		Port: strconv.Itoa(conf.DGPort),
		Db:   "template1",
	}
	err = dg.Connect()
	if err != nil {
		return nil, err
	}
	return &dg, nil
}

func Connect() (*xtable.Deepgreen, error) {
	conf, err := GetConfig()
	if err != nil {
		return nil, err
	}

	dg := xtable.Deepgreen{
		Host: conf.DGHost,
		Port: strconv.Itoa(conf.DGPort),
		Db:   conf.Db,
	}
	err = dg.Connect()
	if err != nil {
		return nil, err
	}
	return &dg, nil
}

type Seg struct {
	Id   int
	Addr string
}

func Segs() ([]Seg, error) {
	ret := make([]Seg, 0)

	db, err := ConnectTemplate1()
	if err != nil {
		return nil, err
	}
	defer db.Disconnect()

	rows, err := db.Conn.Query("select content, address from gp_segment_configuration where content >= 0")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var seg Seg
		rows.Scan(&seg.Id, &seg.Addr)
		ret = append(ret, seg)
	}
	return ret, nil
}

func PsqlCmd(fn string) (string, error) {
	var cmd string
	conf, err := GetConfig()
	if err != nil {
		return cmd, err
	}

	psql := ssh.BinAbs("psql")
	cmd = fmt.Sprintf("%s -h %s -p %d -f %s %s", psql, conf.DGHost, conf.DGPort, fn, conf.Db)
	return cmd, nil
}
