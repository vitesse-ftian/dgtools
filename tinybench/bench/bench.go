package bench

import (
	"github.com/BurntSushi/toml"
	"math/rand"
	"os"
)

func Dir() string {
	return os.Getenv("TINYBENCH_DIR")
}

type Config struct {
	DGHost   string
	DGPort   int
	Db       string
	KiMax    int64
	NOp      int
	OpPerTx  int
	WPercent float64
}

type tomlConfig struct {
	TinyBench Config
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

		rand.Seed(int64(os.Getpid()))
		configOk = true
	}
	return &benchConfig.TinyBench, nil
}

type Tup struct {
	ki int64
	kt string
	vc int64
	vt string
}

func RandomTup(kimax int64, wpercent float64) (*Tup, bool) {
	var t Tup
	t.ki = rand.Int63n(kimax)
	t.kt = "1234567890"
	t.vc = 0
	t.vt = "123456789012345678901234567890123456789012345678901234567890"
	return &t, rand.Float64() < wpercent
}
