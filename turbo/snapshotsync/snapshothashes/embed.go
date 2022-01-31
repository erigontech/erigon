package snapshothashes

import (
	_ "embed"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ledgerwatch/erigon/params/networkname"
	"github.com/pelletier/go-toml/v2"
)

//go:embed erigon-snapshots/mainnet.toml
var mainnet []byte
var Mainnet = fromToml(mainnet)

//go:embed erigon-snapshots/goerli.toml
var goerli []byte
var Goerli = fromToml(goerli)

//go:embed erigon-snapshots/bsc.toml
var bsc []byte
var Bsc = fromToml(bsc)

type Preverified map[string]string

func fromToml(in []byte) (out Preverified) {
	if err := toml.Unmarshal(in, &out); err != nil {
		panic(err)
	}
	return out
}

var (
	MainnetChainSnapshotConfig = newConfig(Mainnet)
	GoerliChainSnapshotConfig  = newConfig(Goerli)
	BscChainSnapshotConfig     = newConfig(Bsc)
)

func newConfig(preverified Preverified) *Config {
	return &Config{
		ExpectBlocks: maxBlockNum(preverified),
		Preverified:  preverified,
	}
}

func maxBlockNum(preverified Preverified) uint64 {
	max := uint64(0)
	for name := range preverified {
		_, fileName := filepath.Split(name)
		ext := filepath.Ext(fileName)
		if ext != ".seg" {
			continue
		}
		onlyName := fileName[:len(fileName)-len(ext)]
		parts := strings.Split(onlyName, "-")
		if parts[0] != "v1" {
			panic("not implemented")
		}
		if parts[3] != "headers" {
			continue
		}
		to, err := strconv.ParseUint(parts[2], 10, 64)
		if err != nil {
			panic(err)
		}
		if max < to {
			max = to
		}
	}
	if max == 0 { // to prevent underflow
		return 0
	}
	return max*1_000 - 1
}

type Config struct {
	ExpectBlocks uint64
	Preverified  Preverified
}

func KnownConfig(networkName string) *Config {
	switch networkName {
	case networkname.MainnetChainName:
		return MainnetChainSnapshotConfig
	case networkname.GoerliChainName:
		return GoerliChainSnapshotConfig
	case networkname.BSCChainName:
		return BscChainSnapshotConfig
	default:
		return newConfig(Preverified{})
	}
}
