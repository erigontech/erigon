package snapshothashes

import (
	_ "embed"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ledgerwatch/erigon/params/networkname"
	"github.com/pelletier/go-toml/v2"
	"golang.org/x/exp/slices"
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

//go:embed erigon-snapshots/ropsten.toml
var ropsten []byte
var Ropsten = fromToml(ropsten)

//go:embed erigon-snapshots/mumbai.toml
var mumbai []byte
var Mumbai = fromToml(mumbai)

//go:embed erigon-snapshots/bor-mainnet.toml
var borMainnet []byte
var BorMainnet = fromToml(borMainnet)

type PreverifiedItem struct {
	Name string
	Hash string
}
type Preverified []PreverifiedItem
type preverified map[string]string

func fromToml(in []byte) (out Preverified) {
	var outMap preverified
	if err := toml.Unmarshal(in, &outMap); err != nil {
		panic(err)
	}
	return doSort(outMap)
}
func doSort(in preverified) Preverified {
	out := make(Preverified, 0, len(in))
	for k, v := range in {
		out = append(out, PreverifiedItem{k, v})
	}
	slices.SortFunc(out, func(i, j PreverifiedItem) bool { return i.Name < j.Name })
	return out
}

var (
	MainnetChainSnapshotConfig    = newConfig(Mainnet)
	GoerliChainSnapshotConfig     = newConfig(Goerli)
	BscChainSnapshotConfig        = newConfig(Bsc)
	RopstenChainSnapshotConfig    = newConfig(Ropsten)
	MumbaiChainSnapshotConfig     = newConfig(Mumbai)
	BorMainnetChainSnapshotConfig = newConfig(BorMainnet)
)

func newConfig(preverified Preverified) *Config {
	return &Config{ExpectBlocks: maxBlockNum(preverified), Preverified: preverified}
}

func maxBlockNum(preverified Preverified) uint64 {
	max := uint64(0)
	for _, p := range preverified {
		_, fileName := filepath.Split(p.Name)
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

var KnownConfigs map[string]*Config = map[string]*Config{
	networkname.MainnetChainName:    MainnetChainSnapshotConfig,
	networkname.GoerliChainName:     GoerliChainSnapshotConfig,
	networkname.BSCChainName:        BscChainSnapshotConfig,
	networkname.RopstenChainName:    RopstenChainSnapshotConfig,
	networkname.MumbaiChainName:     MumbaiChainSnapshotConfig,
	networkname.BorMainnetChainName: BorMainnetChainSnapshotConfig,
}

func KnownConfig(networkName string) *Config {
	if c, ok := KnownConfigs[networkName]; ok {
		return c
	}
	return newConfig(Preverified{})
}
