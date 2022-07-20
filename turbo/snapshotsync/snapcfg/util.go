package snapcfg

import (
	_ "embed"
	"path/filepath"
	"strconv"
	"strings"

	snapshothashes "github.com/ledgerwatch/erigon-snapshot"
	"github.com/ledgerwatch/erigon/params/networkname"
	"github.com/pelletier/go-toml/v2"
	"golang.org/x/exp/slices"
)

var Mainnet = fromToml(snapshothashes.Mainnet)

var Goerli = fromToml(snapshothashes.Goerli)

var Bsc = fromToml(snapshothashes.Bsc)

var Ropsten = fromToml(snapshothashes.Ropsten)

var Mumbai = fromToml(snapshothashes.Mumbai)

var BorMainnet = fromToml(snapshothashes.BorMainnet)

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
	MainnetChainSnapshotCfg    = newCfg(Mainnet)
	GoerliChainSnapshotCfg     = newCfg(Goerli)
	BscChainSnapshotCfg        = newCfg(Bsc)
	RopstenChainSnapshotCfg    = newCfg(Ropsten)
	MumbaiChainSnapshotCfg     = newCfg(Mumbai)
	BorMainnetChainSnapshotCfg = newCfg(BorMainnet)
)

func newCfg(preverified Preverified) *Cfg {
	return &Cfg{ExpectBlocks: maxBlockNum(preverified), Preverified: preverified}
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

type Cfg struct {
	ExpectBlocks uint64
	Preverified  Preverified
}

var KnownCfgs = map[string]*Cfg{
	networkname.MainnetChainName:    MainnetChainSnapshotCfg,
	networkname.GoerliChainName:     GoerliChainSnapshotCfg,
	networkname.BSCChainName:        BscChainSnapshotCfg,
	networkname.RopstenChainName:    RopstenChainSnapshotCfg,
	networkname.MumbaiChainName:     MumbaiChainSnapshotCfg,
	networkname.BorMainnetChainName: BorMainnetChainSnapshotCfg,
}

func KnownCfg(networkName string) *Cfg {
	if c, ok := KnownCfgs[networkName]; ok {
		return c
	}
	return newCfg(Preverified{})
}
