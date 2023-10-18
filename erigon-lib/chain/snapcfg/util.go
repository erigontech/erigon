package snapcfg

import (
	_ "embed"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ledgerwatch/erigon-lib/chain/networkname"
	snapshothashes "github.com/ledgerwatch/erigon-snapshot"
	"github.com/ledgerwatch/erigon-snapshot/webseed"
	"github.com/pelletier/go-toml/v2"
	"golang.org/x/exp/slices"
)

var (
	Mainnet = fromToml(snapshothashes.Mainnet)
	// Holesky    = fromToml(snapshothashes.Holesky)
	Sepolia    = fromToml(snapshothashes.Sepolia)
	Goerli     = fromToml(snapshothashes.Goerli)
	Mumbai     = fromToml(snapshothashes.Mumbai)
	BorMainnet = fromToml(snapshothashes.BorMainnet)
	Gnosis     = fromToml(snapshothashes.Gnosis)
	Chiado     = fromToml(snapshothashes.Chiado)
)

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
	slices.SortFunc(out, func(i, j PreverifiedItem) int { return strings.Compare(i.Name, j.Name) })
	return out
}

var (
	MainnetChainSnapshotCfg = newCfg(Mainnet)
	// HoleskyChainSnapshotCfg    = newCfg(Holesky, HoleskyHistory)
	SepoliaChainSnapshotCfg    = newCfg(Sepolia)
	GoerliChainSnapshotCfg     = newCfg(Goerli)
	MumbaiChainSnapshotCfg     = newCfg(Mumbai)
	BorMainnetChainSnapshotCfg = newCfg(BorMainnet)
	GnosisChainSnapshotCfg     = newCfg(Gnosis)
	ChiadoChainSnapshotCfg     = newCfg(Chiado)
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
	networkname.MainnetChainName: MainnetChainSnapshotCfg,
	// networkname.HoleskyChainName:    HoleskyChainSnapshotCfg,
	networkname.SepoliaChainName:    SepoliaChainSnapshotCfg,
	networkname.GoerliChainName:     GoerliChainSnapshotCfg,
	networkname.MumbaiChainName:     MumbaiChainSnapshotCfg,
	networkname.BorMainnetChainName: BorMainnetChainSnapshotCfg,
	networkname.GnosisChainName:     GnosisChainSnapshotCfg,
	networkname.ChiadoChainName:     ChiadoChainSnapshotCfg,
}

// KnownCfg return list of preverified hashes for given network, but apply whiteList filter if it's not empty
func KnownCfg(networkName string, whiteList, whiteListHistory []string) *Cfg {
	c, ok := KnownCfgs[networkName]
	if !ok {
		return newCfg(Preverified{})
	}

	var result Preverified
	if len(whiteList) == 0 {
		result = c.Preverified
	} else {
		wlMap := make(map[string]struct{}, len(whiteList))
		for _, fName := range whiteList {
			wlMap[fName] = struct{}{}
		}

		result = make(Preverified, 0, len(c.Preverified))
		for _, p := range c.Preverified {
			if _, ok := wlMap[p.Name]; !ok {
				continue
			}
			result = append(result, p)
		}
	}

	return newCfg(result)
}

var KnownWebseeds = map[string][]string{
	networkname.MainnetChainName:    webseedsParse(webseed.Mainnet),
	networkname.SepoliaChainName:    webseedsParse(webseed.Sepolia),
	networkname.GoerliChainName:     webseedsParse(webseed.Goerli),
	networkname.MumbaiChainName:     webseedsParse(webseed.Mumbai),
	networkname.BorMainnetChainName: webseedsParse(webseed.BorMainnet),
	networkname.GnosisChainName:     webseedsParse(webseed.Gnosis),
	networkname.ChiadoChainName:     webseedsParse(webseed.Chiado),
}

func webseedsParse(in []byte) (res []string) {
	a := map[string]string{}
	if err := toml.Unmarshal(in, &a); err != nil {
		panic(err)
	}
	for _, l := range a {
		res = append(res, l)
	}
	slices.Sort(res)
	return res
}
