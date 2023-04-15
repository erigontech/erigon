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

var (
	Mainnet    = fromToml(snapshothashes.Mainnet)
	Sepolia    = fromToml(snapshothashes.Sepolia)
	Goerli     = fromToml(snapshothashes.Goerli)
	Mumbai     = fromToml(snapshothashes.Mumbai)
	BorMainnet = fromToml(snapshothashes.BorMainnet)
	Gnosis     = fromToml(snapshothashes.Gnosis)
	Chiado     = fromToml(snapshothashes.Chiado)

	MainnetHistory    = fromToml(snapshothashes.MainnetHistory)
	SepoliaHistory    = fromToml(snapshothashes.SepoliaHistory)
	GoerliHistory     = fromToml(snapshothashes.GoerliHistory)
	MumbaiHistory     = fromToml(snapshothashes.MumbaiHistory)
	BorMainnetHistory = fromToml(snapshothashes.BorMainnetHistory)
	GnosisHistory     = fromToml(snapshothashes.GnosisHistory)
	ChiadoHistory     = fromToml(snapshothashes.ChiadoHistory)
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
	slices.SortFunc(out, func(i, j PreverifiedItem) bool { return i.Name < j.Name })
	return out
}

var (
	MainnetChainSnapshotCfg    = newCfg(Mainnet, MainnetHistory)
	SepoliaChainSnapshotCfg    = newCfg(Sepolia, SepoliaHistory)
	GoerliChainSnapshotCfg     = newCfg(Goerli, GoerliHistory)
	MumbaiChainSnapshotCfg     = newCfg(Mumbai, MumbaiHistory)
	BorMainnetChainSnapshotCfg = newCfg(BorMainnet, BorMainnetHistory)
	GnosisChainSnapshotCfg     = newCfg(Gnosis, GnosisHistory)
	ChiadoChainSnapshotCfg     = newCfg(Chiado, ChiadoHistory)
)

func newCfg(preverified, preverifiedHistory Preverified) *Cfg {
	return &Cfg{ExpectBlocks: maxBlockNum(preverified), Preverified: preverified, PreverifiedHistory: preverifiedHistory}
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
	ExpectBlocks       uint64
	Preverified        Preverified
	PreverifiedHistory Preverified
}

var KnownCfgs = map[string]*Cfg{
	networkname.MainnetChainName:    MainnetChainSnapshotCfg,
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
		return newCfg(Preverified{}, Preverified{})
	}

	var result, result2 Preverified
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

	if len(whiteList) == 0 {
		result2 = c.PreverifiedHistory
	} else {
		wlMap2 := make(map[string]struct{}, len(whiteListHistory))
		for _, fName := range whiteListHistory {
			wlMap2[filepath.Join("history", fName)] = struct{}{}
		}

		result2 = make(Preverified, 0, len(c.PreverifiedHistory))
		for _, p := range c.PreverifiedHistory {
			if _, ok := wlMap2[p.Name]; !ok {
				continue
			}
			result2 = append(result2, p)
		}
	}

	return newCfg(result, result2)
}
