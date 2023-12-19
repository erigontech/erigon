package snapcfg

import (
	_ "embed"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ledgerwatch/erigon-lib/chain/networkname"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
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
	Amoy       = fromToml(snapshothashes.Amoy)
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
	MainnetChainSnapshotCfg = newCfg(Mainnet, SnapshotVersion)
	// HoleskyChainSnapshotCfg    = newCfg(Holesky, HoleskyHistory)
	SepoliaChainSnapshotCfg    = newCfg(Sepolia, SnapshotVersion)
	GoerliChainSnapshotCfg     = newCfg(Goerli, SnapshotVersion)
	MumbaiChainSnapshotCfg     = newCfg(Mumbai, SnapshotVersion)
	AmoyChainSnapshotCfg       = newCfg(Amoy, SnapshotVersion)
	BorMainnetChainSnapshotCfg = newCfg(BorMainnet, SnapshotVersion)
	GnosisChainSnapshotCfg     = newCfg(Gnosis, SnapshotVersion)
	ChiadoChainSnapshotCfg     = newCfg(Chiado, SnapshotVersion)
)

const (
	SnapshotVersion uint8 = 1
)

func newCfg(preverified Preverified, version uint8) *Cfg {

	if version == 0 {
		version = SnapshotVersion

		if override := dbg.SnapshotVersion(); override != 0 {
			version = override

			var pv Preverified

			for _, p := range preverified {
				if v, _, ok := strings.Cut(p.Name, "-"); ok && strings.HasPrefix(v, "v") {
					if v, err := strconv.ParseUint(v[1:], 10, 8); err == nil && version == uint8(v) {
						pv = append(pv, p)
					}
				}
			}

			preverified = pv
		}
	}

	maxBlockNum, version := cfgInfo(preverified, version)
	return &Cfg{ExpectBlocks: maxBlockNum, Preverified: preverified, Version: version}
}

func cfgInfo(preverified Preverified, defaultVersion uint8) (uint64, uint8) {
	max := uint64(0)
	version := defaultVersion

	for _, p := range preverified {
		_, fileName := filepath.Split(p.Name)
		ext := filepath.Ext(fileName)
		if ext != ".seg" {
			continue
		}
		onlyName := fileName[:len(fileName)-len(ext)]
		parts := strings.Split(onlyName, "-")
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

		if vp := parts[0]; strings.HasPrefix(vp, "v") {
			if v, err := strconv.ParseUint(vp[1:], 10, 8); err == nil {
				version = uint8(v)
			}
		}
	}
	if max == 0 { // to prevent underflow
		return 0, version
	}
	return max*1_000 - 1, version
}

type Cfg struct {
	ExpectBlocks uint64
	Version      uint8
	Preverified  Preverified
}

var KnownCfgs = map[string]*Cfg{
	networkname.MainnetChainName: MainnetChainSnapshotCfg,
	// networkname.HoleskyChainName:    HoleskyChainSnapshotCfg,
	networkname.SepoliaChainName:    SepoliaChainSnapshotCfg,
	networkname.GoerliChainName:     GoerliChainSnapshotCfg,
	networkname.MumbaiChainName:     MumbaiChainSnapshotCfg,
	networkname.AmoyChainName:       AmoyChainSnapshotCfg,
	networkname.BorMainnetChainName: BorMainnetChainSnapshotCfg,
	networkname.GnosisChainName:     GnosisChainSnapshotCfg,
	networkname.ChiadoChainName:     ChiadoChainSnapshotCfg,
}

// KnownCfg return list of preverified hashes for given network, but apply whiteList filter if it's not empty
func KnownCfg(networkName string, whiteList, whiteListHistory []string) *Cfg {
	c, ok := KnownCfgs[networkName]
	if !ok {
		return newCfg(Preverified{}, 0)
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

	return newCfg(result, 0)
}

func VersionedCfg(networkName string, version uint8) *Cfg {
	c, ok := KnownCfgs[networkName]
	if !ok {
		return newCfg(Preverified{}, version)
	}

	return newCfg(c.Preverified, version)
}

var KnownWebseeds = map[string][]string{
	networkname.MainnetChainName:    webseedsParse(webseed.Mainnet),
	networkname.SepoliaChainName:    webseedsParse(webseed.Sepolia),
	networkname.GoerliChainName:     webseedsParse(webseed.Goerli),
	networkname.MumbaiChainName:     webseedsParse(webseed.Mumbai),
	networkname.AmoyChainName:       webseedsParse(webseed.Amoy),
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
