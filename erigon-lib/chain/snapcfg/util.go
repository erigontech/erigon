package snapcfg

import (
	_ "embed"
	"encoding/json"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/ledgerwatch/erigon-lib/chain/networkname"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	snapshothashes "github.com/ledgerwatch/erigon-snapshot"
	"github.com/ledgerwatch/erigon-snapshot/webseed"
	"github.com/pelletier/go-toml/v2"
	"github.com/tidwall/btree"
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

func (p Preverified) Contains(name string) bool {
	i := sort.Search(len(p), func(i int) bool { return p[i].Name >= name })
	return i < len(p) && p[i].Name == name
}

func (p Preverified) Versioned(types []snaptype.Type) Preverified {
	var bestVersions btree.Map[string, PreverifiedItem]

	for _, p := range p {
		v, name, ok := strings.Cut(p.Name, "-")

		if !ok {
			continue
		}

		var preferredVersion, minVersion snaptype.Version

		parts := strings.Split(name, "-")
		typeName, _ := strings.CutSuffix(parts[2], filepath.Ext(parts[2]))
		include := false

		for _, typ := range types {
			if typeName == typ.String() {
				preferredVersion = typ.Versions().Current
				minVersion = typ.Versions().MinSupported
				include = true
				break
			}
		}

		if !include {
			continue
		}

		version, err := snaptype.ParseVersion(v)

		if err != nil {
			continue
		}

		if version < minVersion {
			continue
		}

		if version > preferredVersion {
			continue
		}

		if current, ok := bestVersions.Get(name); ok {
			v, _, _ := strings.Cut(current.Name, "-")
			cv, _ := snaptype.ParseVersion(v)

			if version > cv {
				bestVersions.Set(name, p)
			}
		} else {
			bestVersions.Set(name, p)
		}
	}

	var versioned Preverified

	bestVersions.Scan(func(key string, value PreverifiedItem) bool {
		versioned = append(versioned, value)
		return true
	})

	return versioned
}

func (p Preverified) MaxBlock(version snaptype.Version) (uint64, error) {
	max := uint64(0)

	for _, p := range p {
		_, fileName := filepath.Split(p.Name)
		ext := filepath.Ext(fileName)
		if ext != ".seg" {
			continue
		}
		onlyName := fileName[:len(fileName)-len(ext)]
		parts := strings.Split(onlyName, "-")

		to, err := strconv.ParseUint(parts[2], 10, 64)
		if err != nil {
			return 0, err
		}

		if version != 0 {
			if v, err := snaptype.ParseVersion(parts[0]); err != nil || v != version {
				continue
			}
		}

		if max < to {
			max = to
		}

	}
	if max == 0 { // to prevent underflow
		return 0, nil
	}

	return max*1_000 - 1, nil
}

func (p Preverified) MarshalJSON() ([]byte, error) {
	out := map[string]string{}

	for _, i := range p {
		out[i.Name] = i.Hash
	}

	return json.Marshal(out)
}

func (p *Preverified) UnmarshalJSON(data []byte) error {
	var outMap map[string]string

	if err := json.Unmarshal(data, &outMap); err != nil {
		return err
	}

	*p = doSort(outMap)
	return nil
}

func fromToml(in []byte) (out Preverified) {
	var outMap map[string]string
	if err := toml.Unmarshal(in, &outMap); err != nil {
		panic(err)
	}
	return doSort(outMap)
}

func doSort(in map[string]string) Preverified {
	out := make(Preverified, 0, len(in))
	for k, v := range in {
		out = append(out, PreverifiedItem{k, v})
	}
	slices.SortFunc(out, func(i, j PreverifiedItem) int { return strings.Compare(i.Name, j.Name) })
	return out
}

func newCfg(preverified Preverified) *Cfg {
	maxBlockNum, _ := preverified.MaxBlock(0)
	return &Cfg{ExpectBlocks: maxBlockNum, Preverified: preverified}
}

type Cfg struct {
	ExpectBlocks uint64
	Preverified  Preverified
}

var knownPreverified = map[string]Preverified{
	networkname.MainnetChainName: Mainnet,
	// networkname.HoleskyChainName:    HoleskyChainSnapshotCfg,
	networkname.SepoliaChainName:    Sepolia,
	networkname.GoerliChainName:     Goerli,
	networkname.MumbaiChainName:     Mumbai,
	networkname.AmoyChainName:       Amoy,
	networkname.BorMainnetChainName: BorMainnet,
	networkname.GnosisChainName:     Gnosis,
	networkname.ChiadoChainName:     Chiado,
}

var ethereumTypes = append(snaptype.BlockSnapshotTypes, snaptype.CaplinSnapshotTypes...)
var borTypes = append(snaptype.BlockSnapshotTypes, snaptype.BorSnapshotTypes...)

var knownTypes = map[string][]snaptype.Type{
	networkname.MainnetChainName: ethereumTypes,
	// networkname.HoleskyChainName:    HoleskyChainSnapshotCfg,
	networkname.SepoliaChainName:    ethereumTypes,
	networkname.GoerliChainName:     ethereumTypes,
	networkname.MumbaiChainName:     borTypes,
	networkname.AmoyChainName:       borTypes,
	networkname.BorMainnetChainName: borTypes,
	networkname.GnosisChainName:     ethereumTypes,
	networkname.ChiadoChainName:     ethereumTypes,
}

// KnownCfg return list of preverified hashes for given network, but apply whiteList filter if it's not empty
func KnownCfg(networkName string) *Cfg {
	c, ok := knownPreverified[networkName]

	if !ok {
		return newCfg(Preverified{})
	}

	return newCfg(c.Versioned(knownTypes[networkName]))
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
