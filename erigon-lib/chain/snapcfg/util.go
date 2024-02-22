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

func Merge(p0 Preverified, p1 []PreverifiedItem) Preverified {
	merged := append(p0, p1...)
	slices.SortFunc(merged, func(i, j PreverifiedItem) int { return strings.Compare(i.Name, j.Name) })
	return merged
}

func (p Preverified) Get(name string) (PreverifiedItem, bool) {
	i := sort.Search(len(p), func(i int) bool { return p[i].Name >= name })
	if i >= len(p) || p[i].Name != name {
		return PreverifiedItem{}, false
	}

	return p[i], true
}

func (p Preverified) Contains(name string, ignoreVersion ...bool) bool {
	if len(ignoreVersion) > 0 && ignoreVersion[0] {
		_, name, _ := strings.Cut(name, "-")
		for _, item := range p {
			_, noVersion, _ := strings.Cut(item.Name, "-")
			if noVersion == name {
				return true
			}
		}
		return false
	}

	i := sort.Search(len(p), func(i int) bool { return p[i].Name >= name })
	return i < len(p) && p[i].Name == name
}

func (p Preverified) Typed(types []snaptype.Type) Preverified {
	var bestVersions btree.Map[string, PreverifiedItem]

	for _, p := range p {
		v, name, ok := strings.Cut(p.Name, "-")
		if !ok {
			continue
		}

		var preferredVersion, minVersion snaptype.Version

		parts := strings.Split(name, "-")
		if len(parts) < 3 {
			continue
		}
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

func (p Preverified) Versioned(preferredVersion snaptype.Version, minVersion snaptype.Version, types ...snaptype.Enum) Preverified {
	var bestVersions btree.Map[string, PreverifiedItem]

	for _, p := range p {
		v, name, ok := strings.Cut(p.Name, "-")

		if !ok {
			continue
		}

		parts := strings.Split(name, "-")
		typeName, _ := strings.CutSuffix(parts[2], filepath.Ext(parts[2]))
		include := false

		if len(types) > 0 {
			for _, typ := range types {
				if typeName == typ.String() {
					include = true
					break
				}
			}

			if !include {
				continue
			}
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

func newCfg(networkName string, preverified Preverified) *Cfg {
	maxBlockNum, _ := preverified.MaxBlock(0)
	return &Cfg{ExpectBlocks: maxBlockNum, Preverified: preverified, networkName: networkName}
}

type Cfg struct {
	ExpectBlocks uint64
	Preverified  Preverified
	networkName  string
}

func (c Cfg) Seedable(info snaptype.FileInfo) bool {
	mergeLimit := c.MergeLimit(info.From)
	return info.To-info.From == mergeLimit
}

func (c Cfg) MergeLimit(fromBlock uint64) uint64 {
	for _, p := range c.Preverified {
		if info, ok := snaptype.ParseFileName("", p.Name); ok && info.Ext == ".seg" {
			if fromBlock >= info.From && fromBlock < info.To {
				if info.Len() == snaptype.Erigon2MergeLimit ||
					info.Len() == snaptype.Erigon2OldMergeLimit {
					return info.Len()
				}

				break
			}
		}
	}

	return snaptype.Erigon2MergeLimit
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

func Seedable(networkName string, info snaptype.FileInfo) bool {
	return KnownCfg(networkName).Seedable(info)
}

func MergeLimit(networkName string, fromBlock uint64) uint64 {
	return KnownCfg(networkName).MergeLimit(fromBlock)
}

func MaxSeedableSegment(chain string, dir string) uint64 {
	var max uint64

	if list, err := snaptype.Segments(dir); err == nil {
		for _, info := range list {
			if Seedable(chain, info) && info.Type.Enum() == snaptype.Enums.Headers && info.To > max {
				max = info.To
			}
		}
	}

	return max
}

var oldMergeSteps = append([]uint64{snaptype.Erigon2OldMergeLimit}, snaptype.MergeSteps...)

func MergeSteps(networkName string, fromBlock uint64) []uint64 {
	mergeLimit := MergeLimit(networkName, fromBlock)

	if mergeLimit == snaptype.Erigon2OldMergeLimit {
		return oldMergeSteps
	}

	return snaptype.MergeSteps
}

// KnownCfg return list of preverified hashes for given network, but apply whiteList filter if it's not empty
func KnownCfg(networkName string) *Cfg {
	c, ok := knownPreverified[networkName]

	if !ok {
		return newCfg(networkName, Preverified{})
	}

	return newCfg(networkName, c.Typed(knownTypes[networkName]))
}

func VersionedCfg(networkName string, preferred snaptype.Version, min snaptype.Version) *Cfg {
	c, ok := knownPreverified[networkName]

	if !ok {
		return newCfg(networkName, Preverified{})
	}

	return newCfg(networkName, c.Versioned(preferred, min))
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
