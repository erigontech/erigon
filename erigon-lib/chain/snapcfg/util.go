// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package snapcfg

import (
	_ "embed"
	"encoding/json"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"github.com/pelletier/go-toml/v2"
	"github.com/tidwall/btree"

	snapshothashes "github.com/erigontech/erigon-snapshot"
	"github.com/erigontech/erigon-snapshot/webseed"

	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
)

var (
	Mainnet    = fromToml(snapshothashes.Mainnet)
	Holesky    = fromToml(snapshothashes.Holesky)
	Sepolia    = fromToml(snapshothashes.Sepolia)
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

		countSep := 0
		var lastSep, dot int
		for i := range name {
			if name[i] == '-' {
				countSep++
				lastSep = i
			}
			if name[i] == '.' {
				dot = i
			}
		}

		if countSep < 2 {
			if strings.HasPrefix(p.Name, "domain") || strings.HasPrefix(p.Name, "history") || strings.HasPrefix(p.Name, "idx") || strings.HasPrefix(p.Name, "accessor") {
				bestVersions.Set(p.Name, p)
				continue
			}
			continue
		}

		//typeName, _ := strings.CutSuffix(parts[2], filepath.Ext(parts[2]))
		typeName := name[lastSep+1 : dot]
		include := false
		if strings.Contains(name, "transactions-to-block") { // transactions-to-block should just be "transactions" type
			typeName = "transactions"
		}

		for _, typ := range types {
			if typeName == typ.Name() {
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
			if strings.HasPrefix(p.Name, "domain") || strings.HasPrefix(p.Name, "history") || strings.HasPrefix(p.Name, "idx") || strings.HasPrefix(p.Name, "accessor") {
				bestVersions.Set(p.Name, p)
				continue
			}
			continue
		}

		parts := strings.Split(name, "-")
		if len(parts) < 3 {
			if strings.HasPrefix(p.Name, "domain") || strings.HasPrefix(p.Name, "history") || strings.HasPrefix(p.Name, "idx") || strings.HasPrefix(p.Name, "accessor") {
				bestVersions.Set(p.Name, p)
				continue
			}
			continue
		}
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

		to, err := ExtractBlockFromName(fileName[:len(fileName)-len(ext)], version)
		if err != nil {
			if errors.Is(err, errWrongVersion) {
				continue
			}
			return 0, err
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

var errWrongVersion = errors.New("wrong version")

func ExtractBlockFromName(name string, v snaptype.Version) (block uint64, err error) {
	i := 0
	for i < len(name) && name[i] != '-' {
		i++
	}

	version, err := snaptype.ParseVersion(name[:i])
	if err != nil {
		return 0, err
	}

	if v != 0 && v != version {
		return 0, errWrongVersion
	}

	i++

	for i < len(name) && name[i] != '-' { // skipping parts[1]
		i++
	}

	i++
	start := i
	if start > len(name)-1 {
		return 0, errors.New("invalid name")
	}

	for i < len(name) && name[i] != '-' {
		i++
	}

	end := i

	if i > len(name) {
		end = len(name)
	}

	block, err = strconv.ParseUint(name[start:end], 10, 64)
	if err != nil {
		return 0, err
	}

	return block, nil
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
	mergeLimit := c.MergeLimit(info.Type.Enum(), info.From)
	return info.To-info.From == mergeLimit
}

func (c Cfg) MergeLimit(t snaptype.Enum, fromBlock uint64) uint64 {
	hasType := t == snaptype.MinCoreEnum

	for _, p := range c.Preverified {
		info, _, ok := snaptype.ParseFileName("", p.Name)
		if !ok {
			continue
		}

		if info.Ext != ".seg" || (t != snaptype.Unknown && t != info.Type.Enum()) {
			continue
		}

		hasType = true

		if fromBlock < info.From || fromBlock >= info.To {
			continue
		}

		if info.Len() == snaptype.Erigon2MergeLimit ||
			info.Len() == snaptype.Erigon2OldMergeLimit {
			return info.Len()
		}

		break
	}

	// This should only get called the first time a new type is added and created - as it will
	// not have previous history to check against

	// BeaconBlocks && BlobSidecars follow their own slot based sharding scheme which is
	// not the same as other snapshots which follow a block based sharding scheme
	// TODO: If we add any more sharding schemes (we currently have blocks, state & beacon block schemes)
	// - we may need to add some kind of sharding scheme identifier to snaptype.Type
	if hasType || snaptype.IsCaplinType(t) {
		return snaptype.Erigon2MergeLimit
	}

	return c.MergeLimit(snaptype.MinCoreEnum, fromBlock)
}

var knownPreverified = map[string]Preverified{
	networkname.MainnetChainName:    Mainnet,
	networkname.HoleskyChainName:    Holesky,
	networkname.SepoliaChainName:    Sepolia,
	networkname.AmoyChainName:       Amoy,
	networkname.BorMainnetChainName: BorMainnet,
	networkname.GnosisChainName:     Gnosis,
	networkname.ChiadoChainName:     Chiado,
}

func RegisterKnownTypes(networkName string, types []snaptype.Type) {
	knownTypes[networkName] = types
}

var knownTypes = map[string][]snaptype.Type{}

func Seedable(networkName string, info snaptype.FileInfo) bool {
	if networkName == "" {
		return false
	}
	return KnownCfg(networkName).Seedable(info)
}

func MergeLimitFromCfg(cfg *Cfg, snapType snaptype.Enum, fromBlock uint64) uint64 {
	return cfg.MergeLimit(snapType, fromBlock)
}

func MaxSeedableSegment(chain string, dir string) uint64 {
	var max uint64

	if list, err := snaptype.Segments(dir); err == nil {
		for _, info := range list {
			if Seedable(chain, info) && info.Type.Enum() == snaptype.MinCoreEnum && info.To > max {
				max = info.To
			}
		}
	}

	return max
}

var oldMergeSteps = append([]uint64{snaptype.Erigon2OldMergeLimit}, snaptype.MergeSteps...)

func MergeStepsFromCfg(cfg *Cfg, snapType snaptype.Enum, fromBlock uint64) []uint64 {
	mergeLimit := MergeLimitFromCfg(cfg, snapType, fromBlock)

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
	networkname.AmoyChainName:       webseedsParse(webseed.Amoy),
	networkname.BorMainnetChainName: webseedsParse(webseed.BorMainnet),
	networkname.GnosisChainName:     webseedsParse(webseed.Gnosis),
	networkname.ChiadoChainName:     webseedsParse(webseed.Chiado),
	networkname.HoleskyChainName:    webseedsParse(webseed.Holesky),
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

func LoadRemotePreverified() bool {
	couldFetch := snapshothashes.LoadSnapshots()

	// Re-load the preverified hashes
	Mainnet = fromToml(snapshothashes.Mainnet)
	Holesky = fromToml(snapshothashes.Holesky)
	Sepolia = fromToml(snapshothashes.Sepolia)
	Amoy = fromToml(snapshothashes.Amoy)
	BorMainnet = fromToml(snapshothashes.BorMainnet)
	Gnosis = fromToml(snapshothashes.Gnosis)
	Chiado = fromToml(snapshothashes.Chiado)
	// Update the known preverified hashes
	KnownWebseeds = map[string][]string{
		networkname.MainnetChainName:    webseedsParse(webseed.Mainnet),
		networkname.SepoliaChainName:    webseedsParse(webseed.Sepolia),
		networkname.AmoyChainName:       webseedsParse(webseed.Amoy),
		networkname.BorMainnetChainName: webseedsParse(webseed.BorMainnet),
		networkname.GnosisChainName:     webseedsParse(webseed.Gnosis),
		networkname.ChiadoChainName:     webseedsParse(webseed.Chiado),
		networkname.HoleskyChainName:    webseedsParse(webseed.Holesky),
	}

	knownPreverified = map[string]Preverified{
		networkname.MainnetChainName:    Mainnet,
		networkname.HoleskyChainName:    Holesky,
		networkname.SepoliaChainName:    Sepolia,
		networkname.AmoyChainName:       Amoy,
		networkname.BorMainnetChainName: BorMainnet,
		networkname.GnosisChainName:     Gnosis,
		networkname.ChiadoChainName:     Chiado,
	}
	return couldFetch
}

func SetToml(networkName string, toml []byte) {
	if _, ok := knownPreverified[networkName]; !ok {
		return
	}
	knownPreverified[networkName] = fromToml(toml)
}

func GetToml(networkName string) []byte {
	switch networkName {
	case networkname.MainnetChainName:
		return snapshothashes.Mainnet
	case networkname.HoleskyChainName:
		return snapshothashes.Holesky
	case networkname.SepoliaChainName:
		return snapshothashes.Sepolia
	case networkname.AmoyChainName:
		return snapshothashes.Amoy
	case networkname.BorMainnetChainName:
		return snapshothashes.BorMainnet
	case networkname.GnosisChainName:
		return snapshothashes.Gnosis
	case networkname.ChiadoChainName:
		return snapshothashes.Chiado
	default:
		return nil
	}
}
