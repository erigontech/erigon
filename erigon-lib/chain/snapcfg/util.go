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
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/erigontech/erigon-lib/log/v3"
	snapshothashes "github.com/erigontech/erigon-snapshot"
	"github.com/erigontech/erigon-snapshot/webseed"
	"github.com/pelletier/go-toml/v2"
	"github.com/tidwall/btree"

	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/version"
)

var snapshotGitBranch = dbg.EnvString("SNAPS_GIT_BRANCH", version.DefaultSnapshotGitBranch)

var (
	Mainnet      = fromToml(snapshothashes.Mainnet)
	Holesky      = fromToml(snapshothashes.Holesky)
	Sepolia      = fromToml(snapshothashes.Sepolia)
	Amoy         = fromToml(snapshothashes.Amoy)
	BorMainnet   = fromToml(snapshothashes.BorMainnet)
	Gnosis       = fromToml(snapshothashes.Gnosis)
	Chiado       = fromToml(snapshothashes.Chiado)
	TaikoAlethia = fromToml(snapshothashes.TaikoAlethia) // CHANGE(taiko)
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
		if strings.HasPrefix(p.Name, "salt") && strings.HasSuffix(p.Name, "txt") {
			bestVersions.Set(p.Name, p)
			continue
		}

		v, name, ok := strings.Cut(p.Name, "-")
		if !ok {
			continue
		}

		if strings.HasPrefix(p.Name, "caplin") {
			bestVersions.Set(p.Name, p)
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
	_max := uint64(0)
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

		if _max < to {
			_max = to
		}

	}
	if _max == 0 { // to prevent underflow
		return 0, nil
	}

	return _max*1_000 - 1, nil
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
	cfg := &Cfg{ExpectBlocks: maxBlockNum, Preverified: preverified, networkName: networkName}
	cfg.PreverifiedParsed = make([]*snaptype.FileInfo, len(preverified))
	for i, p := range cfg.Preverified {
		info, _, ok := snaptype.ParseFileName("", p.Name)
		if !ok {
			continue
		}
		cfg.PreverifiedParsed[i] = &info
	}
	return cfg
}

func NewNonSeededCfg(networkName string) *Cfg {
	return newCfg(networkName, Preverified{})
}

type Cfg struct {
	ExpectBlocks      uint64
	Preverified       Preverified          // immutable
	PreverifiedParsed []*snaptype.FileInfo //Preverified field after `snaptype.ParseFileName("", p.Name)`
	networkName       string
}

// Seedable - can seed it over Bittorrent network to other nodes
func (c Cfg) Seedable(info snaptype.FileInfo) bool {
	mergeLimit := c.MergeLimit(info.Type.Enum(), info.From)
	return info.To-info.From == mergeLimit
}

// IsFrozen - can't be merged to bigger files
func (c Cfg) IsFrozen(info snaptype.FileInfo) bool {
	mergeLimit := c.MergeLimit(info.Type.Enum(), info.From)
	return info.To-info.From == mergeLimit
}

func (c Cfg) MergeLimit(t snaptype.Enum, fromBlock uint64) uint64 {
	hasType := t == snaptype.MinCoreEnum

	for _, info := range c.PreverifiedParsed {
		if info == nil {
			continue
		}
		if strings.Contains(info.Name(), "caplin") {
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
	if snaptype.IsCaplinType(t) {
		return snaptype.CaplinMergeLimit
	}
	if hasType {
		return snaptype.Erigon2MergeLimit
	}

	return c.MergeLimit(snaptype.MinCoreEnum, fromBlock)
}

var knownPreverified = map[string]Preverified{
	networkname.Mainnet:      Mainnet,
	networkname.Holesky:      Holesky,
	networkname.Sepolia:      Sepolia,
	networkname.Amoy:         Amoy,
	networkname.BorMainnet:   BorMainnet,
	networkname.Gnosis:       Gnosis,
	networkname.Chiado:       Chiado,
	networkname.TaikoAlethia: TaikoAlethia, // CHANGE(taiko)
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
	var _max uint64
	segConfig := KnownCfg(chain)
	if list, err := snaptype.Segments(dir); err == nil {
		for _, info := range list {
			if segConfig.Seedable(info) && info.Type.Enum() == snaptype.MinCoreEnum && info.To > _max {
				_max = info.To
			}
		}
	}

	return _max
}

var oldMergeSteps = append([]uint64{snaptype.Erigon2OldMergeLimit}, snaptype.MergeSteps...)

func MergeStepsFromCfg(cfg *Cfg, snapType snaptype.Enum, fromBlock uint64) []uint64 {
	mergeLimit := MergeLimitFromCfg(cfg, snapType, fromBlock)

	if mergeLimit == snaptype.Erigon2OldMergeLimit {
		return oldMergeSteps
	}

	return snaptype.MergeSteps
}

func IsFrozen(networkName string, info snaptype.FileInfo) bool {
	if networkName == "" {
		return false
	}
	return KnownCfg(networkName).IsFrozen(info)
}

// KnownCfg return list of preverified hashes for given network, but apply whiteList filter if it's not empty
func KnownCfg(networkName string) *Cfg {
	c, ok := knownPreverified[networkName]
	if !ok {
		return newCfg(networkName, Preverified{})
	}
	return newCfg(networkName, c.Typed(knownTypes[networkName]))
}

func VersionedCfg(networkName string, preferred snaptype.Version, _min snaptype.Version) *Cfg {
	c, ok := knownPreverified[networkName]

	if !ok {
		return newCfg(networkName, Preverified{})
	}

	return newCfg(networkName, c.Versioned(preferred, _min))
}

var KnownWebseeds = map[string][]string{
	networkname.Mainnet:      webseedsParse(webseed.Mainnet),
	networkname.Sepolia:      webseedsParse(webseed.Sepolia),
	networkname.Amoy:         webseedsParse(webseed.Amoy),
	networkname.BorMainnet:   webseedsParse(webseed.BorMainnet),
	networkname.Gnosis:       webseedsParse(webseed.Gnosis),
	networkname.Chiado:       webseedsParse(webseed.Chiado),
	networkname.Holesky:      webseedsParse(webseed.Holesky),
	networkname.TaikoAlethia: webseedsParse(webseed.TaikoAlethia), // CHANGE(taiko)
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

func LoadRemotePreverified(ctx context.Context) (loaded bool, err error) {
	loaded, err = snapshothashes.LoadSnapshots(ctx, snapshothashes.R2, snapshotGitBranch)
	if err != nil {
		log.Root().Warn("Failed to load snapshot hashes from R2; falling back to GitHub", "err", err)

		// Fallback to github if R2 fails
		loaded, err = snapshothashes.LoadSnapshots(ctx, snapshothashes.Github, snapshotGitBranch)
		if err != nil {
			return false, err
		}
	}

	// Re-load the preverified hashes
	Mainnet = fromToml(snapshothashes.Mainnet)
	Holesky = fromToml(snapshothashes.Holesky)
	Sepolia = fromToml(snapshothashes.Sepolia)
	Amoy = fromToml(snapshothashes.Amoy)
	BorMainnet = fromToml(snapshothashes.BorMainnet)
	Gnosis = fromToml(snapshothashes.Gnosis)
	Chiado = fromToml(snapshothashes.Chiado)
	// CHANGE(taiko) : load taiko snapshots
	TaikoAlethia = fromToml(snapshothashes.TaikoAlethia)
	// Update the known preverified hashes
	KnownWebseeds = map[string][]string{
		networkname.Mainnet:    webseedsParse(webseed.Mainnet),
		networkname.Sepolia:    webseedsParse(webseed.Sepolia),
		networkname.Amoy:       webseedsParse(webseed.Amoy),
		networkname.BorMainnet: webseedsParse(webseed.BorMainnet),
		networkname.Gnosis:     webseedsParse(webseed.Gnosis),
		networkname.Chiado:     webseedsParse(webseed.Chiado),
		networkname.Holesky:    webseedsParse(webseed.Holesky),
		// CHANGE(taiko)
		networkname.TaikoAlethia: webseedsParse(webseed.TaikoAlethia),
	}

	knownPreverified = map[string]Preverified{
		networkname.Mainnet:    Mainnet,
		networkname.Holesky:    Holesky,
		networkname.Sepolia:    Sepolia,
		networkname.Amoy:       Amoy,
		networkname.BorMainnet: BorMainnet,
		networkname.Gnosis:     Gnosis,
		networkname.Chiado:     Chiado,
		// CHANGE(taiko)
		networkname.TaikoAlethia: TaikoAlethia,
	}
	return loaded, nil
}

func SetToml(networkName string, toml []byte) {
	if _, ok := knownPreverified[networkName]; !ok {
		return
	}
	knownPreverified[networkName] = fromToml(toml)
}

func GetToml(networkName string) []byte {
	switch networkName {
	case networkname.Mainnet:
		return snapshothashes.Mainnet
	case networkname.Holesky:
		return snapshothashes.Holesky
	case networkname.Sepolia:
		return snapshothashes.Sepolia
	case networkname.Amoy:
		return snapshothashes.Amoy
	case networkname.BorMainnet:
		return snapshothashes.BorMainnet
	case networkname.Gnosis:
		return snapshothashes.Gnosis
	case networkname.Chiado:
		return snapshothashes.Chiado
	// CHANGE(taiko)
	case networkname.TaikoAlethia:
		return snapshothashes.TaikoAlethia
	default:
		return nil
	}
}
