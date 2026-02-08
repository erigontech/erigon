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
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	snapshothashes "github.com/erigontech/erigon-snapshot"
	"github.com/erigontech/erigon-snapshot/webseed"
	"github.com/pelletier/go-toml/v2"
	"github.com/tidwall/btree"

	"github.com/erigontech/erigon/db/preverified"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
	ver "github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain/networkname"
)

type (
	PreverifiedItems = preverified.SortedItems
	PreverifiedItem  = preverified.Item
)

var snapshotGitBranch = dbg.EnvString("SNAPS_GIT_BRANCH", ver.DefaultSnapshotGitBranch)

type preverifiedRegistry struct {
	mu     sync.RWMutex
	data   map[string]Preverified
	cached map[string]*Cfg
}

var registry = &preverifiedRegistry{
	data: map[string]Preverified{
		networkname.Mainnet:    fromEmbeddedToml(snapshothashes.Mainnet),
		networkname.Sepolia:    fromEmbeddedToml(snapshothashes.Sepolia),
		networkname.Amoy:       fromEmbeddedToml(snapshothashes.Amoy),
		networkname.BorMainnet: fromEmbeddedToml(snapshothashes.BorMainnet),
		networkname.Gnosis:     fromEmbeddedToml(snapshothashes.Gnosis),
		networkname.Chiado:     fromEmbeddedToml(snapshothashes.Chiado),
		networkname.Hoodi:      fromEmbeddedToml(snapshothashes.Hoodi),
		networkname.Bloatnet:   fromEmbeddedToml(snapshothashes.Bloatnet),
	},
	cached: make(map[string]*Cfg),
}

func (r *preverifiedRegistry) All() map[string]Preverified {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return maps.Clone(r.data)
}

func (r *preverifiedRegistry) Get(networkName string) (*Cfg, bool) {
	r.mu.RLock()
	if cfg, ok := r.cached[networkName]; ok {
		r.mu.RUnlock()
		return cfg, true
	}
	pv, ok := r.data[networkName]
	r.mu.RUnlock()

	if !ok {
		return newCfg(networkName, Preverified{}), false
	}

	cfg := newCfg(networkName, pv.Typed(knownTypes[networkName]))

	r.mu.Lock()
	// Double-check after acquiring write lock
	if existing, ok := r.cached[networkName]; ok {
		r.mu.Unlock()
		return existing, true
	}
	r.cached[networkName] = cfg
	r.mu.Unlock()

	return cfg, true
}

func (r *preverifiedRegistry) Set(networkName string, pv Preverified) {
	r.mu.Lock()
	r.data[networkName] = pv
	delete(r.cached, networkName) // Invalidate cache atomically
	r.mu.Unlock()
}

func (r *preverifiedRegistry) Reset(data map[string]Preverified) {
	r.mu.Lock()
	r.data = data
	r.cached = make(map[string]*Cfg) // Clear all cached
	r.mu.Unlock()
}

var (
	// This belongs in a generic embed.FS or something.
	allSnapshotHashes = []*[]byte{
		&snapshothashes.Mainnet,
		&snapshothashes.Sepolia,
		&snapshothashes.Amoy,
		&snapshothashes.BorMainnet,
		&snapshothashes.Gnosis,
		&snapshothashes.Chiado,
		&snapshothashes.Hoodi,
	}
)

func fromEmbeddedToml(in []byte) Preverified {
	items := fromToml(in)
	return Preverified{
		Local: false,
		Items: items,
	}
}

type Preverified struct {
	Local bool
	// These should be sorted by Name.
	Items PreverifiedItems
}

func (p Preverified) Typed(types []snaptype.Type) Preverified {
	var bestVersions btree.Map[string, PreverifiedItem]

	for _, p := range p.Items {
		if strings.HasPrefix(p.Name, "salt") && strings.HasSuffix(p.Name, "txt") {
			bestVersions.Set(p.Name, p)
			continue
		}
		if p.Name == "erigondb.toml" {
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
		idxIndex := 0
		if strings.Contains(name, "transactions-to-block") { // transactions-to-block should just be "transactions" type
			idxIndex = 1
			typeName = "transactions"
		}
		if strings.Contains(name, "blocksidecars") {
			typeName = "blobsidecars"
		}

		for _, typ := range types {
			if typeName == typ.Name() {
				var versions version.Versions
				if strings.HasSuffix(p.Name, "idx") {
					versions = typ.Indexes()[idxIndex].Version
				} else {
					versions = typ.Versions()
				}
				preferredVersion = versions.Current
				minVersion = versions.MinSupported
				include = true
				break
			}
		}

		if !include {
			continue
		}

		version, err := ver.ParseVersion(v)
		if err != nil {
			continue
		}

		if version.Less(minVersion) {
			continue
		}

		if preferredVersion.Less(version) {
			continue
		}

		if current, ok := bestVersions.Get(name); ok {
			v, _, _ := strings.Cut(current.Name, "-")
			cv, _ := ver.ParseVersion(v)

			if cv.Less(version) {
				bestVersions.Set(name, p)
			}
		} else {
			bestVersions.Set(name, p)
		}
	}

	var versioned []PreverifiedItem

	// Scanning this can introduce an unexpected order to preverified items as it's not keyed on the
	// item name.
	bestVersions.Scan(func(key string, value PreverifiedItem) bool {
		versioned = append(versioned, value)
		return true
	})
	slices.SortFunc(versioned, func(i, j PreverifiedItem) int {
		return strings.Compare(i.Name, j.Name)
	})
	if len(p.Items) != len(versioned) {
		log.Root().Warn("Preverified list reduced after applying type filter", "from", len(p.Items), "to", len(versioned))
		// for _, v := range p.Items {
		// 	if !slices.ContainsFunc(versioned, func(item PreverifiedItem) bool {
		// 		return item.Name == v.Name
		// 	}) {
		// 		log.Root().Warn("Preverified item removed by type filter", "name", v.Name)
		// 	}
		// }
	} else {
		log.Root().Debug("Preverified list has same len after applying type filter", "len", len(p.Items))
	}
	p.Items = versioned
	return p
}

func (p Preverified) MaxBlock(version ver.Version) (uint64, error) {
	_max := uint64(0)
	for _, p := range p.Items {
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

func ExtractBlockFromName(name string, v ver.Version) (block uint64, err error) {
	i := 0
	for i < len(name) && name[i] != '-' {
		i++
	}

	version, err := ver.ParseVersion(name[:i])
	if err != nil {
		return 0, err
	}

	if !v.IsZero() && v != version {
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

	end := min(i, len(name))

	block, err = strconv.ParseUint(name[start:end], 10, 64)
	if err != nil {
		return 0, err
	}

	return block, nil
}
func fromToml(in []byte) PreverifiedItems {
	var outMap map[string]string
	if err := toml.Unmarshal(in, &outMap); err != nil {
		panic(err)
	}

	return preverified.ItemsFromMap(outMap)
}

func newCfg(networkName string, preverified Preverified) *Cfg {
	maxBlockNum, _ := preverified.MaxBlock(ver.ZeroVersion)
	cfg := &Cfg{
		ExpectBlocks: maxBlockNum,
		Preverified:  preverified,
		networkName:  networkName,
		Local:        preverified.Local,
	}
	cfg.PreverifiedParsed = make([]*snaptype.FileInfo, len(preverified.Items))
	for i, p := range cfg.Preverified.Items {
		// TODO: Pack these into a single array, or consider inlining them into PreverifiedParsed?
		info, _, ok := snaptype.ParseFileName("", p.Name)
		if !ok {
			continue
		}
		cfg.PreverifiedParsed[i] = &info
	}
	return cfg
}

type Cfg struct {
	ExpectBlocks      uint64
	Preverified       Preverified          // immutable
	PreverifiedParsed []*snaptype.FileInfo //Preverified field after `snaptype.ParseFileName("", p.Name)`
	// The preverified list were loaded from local storage. That means they were committed after an
	// initial sync completed successfully.
	Local       bool
	networkName string
}

// Seedable - can seed it over Bittorrent network to other nodes
func (c Cfg) Seedable(info snaptype.FileInfo) bool {
	mergeLimit := c.MergeLimit(info.Type.Enum(), info.From)
	return info.To-info.From == mergeLimit
}

// IsFrozen - can't be merged to bigger files
func (c Cfg) IsFrozen(info snaptype.FileInfo) bool {
	mergeLimit := c.MergeLimit(info.Type.Enum(), info.From)
	return info.To-info.From >= mergeLimit
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

func RegisterKnownTypes(networkName string, types []snaptype.Type) {
	knownTypes[networkName] = types
}

var knownTypes = map[string][]snaptype.Type{}

func Seedable(networkName string, info snaptype.FileInfo) bool {
	if networkName == "" {
		return false
	}
	snapCfg, _ := KnownCfg(networkName)
	return snapCfg.Seedable(info)
}

func MergeLimitFromCfg(cfg *Cfg, snapType snaptype.Enum, fromBlock uint64) uint64 {
	return cfg.MergeLimit(snapType, fromBlock)
}

func MaxSeedableSegment(chain string, dir string) uint64 {
	var _max uint64
	segConfig, _ := KnownCfg(chain)
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

// KnownCfg return list of preverified hashes for given network, but apply whiteList filter if it's not empty
func KnownCfg(networkName string) (*Cfg, bool) {
	return registry.Get(networkName)
}

var KnownWebseeds = map[string][]string{
	networkname.Mainnet:    webseedsParse(webseed.Mainnet),
	networkname.Sepolia:    webseedsParse(webseed.Sepolia),
	networkname.Amoy:       webseedsParse(webseed.Amoy),
	networkname.BorMainnet: webseedsParse(webseed.BorMainnet),
	networkname.Gnosis:     webseedsParse(webseed.Gnosis),
	networkname.Chiado:     webseedsParse(webseed.Chiado),
	networkname.Hoodi:      webseedsParse(webseed.Hoodi),
	networkname.Bloatnet:   webseedsParse(webseed.Bloatnet),
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

const RemotePreverifiedEnvKey = "ERIGON_REMOTE_PREVERIFIED"

func LoadRemotePreverified(ctx context.Context) (err error) {
	if s, ok := os.LookupEnv(RemotePreverifiedEnvKey); ok {
		log.Info("Loading local preverified override file", "file", s)

		b, err := os.ReadFile(s)
		if err != nil {
			return fmt.Errorf("reading remote preverified override file: %w", err)
		}
		for _, sh := range allSnapshotHashes {
			*sh = bytes.Clone(b)
		}
	} else {
		log.Info("Loading remote snapshot hashes")

		err = snapshothashes.LoadSnapshots(ctx, snapshothashes.R2, snapshotGitBranch)
		if err != nil {
			log.Root().Warn("Failed to load snapshot hashes from R2; falling back to GitHub", "err", err)

			// Fallback to GitHub if R2 fails
			err = snapshothashes.LoadSnapshots(ctx, snapshothashes.Github, snapshotGitBranch)
			if err != nil {
				return err
			}
		}
	}

	KnownWebseeds = map[string][]string{
		networkname.Mainnet:    webseedsParse(webseed.Mainnet),
		networkname.Sepolia:    webseedsParse(webseed.Sepolia),
		networkname.Amoy:       webseedsParse(webseed.Amoy),
		networkname.BorMainnet: webseedsParse(webseed.BorMainnet),
		networkname.Gnosis:     webseedsParse(webseed.Gnosis),
		networkname.Chiado:     webseedsParse(webseed.Chiado),
		networkname.Hoodi:      webseedsParse(webseed.Hoodi),
		networkname.Bloatnet:   webseedsParse(webseed.Bloatnet),
	}

	// Re-load the preverified hashes
	registry.Reset(map[string]Preverified{
		networkname.Mainnet:    fromEmbeddedToml(snapshothashes.Mainnet),
		networkname.Sepolia:    fromEmbeddedToml(snapshothashes.Sepolia),
		networkname.Amoy:       fromEmbeddedToml(snapshothashes.Amoy),
		networkname.BorMainnet: fromEmbeddedToml(snapshothashes.BorMainnet),
		networkname.Gnosis:     fromEmbeddedToml(snapshothashes.Gnosis),
		networkname.Chiado:     fromEmbeddedToml(snapshothashes.Chiado),
		networkname.Hoodi:      fromEmbeddedToml(snapshothashes.Hoodi),
		networkname.Bloatnet:   fromEmbeddedToml(snapshothashes.Bloatnet),
	})
	return
}

func SetToml(networkName string, toml []byte, local bool) {
	if _, ok := registry.Get(networkName); ok {
		registry.Set(networkName, Preverified{Local: local, Items: fromToml(toml)})
	}
}

func GetToml(networkName string) []byte {
	switch networkName {
	case networkname.Mainnet:
		return snapshothashes.Mainnet
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
	case networkname.Hoodi:
		return snapshothashes.Hoodi
	case networkname.Bloatnet:
		return snapshothashes.Bloatnet
	default:
		return nil
	}
}

// Gets the current preverified for all chains.
func GetAllCurrentPreverified() map[string]Preverified {
	return registry.All()
}

// Converts webseed value to URL. Mostly this is just stripping v1: for now, as nothing else is in
// active use.
func WebseedToUrl(s string) (_ string, err error) {
	after, ok := strings.CutPrefix(s, "v1:")
	if !ok {
		err = fmt.Errorf("unhandled webseed %q", s)
		return
	}
	return after, nil
}
