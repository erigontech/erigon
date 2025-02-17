package entity_extras

import (
	"github.com/erigontech/erigon-lib/chain/snapcfg"
	"github.com/erigontech/erigon-lib/common/datadir"
)

// EntityId id as a uint64, returned by `RegisterAppendable`. It is dependent on
// the order of registration counting on it being constant across reboots might be tricky
// and is not recommended.
type EntityId uint16

type holder struct {
	name                   string
	snapshotNameBase       string   // name to be used in snapshot file
	indexNameBases         []string // one indexNameBase for each index
	dirs                   datadir.Dirs
	snapshotCreationConfig *SnapshotConfig
}

var entityRegistry []holder
var curr uint16

// RegisterEntity
// not making appendableRegistry/curr thread safe for now, since it's only expected to be setup once
// at the start and then read.
// name: just user-defined name for identification
// dirs: directory where snapshots have to reside
// salt: for creation of indexes.
// pre: preverified files are snapshot file lists that gets downloaded initially.
func RegisterEntity(name string, dirs datadir.Dirs, pre snapcfg.Preverified, options ...EntityIdOption) EntityId {
	h := &holder{
		name: name,
		dirs: dirs,
	}
	for _, opt := range options {
		opt(h)
	}

	if h.snapshotNameBase == "" {
		h.snapshotNameBase = name
	}

	if h.indexNameBases == nil {
		// default
		h.indexNameBases = []string{name}
	}

	if h.snapshotCreationConfig == nil {
		panic("snapshotCreationConfig is required")
	}
	entityRegistry = append(entityRegistry, *h)
	id := EntityId(curr)

	h.snapshotCreationConfig.SetupConfig(id, dirs, pre)

	curr++

	return id
}

type EntityIdOption func(*holder)

func WithSnapshotPrefix(prefix string) EntityIdOption {
	return func(a *holder) {
		a.snapshotNameBase = prefix
	}
}

func WithIndexFileType(indexFileType []string) EntityIdOption {
	return func(a *holder) {
		a.indexNameBases = indexFileType
	}
}

func WithSnapshotCreationConfig(cfg *SnapshotConfig) EntityIdOption {
	return func(a *holder) {
		a.snapshotCreationConfig = cfg
	}
}

func (a EntityId) Id() uint64 {
	return uint64(a)
}

func (a EntityId) Name() string {
	return entityRegistry[a].name
}

func (a EntityId) SnapshotPrefix() string {
	return entityRegistry[a].snapshotNameBase
}

func (a EntityId) IndexPrefix() []string {
	return entityRegistry[a].indexNameBases
}

func (a EntityId) String() string {
	return entityRegistry[a].name
}

func (a EntityId) Dirs() datadir.Dirs {
	return entityRegistry[a].dirs
}

func (a EntityId) SnapshotConfig() *SnapshotConfig {
	return entityRegistry[a].snapshotCreationConfig
}
