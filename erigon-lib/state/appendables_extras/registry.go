package appendables_extras

import (
	"github.com/erigontech/erigon-lib/chain/snapcfg"
	"github.com/erigontech/erigon-lib/common/datadir"
)

// AppendableId id as a uint64, returned by `RegisterAppendable`. It is dependent on
// the order of registration counting on it being constant across reboots might be tricky
// and is not recommended.
type AppendableId uint16

type holder struct {
	name                   string
	snapshotNameBase       string   // name to be used in snapshot file
	indexNameBases         []string // one indexNameBase for each index
	dirs                   datadir.Dirs
	snapshotCreationConfig *SnapshotCreationConfig
}

var appendableRegistry []holder
var curr uint16

// RegisterAppendable
// not making appendableRegistry/curr thread safe for now, since it's only expected to be setup once
// at the start and then read.
// name: just user-defined name for identification
// dirs: directory where snapshots have to reside
// salt: for creation of indexes.
// pre: preverified files are snapshot file lists that gets downloaded initially.
func RegisterAppendable(name string, dirs datadir.Dirs, salt uint32, pre snapcfg.Preverified, options ...AppendableIdOption) AppendableId {
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
	appendableRegistry = append(appendableRegistry, *h)
	id := AppendableId(curr)

	h.snapshotCreationConfig.parseConfig(id, dirs, pre)

	curr++

	return id
}

type AppendableIdOption func(*holder)

func WithSnapshotPrefix(prefix string) AppendableIdOption {
	return func(a *holder) {
		a.snapshotNameBase = prefix
	}
}

func WithIndexFileType(indexFileType []string) AppendableIdOption {
	return func(a *holder) {
		a.indexNameBases = indexFileType
	}
}

func WithSnapshotCreationConfig(cfg *SnapshotCreationConfig) AppendableIdOption {
	return func(a *holder) {
		a.snapshotCreationConfig = cfg
	}
}

func (a AppendableId) Id() uint64 {
	return uint64(a)
}

func (a AppendableId) Name() string {
	return appendableRegistry[a].name
}

func (a AppendableId) SnapshotPrefix() string {
	return appendableRegistry[a].snapshotNameBase
}

func (a AppendableId) IndexPrefix() []string {
	return appendableRegistry[a].indexNameBases
}

func (a AppendableId) String() string {
	return appendableRegistry[a].name
}

func (a AppendableId) Dirs() datadir.Dirs {
	return appendableRegistry[a].dirs
}

func (a AppendableId) SnapshotCreationConfig() *SnapshotCreationConfig {
	return appendableRegistry[a].snapshotCreationConfig
}
