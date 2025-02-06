package appendableutils

import (
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/state"
)

// AppendableId id as a uint64, returned by `RegisterAppendable`
// dependent on the order of registration
// counting on it being constant across reboots might be tricky
// and is not recommended.
type AppendableId struct {
	*appendableId
}

type appendableId struct {
	uint64
	name           string
	snapshotPrefix string
	indexBuilders  []state.AccessorIndexBuilder
	indexPrefix    []string
	freezer        state.Freezer
	dirs           datadir.Dirs
}

var appendableRegistry []AppendableId
var last uint64

// decisions/TODOs:
// 1. last and appendableRegistry should be made concurrency safe
// 2. is it better to return a simple uint64 (and make fielld accesses more expensive); or to return pointer to struct (and maybe cheaper field access?)
func RegisterAppendable(name string, dirs datadir.Dirs, salt uint32, options ...AppendableIdOptions) AppendableId {
	curr := last
	id := appendableId{
		name: name,
		dirs: dirs,
	}
	for _, opt := range options {
		opt(&id)
	}

	if id.snapshotPrefix == "" {
		id.snapshotPrefix = name
	}

	if id.indexBuilders == nil {
		// default
		// mapping num -> offset (ordinal map)
		builder := NewSimpleAccessorBuilder(NewAccessorArgs(true, false, false, salt), curr)
		id.indexBuilders = []state.AccessorIndexBuilder{builder}
		id.indexPrefix = []string{name}
	}

	aid := AppendableId{&id}
	appendableRegistry = append(appendableRegistry, aid)
	last++
	return aid
}

type AppendableIdOptions func(*appendableId)

func WithSnapshotPrefix(prefix string) AppendableIdOptions {
	return func(a *appendableId) {
		a.snapshotPrefix = prefix
	}
}

func WithIndexBuilders(builders []state.AccessorIndexBuilder) AppendableIdOptions {
	return func(a *appendableId) {
		a.indexBuilders = builders
	}
}

func WithIndexFileType(indexFileType []string) AppendableIdOptions {
	return func(a *appendableId) {
		a.indexPrefix = indexFileType
	}
}

func WithFreezer(freezer state.Freezer) AppendableIdOptions {
	return func(a *appendableId) {
		a.freezer = freezer
	}
}

func WithDirs(dirs datadir.Dirs) AppendableIdOptions {
	return func(a *appendableId) {
		a.dirs = dirs
	}
}

func (a AppendableId) Id() uint64 {
	return a.uint64
}

func (a AppendableId) Name() string {
	return a.name
}

func (a AppendableId) SnapshotPrefix() string {
	return a.snapshotPrefix
}

func (a AppendableId) IndexBuilders() []state.AccessorIndexBuilder {
	return a.indexBuilders
}

func (a AppendableId) IndexPrefix() []string {
	return a.indexPrefix
}

func (a AppendableId) Freezer() state.Freezer {
	return a.freezer
}
