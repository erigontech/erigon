package state

import (
	"github.com/erigontech/erigon-lib/common/datadir"
)

// AppendableId id as a uint64, returned by `RegisterAppendable`
// dependent on the order of registration
// counting on it being constant across reboots might be tricky
// and is not recommended.

type AppendableId uint16

type holder struct {
	name             string
	snapshotNameBase string   // name to be used in snapshot file
	indexNameBases   []string // one indexNameBase for each index
	dirs             datadir.Dirs
}

var appendableRegistry []holder
var curr uint16

// decisions/TODOs:
// 1. curr and appendableRegistry should be made concurrency safe
func RegisterAppendable(name string, dirs datadir.Dirs, salt uint32, options ...AppendableIdOption) AppendableId {
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

	appendableRegistry = append(appendableRegistry, *h)
	curr++

	return AppendableId(curr - 1)
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
