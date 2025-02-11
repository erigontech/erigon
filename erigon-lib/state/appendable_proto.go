package state

import (
	"context"
	"sync"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	ae "github.com/erigontech/erigon-lib/state/appendables_extras"

	"github.com/tidwall/btree"
	btree2 "github.com/tidwall/btree"
)

// appendable struct with basic functionality it's not intended to be used directly.
// Can be embedded in other concrete appendable structs
type ProtoAppendable struct {
	freezer Freezer

	a          ae.AppendableId
	builders   []AccessorIndexBuilder
	dirtyFiles *btree.BTreeG[*filesItem]
	_visible   visibleFiles

	visibleLock   sync.RWMutex
	sameKeyAsRoot bool

	logger log.Logger
}

func NewProto(a ae.AppendableId, builders []AccessorIndexBuilder, freezer Freezer, logger log.Logger) *ProtoAppendable {
	return &ProtoAppendable{
		a:          a,
		builders:   builders,
		freezer:    freezer,
		dirtyFiles: btree2.NewBTreeGOptions[*filesItem](filesItemLess, btree2.Options{Degree: 128, NoLocks: false}),
		logger:     logger,
	}
}

func (a *ProtoAppendable) VisibleFilesMaxRootNum() ae.RootNum {
	latest := a._visible[len(a._visible)-1]
	return ae.RootNum(latest.src.endTxNum)
}

func (a *ProtoAppendable) DirtyFilesMaxRootNum() ae.RootNum {
	latest, found := a.dirtyFiles.Max()
	if latest == nil || !found {
		return 0
	}
	return ae.RootNum(latest.endTxNum)
}

func (a *ProtoAppendable) VisibleFilesMaxNum() RootNum {
	//latest := a._visible[len(a._visible)-1]
	// need to store first entity num in snapshots for this
	// TODO: just sending max root num now; so it won't work if rootnum!=num;
	// maybe we should just test bodies and headers till then.

	return a.VisibleFilesMaxRootNum()
}

func (a *ProtoAppendable) BuildFiles(ctx context.Context, from, to RootNum, dv kv.RoDB, ps *background.ProgressSet) error {

	return nil
}
