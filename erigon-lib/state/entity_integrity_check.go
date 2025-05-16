package state

import (
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	btree2 "github.com/tidwall/btree"
)

// this is an equivalent of snap_integrity_check.go, but for
// current dhii (which don't have SnapNameSchema).
// dependency/referred: account/storage
// dependent/referencing: commitment
type EntityIntegrityChecker interface {
	CheckAllDependentPresent(dependencyDomain kv.Domain, startTxNum, endTxNum uint64) (IsPresent bool)
}

type DirtyFilesGetter func() *btree2.BTreeG[*filesItem]

// an DependencyIntegrityChecker used when a dependent domain has
// references to a dependency domain. e.g. commitment.kv has
// references to accounts.kv.
// instance should be held by dependency domain
// (accounts in this example)
type DependencyIntegrityChecker struct {
	dependencyMap map[kv.Domain][]*DependentInfo
	dirs          datadir.Dirs
	trace         bool
	logger        log.Logger
}

type DependentInfo struct {
	domain      kv.Domain
	filesGetter DirtyFilesGetter
	accessors   Accessors
}

func NewDependencyIntegrityChecker(dirs datadir.Dirs, logger log.Logger) *DependencyIntegrityChecker {
	return &DependencyIntegrityChecker{
		dependencyMap: make(map[kv.Domain][]*DependentInfo),
		dirs:          dirs,
		logger:        logger,
	}
}

func (d *DependencyIntegrityChecker) SetTrace(trace bool) {
	d.trace = trace
}

func (d *DependencyIntegrityChecker) AddDependency(dependency kv.Domain, dependent *DependentInfo) {
	arr, ok := d.dependencyMap[dependency]
	if !ok {
		arr = make([]*DependentInfo, 0)
	}
	arr = append(arr, dependent)
	d.dependencyMap[dependency] = arr
}

// dependency: account
// is commitment.0-2 present? if no, don't delete account.0-2, also don't use it for visibleFiles...
func (d *DependencyIntegrityChecker) CheckAllDependentPresent(dependency kv.Domain, startTxNum, endTxNum uint64) (IsPresent bool) {
	arr, ok := d.dependencyMap[dependency]
	if !ok {
		return true
	}

	for _, dependent := range arr {
		dependentFiles := dependent.filesGetter()
		file, found := dependentFiles.Get(&filesItem{startTxNum: startTxNum, endTxNum: endTxNum})

		if !found {
			if d.trace {
				d.logger.Warn("[dbg: Depic]", "dependent", dependent.domain.String(), "startTxNum", startTxNum, "endTxNum", endTxNum, "found", found)
			}
			return false
		}

		if !checkFilesItemFields(file, dependent.accessors, false) {
			if d.trace {
				d.logger.Warn("[dbg: Depic]", "dependent", dependent.domain.String(), "startTxNum", startTxNum, "endTxNum", endTxNum, "found", true, "checkField", false)
			}
			return false
		}
	}

	if d.trace {
		d.logger.Warn("[dbg: Depic]", "dependent", "all present", "startTxNum", startTxNum, "endTxNum", endTxNum)
	}

	return true
}

// 1.should delete this file? -- only care about the file (no index etc.)
// 2. should use recalcVisibleFiles? (commit.0-1 should result in account.0-1 and storage.0-1 in visibleFiles...)
// both only care about file presence (no index presence check etc.)
