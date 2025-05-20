package state

import (
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	btree2 "github.com/tidwall/btree"
)

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

// dependency/referred: account/storage
// dependent/referencing: commitment
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

type Quantifier int

const (
	All Quantifier = iota
	Any            = 1
)

func (e Quantifier) All() bool {
	return e == All
}

func (e Quantifier) Any() bool {
	return e == Any
}

// CheckDependentPresent checks if the dependent domain file is present. All/Any are the two quantifiers provided here
// All: all dependent files are present
// Any: there exists a dependent file, which is present
// NOTE: the caller MUST hold a lock on btree2.BTreeG[*filesItem] returned by filesGetter.
// examples:
// dependency: account
// is (dependent) commitment.0-2 present?
// - if no (or !checkVisibility), don't use it for visibleFiles.
// - Also don't consider it for "consuming" (deleting) the smaller files commitment.0-1, 1-2
func (d *DependencyIntegrityChecker) CheckDependentPresent(dependency kv.Domain, allOrAny Quantifier, startTxNum, endTxNum uint64) (IsPresent bool) {
	arr, ok := d.dependencyMap[dependency]
	if !ok {
		return true
	}

	if d.trace {
		d.logger.Warn("[dbg: Depic]", "CheckDependentPresent", dependency)
	}

	for _, dependent := range arr {
		dependentFiles := dependent.filesGetter()
		file, found := dependentFiles.Get(&filesItem{startTxNum: startTxNum, endTxNum: endTxNum})

		if allOrAny.All() {
			// ALL: used for visibleFilesCalc
			// all dependent (e.g. commitment) file should be present as well as visible-able
			if !found || !checkForVisibility(file, dependent.accessors, d.trace) {
				if d.trace {
					d.logger.Warn("[dbg: Depic]", "dependent", dependent.domain.String(), "startTxNum", startTxNum, "endTxNum", endTxNum, "found", found)
				}
				return false
			}
		} else {
			// Any: used for garbage collection
			// any dependent (e.g. commiment) file is present => dependency file can't be deleted
			if found {
				if d.trace {
					d.logger.Warn("[dbg: Depic]", "dependent", dependent.domain.String(), "startTxNum", startTxNum, "endTxNum", endTxNum, "found", true)
				}
				return true
			}
		}
	}

	if d.trace {
		d.logger.Warn("[dbg: Depic]", "dependent", "all present", "startTxNum", startTxNum, "endTxNum", endTxNum)
	}

	return allOrAny.All()
}
