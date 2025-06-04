package state

import (
	"fmt"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	btree2 "github.com/tidwall/btree"
)

// high 16 bits specific domain/ii/forkables;
// low 16 bits identifies "category" domain(0)/ii(1)/history(2)/forkables(3) etc.
// e.g.
// 0x0000000000000001 0000000000000000  - storage domain
// 0x0000000000000001 0000000000000001  - storage history
// 0x0000000000000001 0000000000000010  - storage ii

type UniversalEntity uint32

func FromDomain(d kv.Domain) UniversalEntity {
	return UniversalEntity(uint32(d) << 16)
}

func FromII(ii kv.InvertedIdx) UniversalEntity {
	return UniversalEntity(uint32(ii)<<16 | 0x10)
}

func (ue UniversalEntity) String() string {
	category := ue & 0xFFFF
	if category == 0 {
		return fmt.Sprintf("domain:%s", kv.Domain(ue>>16))
	}
	if category == 1 {
		return fmt.Sprintf("history:%s", kv.InvertedIdx(ue>>16))
	}
	if category == 2 {
		return fmt.Sprintf("ii:%s", kv.InvertedIdx(ue>>16))
	}
	return fmt.Sprintf("unknown:%d", ue)
}

var (
	AccountDomainUniversal    = FromDomain(kv.AccountsDomain)
	StorageDomainUniversal    = FromDomain(kv.StorageDomain)
	CommitmentDomainUniversal = FromDomain(kv.CommitmentDomain)
)

type DirtyFilesGetter func() *btree2.BTreeG[*filesItem]

// an DependencyIntegrityChecker used when a dependent domain has
// references to a dependency domain. e.g. commitment.kv has
// references to accounts.kv.
// instance should be held by dependency domain
// (accounts in this example)
type DependencyIntegrityChecker struct {
	dependencyMap map[UniversalEntity][]*DependentInfo
	dirs          datadir.Dirs
	trace         bool
	logger        log.Logger
	disable       bool
}

type DependentInfo struct {
	entity      UniversalEntity
	filesGetter DirtyFilesGetter
	accessors   Accessors
}

// dependency/referred: account/storage
// dependent/referencing: commitment
func NewDependencyIntegrityChecker(dirs datadir.Dirs, logger log.Logger) *DependencyIntegrityChecker {
	return &DependencyIntegrityChecker{
		dependencyMap: make(map[UniversalEntity][]*DependentInfo),
		dirs:          dirs,
		logger:        logger,
		//		trace:         true,
	}
}

func (d *DependencyIntegrityChecker) SetTrace(trace bool) {
	d.trace = trace
}

func (d *DependencyIntegrityChecker) AddDependency(dependency UniversalEntity, dependent *DependentInfo) {
	arr, ok := d.dependencyMap[dependency]
	if !ok {
		arr = make([]*DependentInfo, 0)
	}
	arr = append(arr, dependent)
	d.dependencyMap[dependency] = arr
}

func (d *DependencyIntegrityChecker) Enable() {
	d.disable = false
}

func (d *DependencyIntegrityChecker) Disable() {
	d.disable = true
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
func (d *DependencyIntegrityChecker) CheckDependentPresent(dependency UniversalEntity, allOrAny Quantifier, startTxNum, endTxNum uint64) (IsPresent bool) {
	arr, ok := d.dependencyMap[dependency]
	if !ok || d.disable {
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
					d.logger.Warn("[dbg: Depic]", "dependent", dependent.entity.String(), "startTxNum", startTxNum, "endTxNum", endTxNum, "found", found)
				}
				return false
			}
		} else {
			// Any: used for garbage collection
			// any dependent (e.g. commiment) file is present => dependency file can't be deleted
			if found {
				if d.trace {
					d.logger.Warn("[dbg: Depic]", "dependent", dependent.entity.String(), "startTxNum", startTxNum, "endTxNum", endTxNum, "found", true)
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
