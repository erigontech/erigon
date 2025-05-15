package entity_extras

import (
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
)

// this is an equivalent of snap_integrity_check.go, but for
// current dhii (which don't have SnapNameSchema).
// dependency/referred: account/storage
// dependent/referencing: commitment
type EntityIntegrityChecker interface {
	CheckAllDependentPresent(dependencyDomain kv.Domain, fromStep, toStep uint64) (IsPresent bool)
}

type KvFileNameGen func(fromStep, toStep uint64) string

// an DependencyIntegrityChecker used when a dependent domain has
// references to a dependency domain. e.g. commitment.kv has
// references to accounts.kv.
// instance should be held by dependency domain
// (accounts in this example)
type DependencyIntegrityChecker struct {
	dependencyMap map[kv.Domain][]*dependentInfo
	dirs          datadir.Dirs
}

type dependentInfo struct {
	dependentDomain kv.Domain
	dependentGen    KvFileNameGen
}

func NewDependencyIntegrityChecker(dirs datadir.Dirs) *DependencyIntegrityChecker {
	return &DependencyIntegrityChecker{dependencyMap: make(map[kv.Domain][]*dependentInfo), dirs: dirs}
}

func (d *DependencyIntegrityChecker) AddDependency(dependency kv.Domain, dependent kv.Domain, dependentGen KvFileNameGen) {
	arr, ok := d.dependencyMap[dependency]
	if !ok {
		arr = make([]*dependentInfo, 0)
	}
	arr = append(arr, &dependentInfo{
		dependentDomain: dependent,
		dependentGen:    dependentGen,
	})
	d.dependencyMap[dependency] = arr
}

// dependency: account
// is commitment.0-2 present? if no, don't delete account.0-2, also don't use it for visibleFiles...
func (d *DependencyIntegrityChecker) CheckAllDependentPresent(dependency kv.Domain, fromStep, toStep uint64) (IsPresent bool) {
	arr, ok := d.dependencyMap[dependency]
	if !ok {
		return false
	}

	for _, dependent := range arr {
		dependentName := dependent.dependentGen(fromStep, toStep)
		if !d.checkFilePresent(d.dirs.Snap, dependentName) {
			return false
		}
	}

	return true
}

func (d *DependencyIntegrityChecker) checkFilePresent(dir, filename string) bool {
	return true
}

// 1.should delete this file? -- only care about the file (no index etc.)
// 2. should use recalcVisibleFiles? (commit.0-1 should result in account.0-1 and storage.0-1 in visibleFiles...)
// both only care about file presence (no index presence check etc.)
