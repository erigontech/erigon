package entity_extras

import "github.com/erigontech/erigon-lib/kv"

// this is an equivalent of snap_integrity_check.go, but for
// current dhii (which don't have SnapNameSchema).
// dependency/referred: account/storage
// dependent/referencing: commitment
type EntityIntegrityChecker interface {
	CheckAnyDependentPresent(dependencyDomain kv.Domain, fromStep, toStep uint64) (IsPresent bool)
}

type KvFileNameGen func(fromStep, toStep uint64) string

// an DependencyIntegrityChecker used when a dependent domain has
// references to a dependency domain. e.g. commitment.kv has
// references to accounts.kv.
// instance should be held by dependency domain
// (accounts in this example)
type DependencyIntegrityChecker struct {
	dependencyMap map[kv.Domain][]*dependencyInfo
}

type dependencyInfo struct {
	filenameGen     KvFileNameGen
	dependentDomain kv.Domain
	dependentGen    KvFileNameGen
}

func NewDependencyIntegrityChecker() *DependencyIntegrityChecker {
	return &DependencyIntegrityChecker{}
}

func (d *DependencyIntegrityChecker) AddDependency(dependency kv.Domain, dependencyGen KvFileNameGen, dependent kv.Domain, dependentGen KvFileNameGen) {
	arr, ok := d.dependencyMap[dependency]
	if !ok {
		arr = make([]*dependencyInfo, 0)
	}
	arr = append(arr, &dependencyInfo{
		filenameGen:     dependencyGen,
		dependentDomain: dependent,
		dependentGen:    dependentGen,
	})
	d.dependencyMap[dependency] = arr
}

func (d *DependencyIntegrityChecker) CheckAnyDependentPresent(dependency kv.Domain, fromStep, toStep uint64) (IsPresent bool) {
	arr, ok := d.dependencyMap[dependency]
	if !ok {
		return false
	}

	for _, dependency := range arr {

	}

}

// 1.should delete this file? -- only care about the file (no index etc.)
// 2. should use recalcVisibleFiles? (commit.0-1 should result in account.0-1 and storage.0-1 in visibleFiles...)
// both only care about file presence (no index presence check etc.)
