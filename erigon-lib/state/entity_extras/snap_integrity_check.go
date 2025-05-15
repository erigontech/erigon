package entity_extras

import (
	"os"

	"github.com/erigontech/erigon-lib/version"
)

// Example: if 0-1.commitment.kv is visible and has foreign keys to 0-1.account.kv -
// then 0-1.account.kv must be visible AND 0-2.account.kv must NOT be visible
// Couple of things:
// - should i protect this dirtyFile from deletion? (above example)
// - should i move this dirtyFile to visibleFile? (again above example)
// - crossDomainIntegrityCheck as it's implemented today
// I think we can do away with (3) and just implement the first 2 functionalities
// using `IntegrityChecker`
type IntegrityChecker interface {
	Check(fromRootNum, toRootNum RootNum) (fine bool)
}

// an integrity checker used when a dependent domain has
// references to a dependency domain. e.g. commitment.kv has
// references to accounts.kv.
// instance should be held by dependency domain
// (accounts in this example)
type ReferencingIntegrityChecker struct {
	dependents []SnapNameSchema
}

func NewReferencingIntegrityChecker(dependents ...SnapNameSchema) *ReferencingIntegrityChecker {
	return &ReferencingIntegrityChecker{
		dependents: dependents,
	}
}

func (r *ReferencingIntegrityChecker) Check(fromRootNum, toRootNum RootNum) (fine bool) {
	for _, dependent := range r.dependents {
		fullpath := dependent.DataFile(version.V1_0, fromRootNum, toRootNum)
		if _, err := os.Stat(fullpath); os.IsNotExist(err) {
			return false
		}
	}
	return true
}
