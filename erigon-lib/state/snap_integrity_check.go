package state

// couple of things:
// - should i protect this dirtyFile from deletion? (again alex's example of commitment)
// - should i move this dirtyFile to visibleFile? (again alex's example of commitment)
// - crossDomainIntegrityCheck as it's implemented today
// I think we can do away with (3) and just use first 2 functionalities.
type IntegrityChecker interface {
	Check(fromRootNum, toRootNum uint64) (fine bool)
}


// problem account needs commitment snap_schema
// how? 