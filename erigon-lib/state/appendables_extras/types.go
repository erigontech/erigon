package appendables_extras

/** custom types **/

// canonical sequence number of entity (in context)
type Num uint64

// sequence number of entity - might contain non-canonical values
type Id uint64

// canonical sequence number of the root entity (or secondary key)
type RootNum uint64

type Bytes []byte

func (n Num) Step(a AppendableId) uint64 {
	return uint64(n) / a.SnapshotConfig().EntitiesPerStep
}

func (n RootNum) Step(a AppendableId) uint64 {
	return uint64(n) / a.SnapshotConfig().EntitiesPerStep
}
