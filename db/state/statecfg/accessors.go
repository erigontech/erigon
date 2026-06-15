package statecfg

import "strings"

type ExistenceFilterMode uint8

const (
	ExistenceFilterInMem    ExistenceFilterMode = 0 // keep bytes in application memory (default)
	ExistenceFilterWillNeed ExistenceFilterMode = 1 // mmap + MADV_WILLNEED
	ExistenceFilterNormal   ExistenceFilterMode = 2 // mmap + MADV_NORMAL
	ExistenceFilterRandom   ExistenceFilterMode = 3 // mmap + MADV_RANDOM
)

func (m ExistenceFilterMode) String() string {
	switch m {
	case ExistenceFilterInMem:
		return "InMem"
	case ExistenceFilterWillNeed:
		return "WillNeed"
	case ExistenceFilterNormal:
		return "Normal"
	case ExistenceFilterRandom:
		return "Random"
	default:
		return "Unknown"
	}
}

type Accessors int

func (l Accessors) Has(target Accessors) bool { return l&target != 0 }
func (l Accessors) Add(a Accessors) Accessors { return l | a }

const (
	AccessorBTree     Accessors = 0b1
	AccessorHashMap   Accessors = 0b10
	AccessorExistence Accessors = 0b100
)

func (l Accessors) String() string {
	var out []string
	if l.Has(AccessorBTree) {
		out = append(out, "BTree")
	}
	if l.Has(AccessorHashMap) {
		out = append(out, "HashMap")
	}
	if l.Has(AccessorExistence) {
		out = append(out, "Existence")
	}
	return strings.Join(out, ",")
}
