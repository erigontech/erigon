package statecfg

import "strings"

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
