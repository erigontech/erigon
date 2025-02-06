package appendables

import "github.com/tidwall/btree"

type ProtoAppendable struct {
	freezer freezer

	dirtyFiles *btree.BTreeG[*dirtyFiles]
}
