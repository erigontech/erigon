package state

import "github.com/erigontech/erigon/db/kv"

type RootRelationI interface {
	RootNum2Num(from RootNum, tx kv.Tx) (Num, error)
}

type IdentityRootRelation struct{}

func (i *IdentityRootRelation) RootNum2Num(rootNum RootNum, tx kv.Tx) (num Num, err error) {
	return Num(rootNum), nil
}

var IdentityRootRelationInstance = &IdentityRootRelation{}
