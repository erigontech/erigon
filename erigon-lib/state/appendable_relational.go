package state

import "github.com/erigontech/erigon-lib/kv"

type RootRelationI interface {
	RootNum2Id(from RootNum, tx kv.Tx) (Id, error)
	Num2Id(from Num, tx kv.Tx) (Id, error)
}
