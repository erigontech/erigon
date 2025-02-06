package appendables

import "github.com/erigontech/erigon-lib/kv"

type RelationI interface {
	RootNum2Id(from RootNum, tx kv.Tx) (Id, error)
	Num2Id(from Num, tx kv.Tx) (Id, error)
}
