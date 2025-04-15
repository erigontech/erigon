package entity_extras

import (
	"bytes"
	"encoding/binary"

	"github.com/erigontech/erigon-lib/kv"
)

func step[T ~uint64](n T, a ForkableId) uint64 {
	return uint64(n) / a.SnapshotConfig().RootNumPerStep
}

func EncToBytes[T ~uint64](x T, x8Bytes bool) (out []byte) {
	if x8Bytes {
		out = make([]byte, 8)
		binary.BigEndian.PutUint64(out, uint64(x))
	} else {
		out = make([]byte, 4)
		binary.BigEndian.PutUint32(out, uint32(x))
	}
	return
}

func Decode64FromBytes(buf []byte, x8Bytes bool) (x uint64) {
	if x8Bytes {
		x = binary.BigEndian.Uint64(buf)
	} else {
		x = uint64(binary.BigEndian.Uint32(buf))
	}
	return
}

// toPrefix exclusive
func DeleteRangeFromTbl(tbl string, fromPrefix, toPrefix []byte, limit uint64, rwTx kv.RwTx) (delCount uint64, err error) {
	c, err := rwTx.RwCursor(tbl) // TODO: no dupsort tbl assumed
	if err != nil {
		return
	}

	defer c.Close()
	// bigendianess assumed (for key comparison)
	// imo this can be generalized if needed, by using key comparison functions, which mdbx provides.
	for k, _, err := c.Seek(fromPrefix); k != nil && (toPrefix == nil || bytes.Compare(k, toPrefix) < 0) && limit > 0; k, _, err = c.Next() {
		if err != nil {
			return delCount, err
		}

		if err := c.DeleteCurrent(); err != nil {
			return delCount, err
		}
		limit--
		delCount++
	}

	return
}

type IdentityRootRelation struct{}

func (i *IdentityRootRelation) RootNum2Num(rootNum RootNum, tx kv.Tx) (num Num, err error) {
	return Num(rootNum), nil
}

var IdentityRootRelationInstance = &IdentityRootRelation{}
