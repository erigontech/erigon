package appendables_extras

import (
	"bytes"
	"encoding/binary"

	"github.com/erigontech/erigon-lib/kv"
)

func step[T ~uint64](n T, a AppendableId) uint64 {
	return uint64(n) / a.SnapshotConfig().EntitiesPerStep
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

// toPrefix inclusive
// bigendianess assumed for key comparison
func DeleteRangeFromTbl(tbl string, fromPrefix, toPrefix []byte, limit int, rwTx kv.RwTx) error {
	c, err := rwTx.RwCursor(tbl) // TODO: no dupsort tbl assumed
	if err != nil {
		return err
	}

	defer c.Close()
	for k, _, err := c.Seek(fromPrefix); k != nil && (toPrefix == nil || bytes.Compare(k, toPrefix) < 0); k, _, err = c.Next() {
		if err != nil {
			return err
		}

		limit--
		if limit < 0 {
			break
		}

		if err := c.DeleteCurrent(); err != nil {
			return err
		}
	}

	return nil
}
