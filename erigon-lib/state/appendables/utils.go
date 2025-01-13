package appendables

import (
	"bytes"
	"encoding/binary"

	"github.com/erigontech/erigon-lib/kv"
)

const MaxUint64 = ^uint64(0)

func Encode64ToBytes(x uint64, x8Bytes bool) (out []byte) {
	if x8Bytes {
		out = make([]byte, 8)
		binary.BigEndian.PutUint64(out, x)
	} else {
		out = make([]byte, 4)
		binary.BigEndian.PutUint32(out, uint32(x))
	}
	return
}

func Decode64FromBytes(buf []byte, x8Bytes bool) (x uint64) {
	if x8Bytes {
		return binary.BigEndian.Uint64(buf)
	} else {
		return uint64(binary.BigEndian.Uint32(buf))
	}
}

func DeleteRangeFromTbl(tbl string, fromPrefix, toPrefix []byte, limit uint64, rwTx kv.RwTx) error {
	c, err := rwTx.Cursor(tbl)
	if err != nil {
		return err
	}

	defer c.Close()
	for k, _, err := c.Seek(fromPrefix); k != nil && (toPrefix == nil || bytes.Compare(k, toPrefix) < 0); k, _, err = c.Next() {
		if err != nil {
			return err
		}

		if err := rwTx.Delete(tbl, k); err != nil {
			return err
		}

		limit--
		if limit <= 0 {
			break
		}
	}

	return nil
}
