package stateless

import (
	"encoding/binary"
	"testing"
)

func TestDeriveDbKey(t *testing.T) {
	for i := 0; i < 1000000; i += 1003 {
		for j := 0; j < 10000; j += 111 {
			key := deriveDbKey(uint64(i), uint32(j))

			block := binary.LittleEndian.Uint64(key[:8])
			if uint64(i) != block {
				t.Errorf("cant unmarshall a block number from key; expected: %v got: %v", i, block)
			}

			limit := binary.LittleEndian.Uint32(key[8:])
			if uint32(j) != limit {
				t.Errorf("cant unmarshall a limit from key; expected: %v got: %v", j, limit)
			}
		}
	}
}
