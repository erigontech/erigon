package stagedsync

import (
	"bytes"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types"
)

type countingBlockAccessListGetter struct {
	kv.Getter
	data  []byte
	calls int
}

func (g *countingBlockAccessListGetter) GetOne(string, []byte) ([]byte, error) {
	g.calls++
	return g.data, nil
}

func TestBlockAccessListBytes(t *testing.T) {
	nonEmptyBALHash := common.Hash{1}
	storedBAL := []byte{1, 2, 3}
	tests := []struct {
		name      string
		hash      *common.Hash
		blockBAL  []byte
		storedBAL []byte
		wantBAL   []byte
		wantReads int
	}{
		{name: "missing commitment"},
		{name: "empty commitment", hash: &empty.BlockAccessListHash},
		{name: "carried BAL", hash: &nonEmptyBALHash, blockBAL: storedBAL, wantBAL: storedBAL},
		{name: "non-empty commitment", hash: &nonEmptyBALHash, storedBAL: storedBAL, wantBAL: storedBAL, wantReads: 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			getter := &countingBlockAccessListGetter{data: test.storedBAL}
			block := types.NewBlockFromStorage(common.Hash{}, &types.Header{BlockAccessListHash: test.hash}, nil, nil, nil, test.blockBAL)

			got, err := blockAccessListBytes(getter, block, 1)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, test.wantBAL) {
				t.Fatalf("block access list = %x, want %x", got, test.wantBAL)
			}
			if getter.calls != test.wantReads {
				t.Fatalf("DB reads = %d, want %d", getter.calls, test.wantReads)
			}
		})
	}
}
