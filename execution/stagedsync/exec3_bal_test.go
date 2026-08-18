package stagedsync

import (
	"reflect"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
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

func TestBlockAccessList(t *testing.T) {
	nonEmptyBALHash := common.Hash{1}
	storedBAL := types.BlockAccessList{{Address: accounts.InternAddress(common.Address{1})}}
	storedBALBytes, err := types.EncodeBlockAccessListBytes(storedBAL)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name      string
		hash      *common.Hash
		blockBAL  types.BlockAccessList
		storedBAL []byte
		wantBAL   types.BlockAccessList
		wantReads int
	}{
		{name: "missing commitment"},
		{name: "empty commitment", hash: &empty.BlockAccessListHash},
		{name: "carried BAL", hash: &nonEmptyBALHash, blockBAL: storedBAL, wantBAL: storedBAL},
		{name: "non-empty commitment", hash: &nonEmptyBALHash, storedBAL: storedBALBytes, wantBAL: storedBAL, wantReads: 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			getter := &countingBlockAccessListGetter{data: test.storedBAL}
			block := types.NewBlockFromStorage(common.Hash{}, &types.Header{BlockAccessListHash: test.hash}, nil, nil, nil, test.blockBAL)

			got, err := blockAccessList(getter, block, 1)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.wantBAL) {
				t.Fatalf("block access list = %v, want %v", got, test.wantBAL)
			}
			if getter.calls != test.wantReads {
				t.Fatalf("DB reads = %d, want %d", getter.calls, test.wantReads)
			}
		})
	}
}
