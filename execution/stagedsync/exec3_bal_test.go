package stagedsync

import (
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types"
)

type countingBlockAccessListGetter struct {
	kv.Getter
	calls int
}

func (g *countingBlockAccessListGetter) GetOne(string, []byte) ([]byte, error) {
	g.calls++
	return nil, nil
}

func TestBlockAccessListBytesSkipsDBWithoutBALField(t *testing.T) {
	getter := &countingBlockAccessListGetter{}
	block := types.NewBlockFromStorage(common.Hash{}, &types.Header{}, nil, nil, nil)

	if _, err := blockAccessListBytes(getter, block, 1); err != nil {
		t.Fatal(err)
	}
	if getter.calls != 0 {
		t.Fatalf("unexpected DB reads: %d", getter.calls)
	}
}

func TestBlockAccessListBytesSkipsDBForEmptyBAL(t *testing.T) {
	getter := &countingBlockAccessListGetter{}
	block := types.NewBlockFromStorage(common.Hash{}, &types.Header{BlockAccessListHash: &empty.BlockAccessListHash}, nil, nil, nil)

	if _, err := blockAccessListBytes(getter, block, 1); err != nil {
		t.Fatal(err)
	}
	if getter.calls != 0 {
		t.Fatalf("unexpected DB reads: %d", getter.calls)
	}
}
