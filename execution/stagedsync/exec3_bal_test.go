package stagedsync

import (
	"bytes"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/bal/tempbal"
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
		storedBAL []byte
		wantBAL   []byte
		wantReads int
	}{
		{name: "missing commitment"},
		{name: "empty commitment", hash: &empty.BlockAccessListHash},
		{name: "non-empty commitment", hash: &nonEmptyBALHash, storedBAL: storedBAL, wantBAL: storedBAL, wantReads: 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			getter := &countingBlockAccessListGetter{data: test.storedBAL}
			block := types.NewBlockFromStorage(common.Hash{}, &types.Header{BlockAccessListHash: test.hash}, nil, nil, nil)

			got, err := blockAccessListBytes(getter, block, 1, nil)
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

// TestBlockAccessListBytesTempBAL covers --use-temp-bal: a synthetic BAL is
// served for a header carrying no BAL, and only when the block hash matches.
func TestBlockAccessListBytesTempBAL(t *testing.T) {
	dir := t.TempDir()
	tempBALBytes := []byte("temp-bal-payload")
	blockHash := common.Hash{0xAA}

	w, err := tempbal.NewWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(7, blockHash, tempBALBytes); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := tempbal.OpenReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Header has no BAL commitment (the mainnet case) — the temp store supplies it.
	block := types.NewBlockFromStorage(blockHash, &types.Header{}, nil, nil, nil)
	getter := &countingBlockAccessListGetter{}

	got, err := blockAccessListBytes(getter, block, 7, reader)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, tempBALBytes) {
		t.Fatalf("temp BAL = %q, want %q", got, tempBALBytes)
	}
	if getter.calls != 0 {
		t.Fatalf("DB reads = %d, want 0 (served from temp store)", getter.calls)
	}

	// Different block number with no stored temp BAL → no bytes, no DB read.
	if got, err := blockAccessListBytes(getter, block, 8, reader); err != nil || got != nil {
		t.Fatalf("Get(8) = %q,%v, want nil,nil", got, err)
	}
}
