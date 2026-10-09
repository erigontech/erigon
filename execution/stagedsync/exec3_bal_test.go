package stagedsync

import (
	"context"
	"reflect"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/bal/offlinebal"
	"github.com/erigontech/erigon/execution/types"
)

type countingBlockAccessListGetter struct {
	kv.Getter
	data  []byte
	calls int
}

type warnCounter struct{ warns int }

func (h *warnCounter) Log(r *log.Record) error {
	if r.Lvl == log.LvlWarn {
		h.warns++
	}
	return nil
}

func (h *warnCounter) Enabled(context.Context, log.Lvl) bool { return true }

func (g *countingBlockAccessListGetter) GetOne(string, []byte) ([]byte, error) {
	g.calls++
	return g.data, nil
}

func TestBlockAccessList(t *testing.T) {
	nonEmptyBALHash := common.Hash{1}
	storedBAL := types.BlockAccessList{{Address: common.Address{1}}}
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
			block := types.NewBlockFromStorage(common.Hash{}, &types.Header{BlockAccessListHash: test.hash}, nil, nil, nil, types.NewBlockAccessListSidecar(test.blockBAL))

			got, err := blockAccessList(getter, block, 1, nil, log.New())
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

// TestBlockAccessListOfflineBAL pins that a stored offline BAL is served for a header
// without a BAL commitment, keyed by block number and hash.
func TestBlockAccessListOfflineBAL(t *testing.T) {
	dir := t.TempDir()
	offlineBAL := types.BlockAccessList{{Address: common.Address{7}}}
	offlineBALBytes, err := types.EncodeBlockAccessListBytes(offlineBAL)
	if err != nil {
		t.Fatal(err)
	}
	blockHash := common.Hash{0xAA}

	w, err := offlinebal.NewWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(7, blockHash, offlineBALBytes); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := offlinebal.OpenReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	block := types.NewBlockFromStorage(blockHash, &types.Header{}, nil, nil, nil, nil)
	getter := &countingBlockAccessListGetter{}

	warns := &warnCounter{}
	logger := log.New()
	logger.SetHandler(warns)

	got, err := blockAccessList(getter, block, 7, reader, logger)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, offlineBAL) {
		t.Fatalf("offline BAL = %v, want %v", got, offlineBAL)
	}
	if getter.calls != 0 {
		t.Fatalf("DB reads = %d, want 0", getter.calls)
	}
	if warns.warns != 0 {
		t.Fatalf("warnings for a stored block = %d, want 0", warns.warns)
	}

	if got, err := blockAccessList(getter, block, 8, reader, logger); err != nil || got != nil {
		t.Fatalf("block 8 = %v,%v, want nil,nil", got, err)
	}
	if warns.warns != 1 {
		t.Fatalf("warnings for a block missing from the store = %d, want 1", warns.warns)
	}
}
