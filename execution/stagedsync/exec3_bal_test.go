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

type warnCounter struct{ warns, infos int }

func (h *warnCounter) Log(r *log.Record) error {
	switch r.Lvl {
	case log.LvlWarn:
		h.warns++
	case log.LvlInfo:
		h.infos++
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

			got, err := blockAccessList(getter, block, 1, offlinebal.Store{}, log.New())
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

	got, err := blockAccessList(getter, block, 7, offlinebal.Store{Reader: reader}, logger)
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

	used, missing := mxOfflineBALBlocks.WithLabelValues("used").GetValueUint64(), mxOfflineBALBlocks.WithLabelValues("missing").GetValueUint64()
	for _, n := range []uint64{8, 9} {
		if got, err := blockAccessList(getter, block, n, offlinebal.Store{Reader: reader}, logger); err != nil || got != nil {
			t.Fatalf("block %d = %v,%v, want nil,nil", n, got, err)
		}
	}
	if warns.warns != 0 || warns.infos != 1 {
		t.Fatalf("logs past the store end: warns=%d infos=%d, want 0 and 1", warns.warns, warns.infos)
	}
	if _, err := blockAccessList(getter, block, 7, offlinebal.Store{Reader: reader}, logger); err != nil {
		t.Fatal(err)
	}
	if d := mxOfflineBALBlocks.WithLabelValues("used").GetValueUint64() - used; d != 1 {
		t.Fatalf("used blocks counted = %d, want 1", d)
	}
	if d := mxOfflineBALBlocks.WithLabelValues("missing").GetValueUint64() - missing; d != 2 {
		t.Fatalf("missing blocks counted = %d, want 2", d)
	}
}

// TestBlockAccessListOfflineBALGap pins that a block missing inside the stored
// range is still a warning: the store was expected to hold it.
func TestBlockAccessListOfflineBALGap(t *testing.T) {
	dir := t.TempDir()
	w, err := offlinebal.NewWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []uint64{5, 7} {
		if err := w.Append(n, common.Hash{byte(n)}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := offlinebal.OpenReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	warns := &warnCounter{}
	logger := log.New()
	logger.SetHandler(warns)
	block := types.NewBlockFromStorage(common.Hash{6}, &types.Header{}, nil, nil, nil, nil)
	if _, err := blockAccessList(&countingBlockAccessListGetter{}, block, 6, offlinebal.Store{Reader: reader}, logger); err != nil {
		t.Fatal(err)
	}
	if warns.warns != 1 {
		t.Fatalf("warnings for a gap = %d, want 1", warns.warns)
	}
}
