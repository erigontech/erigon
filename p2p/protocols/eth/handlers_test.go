package eth

import (
	"bytes"
	"context"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

// mockReceiptsGetter implements ReceiptsGetter for tests using only the cache path.
type mockReceiptsGetter struct {
	cached map[common.Hash]types.Receipts
}

func (m *mockReceiptsGetter) GetCachedReceipts(_ context.Context, hash common.Hash) (types.Receipts, bool) {
	r, ok := m.cached[hash]
	return r, ok
}

func (m *mockReceiptsGetter) GetReceipts(_ context.Context, _ *chain.Config, _ kv.TemporalTx, _ *types.Block) (types.Receipts, error) {
	panic("not expected in cache-only tests")
}

// makeReceipt creates a minimal receipt with the given cumulative gas and log data size.
func makeReceipt(cumulativeGas uint64, logDataSize int) *types.Receipt {
	r := &types.Receipt{
		Type:              types.LegacyTxType,
		Status:            types.ReceiptStatusSuccessful,
		CumulativeGasUsed: cumulativeGas,
	}
	if logDataSize > 0 {
		r.Logs = []*types.Log{{
			Address: common.BytesToAddress([]byte{0x01}),
			Topics:  []common.Hash{common.HexToHash("aa")},
			Data:    make([]byte, logDataSize),
		}}
	}
	return r
}

// eth70Opts returns ReceiptQueryOpts for eth/70 with the given firstBlockReceiptIndex.
func eth70Opts(firstBlockReceiptIndex uint64) ReceiptQueryOpts {
	return ReceiptQueryOpts{
		EthVersion:             70,
		FirstBlockReceiptIndex: firstBlockReceiptIndex,
		SizeLimit:              Eth70ResponseSizeLimit,
	}
}

// receiptEncodedSize returns the eth/69 encoded size of a single receipt
// wrapped in an RLP list (i.e. the full encoded block-receipts list for one receipt).
func receiptEncodedSize(r *types.Receipt) int {
	encoded, _, _, _ := encodeBlockReceipts69WithLimit(types.Receipts{r}, 0, 1<<30)
	return len(encoded)
}

func TestEncodeBlockReceipts69WithLimit_EmptyReceipts(t *testing.T) {
	encoded, size, complete, err := encodeBlockReceipts69WithLimit(nil, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if !complete {
		t.Fatal("expected complete for nil receipts")
	}
	// Should encode as an empty RLP list: 0xc0
	if size != len(encoded) {
		t.Fatalf("size mismatch: reported %d, actual %d", size, len(encoded))
	}
	// Verify it decodes as an empty list
	var decoded []rlp.RawValue
	if err := rlp.DecodeBytes(encoded, &decoded); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(decoded) != 0 {
		t.Fatalf("expected empty list, got %d items", len(decoded))
	}
}

func TestEncodeBlockReceipts69WithLimit_AllFit(t *testing.T) {
	receipts := types.Receipts{
		makeReceipt(100, 10),
		makeReceipt(200, 10),
		makeReceipt(300, 10),
	}
	encoded, size, complete, err := encodeBlockReceipts69WithLimit(receipts, 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if !complete {
		t.Fatal("expected complete when all receipts fit")
	}
	if size != len(encoded) {
		t.Fatalf("size mismatch: reported %d, actual %d", size, len(encoded))
	}
	// Verify the correct number of receipts
	var decoded []rlp.RawValue
	if err := rlp.DecodeBytes(encoded, &decoded); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(decoded) != 3 {
		t.Fatalf("expected 3 receipts, got %d", len(decoded))
	}
}

func TestEncodeBlockReceipts69WithLimit_Truncated(t *testing.T) {
	receipts := types.Receipts{
		makeReceipt(100, 10),
		makeReceipt(200, 10),
		makeReceipt(300, 10),
	}
	// Find the size of one receipt's encoded block, then set limit to fit ~2
	oneSize := receiptEncodedSize(makeReceipt(100, 10))
	// sizeLimit that allows 2 receipts but not 3
	// totalBytes starts at 0, so the limit should be just above 2 receipts' list encoding
	limit := oneSize*2 + 20 // generous for 2, tight for 3

	encoded, size, complete, err := encodeBlockReceipts69WithLimit(receipts, 0, limit)
	if err != nil {
		t.Fatal(err)
	}
	if complete {
		t.Fatal("expected incomplete when limit is tight")
	}
	if size != len(encoded) {
		t.Fatalf("size mismatch: reported %d, actual %d", size, len(encoded))
	}
	var decoded []rlp.RawValue
	if err := rlp.DecodeBytes(encoded, &decoded); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(decoded) != 2 {
		t.Fatalf("expected 2 receipts, got %d", len(decoded))
	}
}

func TestEncodeBlockReceipts69WithLimit_FirstReceiptAlwaysIncluded(t *testing.T) {
	// Even if a single receipt exceeds the limit, it should still be included
	// (the check is `len(perReceipt) > 0` before breaking)
	receipt := makeReceipt(100, 1000)
	encoded, size, complete, err := encodeBlockReceipts69WithLimit(types.Receipts{receipt}, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !complete {
		t.Fatal("single receipt should always be included even if over limit")
	}
	if size != len(encoded) {
		t.Fatalf("size mismatch: reported %d, actual %d", size, len(encoded))
	}
	var decoded []rlp.RawValue
	if err := rlp.DecodeBytes(encoded, &decoded); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(decoded) != 1 {
		t.Fatalf("expected 1 receipt, got %d", len(decoded))
	}
}

func TestEncodeBlockReceipts69WithLimit_TotalBytesOffset(t *testing.T) {
	// When totalBytes is already high, even small receipts trigger truncation
	receipts := types.Receipts{
		makeReceipt(100, 10),
		makeReceipt(200, 10),
	}
	limit := 500
	// Pretend we already have limit-1 bytes; only the first receipt should fit
	encoded, _, complete, err := encodeBlockReceipts69WithLimit(receipts, limit-1, limit)
	if err != nil {
		t.Fatal(err)
	}
	// First receipt is always included since perReceipt is empty
	if complete {
		t.Fatal("expected incomplete with high totalBytes offset")
	}
	var decoded []rlp.RawValue
	if err := rlp.DecodeBytes(encoded, &decoded); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(decoded) != 1 {
		t.Fatalf("expected 1 receipt, got %d", len(decoded))
	}
}

func TestAnswerGetReceiptsQueryCacheOnly70_Basic(t *testing.T) {
	hash1 := common.HexToHash("0x01")
	hash2 := common.HexToHash("0x02")
	getter := &mockReceiptsGetter{
		cached: map[common.Hash]types.Receipts{
			hash1: {makeReceipt(100, 10), makeReceipt(200, 10)},
			hash2: {makeReceipt(300, 10)},
		},
	}
	query := GetReceiptsPacket{hash1, hash2}
	result, needMore, err := AnswerGetReceiptsQueryCacheOnly(context.Background(), getter, query, eth70Opts(0))
	if err != nil {
		t.Fatal(err)
	}
	if needMore {
		t.Fatal("should not need more when all blocks are cached")
	}
	if result.LastBlockIncomplete {
		t.Fatal("should not be incomplete")
	}
	if len(result.EncodedReceipts) != 2 {
		t.Fatalf("expected 2 block receipt lists, got %d", len(result.EncodedReceipts))
	}
	// First block should have 2 receipts
	var decoded []rlp.RawValue
	if err := rlp.DecodeBytes(result.EncodedReceipts[0], &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 2 {
		t.Fatalf("first block: expected 2 receipts, got %d", len(decoded))
	}
}

func TestAnswerGetReceiptsQueryCacheOnly70_FirstBlockReceiptIndex(t *testing.T) {
	hash1 := common.HexToHash("0x01")
	getter := &mockReceiptsGetter{
		cached: map[common.Hash]types.Receipts{
			hash1: {makeReceipt(100, 10), makeReceipt(200, 10), makeReceipt(300, 10)},
		},
	}
	query := GetReceiptsPacket{hash1}

	// Skip first 2 receipts
	result, _, err := AnswerGetReceiptsQueryCacheOnly(context.Background(), getter, query, eth70Opts(2))
	if err != nil {
		t.Fatal(err)
	}
	if result.LastBlockIncomplete {
		t.Fatal("should not be incomplete")
	}
	if len(result.EncodedReceipts) != 1 {
		t.Fatalf("expected 1 block receipt list, got %d", len(result.EncodedReceipts))
	}
	var decoded []rlp.RawValue
	if err := rlp.DecodeBytes(result.EncodedReceipts[0], &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 1 {
		t.Fatalf("expected 1 receipt after skipping 2, got %d", len(decoded))
	}
}

func TestAnswerGetReceiptsQueryCacheOnly70_FirstBlockReceiptIndexBeyondEnd(t *testing.T) {
	hash1 := common.HexToHash("0x01")
	getter := &mockReceiptsGetter{
		cached: map[common.Hash]types.Receipts{
			hash1: {makeReceipt(100, 10)},
		},
	}
	query := GetReceiptsPacket{hash1}

	// Index beyond all receipts
	result, _, err := AnswerGetReceiptsQueryCacheOnly(context.Background(), getter, query, eth70Opts(999))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.EncodedReceipts) != 1 {
		t.Fatalf("expected 1 block receipt list, got %d", len(result.EncodedReceipts))
	}
	// Should be an empty receipt list
	var decoded []rlp.RawValue
	if err := rlp.DecodeBytes(result.EncodedReceipts[0], &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 0 {
		t.Fatalf("expected 0 receipts when index beyond end, got %d", len(decoded))
	}
}

func TestAnswerGetReceiptsQueryCacheOnly70_FirstBlockReceiptIndexOnlyAppliesToFirstBlock(t *testing.T) {
	hash1 := common.HexToHash("0x01")
	hash2 := common.HexToHash("0x02")
	getter := &mockReceiptsGetter{
		cached: map[common.Hash]types.Receipts{
			hash1: {makeReceipt(100, 10), makeReceipt(200, 10)},
			hash2: {makeReceipt(300, 10), makeReceipt(400, 10)},
		},
	}
	query := GetReceiptsPacket{hash1, hash2}

	// Skip 1 receipt from first block only
	result, _, err := AnswerGetReceiptsQueryCacheOnly(context.Background(), getter, query, eth70Opts(1))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.EncodedReceipts) != 2 {
		t.Fatalf("expected 2 block receipt lists, got %d", len(result.EncodedReceipts))
	}
	// First block: 1 receipt (skipped 1)
	var d1 []rlp.RawValue
	if err := rlp.DecodeBytes(result.EncodedReceipts[0], &d1); err != nil {
		t.Fatal(err)
	}
	if len(d1) != 1 {
		t.Fatalf("first block: expected 1 receipt, got %d", len(d1))
	}
	// Second block: 2 receipts (no skip)
	var d2 []rlp.RawValue
	if err := rlp.DecodeBytes(result.EncodedReceipts[1], &d2); err != nil {
		t.Fatal(err)
	}
	if len(d2) != 2 {
		t.Fatalf("second block: expected 2 receipts, got %d", len(d2))
	}
}

func TestAnswerGetReceiptsQueryCacheOnly70_LastBlockIncomplete(t *testing.T) {
	hash1 := common.HexToHash("0x01")
	// Create receipts with large log data to force truncation
	var bigReceipts types.Receipts
	for i := 0; i < 20; i++ {
		bigReceipts = append(bigReceipts, makeReceipt(uint64(i+1)*100, 1024*1024)) // 1MB log data each
	}
	getter := &mockReceiptsGetter{
		cached: map[common.Hash]types.Receipts{
			hash1: bigReceipts,
		},
	}
	query := GetReceiptsPacket{hash1}

	result, needMore, err := AnswerGetReceiptsQueryCacheOnly(context.Background(), getter, query, eth70Opts(0))
	if err != nil {
		t.Fatal(err)
	}
	if needMore {
		t.Fatal("should not need more when truncated mid-block")
	}
	if !result.LastBlockIncomplete {
		t.Fatal("expected LastBlockIncomplete when receipts exceed size limit")
	}
	if len(result.EncodedReceipts) != 1 {
		t.Fatalf("expected 1 block receipt list, got %d", len(result.EncodedReceipts))
	}
	// Should have fewer than all 20 receipts
	var decoded []rlp.RawValue
	if err := rlp.DecodeBytes(result.EncodedReceipts[0], &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded) >= 20 {
		t.Fatalf("expected fewer than 20 receipts, got %d", len(decoded))
	}
	if len(decoded) == 0 {
		t.Fatal("expected at least 1 receipt")
	}
}

func TestAnswerGetReceiptsQueryCacheOnly70_MultipleBlocksTruncatesLast(t *testing.T) {
	hash1 := common.HexToHash("0x01")
	hash2 := common.HexToHash("0x02")
	// First block: small receipts that fit easily
	// Second block: very large receipts that force truncation
	var bigReceipts types.Receipts
	for i := 0; i < 20; i++ {
		bigReceipts = append(bigReceipts, makeReceipt(uint64(i+1)*100, 1024*1024))
	}
	getter := &mockReceiptsGetter{
		cached: map[common.Hash]types.Receipts{
			hash1: {makeReceipt(100, 10)},
			hash2: bigReceipts,
		},
	}
	query := GetReceiptsPacket{hash1, hash2}

	result, needMore, err := AnswerGetReceiptsQueryCacheOnly(context.Background(), getter, query, eth70Opts(0))
	if err != nil {
		t.Fatal(err)
	}
	if needMore {
		t.Fatal("should not need more when truncated mid-block")
	}
	if !result.LastBlockIncomplete {
		t.Fatal("expected LastBlockIncomplete")
	}
	if len(result.EncodedReceipts) != 2 {
		t.Fatalf("expected 2 block receipt lists, got %d", len(result.EncodedReceipts))
	}
	// First block should be complete (1 receipt)
	var d1 []rlp.RawValue
	if err := rlp.DecodeBytes(result.EncodedReceipts[0], &d1); err != nil {
		t.Fatal(err)
	}
	if len(d1) != 1 {
		t.Fatalf("first block: expected 1 receipt, got %d", len(d1))
	}
	// Second block should be truncated
	var d2 []rlp.RawValue
	if err := rlp.DecodeBytes(result.EncodedReceipts[1], &d2); err != nil {
		t.Fatal(err)
	}
	if len(d2) >= 20 {
		t.Fatalf("second block: expected fewer than 20 receipts, got %d", len(d2))
	}
}

func TestAnswerGetReceiptsQueryCacheOnly70_CacheMiss(t *testing.T) {
	hash1 := common.HexToHash("0x01")
	hash2 := common.HexToHash("0x02")
	getter := &mockReceiptsGetter{
		cached: map[common.Hash]types.Receipts{
			hash1: {makeReceipt(100, 10)},
			// hash2 not cached
		},
	}
	query := GetReceiptsPacket{hash1, hash2}

	result, needMore, err := AnswerGetReceiptsQueryCacheOnly(context.Background(), getter, query, eth70Opts(0))
	if err != nil {
		t.Fatal(err)
	}
	if !needMore {
		t.Fatal("should need more when cache misses exist")
	}
	if len(result.EncodedReceipts) != 1 {
		t.Fatalf("expected 1 block receipt list from cache, got %d", len(result.EncodedReceipts))
	}
	if result.PendingIndex != 1 {
		t.Fatalf("expected PendingIndex=1, got %d", result.PendingIndex)
	}
}

func TestAnswerGetReceiptsQueryCacheOnly70_EmptyQuery(t *testing.T) {
	getter := &mockReceiptsGetter{cached: map[common.Hash]types.Receipts{}}
	query := GetReceiptsPacket{}

	result, needMore, err := AnswerGetReceiptsQueryCacheOnly(context.Background(), getter, query, eth70Opts(0))
	if err != nil {
		t.Fatal(err)
	}
	if needMore {
		t.Fatal("should not need more for empty query")
	}
	if len(result.EncodedReceipts) != 0 {
		t.Fatalf("expected 0 block receipt lists, got %d", len(result.EncodedReceipts))
	}
}

// mockHeaderReader implements services.HeaderReader for TestAnswerGetBlockHeadersQuery*.
// It exposes a canonical chain keyed by block number; each block's hash is derived
// deterministically from its number so the ancestor walk works both ways.
type mockHeaderReader struct {
	headers map[uint64]*types.Header // number -> header
	byHash  map[common.Hash]uint64   // hash -> number
}

func newMockHeaderReader(chainLen int) *mockHeaderReader {
	m := &mockHeaderReader{
		headers: make(map[uint64]*types.Header, chainLen),
		byHash:  make(map[common.Hash]uint64, chainLen),
	}
	var parent common.Hash
	for i := uint64(0); i < uint64(chainLen); i++ {
		h := &types.Header{ParentHash: parent}
		h.Number.SetUint64(i)
		hash := h.Hash()
		m.headers[i] = h
		m.byHash[hash] = i
		parent = hash
	}
	return m
}

func (m *mockHeaderReader) Header(_ context.Context, _ kv.Getter, hash common.Hash, blockNum uint64) (*types.Header, error) {
	if h, ok := m.headers[blockNum]; ok && h.Hash() == hash {
		return h, nil
	}
	return nil, nil
}
func (m *mockHeaderReader) HeaderByNumber(_ context.Context, _ kv.Getter, blockNum uint64) (*types.Header, error) {
	return m.headers[blockNum], nil
}
func (m *mockHeaderReader) HeaderNumber(_ context.Context, _ kv.Getter, hash common.Hash) (*uint64, error) {
	if n, ok := m.byHash[hash]; ok {
		return &n, nil
	}
	return nil, nil
}
func (m *mockHeaderReader) HeaderByHash(_ context.Context, _ kv.Getter, hash common.Hash) (*types.Header, error) {
	n, ok := m.byHash[hash]
	if !ok {
		return nil, nil
	}
	return m.headers[n], nil
}
func (m *mockHeaderReader) ReadAncestor(_ kv.Getter, hash common.Hash, number, ancestor uint64, _ *uint64) (common.Hash, uint64) {
	n, ok := m.byHash[hash]
	if !ok || n != number || ancestor > number {
		return common.Hash{}, 0
	}
	anc := m.headers[number-ancestor]
	return anc.Hash(), number - ancestor
}
func (m *mockHeaderReader) HeadersRange(_ context.Context, _ func(*types.Header) error) error {
	return nil
}
func (m *mockHeaderReader) Integrity(_ context.Context) error { return nil }

// TestAnswerGetBlockHeadersQuery_HashModeSkip guards against a regression where
// the non-reverse hash-mode branch read `query.Origin.Number` (the current block)
// instead of `next`, causing the ancestor check to fail and the handler to
// return only the origin header when Amount > 1.
func TestAnswerGetBlockHeadersQuery_HashModeSkip(t *testing.T) {
	reader := newMockHeaderReader(10)
	origin := reader.headers[1]

	query := &GetBlockHeadersPacket{
		Origin:  HashOrNumber{Hash: origin.Hash()},
		Amount:  3,
		Skip:    1,
		Reverse: false,
	}
	headers, err := AnswerGetBlockHeadersQuery(nil, query, reader)
	if err != nil {
		t.Fatalf("AnswerGetBlockHeadersQuery returned error: %v", err)
	}
	if len(headers) != 3 {
		t.Fatalf("expected 3 headers, got %d", len(headers))
	}
	expectedNumbers := []uint64{1, 3, 5}
	for i, h := range headers {
		if h.Number.Uint64() != expectedNumbers[i] {
			t.Fatalf("header %d: expected block %d, got %d", i, expectedNumbers[i], h.Number.Uint64())
		}
	}
}

// --- eth/71 AnswerGetBlockAccessListsQuery tests ---

// balHeaderReader satisfies services.HeaderReader for BAL handler tests by
// resolving hash → block number from a fixed map and panicking on every other
// method (they're not called by AnswerGetBlockAccessListsQuery).
type balHeaderReader map[common.Hash]uint64

func (m balHeaderReader) HeaderNumber(_ context.Context, _ kv.Getter, hash common.Hash) (*uint64, error) {
	n, ok := m[hash]
	if !ok {
		return nil, nil
	}
	return &n, nil
}
func (balHeaderReader) Header(context.Context, kv.Getter, common.Hash, uint64) (*types.Header, error) {
	panic("not expected")
}
func (balHeaderReader) HeaderByNumber(context.Context, kv.Getter, uint64) (*types.Header, error) {
	panic("not expected")
}
func (balHeaderReader) HeaderByHash(context.Context, kv.Getter, common.Hash) (*types.Header, error) {
	panic("not expected")
}
func (balHeaderReader) ReadAncestor(kv.Getter, common.Hash, uint64, uint64, *uint64) (common.Hash, uint64) {
	panic("not expected")
}
func (balHeaderReader) HeadersRange(context.Context, func(*types.Header) error) error {
	panic("not expected")
}
func (balHeaderReader) Integrity(context.Context) error { panic("not expected") }

// TestAnswerGetBlockAccessListsQuery_OrderedResponseWithMissing verifies that
// the handler returns one entry per requested hash in request order, returning
// the "not available" sentinel (0x80, an empty RLP string per EIP-8159 post-
// ethereum/EIPs#11553) for any hash we don't have stored — including unknown
// blocks and known blocks with no BAL recorded.
func TestAnswerGetBlockAccessListsQuery_OrderedResponseWithMissing(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		t.Fatalf("begin rw: %v", err)
	}
	defer tx.Rollback()

	hashKnownWithBAL := common.Hash{0x01}
	hashKnownNoBAL := common.Hash{0x02}
	hashUnknown := common.Hash{0x03}

	reader := balHeaderReader{
		hashKnownWithBAL: 100,
		hashKnownNoBAL:   101,
		// hashUnknown intentionally absent
	}

	bal := []byte{0xc3, 0x01, 0x02, 0x03} // short valid RLP payload (non-empty)
	if err := rawdb.WriteBlockAccessListBytes(tx, hashKnownWithBAL, 100, bal); err != nil {
		t.Fatalf("WriteBlockAccessListBytes: %v", err)
	}

	query := GetBlockAccessListsPacket{hashKnownWithBAL, hashUnknown, hashKnownNoBAL}
	result := AnswerGetBlockAccessListsQuery(tx, query, reader)

	if len(result) != 3 {
		t.Fatalf("result len: have %d, want 3", len(result))
	}
	if !bytes.Equal(result[0], bal) {
		t.Errorf("result[0] (known+BAL): have %x, want %x", result[0], bal)
	}
	if !bytes.Equal(result[1], []byte{0x80}) {
		t.Errorf("result[1] (unknown block): have %x, want 0x80", result[1])
	}
	if !bytes.Equal(result[2], []byte{0x80}) {
		t.Errorf("result[2] (known, no BAL): have %x, want 0x80", result[2])
	}
}

// TestAnswerGetBlockAccessListsQuery_SoftSizeLimit verifies the handler
// respects softResponseLimit by truncating the response (not padding).
func TestAnswerGetBlockAccessListsQuery_SoftSizeLimit(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		t.Fatalf("begin rw: %v", err)
	}
	defer tx.Rollback()

	// Each BAL just over 1 MiB so that three of them exceed softResponseLimit (2 MiB)
	// but the first two plus the current entry still trigger the break after the
	// second full BAL is appended.
	balSize := 1024*1024 + 1
	big := make([]byte, balSize)
	for i := range big {
		big[i] = byte(i)
	}
	// Wrap in a valid RLP byte-string so the wire is well-formed.
	bal, err := rlp.EncodeToBytes(big)
	if err != nil {
		t.Fatalf("encode bal: %v", err)
	}

	reader := balHeaderReader{}
	query := make(GetBlockAccessListsPacket, 0, 5)
	for i := 0; i < 5; i++ {
		h := common.Hash{byte(i + 1)}
		num := uint64(1000 + i)
		reader[h] = num
		if err := rawdb.WriteBlockAccessListBytes(tx, h, num, bal); err != nil {
			t.Fatalf("WriteBlockAccessListBytes: %v", err)
		}
		query = append(query, h)
	}

	result := AnswerGetBlockAccessListsQuery(tx, query, reader)
	if len(result) < 1 || len(result) >= len(query) {
		t.Fatalf("expected truncation: have %d entries, want 1..%d", len(result), len(query)-1)
	}
	// Every returned entry must be a full BAL (no padding, no partials).
	for i, e := range result {
		if !bytes.Equal(e, bal) {
			t.Errorf("result[%d] mismatch (len=%d, want=%d)", i, len(e), len(bal))
		}
	}
}
