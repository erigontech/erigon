package l1sync

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"math/big"
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/nitro-erigon/arbos/arbostypes"
)

func newTestService(t *testing.T) *L1SyncService {
	t.Helper()
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	return &L1SyncService{config: &DefaultConfig, db: db, logger: log.New()}
}

func TestMsgKey(t *testing.T) {
	k := msgKey(5, 3)
	if len(k) != 16 {
		t.Fatalf("expected 16 bytes, got %d", len(k))
	}
	batch := binary.BigEndian.Uint64(k[:8])
	idx := binary.BigEndian.Uint64(k[8:])
	if batch != 5 || idx != 3 {
		t.Fatalf("expected (5,3), got (%d,%d)", batch, idx)
	}
	if bytes.Equal(msgKey(1, 2), msgKey(2, 1)) {
		t.Fatal("msgKey(1,2) should differ from msgKey(2,1)")
	}
}

func TestDelayedMessageRoundtrip(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	reqID := common.HexToHash("0xdeadbeef")
	orig := &arbostypes.L1IncomingMessage{
		Header: &arbostypes.L1IncomingMessageHeader{
			Kind:        arbostypes.L1MessageType_L2Message,
			Poster:      common.HexToAddress("0x1111111111111111111111111111111111111111"),
			BlockNumber: 12345,
			Timestamp:   1700000000,
			RequestId:   &reqID,
			L1BaseFee:   big.NewInt(1000000000),
		},
		L2msg: []byte("hello l2"),
	}

	tx, err := svc.db.BeginRw(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	if err := putDelayedMessage(tx, 7, orig); err != nil {
		t.Fatal(err)
	}

	got, err := getDelayedMessage(tx, 7)
	if err != nil {
		t.Fatal(err)
	}
	if got.Header.Kind != orig.Header.Kind {
		t.Fatalf("Kind: expected %d, got %d", orig.Header.Kind, got.Header.Kind)
	}
	if got.Header.Poster != orig.Header.Poster {
		t.Fatalf("Poster: expected %s, got %s", orig.Header.Poster, got.Header.Poster)
	}
	if got.Header.BlockNumber != orig.Header.BlockNumber {
		t.Fatalf("BlockNumber: expected %d, got %d", orig.Header.BlockNumber, got.Header.BlockNumber)
	}
	if !bytes.Equal(got.L2msg, orig.L2msg) {
		t.Fatalf("L2msg: expected %q, got %q", orig.L2msg, got.L2msg)
	}
}

func TestGetStoredBatch(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	rawData := []byte("test-batch-data")
	var msgCount uint64 = 5

	tx, err := svc.db.BeginRw(ctx)
	if err != nil {
		t.Fatal(err)
	}
	val := make([]byte, 8+len(rawData))
	binary.BigEndian.PutUint64(val[:8], msgCount)
	copy(val[8:], rawData)
	if err := tx.Put(kv.ArbL1SyncBatch, uint64Key(10), val); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	data, count, err := svc.GetStoredBatch(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if count != msgCount {
		t.Fatalf("msgCount: expected %d, got %d", msgCount, count)
	}
	if !bytes.Equal(data, rawData) {
		t.Fatalf("data: expected %q, got %q", rawData, data)
	}
}

func TestGetStoredMessage(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	orig := &arbostypes.MessageWithMetadata{
		Message: &arbostypes.L1IncomingMessage{
			Header: &arbostypes.L1IncomingMessageHeader{
				Kind:      arbostypes.L1MessageType_L2Message,
				Poster:    common.HexToAddress("0x2222222222222222222222222222222222222222"),
				Timestamp: 1700000000,
				L1BaseFee: big.NewInt(0),
			},
			L2msg: []byte("tx-data"),
		},
		DelayedMessagesRead: 3,
	}

	tx, err := svc.db.BeginRw(ctx)
	if err != nil {
		t.Fatal(err)
	}
	msgBytes, err := json.Marshal(orig)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Put(kv.ArbL1SyncMsg, msgKey(1, 0), msgBytes); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	got, err := svc.GetStoredMessage(ctx, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got.DelayedMessagesRead != 3 {
		t.Fatalf("DelayedMessagesRead: expected 3, got %d", got.DelayedMessagesRead)
	}
	if got.Message.Header.Kind != arbostypes.L1MessageType_L2Message {
		t.Fatalf("Kind: expected %d, got %d", arbostypes.L1MessageType_L2Message, got.Message.Header.Kind)
	}
}

func TestGetProgress(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	// Empty DB
	batch, l1Block, err := svc.GetProgress(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if batch != 0 || l1Block != 0 {
		t.Fatalf("empty progress: expected (0,0), got (%d,%d)", batch, l1Block)
	}

	// Write progress
	tx, err := svc.db.BeginRw(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stages.SaveStageProgress(tx, StageL1SyncBatch, 50); err != nil {
		t.Fatal(err)
	}
	if err := stages.SaveStageProgress(tx, StageL1SyncL1Block, 12345); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	batch, l1Block, err = svc.GetProgress(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if batch != 50 {
		t.Fatalf("batch: expected 50, got %d", batch)
	}
	if l1Block != 12345 {
		t.Fatalf("l1Block: expected 12345, got %d", l1Block)
	}
}

func TestGetLastDelayedMessagesRead(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	var batchSeqNum uint64 = 10
	var msgCount uint64 = 3

	tx, err := svc.db.BeginRw(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Store batch entry with msgCount
	batchVal := make([]byte, 8)
	binary.BigEndian.PutUint64(batchVal, msgCount)
	if err := tx.Put(kv.ArbL1SyncBatch, uint64Key(batchSeqNum), batchVal); err != nil {
		t.Fatal(err)
	}

	// Store 3 messages, last one has DelayedMessagesRead=42
	for i := uint64(0); i < msgCount; i++ {
		msg := &arbostypes.MessageWithMetadata{
			Message: &arbostypes.L1IncomingMessage{
				Header: &arbostypes.L1IncomingMessageHeader{
					Kind:      arbostypes.L1MessageType_L2Message,
					L1BaseFee: big.NewInt(0),
				},
				L2msg: []byte("data"),
			},
			DelayedMessagesRead: i * 20, // 0, 20, 40
		}
		if i == msgCount-1 {
			msg.DelayedMessagesRead = 42
		}
		msgBytes, err := json.Marshal(msg)
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Put(kv.ArbL1SyncMsg, msgKey(batchSeqNum, i), msgBytes); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	dmr, err := svc.getLastDelayedMessagesRead(ctx, batchSeqNum)
	if err != nil {
		t.Fatal(err)
	}
	if dmr != 42 {
		t.Fatalf("expected 42, got %d", dmr)
	}
}

func TestUnpackBatchEmpty(t *testing.T) {
	// A 40-byte header with no payload produces an empty sequencer message.
	// The multiplexer will still produce one message (empty/invalid) and advance.
	header := make([]byte, 40)
	binary.BigEndian.PutUint64(header[0:8], 1000)  // minTimestamp
	binary.BigEndian.PutUint64(header[8:16], 2000) // maxTimestamp
	binary.BigEndian.PutUint64(header[16:24], 100) // minL1Block
	binary.BigEndian.PutUint64(header[24:32], 200) // maxL1Block
	binary.BigEndian.PutUint64(header[32:40], 2)   // afterDelayedMessages

	msgs, err := UnpackBatch(context.Background(), 1, header, common.Hash{}, nil, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	// Empty batch still produces at least one message
	if len(msgs) == 0 {
		t.Fatal("expected at least one message from empty batch")
	}
	// The message should have DelayedMessagesRead=2 (afterDelayedMessages=2)
	if msgs[0].DelayedMessagesRead != 2 {
		t.Fatalf("expected DelayedMessagesRead=2, got %d", msgs[0].DelayedMessagesRead)
	}
}

func TestUnpackBatchWithDelayedMessages(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	ctx := context.Background()

	// Store 2 delayed messages in DB
	tx, err := db.BeginRw(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < 4; i++ {
		msg := &arbostypes.L1IncomingMessage{
			Header: &arbostypes.L1IncomingMessageHeader{
				Kind:      arbostypes.L1MessageType_L2Message,
				Poster:    common.HexToAddress("0xbbbb"),
				Timestamp: 1700000000 + i,
				L1BaseFee: big.NewInt(0),
			},
			L2msg: []byte("delayed-data"),
		}
		if err := putDelayedMessage(tx, i, msg); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Empty batch with afterDelayedMessages=2 — multiplexer reads 2 delayed msgs from DB
	header := make([]byte, 40)
	binary.BigEndian.PutUint64(header[0:8], 1000)  // minTimestamp
	binary.BigEndian.PutUint64(header[8:16], 2000) // maxTimestamp
	binary.BigEndian.PutUint64(header[16:24], 100) // minL1Block
	binary.BigEndian.PutUint64(header[24:32], 200) // maxL1Block
	binary.BigEndian.PutUint64(header[32:40], 4)   // afterDelayedMessages

	msgs, err := UnpackBatch(ctx, 1, header, common.Hash{}, nil, db, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}
	// Each message should be the delayed message we stored
	for i, msg := range msgs {
		if msg.Message.Header.Poster != common.HexToAddress("0xbbbb") {
			t.Fatalf("msg %d: expected poster 0xbbbb, got %s", i, msg.Message.Header.Poster)
		}
		if !bytes.Equal(msg.Message.L2msg, []byte("delayed-data")) {
			t.Fatalf("msg %d: unexpected L2msg", i)
		}
		// delayedMessagesRead starts at 2, so we read seqNums 2 and 3
		// which have timestamps 1700000002 and 1700000003
		expectedTs := uint64(1700000002) + uint64(i)
		if msg.Message.Header.Timestamp != expectedTs {
			t.Fatalf("msg %d: expected timestamp %d, got %d", i, expectedTs, msg.Message.Header.Timestamp)
		}
	}
	// DelayedMessagesRead should increment: 3, 4
	if msgs[0].DelayedMessagesRead != 3 {
		t.Fatalf("msg 0: expected DelayedMessagesRead=3, got %d", msgs[0].DelayedMessagesRead)
	}
	if msgs[1].DelayedMessagesRead != 4 {
		t.Fatalf("msg 1: expected DelayedMessagesRead=4, got %d", msgs[1].DelayedMessagesRead)
	}
}

func TestCreateBlockFromMessage(t *testing.T) {
	t.Run("nil message", func(t *testing.T) {
		block, err := createBlockFromMessage(nil, nil, big.NewInt(421614))
		if err != nil {
			t.Fatal(err)
		}
		if block != nil {
			t.Fatal("expected nil block for nil message")
		}
	})

	t.Run("nil inner message", func(t *testing.T) {
		msg := &arbostypes.MessageWithMetadata{Message: nil}
		block, err := createBlockFromMessage(msg, nil, big.NewInt(421614))
		if err != nil {
			t.Fatal(err)
		}
		if block != nil {
			t.Fatal("expected nil block for nil inner message")
		}
	})

	t.Run("valid message", func(t *testing.T) {
		msg := &arbostypes.MessageWithMetadata{
			Message: &arbostypes.L1IncomingMessage{
				Header: &arbostypes.L1IncomingMessageHeader{
					Kind:        arbostypes.L1MessageType_L2Message,
					Poster:      common.HexToAddress("0xaaaa"),
					BlockNumber: 500,
					Timestamp:   1700000000,
					L1BaseFee:   big.NewInt(1000000000),
				},
				L2msg: []byte{},
			},
			DelayedMessagesRead: 0,
		}
		block, err := createBlockFromMessage(msg, nil, big.NewInt(421614))
		if err != nil {
			t.Fatal(err)
		}
		if block == nil {
			t.Fatal("expected non-nil block")
		}
		// Should have at least the internal start tx
		if len(block.Transactions()) == 0 {
			t.Fatal("expected at least one tx (internal start tx)")
		}
		// Block number should be 0 (no prev header)
		if block.NumberU64() != 0 {
			t.Fatalf("expected block number 0, got %d", block.NumberU64())
		}
		// Coinbase should be the poster
		if block.Coinbase() != common.HexToAddress("0xaaaa") {
			t.Fatalf("expected coinbase 0xaaaa, got %s", block.Coinbase())
		}
	})
}

func TestSingleBatchBackend(t *testing.T) {
	data := []byte("batch-payload")
	hash := common.HexToHash("0xabcdef")

	t.Run("peek and advance", func(t *testing.T) {
		b := &singleBatchBackend{data: data, blockHash: hash, seqNum: 7}

		d, h, err := b.PeekSequencerInbox()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(d, data) {
			t.Fatalf("data mismatch")
		}
		if h != hash {
			t.Fatalf("hash mismatch")
		}
		if b.GetSequencerInboxPosition() != 7 {
			t.Fatalf("expected seqNum 7")
		}

		b.AdvanceSequencerInbox()
		_, _, err = b.PeekSequencerInbox()
		if err == nil {
			t.Fatal("expected error after advance")
		}
	})

	t.Run("position within message", func(t *testing.T) {
		b := &singleBatchBackend{}
		if b.GetPositionWithinMessage() != 0 {
			t.Fatal("expected initial position 0")
		}
		b.SetPositionWithinMessage(42)
		if b.GetPositionWithinMessage() != 42 {
			t.Fatal("expected position 42")
		}
	})

	t.Run("ReadDelayedInbox nil db", func(t *testing.T) {
		b := &singleBatchBackend{db: nil}
		_, err := b.ReadDelayedInbox(0)
		if err == nil {
			t.Fatal("expected error with nil db")
		}
	})

	t.Run("ReadDelayedInbox with db", func(t *testing.T) {
		db := memdb.NewTestDB(t, dbcfg.ChainDB)
		ctx := context.Background()

		// Store a delayed message
		tx, err := db.BeginRw(ctx)
		if err != nil {
			t.Fatal(err)
		}
		msg := &arbostypes.L1IncomingMessage{
			Header: &arbostypes.L1IncomingMessageHeader{
				Kind:      arbostypes.L1MessageType_L2Message,
				L1BaseFee: big.NewInt(0),
			},
			L2msg: []byte("delayed"),
		}
		if err := putDelayedMessage(tx, 5, msg); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}

		b := &singleBatchBackend{ctx: context.Background(), db: db}
		got, err := b.ReadDelayedInbox(5)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.L2msg, []byte("delayed")) {
			t.Fatalf("L2msg mismatch: %q", got.L2msg)
		}

		// Missing delayed message
		_, err = b.ReadDelayedInbox(999)
		if err == nil {
			t.Fatal("expected error for missing delayed message")
		}
	})
}
