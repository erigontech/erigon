package l1sync

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/nitro-erigon/arbos/arbostypes"
	"github.com/erigontech/nitro-erigon/arbstate"
	"github.com/erigontech/nitro-erigon/arbstate/daprovider"
)

// DB key prefixes for ArbL1SyncBucket
var (
	progressLastBatchSeqNum = []byte("lastBatchSeqNum")
	progressLastL1Block     = []byte("lastL1Block")
	prefixBatchRaw          = []byte("batch")
	prefixMsg               = []byte("msg")
	prefixBatchMsgCount     = []byte("batchMsgCount")
	prefixDelayedMsg        = []byte("delayed")
)

// singleBatchBackend implements arbstate.InboxBackend for a single batch
// It feeds one batch's serialized data to the InboxMultiplexer and signals
// when the batch is fully consumed.
type singleBatchBackend struct {
	data                  []byte
	blockHash             common.Hash
	seqNum                uint64
	consumed              bool
	positionWithinMessage uint64
	db                    kv.RwDB
}

func (b *singleBatchBackend) PeekSequencerInbox() ([]byte, common.Hash, error) {
	if b.consumed {
		return nil, common.Hash{}, errors.New("batch already consumed")
	}
	return b.data, b.blockHash, nil
}

func (b *singleBatchBackend) GetSequencerInboxPosition() uint64 {
	return b.seqNum
}

func (b *singleBatchBackend) AdvanceSequencerInbox() {
	b.consumed = true
}

func (b *singleBatchBackend) GetPositionWithinMessage() uint64 {
	return b.positionWithinMessage
}

func (b *singleBatchBackend) SetPositionWithinMessage(pos uint64) {
	b.positionWithinMessage = pos
}

func (b *singleBatchBackend) ReadDelayedInbox(seqNum uint64) (*arbostypes.L1IncomingMessage, error) {
	if b.db == nil {
		return nil, fmt.Errorf("delayed inbox not available (requested seqNum %d)", seqNum)
	}
	var msg *arbostypes.L1IncomingMessage
	err := b.db.View(context.Background(), func(tx kv.Tx) error {
		var e error
		msg, e = getDelayedMessage(tx, seqNum)
		return e
	})
	return msg, err
}

// UnpackBatch takes serialized batch data and extracts all MessageWithMetadata from it
func UnpackBatch(ctx context.Context, seqNum uint64, data []byte, blockHash common.Hash, dapReaders []daprovider.Reader, db kv.RwDB) ([]*arbostypes.MessageWithMetadata, error) {
	backend := &singleBatchBackend{
		data:      data,
		blockHash: blockHash,
		seqNum:    seqNum,
		db:        db,
	}
	multiplexer := arbstate.NewInboxMultiplexer(backend, 0, dapReaders, daprovider.KeysetValidate)

	var messages []*arbostypes.MessageWithMetadata
	for !backend.consumed {
		msg, err := multiplexer.Pop(ctx)
		if err != nil {
			return messages, fmt.Errorf("error unpacking batch %d message %d: %w", seqNum, len(messages), err)
		}
		messages = append(messages, msg)
	}
	return messages, nil
}

// ProcessBatch unpacks a batch into messages, stores everything in DB, and executes L2 blocks via DigestMessage
func (s *L1SyncService) ProcessBatch(ctx context.Context, seqNum uint64, data []byte, blockHash common.Hash, l1BlockNumber uint64) error {
	messages, err := UnpackBatch(ctx, seqNum, data, blockHash, s.dapReaders, s.db)
	if err != nil {
		return err
	}

	s.logger.Info("unpacked batch", "batchSeqNum", seqNum, "messageCount", len(messages), "l1Block", l1BlockNumber)

	tx, err := s.db.BeginRw(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin DB transaction: %w", err)
	}
	defer tx.Rollback()

	// Store raw batch data
	batchKey := makeBatchRawKey(seqNum)
	if err := tx.Put(kv.ArbL1SyncBucket, batchKey, data); err != nil {
		return fmt.Errorf("failed to store raw batch %d: %w", seqNum, err)
	}

	// Store message count for this batch
	countKey := makeBatchMsgCountKey(seqNum)
	countVal := make([]byte, 8)
	binary.BigEndian.PutUint64(countVal, uint64(len(messages)))
	if err := tx.Put(kv.ArbL1SyncBucket, countKey, countVal); err != nil {
		return fmt.Errorf("failed to store batch msg count %d: %w", seqNum, err)
	}

	for i, msg := range messages {
		// Store decoded message
		msgKey := makeMsgKey(seqNum, uint64(i))
		msgBytes, err := json.Marshal(msg)
		if err != nil {
			return fmt.Errorf("failed to marshal message %d/%d: %w", seqNum, i, err)
		}
		if err := tx.Put(kv.ArbL1SyncBucket, msgKey, msgBytes); err != nil {
			return fmt.Errorf("failed to store message %d/%d: %w", seqNum, i, err)
		}

		// // Execute L2 block
		// result, err := s.exec.DigestMessage(msgNum, msg, nil)
		// if err != nil {
		// 	return fmt.Errorf("failed to digest message %d (batch %d, msg %d): %w", msgNum, seqNum, i, err)
		// }

		// s.logger.Debug("processed message", "batchSeqNum", seqNum, "msgIdx", i, "msgNum", msgNum, "blockHash", result)
	}

	// Update progress
	if err := putUint64(tx, progressLastBatchSeqNum, seqNum); err != nil {
		return fmt.Errorf("failed to update lastBatchSeqNum: %w", err)
	}
	if err := putUint64(tx, progressLastL1Block, l1BlockNumber); err != nil {
		return fmt.Errorf("failed to update lastL1Block: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit batch %d: %w", seqNum, err)
	}

	s.logger.Info("batch processed and stored", "batchSeqNum", seqNum, "messages", len(messages), "l1Block", l1BlockNumber)
	return nil
}

// GetProgress reads the last processed batch sequence number and L1 block from DB
func (s *L1SyncService) GetProgress(ctx context.Context) (lastBatchSeqNum uint64, lastL1Block uint64, err error) {
	err = s.db.View(ctx, func(tx kv.Tx) error {
		var e error
		lastBatchSeqNum, e = getUint64(tx, progressLastBatchSeqNum)
		if e != nil {
			return e
		}
		lastL1Block, e = getUint64(tx, progressLastL1Block)
		return e
	})
	return
}

func (s *L1SyncService) GetStoredBatch(ctx context.Context, seqNum uint64) ([]byte, error) {
	var data []byte
	err := s.db.View(ctx, func(tx kv.Tx) error {
		var e error
		data, e = tx.GetOne(kv.ArbL1SyncBucket, makeBatchRawKey(seqNum))
		return e
	})
	return data, err
}

func (s *L1SyncService) GetStoredMessage(ctx context.Context, seqNum uint64, msgIdx uint64) (*arbostypes.MessageWithMetadata, error) {
	var msg arbostypes.MessageWithMetadata
	err := s.db.View(ctx, func(tx kv.Tx) error {
		data, e := tx.GetOne(kv.ArbL1SyncBucket, makeMsgKey(seqNum, msgIdx))
		if e != nil {
			return e
		}
		if data == nil {
			return fmt.Errorf("message %d/%d not found", seqNum, msgIdx)
		}
		return json.Unmarshal(data, &msg)
	})
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

func makeDelayedMsgKey(seqNum uint64) []byte {
	key := make([]byte, 0, len(prefixDelayedMsg)+8)
	key = append(key, prefixDelayedMsg...)
	key = appendUint64(key, seqNum)
	return key
}

func putDelayedMessage(tx kv.RwTx, seqNum uint64, msg *arbostypes.L1IncomingMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal delayed message %d: %w", seqNum, err)
	}
	return tx.Put(kv.ArbL1SyncBucket, makeDelayedMsgKey(seqNum), data)
}

func getDelayedMessage(tx kv.Tx, seqNum uint64) (*arbostypes.L1IncomingMessage, error) {
	data, err := tx.GetOne(kv.ArbL1SyncBucket, makeDelayedMsgKey(seqNum))
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, fmt.Errorf("delayed message %d not found in DB", seqNum)
	}
	var msg arbostypes.L1IncomingMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal delayed message %d: %w", seqNum, err)
	}
	return &msg, nil
}

func makeBatchRawKey(seqNum uint64) []byte {
	key := make([]byte, 0, len(prefixBatchRaw)+8)
	key = append(key, prefixBatchRaw...)
	key = appendUint64(key, seqNum)
	return key
}

func makeBatchMsgCountKey(seqNum uint64) []byte {
	key := make([]byte, 0, len(prefixBatchMsgCount)+8)
	key = append(key, prefixBatchMsgCount...)
	key = appendUint64(key, seqNum)
	return key
}

func makeMsgKey(seqNum uint64, msgIdx uint64) []byte {
	key := make([]byte, 0, len(prefixMsg)+8+8)
	key = append(key, prefixMsg...)
	key = appendUint64(key, seqNum)
	key = appendUint64(key, msgIdx)
	return key
}

func appendUint64(buf []byte, v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return append(buf, b...)
}

func putUint64(tx kv.RwTx, key []byte, val uint64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, val)
	return tx.Put(kv.ArbL1SyncBucket, key, buf)
}

func getUint64(tx kv.Tx, key []byte) (uint64, error) {
	data, err := tx.GetOne(kv.ArbL1SyncBucket, key)
	if err != nil {
		return 0, err
	}
	if data == nil || len(data) < 8 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(data), nil
}

