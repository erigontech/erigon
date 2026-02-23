package l1sync

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/nitro-erigon/arbos/arbostypes"
	"github.com/erigontech/nitro-erigon/arbstate"
	"github.com/erigontech/nitro-erigon/arbstate/daprovider"
)

// SyncStage keys for progress tracking via SyncStageProgress table
var (
	StageL1SyncBatch   stages.SyncStage = "L1SyncBatch"
	StageL1SyncL1Block stages.SyncStage = "L1SyncL1Block"
)

// singleBatchBackend implements arbstate.InboxBackend for a single batch
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

	// Store raw batch data with msg count: value = uint64(msgCount) + rawData
	batchVal := make([]byte, 8+len(data))
	binary.BigEndian.PutUint64(batchVal[:8], uint64(len(messages)))
	copy(batchVal[8:], data)
	if err := tx.Put(kv.ArbL1SyncBatch, uint64Key(seqNum), batchVal); err != nil {
		return fmt.Errorf("failed to store batch %d: %w", seqNum, err)
	}

	// Store decoded messages
	for i, msg := range messages {
		msgBytes, err := json.Marshal(msg)
		if err != nil {
			return fmt.Errorf("failed to marshal message %d/%d: %w", seqNum, i, err)
		}
		if err := tx.Put(kv.ArbL1SyncMsg, msgKey(seqNum, uint64(i)), msgBytes); err != nil {
			return fmt.Errorf("failed to store message %d/%d: %w", seqNum, i, err)
		}

		// // Execute L2 block
		// result, err := s.exec.DigestMessage(msgNum, msg, nil)
		// if err != nil {
		// 	return fmt.Errorf("failed to digest message %d (batch %d, msg %d): %w", msgNum, seqNum, i, err)
		// }

		// s.logger.Debug("processed message", "batchSeqNum", seqNum, "msgIdx", i, "msgNum", msgNum, "blockHash", result)
	}

	// Update progress via SyncStageProgress
	if err := stages.SaveStageProgress(tx, StageL1SyncBatch, seqNum); err != nil {
		return fmt.Errorf("failed to update L1SyncBatch progress: %w", err)
	}
	if err := stages.SaveStageProgress(tx, StageL1SyncL1Block, l1BlockNumber); err != nil {
		return fmt.Errorf("failed to update L1SyncL1Block progress: %w", err)
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
		lastBatchSeqNum, e = stages.GetStageProgress(tx, StageL1SyncBatch)
		if e != nil {
			return e
		}
		lastL1Block, e = stages.GetStageProgress(tx, StageL1SyncL1Block)
		return e
	})
	return
}

func (s *L1SyncService) GetStoredBatch(ctx context.Context, seqNum uint64) (data []byte, msgCount uint64, err error) {
	err = s.db.View(ctx, func(tx kv.Tx) error {
		val, e := tx.GetOne(kv.ArbL1SyncBatch, uint64Key(seqNum))
		if e != nil {
			return e
		}
		if val == nil || len(val) < 8 {
			return fmt.Errorf("batch %d not found", seqNum)
		}
		msgCount = binary.BigEndian.Uint64(val[:8])
		data = val[8:]
		return nil
	})
	return
}

func (s *L1SyncService) GetStoredMessage(ctx context.Context, seqNum uint64, msgIdx uint64) (*arbostypes.MessageWithMetadata, error) {
	var msg arbostypes.MessageWithMetadata
	err := s.db.View(ctx, func(tx kv.Tx) error {
		data, e := tx.GetOne(kv.ArbL1SyncMsg, msgKey(seqNum, msgIdx))
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

// --- delayed message helpers ---

func putDelayedMessage(tx kv.RwTx, seqNum uint64, msg *arbostypes.L1IncomingMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal delayed message %d: %w", seqNum, err)
	}
	return tx.Put(kv.ArbL1SyncDelayedMsg, uint64Key(seqNum), data)
}

func getDelayedMessage(tx kv.Tx, seqNum uint64) (*arbostypes.L1IncomingMessage, error) {
	data, err := tx.GetOne(kv.ArbL1SyncDelayedMsg, uint64Key(seqNum))
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

// --- key helpers ---

func uint64Key(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func msgKey(batchSeqNum uint64, msgIdx uint64) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[:8], batchSeqNum)
	binary.BigEndian.PutUint64(b[8:], msgIdx)
	return b
}
