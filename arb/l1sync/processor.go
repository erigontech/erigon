package l1sync

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/nitro-erigon/arbos"
	"github.com/erigontech/nitro-erigon/arbos/arbostypes"
	"github.com/erigontech/nitro-erigon/arbos/l2pricing"
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
func UnpackBatch(ctx context.Context, seqNum uint64, data []byte, blockHash common.Hash, dapReaders []daprovider.Reader, db kv.RwDB, delayedMessagesRead uint64) ([]*arbostypes.MessageWithMetadata, error) {
	backend := &singleBatchBackend{
		data:      data,
		blockHash: blockHash,
		seqNum:    seqNum,
		db:        db,
	}
	multiplexer := arbstate.NewInboxMultiplexer(backend, delayedMessagesRead, dapReaders, daprovider.KeysetValidate)

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
	messages, err := UnpackBatch(ctx, seqNum, data, blockHash, s.dapReaders, s.db, s.delayedMessagesRead)
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
		block, err := createBlockFromMessage(msg, nil)
		if err != nil {
			fmt.Println("err", err)
		} else if block.Number() != nil && block.NumberU64()%1000 == 0 {
			// fmt.Println("lol")
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

	// Update in-memory delayedMessagesRead from the last message of this batch
	if len(messages) > 0 {
		s.delayedMessagesRead = messages[len(messages)-1].DelayedMessagesRead
	}

	s.logger.Info("batch processed and stored", "batchSeqNum", seqNum, "messages", len(messages), "l1Block", l1BlockNumber)
	return nil
}

// getLastDelayedMessagesRead returns the DelayedMessagesRead from the last message
// of the given batch
func (s *L1SyncService) getLastDelayedMessagesRead(ctx context.Context, batchSeqNum uint64) (uint64, error) {
	var result uint64
	err := s.db.View(ctx, func(tx kv.Tx) error {
		// Get msg count from the batch entry
		val, e := tx.GetOne(kv.ArbL1SyncBatch, uint64Key(batchSeqNum))
		if e != nil {
			return e
		}
		if val == nil || len(val) < 8 {
			return nil
		}
		msgCount := binary.BigEndian.Uint64(val[:8])
		if msgCount == 0 {
			return nil
		}
		// Read the last message of this batch
		lastMsgData, e := tx.GetOne(kv.ArbL1SyncMsg, msgKey(batchSeqNum, msgCount-1))
		if e != nil {
			return e
		}
		if lastMsgData == nil {
			return nil
		}
		var msg arbostypes.MessageWithMetadata
		if e := json.Unmarshal(lastMsgData, &msg); e != nil {
			return e
		}
		result = msg.DelayedMessagesRead
		return nil
	})
	return result, err
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

type L1Info struct {
	poster        common.Address
	l1BlockNumber uint64
	l1Timestamp   uint64
}

func createBlockFromMessage(msg *arbostypes.MessageWithMetadata, lastBlockHeader *types.Header) (*types.Block, error) {
	if msg == nil || msg.Message == nil {
		return nil, nil
	}
	arbTxes, err := arbos.ParseL2Transactions(msg.Message, big.NewInt(421614))
	if err != nil {
		// log.Warn("error parsing incoming message", "err", err)
		arbTxes = arbTxes[:0]
	}
	txes := make([]types.Transaction, len(arbTxes))
	for i := 0; i < len(arbTxes); i++ {
		txes[i] = arbTxes[i].GetInner()
	}
	// txes := types.WrapArbTransactions(arbTxes)

	l1Header := msg.Message.Header
	poster := l1Header.Poster

	l1Info := &L1Info{
		poster:        poster,
		l1BlockNumber: l1Header.BlockNumber,
		l1Timestamp:   l1Header.Timestamp,
	}

	header := createNewHeader(lastBlockHeader, l1Info)

	l1BlockNum := l1Info.l1BlockNumber

	// Prepend a tx before all others to touch up the osState (update the L1 block num, pricing pools, etc)
	startTx := arbos.InternalTxStartBlock(big.NewInt(421614), l1Header.L1BaseFee, l1BlockNum, header, lastBlockHeader)
	txes = append(types.Transactions{startTx}, txes...)
	// startTx := arbos.InternalTxStartBlock(e.chainConfig.ChainID, l1Header.L1BaseFee, l1BlockNum, header, header)
	// txes = append(types.Transactions{types.NewArbTx(startTx)}, txes...)

	block := types.NewBlock(header, txes, nil, nil, nil)

	return block, nil
}

func createNewHeader(prevHeader *types.Header, l1info *L1Info) *types.Header {

	var lastBlockHash common.Hash
	blockNumber := big.NewInt(0)
	timestamp := uint64(0)
	coinbase := common.Address{}
	if l1info != nil {
		timestamp = l1info.l1Timestamp
		coinbase = l1info.poster
	}
	extra := common.Hash{}.Bytes()
	mixDigest := common.Hash{}
	if prevHeader != nil {
		lastBlockHash = prevHeader.Hash()
		blockNumber.Add(prevHeader.Number, big.NewInt(1))
		if timestamp < prevHeader.Time {
			timestamp = prevHeader.Time
		}
		copy(extra, prevHeader.Extra)
		mixDigest = prevHeader.MixDigest
	}
	header := &types.Header{
		ParentHash:  lastBlockHash,
		UncleHash:   empty.UncleHash, // Post-merge Ethereum will require this to be types.EmptyUncleHash
		Coinbase:    coinbase,
		Root:        [32]byte{},    // Filled in later
		TxHash:      [32]byte{},    // Filled in later
		ReceiptHash: [32]byte{},    // Filled in later
		Bloom:       [256]byte{},   // Filled in later
		Difficulty:  big.NewInt(1), // Eventually, Ethereum plans to require this to be zero
		Number:      blockNumber,
		GasLimit:    l2pricing.GethBlockGasLimit,
		GasUsed:     0,
		Time:        timestamp,
		Extra:       extra,     // used by NewEVMBlockContext
		MixDigest:   mixDigest, // used by NewEVMBlockContext
		Nonce:       [8]byte{}, // Filled in later; post-merge Ethereum will require this to be zero
	}
	return header
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
