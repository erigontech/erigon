package sync

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/nitro-erigon/arbos"
	"github.com/erigontech/nitro-erigon/arbos/arbostypes"
	"github.com/erigontech/nitro-erigon/arbos/l2pricing"
	"github.com/erigontech/nitro-erigon/arbutil"
	"github.com/erigontech/nitro-erigon/execution"
)

// EngineAPIExecutionSequencer implements the ExecutionSequencer interface
// and communicates with the Ethereum Engine API via gRPC
type EngineAPIExecutionSequencer struct {
	client      ExecutionClient
	logger      log.Logger
	chainID     *big.Int
	chainConfig *chain.Config
	headNumber  arbutil.MessageIndex
}

// NewEngineAPIExecutionSequencerWithClient creates a new Engine API execution sequencer with an existing execution client
func NewEngineAPIExecutionSequencerWithClient(chainID *big.Int, executionClient ExecutionClient, chainConfig *chain.Config) (*EngineAPIExecutionSequencer, error) {
	engineSequencer := &EngineAPIExecutionSequencer{
		client:      executionClient,
		logger:      log.New(),
		chainID:     chainID,
		chainConfig: chainConfig,
	}

	// Verify the connection is ready
	// if err := engineSequencer.Prepare(context.Background()); err != nil {
	// 	return nil, fmt.Errorf("failed to prepare Engine API connection: %w", err)
	// }

	return engineSequencer, nil
}

// DigestMessage processes a message and creates a block, then inserts it via Engine API
func (e *EngineAPIExecutionSequencer) DigestMessage(num arbutil.MessageIndex, msg *arbostypes.MessageWithMetadata, msgForPrefetch *arbostypes.MessageWithMetadata) (*execution.MessageResult, error) {
	e.logger.Info("DigestMessage called", "num", num, "msg", msg)

	lastHeader, err := e.client.CurrentHeader(context.TODO())
	if lastHeader == nil || err != nil {
		return nil, fmt.Errorf("failed to get current header: %w", err)
	}
	block, err := e.createBlockFromMessage(msg, lastHeader)
	if err != nil {
		return nil, fmt.Errorf("failed to create block from message: %w", err)
	}
	log.Info("block", "block.header", block.Header(), "block.body", block.Body(), "block.hash", block.Hash(), "block.number", block.Number(), "block.txs", block.Transactions())
	if block == nil {
		return &execution.MessageResult{
			BlockHash: common.Hash{},
			SendRoot:  common.Hash{},
		}, nil
	}

	err = e.client.InsertBlocks(context.Background(), []*types.Block{block})
	if err != nil {
		return nil, fmt.Errorf("failed to insert block via Engine API: %w", err)
	}

	latestValidHash, err := e.client.UpdateForkChoice(context.Background(), block.Header(), block.Header())
	if err != nil {
		return nil, fmt.Errorf("failed to update fork choice, latestValidHash %s: %w", latestValidHash, err)
	}

	e.headNumber = num

	return &execution.MessageResult{
		BlockHash: block.Hash(),
		SendRoot:  common.Hash{}, // TODO: calculate send root if needed
	}, nil
}

// Reorg handles chain reorganization by updating fork choice
func (e *EngineAPIExecutionSequencer) Reorg(count arbutil.MessageIndex, newMessages []arbostypes.MessageWithMetadataAndBlockHash, oldMessages []*arbostypes.MessageWithMetadata) ([]*execution.MessageResult, error) {
	return nil, nil
}

func (e *EngineAPIExecutionSequencer) HeadMessageNumber() (arbutil.MessageIndex, error) {
	return e.headNumber, nil
}

func (e *EngineAPIExecutionSequencer) HeadMessageNumberSync(t *testing.T) (arbutil.MessageIndex, error) {
	return e.headNumber, nil
}

func (e *EngineAPIExecutionSequencer) ResultAtPos(pos arbutil.MessageIndex) (*execution.MessageResult, error) {
	e.logger.Info("ResultAtPos called")
	return nil, nil
}

func (e *EngineAPIExecutionSequencer) Pause() {
	e.logger.Info("Pause called")
}

func (e *EngineAPIExecutionSequencer) Activate() {
	e.logger.Info("Activate called")
}

func (e *EngineAPIExecutionSequencer) ForwardTo(url string) error {
	return errors.New("forwarding not supported for Engine API execution sequencer")
}

func (e *EngineAPIExecutionSequencer) SequenceDelayedMessage(message *arbostypes.L1IncomingMessage, delayedSeqNum uint64) error {
	e.logger.Info("SequenceDelayedMessage called", "delayedSeqNum", delayedSeqNum)
	return nil
}

func (e *EngineAPIExecutionSequencer) NextDelayedMessageNumber() (uint64, error) {
	return 0, nil
}

func (e *EngineAPIExecutionSequencer) MarkFeedStart(to arbutil.MessageIndex) {
	e.logger.Info("MarkFeedStart called", "to", to)
}

func (e *EngineAPIExecutionSequencer) Synced() bool {
	return false
}

func (e *EngineAPIExecutionSequencer) FullSyncProgressMap() map[string]interface{} {
	return nil
}

type L1Info struct {
	poster        common.Address
	l1BlockNumber uint64
	l1Timestamp   uint64
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

// createBlockFromMessage creates a block from a message
func (e *EngineAPIExecutionSequencer) createBlockFromMessage(msg *arbostypes.MessageWithMetadata, lastBlockHeader *types.Header) (*types.Block, error) {
	if msg == nil || msg.Message == nil {
		return nil, nil
	}
	arbTxes, err := arbos.ParseL2Transactions(msg.Message, e.chainID)
	if err != nil {
		log.Warn("error parsing incoming message", "err", err)
		arbTxes = arbTxes[:0]
	}
	txes := make([]types.Transaction, len(arbTxes))
	for i := 0; i < len(arbTxes); i++ {
		txes[i] = arbTxes[i].GetInner()
	}

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
	startTx := arbos.InternalTxStartBlock(e.chainConfig.ChainID, l1Header.L1BaseFee, l1BlockNum, header, lastBlockHeader)
	txes = append(types.Transactions{startTx}, txes...)
	block := types.NewBlock(header, txes, nil, nil, nil)

	return block, nil
}
