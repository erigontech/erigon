package sync

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/executionproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	eth1utils "github.com/erigontech/erigon/execution/eth1/eth1_utils"
	"github.com/erigontech/nitro-erigon/arbos/arbostypes"
	"github.com/erigontech/nitro-erigon/arbutil"
	"github.com/erigontech/nitro-erigon/execution"
	"google.golang.org/protobuf/types/known/emptypb"
)

// EngineAPIExecutionSequencer implements the ExecutionSequencer interface
// and communicates with the Ethereum Engine API via gRPC
type EngineAPIExecutionSequencer struct {
	client     executionproto.ExecutionClient
	logger     log.Logger
	chainID    *big.Int
	headNumber arbutil.MessageIndex
}


// NewEngineAPIExecutionSequencerWithClient creates a new Engine API execution sequencer with an existing execution client
func NewEngineAPIExecutionSequencerWithClient(chainID *big.Int, executionClient executionproto.ExecutionClient) (*EngineAPIExecutionSequencer, error) {
	engineSequencer := &EngineAPIExecutionSequencer{
		client:  executionClient,
		logger:  log.New(),
		chainID: chainID,
	}

	// Verify the connection is ready
	if err := engineSequencer.Prepare(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to prepare Engine API connection: %w", err)
	}

	return engineSequencer, nil
}

// Prepare checks if the Engine API is ready
func (e *EngineAPIExecutionSequencer) Prepare(ctx context.Context) error {
	ready, err := e.client.Ready(ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}

	if !ready.Ready {
		return errors.New("execution client not ready")
	}

	return nil
}

// DigestMessage processes a message and creates a block, then inserts it via Engine API
func (e *EngineAPIExecutionSequencer) DigestMessage(num arbutil.MessageIndex, msg *arbostypes.MessageWithMetadata, msgForPrefetch *arbostypes.MessageWithMetadata) (*execution.MessageResult, error) {
	e.logger.Info("DigestMessage called", "num", num, "msg", msg)

	block, err := e.createBlockFromMessage(num, msg)
	if err != nil {
		return nil, fmt.Errorf("failed to create block from message: %w", err)
	}

	if block == nil {
		return &execution.MessageResult{
			BlockHash: common.Hash{},
			SendRoot:  common.Hash{},
		}, nil
	}

	err = e.InsertBlocks(context.Background(), []*types.Block{block})
	if err != nil {
		return nil, fmt.Errorf("failed to insert block via Engine API: %w", err)
	}

	err = e.UpdateForkChoice(context.Background(), block.Header(), block.Header())
	if err != nil {
		return nil, fmt.Errorf("failed to update fork choice: %w", err)
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

// InsertBlocks inserts blocks via Engine API with retry logic
func (e *EngineAPIExecutionSequencer) InsertBlocks(ctx context.Context, blocks []*types.Block) error {
	if len(blocks) == 0 {
		return nil
	}

	request := &executionproto.InsertBlocksRequest{
		Blocks: eth1utils.ConvertBlocksToRPC(blocks),
	}

	return e.retryBusy(ctx, "insertBlocks", func() error {
		response, err := e.client.InsertBlocks(ctx, request)
		if err != nil {
			return err
		}

		status := response.Result
		switch status {
		case executionproto.ExecutionStatus_Success:
			return nil
		case executionproto.ExecutionStatus_Busy:
			return ErrExecutionClientBusy
		default:
			return fmt.Errorf("insert blocks failure status: %s", status.String())
		}
	})
}

// UpdateForkChoice updates the fork choice via Engine API with retry logic
func (e *EngineAPIExecutionSequencer) UpdateForkChoice(ctx context.Context, tip *types.Header, finalizedHeader *types.Header) error {
	tipHash := tip.Hash()

	request := executionproto.ForkChoice{
		HeadBlockHash:      gointerfaces.ConvertHashToH256(tipHash),
		SafeBlockHash:      gointerfaces.ConvertHashToH256(tipHash),
		FinalizedBlockHash: gointerfaces.ConvertHashToH256(finalizedHeader.Hash()),
		Timeout:            0,
	}

	return e.retryBusy(ctx, "updateForkChoice", func() error {
		response, err := e.client.UpdateForkChoice(ctx, &request)
		if err != nil {
			return err
		}

		switch response.Status {
		case executionproto.ExecutionStatus_Success:
			return nil
		case executionproto.ExecutionStatus_BadBlock:
			return errors.New("bad block as forkchoice")
		case executionproto.ExecutionStatus_InvalidForkchoice:
			return errors.New("invalid forkchoice")
		case executionproto.ExecutionStatus_Busy:
			return ErrExecutionClientBusy
		default:
			return fmt.Errorf("fork choice update failure status: %s", response.Status.String())
		}
	})
}

func (e *EngineAPIExecutionSequencer) retryBusy(ctx context.Context, label string, f func() error) error {
	backOff := 50 * time.Millisecond
	logEvery := 5 * time.Second
	logEveryXAttempt := int64(logEvery / backOff)
	attempt := int64(1)
	operation := func() error {
		err := f()
		if err == nil {
			return nil
		}

		if errors.Is(err, ErrExecutionClientBusy) {
			if attempt%logEveryXAttempt == 1 {
				e.logger.Debug("execution client busy - retrying", "in", backOff, "label", label, "attempt", attempt)
			}
			attempt++
			return err
		}

		return backoff.Permanent(err)
	}

	return backoff.Retry(operation, backoff.WithContext(backoff.NewConstantBackOff(backOff), ctx))
}

// createBlockFromMessage creates a block from a message
func (e *EngineAPIExecutionSequencer) createBlockFromMessage(num arbutil.MessageIndex, msg *arbostypes.MessageWithMetadata) (*types.Block, error) {
	if msg == nil || msg.Message == nil {
		return nil, nil
	}
	// ProduceBlockHeader
	// Create a simple block header
	header := &types.Header{
		Number:     big.NewInt(int64(num)),
		Time:       uint64(time.Now().Unix()),
		Difficulty: big.NewInt(1),
		GasLimit:   30000000,
		GasUsed:    0,
		Coinbase:   common.Address{},
		Root:       common.Hash{},
		ParentHash: common.Hash{},
		UncleHash:  types.EmptyUncleHash,
		Extra:      []byte{},
		MixDigest:  common.Hash{},
		Nonce:      types.BlockNonce{},
		BaseFee:    big.NewInt(0),
	}
	block := types.NewBlock(header, nil, nil, nil, nil)

	return block, nil
}
