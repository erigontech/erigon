package bor_test

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/bor"
	"github.com/ledgerwatch/erigon/consensus/bor/clerk"
	"github.com/ledgerwatch/erigon/consensus/bor/contract"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/checkpoint"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/span"
	"github.com/ledgerwatch/erigon/consensus/bor/valset"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/ledgerwatch/log/v3"
)

type test_heimdall struct {
	lastEndBlock uint64
	chainConfig  *chain.Config
	validatorSet *valset.ValidatorSet
}

func (h test_heimdall) StateSyncEvents(ctx context.Context, fromID uint64, to int64) ([]*clerk.EventRecordWithTime, error) {
	return nil, fmt.Errorf("TODO")
}

func (h *test_heimdall) Span(ctx context.Context, spanID uint64) (*span.HeimdallSpan, error) {
	var startBlock uint64

	if h.lastEndBlock == 0 {
		startBlock = 256
	} else {
		startBlock = h.lastEndBlock + 1
	}

	h.lastEndBlock = startBlock + (100 * h.chainConfig.Bor.CalculateSprint(startBlock)) - 1

	selectedProducers := make([]valset.Validator, len(h.validatorSet.Validators))

	for i, v := range h.validatorSet.Validators {
		selectedProducers[i] = *v
	}

	return &span.HeimdallSpan{
		Span: span.Span{
			StartBlock: startBlock,
			EndBlock:   h.lastEndBlock,
		},
		ValidatorSet:      *h.validatorSet,
		SelectedProducers: selectedProducers,
		ChainID:           h.chainConfig.ChainID.String(),
	}, nil
}

func (h test_heimdall) FetchCheckpoint(ctx context.Context, number int64) (*checkpoint.Checkpoint, error) {
	return nil, fmt.Errorf("TODO")
}

func (h test_heimdall) FetchCheckpointCount(ctx context.Context) (int64, error) {
	return 0, fmt.Errorf("TODO")
}

func (h test_heimdall) Close() {}

type test_genesisContract struct {
}

func (g test_genesisContract) CommitState(event *clerk.EventRecordWithTime, syscall consensus.SystemCall) error {
	return nil
}

func (g test_genesisContract) LastStateId(syscall consensus.SystemCall) (*big.Int, error) {
	return nil, fmt.Errorf("TODO")
}

type headerReader struct {
	sentry *stages.MockSentry
	parent *types.Block
}

func (r headerReader) Config() *chain.Config {
	return r.sentry.ChainConfig
}

func (r headerReader) CurrentHeader() *types.Header {
	return nil
}

func (r headerReader) GetHeader(_ libcommon.Hash, blockNo uint64) *types.Header {
	if r.parent != nil {
		if r.parent.NumberU64() == blockNo {
			return r.parent.Header()
		}
	}
	return nil
}

func (r headerReader) GetHeaderByNumber(blockNo uint64) *types.Header {
	if r.parent != nil {
		if r.parent.NumberU64() == blockNo {
			return r.parent.Header()
		}
	}
	return nil
}

func (r headerReader) GetHeaderByHash(libcommon.Hash) *types.Header {
	return nil
}

func (r headerReader) GetTd(libcommon.Hash, uint64) *big.Int {
	return nil
}

func newValidator(t *testing.T, heimdall *test_heimdall) *stages.MockSentry {
	logger := log.Root()

	bor := bor.New(
		heimdall.chainConfig,
		memdb.New(""),
		span.NewChainSpanner(contract.ValidatorSet(), heimdall.chainConfig, false, logger),
		heimdall,
		test_genesisContract{},
		logger,
	)

	validatorKey, _ := crypto.GenerateKey()
	validatorAddress := crypto.PubkeyToAddress(validatorKey.PublicKey)

	if heimdall.validatorSet == nil {
		heimdall.validatorSet = valset.NewValidatorSet([]*valset.Validator{
			{
				ID:               1,
				Address:          validatorAddress,
				VotingPower:      1000,
				ProposerPriority: 1,
			},
		}, logger)
	} else {
		heimdall.validatorSet.UpdateWithChangeSet([]*valset.Validator{
			{
				ID:               uint64(len(heimdall.validatorSet.Validators) + 1),
				Address:          validatorAddress,
				VotingPower:      1000,
				ProposerPriority: 1,
			},
		}, logger)
	}

	bor.Authorize(validatorAddress, func(_ libcommon.Address, mimeType string, message []byte) ([]byte, error) {
		return crypto.Sign(crypto.Keccak256(message), validatorKey)
	})

	return stages.MockWithEverything(t, &types.Genesis{Config: heimdall.chainConfig}, validatorKey, prune.DefaultMode, bor, false, false)
}

func TestValidatorCreate(t *testing.T) {
	newValidator(t, &test_heimdall{0, params.BorDevnetChainConfig, nil})
}

func TestVerifyHeader(t *testing.T) {
	v := newValidator(t, &test_heimdall{0, params.BorDevnetChainConfig, nil})

	hr := headerReader{v, nil}

	chain, err := core.GenerateChain(v.ChainConfig, v.Genesis, v.Engine, v.DB, 1, func(i int, block *core.BlockGen) {
		hr.parent = block.GetParent()
		v.Engine.Prepare(hr, block.GetHeader(), nil)
	}, false)

	if err != nil {
		t.Fatalf("generate blocks failed: %v", err)
	}

	sealedBlocks := make(chan *types.Block)

	err = v.Engine.Seal(hr, chain.Blocks[0], sealedBlocks, nil)

	if err != nil {
		t.Fatalf("seal block failed: %v", err)
	}

	sealedBlock := <-sealedBlocks

	err = v.Engine.VerifyHeader(hr, sealedBlock.Header(), false)

	if err != nil {
		t.Fatalf("verify header failed: %v", err)
	}
}
