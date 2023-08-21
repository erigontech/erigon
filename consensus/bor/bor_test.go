package bor_test

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
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
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/stages/mock"
	"github.com/ledgerwatch/log/v3"
)

type test_heimdall struct {
	currentSpan  *span.HeimdallSpan
	chainConfig  *chain.Config
	validatorSet *valset.ValidatorSet
	spans        map[uint64]*span.HeimdallSpan
}

func newTestHeimdall(chainConfig *chain.Config) *test_heimdall {
	return &test_heimdall{nil, chainConfig, nil, map[uint64]*span.HeimdallSpan{}}
}

func (h test_heimdall) StateSyncEvents(ctx context.Context, fromID uint64, to int64) ([]*clerk.EventRecordWithTime, error) {
	return nil, nil
}

func (h *test_heimdall) Span(ctx context.Context, spanID uint64) (*span.HeimdallSpan, error) {

	if span, ok := h.spans[spanID]; ok {
		h.currentSpan = span
		return span, nil
	}

	var nextSpan = span.Span{
		ID: spanID,
	}

	if h.currentSpan == nil || spanID == 0 {
		nextSpan.StartBlock = 1 //256
	} else {
		if spanID != h.currentSpan.ID+1 {
			return nil, fmt.Errorf("Can't initialize span: non consecutive span")
		}

		nextSpan.StartBlock = h.currentSpan.EndBlock + 1
	}

	nextSpan.EndBlock = nextSpan.StartBlock + (100 * h.chainConfig.Bor.CalculateSprint(nextSpan.StartBlock)) - 1

	// TODO we should use a subset here - see: https://wiki.polygon.technology/docs/pos/bor/

	selectedProducers := make([]valset.Validator, len(h.validatorSet.Validators))

	for i, v := range h.validatorSet.Validators {
		selectedProducers[i] = *v
	}

	h.currentSpan = &span.HeimdallSpan{
		Span:              nextSpan,
		ValidatorSet:      *h.validatorSet,
		SelectedProducers: selectedProducers,
		ChainID:           h.chainConfig.ChainID.String(),
	}

	h.spans[h.currentSpan.ID] = h.currentSpan

	return h.currentSpan, nil
}

func (h test_heimdall) currentSprintLength() int {
	if h.currentSpan != nil {
		return int(h.chainConfig.Bor.CalculateSprint(h.currentSpan.StartBlock))
	}

	return int(h.chainConfig.Bor.CalculateSprint(256))
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

func (g test_genesisContract) CommitState(event rlp.RawValue, syscall consensus.SystemCall) error {
	return nil
}

func (g test_genesisContract) LastStateId(syscall consensus.SystemCall) (*big.Int, error) {
	return big.NewInt(0), nil
}

type headerReader struct {
	validator validator
}

func (r headerReader) Config() *chain.Config {
	return r.validator.ChainConfig
}

func (r headerReader) FrozenBlocks() uint64 {
	return 0
}

func (r headerReader) CurrentHeader() *types.Header {
	return nil
}

func (r headerReader) GetHeader(_ libcommon.Hash, blockNo uint64) *types.Header {
	return r.GetHeaderByNumber(blockNo)
}

func (r headerReader) GetHeaderByNumber(blockNo uint64) *types.Header {
	if r.validator.blocks != nil {
		if block, ok := r.validator.blocks[blockNo]; ok {
			return block.Header()
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

type spanner struct {
	*span.ChainSpanner
	currentSpan span.Span
}

func (c spanner) GetCurrentSpan(_ consensus.SystemCall) (*span.Span, error) {
	return &c.currentSpan, nil
}

func (c *spanner) CommitSpan(heimdallSpan span.HeimdallSpan, syscall consensus.SystemCall) error {
	c.currentSpan = heimdallSpan.Span
	return nil
}

type validator struct {
	*mock.MockSentry
	heimdall *test_heimdall
	blocks   map[uint64]*types.Block
}

func (v validator) generateChain(length int) (*core.ChainPack, error) {
	return core.GenerateChain(v.ChainConfig, v.Genesis, v.Engine, v.DB, length, func(i int, block *core.BlockGen) {
		v.blocks[block.GetParent().NumberU64()] = block.GetParent()
	})
}

func (v validator) IsProposer(block *types.Block) (bool, error) {
	return v.Engine.(*bor.Bor).IsProposer(block.Header())
}

func (v validator) sealBlocks(blocks []*types.Block) ([]*types.Block, error) {
	sealedBlocks := make([]*types.Block, 0, len(blocks))

	sealResults := make(chan *types.Block)

	hr := headerReader{v}

	for _, block := range blocks {
		header := block.HeaderNoCopy()

		if err := v.Engine.Prepare(hr, header, nil); err != nil {
			return nil, err
		}

		if parent := hr.GetHeaderByNumber(header.Number.Uint64() - 1); parent != nil {
			header.ParentHash = parent.Hash()
		}

		if err := v.Engine.Seal(hr, block, sealResults, nil); err != nil {
			return nil, err
		}

		sealedBlock := <-sealResults
		v.blocks[sealedBlock.NumberU64()] = sealedBlock
		sealedBlocks = append(sealedBlocks, sealedBlock)
	}

	return sealedBlocks, nil
}

func (v validator) verifyBlocks(blocks []*types.Block) error {
	hr := headerReader{v}

	for i, block := range blocks {
		if err := v.Engine.VerifyHeader(hr, block.Header(), false); err != nil {
			return fmt.Errorf("block %d: failed to verify: %w", i, err)
		}
	}

	return nil
}

func newValidator(t *testing.T, heimdall *test_heimdall, blocks map[uint64]*types.Block) validator {
	logger := log.Root()

	bor := bor.New(
		heimdall.chainConfig,
		memdb.New(""),
		nil, /* blockReader */
		&spanner{span.NewChainSpanner(contract.ValidatorSet(), heimdall.chainConfig, false, logger), span.Span{}},
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

	checkStateRoot := true
	return validator{
		mock.MockWithEverything(t, &types.Genesis{Config: heimdall.chainConfig}, validatorKey, prune.DefaultMode, bor, 1024, false, false, checkStateRoot),
		heimdall,
		blocks,
	}
}

func TestValidatorCreate(t *testing.T) {
	newValidator(t, newTestHeimdall(params.BorDevnetChainConfig), map[uint64]*types.Block{})
}

func TestVerifyHeader(t *testing.T) {
	v := newValidator(t, newTestHeimdall(params.BorDevnetChainConfig), map[uint64]*types.Block{})

	chain, err := v.generateChain(1)

	if err != nil {
		t.Fatalf("generate blocks failed: %v", err)
	}

	sealedBlocks, err := v.sealBlocks(chain.Blocks)

	if err != nil {
		t.Fatalf("seal block failed: %v", err)
	}

	err = v.verifyBlocks(sealedBlocks)

	if err != nil {
		t.Fatalf("verify blocks failed: %v", err)
	}
}

func TestVerifyRun(t *testing.T) {
	//testVerify(t, 5, 8)
}

func TestVerifySprint(t *testing.T) {
	//testVerify(t, 10, 4, int(params.BorDevnetChainConfig.Bor.CalculateSprint(256)))
}

func TestVerifySpan(t *testing.T) {
	//testVerify(t, 10, 4 /*100**/ *int(params.BorDevnetChainConfig.Bor.CalculateSprint(256)))
}

func testVerify(t *testing.T, noValidators int, chainLength int) {
	log.Root().SetHandler(log.StderrHandler)

	heimdall := newTestHeimdall(params.BorDevnetChainConfig)
	blocks := map[uint64]*types.Block{}

	validators := make([]validator, noValidators)

	for i := 0; i < noValidators; i++ {
		validators[i] = newValidator(t, heimdall, blocks)
	}

	chains := make([]*core.ChainPack, noValidators)

	for i, v := range validators {
		chain, err := v.generateChain(chainLength)

		if err != nil {
			t.Fatalf("generate blocks failed: %v", err)
		}

		chains[i] = chain
	}

	lastProposerIndex := -1

	for bi := 0; bi < chainLength; bi++ {
		for vi, v := range validators {
			block := chains[vi].Blocks[bi]

			isProposer, err := v.IsProposer(block)

			if err != nil {
				t.Fatal(err)
			}

			if isProposer {

				if vi != lastProposerIndex {
					sprintLen := params.BorDevnetChainConfig.Bor.CalculateSprint(block.NumberU64())
					if block.NumberU64() > 1 && block.NumberU64()%sprintLen != 0 {
						t.Fatalf("Unexpected sprint boundary at %d for: %d", bi, block.NumberU64())
					}
					lastProposerIndex = vi
				}

				sealedBlocks, err := v.sealBlocks([]*types.Block{block})

				if err != nil {
					t.Fatalf("seal block failed: %v", err)
				}

				err = v.verifyBlocks(sealedBlocks)

				if err != nil {
					t.Fatalf("verify blocks failed: %v", err)
				}
			}
		}
	}
}

func TestSendBlock(t *testing.T) {
	heimdall := newTestHeimdall(params.BorDevnetChainConfig)
	blocks := map[uint64]*types.Block{}

	s := newValidator(t, heimdall, blocks)
	r := newValidator(t, heimdall, blocks)

	chain, err := s.generateChain(1)

	if err != nil {
		t.Fatalf("generate blocks failed: %v", err)
	}

	sealedBlocks, err := s.sealBlocks(chain.Blocks)

	if err != nil {
		t.Fatalf("seal block failed: %v", err)
	}

	err = s.verifyBlocks(sealedBlocks)

	if err != nil {
		t.Fatalf("verify blocks failed: %v", err)
	}

	b, err := rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: sealedBlocks[0],
		TD:    big.NewInt(1), // This is ignored anyway
	})

	if err != nil {
		t.Fatal(err)
	}

	r.ReceiveWg.Add(1)
	for _, err = range r.Send(&sentry.InboundMessage{Id: sentry.MessageId_NEW_BLOCK_66, Data: b, PeerId: s.PeerId}) {
		if err != nil {
			t.Fatal(err)
		}
	}
	r.ReceiveWg.Wait()
}

/*

	if err = m.InsertChain(longerChain, nil); err != nil {
		t.Fatal(err)
	}
*/
