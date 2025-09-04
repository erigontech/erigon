// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package bor_test

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/stages/mock"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/polygon/bor/borabi"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	polychain "github.com/erigontech/erigon/polygon/chain"
	"github.com/erigontech/erigon/polygon/heimdall"
)

type test_heimdall struct {
	currentSpan  *heimdall.Span
	chainConfig  *chain.Config
	borConfig    *borcfg.BorConfig
	validatorSet *heimdall.ValidatorSet
	spans        map[heimdall.SpanId]*heimdall.Span
}

func newTestHeimdall(chainConfig *chain.Config) *test_heimdall {
	return &test_heimdall{
		currentSpan:  nil,
		chainConfig:  chainConfig,
		borConfig:    chainConfig.Bor.(*borcfg.BorConfig),
		validatorSet: nil,
		spans:        map[heimdall.SpanId]*heimdall.Span{},
	}
}

func (h *test_heimdall) BorConfig() *borcfg.BorConfig {
	return h.borConfig
}

func (h *test_heimdall) FetchStatus(ctx context.Context) (*heimdall.Status, error) {
	return nil, nil
}

func (h *test_heimdall) FetchSpan(ctx context.Context, spanID uint64) (*heimdall.Span, error) {

	if span, ok := h.spans[heimdall.SpanId(spanID)]; ok {
		h.currentSpan = span
		return span, nil
	}

	var nextSpan = heimdall.Span{
		Id:           heimdall.SpanId(spanID),
		ValidatorSet: *h.validatorSet,
		ChainID:      h.chainConfig.ChainID.String(),
	}

	if h.currentSpan == nil || spanID == 0 {
		nextSpan.StartBlock = 1 //256
	} else {
		if spanID != uint64(h.currentSpan.Id+1) {
			return nil, errors.New("Can't initialize span: non consecutive span")
		}

		nextSpan.StartBlock = h.currentSpan.EndBlock + 1
	}

	nextSpan.EndBlock = nextSpan.StartBlock + (100 * h.borConfig.CalculateSprintLength(nextSpan.StartBlock)) - 1

	// TODO we should use a subset here - see: https://wiki.polygon.technology/docs/pos/bor/

	nextSpan.SelectedProducers = make([]heimdall.Validator, len(h.validatorSet.Validators))

	for i, v := range h.validatorSet.Validators {
		nextSpan.SelectedProducers[i] = *v
	}

	h.currentSpan = &nextSpan

	h.spans[h.currentSpan.Id] = h.currentSpan

	return h.currentSpan, nil
}

func (h *test_heimdall) FetchSpans(ctx context.Context, page uint64, limit uint64) ([]*heimdall.Span, error) {
	return nil, errors.New("TODO")
}

func (h test_heimdall) currentSprintLength() int {
	if h.currentSpan != nil {
		return int(h.borConfig.CalculateSprintLength(h.currentSpan.StartBlock))
	}

	return int(h.borConfig.CalculateSprintLength(256))
}

func (h test_heimdall) FetchCheckpoint(ctx context.Context, number int64) (*heimdall.Checkpoint, error) {
	return nil, errors.New("TODO")
}

func (h test_heimdall) FetchCheckpointCount(ctx context.Context) (int64, error) {
	return 0, errors.New("TODO")
}

func (h *test_heimdall) FetchCheckpoints(ctx context.Context, page uint64, limit uint64) ([]*heimdall.Checkpoint, error) {
	return nil, errors.New("TODO")
}

func (h test_heimdall) FetchMilestone(ctx context.Context, number int64) (*heimdall.Milestone, error) {
	return nil, errors.New("TODO")
}

func (h test_heimdall) FetchMilestoneCount(ctx context.Context) (int64, error) {
	return 0, errors.New("TODO")
}

func (h test_heimdall) FetchFirstMilestoneNum(ctx context.Context) (int64, error) {
	return 0, errors.New("TODO")
}

func (h test_heimdall) FetchNoAckMilestone(ctx context.Context, milestoneID string) error {
	return errors.New("TODO")
}

func (h test_heimdall) FetchLastNoAckMilestone(ctx context.Context) (string, error) {
	return "", errors.New("TODO")
}

func (h test_heimdall) FetchMilestoneID(ctx context.Context, milestoneID string) error {
	return errors.New("TODO")
}
func (h test_heimdall) FetchLatestSpan(ctx context.Context) (*heimdall.Span, error) {
	return nil, errors.New("TODO")
}

func (h test_heimdall) Close() {}

type headerReader struct {
	validator validator
}

func (r headerReader) Config() *chain.Config {
	return r.validator.ChainConfig
}

func (r headerReader) FrozenBlocks() uint64              { return 0 }
func (r headerReader) FrozenBorBlocks(align bool) uint64 { return 0 }

func (r headerReader) CurrentHeader() *types.Header {
	return nil
}

func (r headerReader) CurrentFinalizedHeader() *types.Header {
	return nil
}
func (r headerReader) CurrentSafeHeader() *types.Header {
	return nil
}

func (r headerReader) GetHeader(_ common.Hash, blockNo uint64) *types.Header {
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

func (r headerReader) GetHeaderByHash(common.Hash) *types.Header {
	return nil
}

func (r headerReader) GetTd(common.Hash, uint64) *big.Int {
	return nil
}

type spanner struct {
	*bor.ChainSpanner
	validatorAddress common.Address
	currentSpan      heimdall.Span
}

func (c spanner) GetCurrentSpan(_ consensus.SystemCall) (*heimdall.Span, error) {
	return &c.currentSpan, nil
}

func (c *spanner) CommitSpan(heimdallSpan heimdall.Span, syscall consensus.SystemCall) error {
	c.currentSpan = heimdallSpan
	return nil
}

func (c *spanner) GetCurrentValidators(spanId uint64, chain bor.ChainHeaderReader) ([]*heimdall.Validator, error) {
	return []*heimdall.Validator{
		{
			ID:               1,
			Address:          c.validatorAddress,
			VotingPower:      1000,
			ProposerPriority: 1,
		}}, nil
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

func (v validator) sealBlocks(blocks []*types.Block, receipts []types.Receipts) ([]*types.Block, error) {
	sealedBlocks := make([]*types.Block, 0, len(blocks))

	hr := headerReader{v}

	for i, block := range blocks {
		header := block.HeaderNoCopy()

		if err := v.Engine.Prepare(hr, header, nil); err != nil {
			return nil, err
		}

		if parent := hr.GetHeaderByNumber(header.Number.Uint64() - 1); parent != nil {
			header.ParentHash = parent.Hash()
		}

		sealResults := make(chan *types.BlockWithReceipts, 1)

		blockWithReceipts := &types.BlockWithReceipts{Block: block, Receipts: receipts[i]}
		if err := v.Engine.Seal(hr, blockWithReceipts, sealResults, nil); err != nil {
			return nil, err
		}

		sealedBlock := <-sealResults
		v.blocks[sealedBlock.Block.NumberU64()] = sealedBlock.Block
		sealedBlocks = append(sealedBlocks, sealedBlock.Block)
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

func newValidator(t *testing.T, testHeimdall *test_heimdall, blocks map[uint64]*types.Block) validator {
	logger := log.Root()
	ctrl := gomock.NewController(t)
	stateReceiver := bor.NewMockStateReceiver(ctrl)
	stateReceiver.EXPECT().CommitState(gomock.Any(), gomock.Any()).AnyTimes()
	validatorKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	validatorAddress := crypto.PubkeyToAddress(validatorKey.PublicKey)
	bor := bor.New(
		testHeimdall.chainConfig,
		nil, /* blockReader */
		&spanner{
			ChainSpanner:     bor.NewChainSpanner(borabi.ValidatorSetContractABI(), testHeimdall.chainConfig, false, logger),
			validatorAddress: validatorAddress,
		},
		stateReceiver,
		logger,
		nil,
		nil,
	)

	/*fmt.Printf("Private: 0x%s\nPublic: 0x%s\nAddress: %s\n",
	hex.EncodeToString(crypto.FromECDSA(validatorKey)),
	hex.EncodeToString(crypto.MarshalPubkey(&validatorKey.PublicKey)),
	strings.ToLower(validatorAddress.Hex()))*/

	if testHeimdall.validatorSet == nil {
		testHeimdall.validatorSet = heimdall.NewValidatorSet([]*heimdall.Validator{
			{
				ID:               1,
				Address:          validatorAddress,
				VotingPower:      1000,
				ProposerPriority: 1,
			},
		})
	} else {
		testHeimdall.validatorSet.UpdateWithChangeSet([]*heimdall.Validator{
			{
				ID:               uint64(len(testHeimdall.validatorSet.Validators) + 1),
				Address:          validatorAddress,
				VotingPower:      1000,
				ProposerPriority: 1,
			},
		})
	}

	bor.Authorize(validatorAddress, func(_ common.Address, mimeType string, message []byte) ([]byte, error) {
		return crypto.Sign(crypto.Keccak256(message), validatorKey)
	})

	return validator{
		mock.MockWithEverything(t, &types.Genesis{Config: testHeimdall.chainConfig}, validatorKey, prune.DefaultMode, bor, 1024, false, false),
		testHeimdall,
		blocks,
	}
}

func TestValidatorCreate(t *testing.T) {
	t.Skip("issue #15017")
	newValidator(t, newTestHeimdall(polychain.BorDevnet.Config), map[uint64]*types.Block{})
}

func TestVerifyHeader(t *testing.T) {
	t.Skip("issue #15017")
	v := newValidator(t, newTestHeimdall(polychain.BorDevnet.Config), map[uint64]*types.Block{})

	chain, err := v.generateChain(1)

	if err != nil {
		t.Fatalf("generate blocks failed: %v", err)
	}

	sealedBlocks, err := v.sealBlocks(chain.Blocks, chain.Receipts)

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
	//testVerify(t, 10, 4, int(polychain.BorDevnetChainConfig.Bor.CalculateSprintLength(256)))
}

func TestVerifySpan(t *testing.T) {
	//testVerify(t, 10, 4 /*100**/ *int(polychain.BorDevnetChainConfig.Bor.CalculateSprintLength(256)))
}

func testVerify(t *testing.T, noValidators int, chainLength int) {
	log.Root().SetHandler(log.StderrHandler)

	heimdall := newTestHeimdall(polychain.BorDevnet.Config)
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
			receipts := chains[vi].Receipts[bi]

			isProposer, err := v.IsProposer(block)

			if err != nil {
				t.Fatal(err)
			}

			if isProposer {

				if vi != lastProposerIndex {
					sprintLen := heimdall.BorConfig().CalculateSprintLength(block.NumberU64())
					if block.NumberU64() > 1 && block.NumberU64()%sprintLen != 0 {
						t.Fatalf("Unexpected sprint boundary at %d for: %d", bi, block.NumberU64())
					}
					lastProposerIndex = vi
				}

				sealedBlocks, err := v.sealBlocks([]*types.Block{block}, []types.Receipts{receipts})

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
	t.Skip("issue #15017")
	heimdall := newTestHeimdall(polychain.BorDevnet.Config)
	blocks := map[uint64]*types.Block{}

	s := newValidator(t, heimdall, blocks)
	r := newValidator(t, heimdall, blocks)

	chain, err := s.generateChain(1)

	if err != nil {
		t.Fatalf("generate blocks failed: %v", err)
	}

	sealedBlocks, err := s.sealBlocks(chain.Blocks, chain.Receipts)

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
	for _, err = range r.Send(&sentryproto.InboundMessage{Id: sentryproto.MessageId_NEW_BLOCK_66, Data: b, PeerId: s.PeerId}) {
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
