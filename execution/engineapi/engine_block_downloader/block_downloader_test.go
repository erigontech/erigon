// Copyright 2026 The Erigon Authors
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

package engine_block_downloader

import (
	"context"
	"fmt"
	"testing"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
)

func newTestBadHeadersDownloader(t *testing.T) *EngineBlockDownloader {
	t.Helper()
	cache, err := lru.New[common.Hash, BadHeaderEntry](16)
	require.NoError(t, err)
	return &EngineBlockDownloader{badHeaders: cache}
}

func TestBadHeader_RoundTripWithValidationErr(t *testing.T) {
	e := newTestBadHeadersDownloader(t)

	badHash := common.HexToHash("0xdead")
	lastValid := common.HexToHash("0xbeef")
	const errStr = "max initcode size exceeded"

	bad, _, _ := e.IsBadHeader(badHash)
	require.False(t, bad)

	e.ReportBadHeader(badHash, lastValid, errStr)

	bad, gotLastValid, gotErr := e.IsBadHeader(badHash)
	require.True(t, bad)
	require.Equal(t, lastValid, gotLastValid)
	require.Equal(t, errStr, gotErr)
}

func TestBadHeader_EmptyValidationErrPreserved(t *testing.T) {
	e := newTestBadHeadersDownloader(t)

	badHash := common.HexToHash("0xdead")
	lastValid := common.HexToHash("0xbeef")

	e.ReportBadHeader(badHash, lastValid, "")

	bad, gotLastValid, gotErr := e.IsBadHeader(badHash)
	require.True(t, bad)
	require.Equal(t, lastValid, gotLastValid)
	require.Empty(t, gotErr)
}

func TestBadHeader_LaterReportOverridesValidationErr(t *testing.T) {
	e := newTestBadHeadersDownloader(t)

	badHash := common.HexToHash("0xdead")
	lastValid := common.HexToHash("0xbeef")

	e.ReportBadHeader(badHash, lastValid, "first reason")
	e.ReportBadHeader(badHash, lastValid, "second reason")

	_, _, gotErr := e.IsBadHeader(badHash)
	require.Equal(t, "second reason", gotErr)
}

func TestNewPayloadGapExceedsLimit(t *testing.T) {
	tests := []struct {
		name        string
		currentHead uint64
		missingNum  uint64
		limit       uint64
		want        bool
	}{
		{"behind by more than limit", 100, 500, 96, true},
		{"ahead by more than limit", 500, 100, 96, true},
		{"behind by exact limit", 100, 196, 96, false},
		{"behind by one over limit", 100, 197, 96, true},
		{"ahead by exact limit", 196, 100, 96, false},
		{"ahead by one over limit", 197, 100, 96, true},
		{"same block", 100, 100, 96, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, newPayloadGapExceedsLimit(tt.currentHead, tt.missingNum, tt.limit))
		})
	}
}

// domainAheadExecStub answers like an execution module whose state files run
// ahead of its TxNums index.
type domainAheadExecStub struct {
	execmodule.ExecutionModule
	fcuStatus      execmodule.ExecutionStatus
	fcuLatestValid common.Hash
	fcuCalls       int
}

func (*domainAheadExecStub) ValidateChain(context.Context, *types.Block) (execmodule.ValidationResult, error) {
	return execmodule.ValidationResult{ValidationStatus: execmodule.ExecutionStatusTooFarAway}, nil
}

func (s *domainAheadExecStub) UpdateForkChoice(context.Context, common.Hash, common.Hash, common.Hash) (execmodule.ForkChoiceResult, error) {
	s.fcuCalls++
	return execmodule.ForkChoiceResult{Status: s.fcuStatus, ValidationError: "domain ahead of blocks", LatestValidHash: s.fcuLatestValid}, nil
}

// A domain-ahead fork choice has already stored the batch's canonical markers
// and TxNums, so the download must go on instead of dropping the run.
func TestExecDownloadedBatch_DomainAheadOfBlocksKeepsDownloading(t *testing.T) {
	e := newTestBadHeadersDownloader(t)
	e.logger = testlog.Logger(t, log.LvlCrit)
	exec := &domainAheadExecStub{fcuStatus: execmodule.ExecutionStatusTooFarAway}
	e.chainRW = chainreader.NewChainReaderEth1(nil, exec, 0)
	block := types.NewBlockWithHeader(&types.Header{Number: *uint256.NewInt(5000)}, nil)
	require.NoError(t, e.execDownloadedBatch(t.Context(), block, block.Hash()))
	require.Equal(t, 1, exec.fcuCalls)
}

func TestExecDownloadedBatch_RejectedForkChoiceFails(t *testing.T) {
	for _, status := range []execmodule.ExecutionStatus{execmodule.ExecutionStatusBusy, execmodule.ExecutionStatusBadBlock} {
		t.Run(status.String(), func(t *testing.T) {
			e := newTestBadHeadersDownloader(t)
			e.logger = testlog.Logger(t, log.LvlCrit)
			exec := &domainAheadExecStub{fcuStatus: status}
			e.chainRW = chainreader.NewChainReaderEth1(nil, exec, 0)
			block := types.NewBlockWithHeader(&types.Header{Number: *uint256.NewInt(5000)}, nil)
			require.Error(t, e.execDownloadedBatch(t.Context(), block, block.Hash()))
			require.Equal(t, 1, exec.fcuCalls)
		})
	}
}

// The fork choice names the pre-batch head as latest valid, which the batch
// blocks below the bad one are not known to fail, so the ancestor stays unknown.
func TestExecDownloadedBatch_BadBlockForkChoiceMarksBatchBad(t *testing.T) {
	e := newTestBadHeadersDownloader(t)
	e.logger = testlog.Logger(t, log.LvlCrit)
	exec := &domainAheadExecStub{fcuStatus: execmodule.ExecutionStatusBadBlock, fcuLatestValid: common.Hash{0x01}}
	e.chainRW = chainreader.NewChainReaderEth1(nil, exec, 0)
	block := types.NewBlockWithHeader(&types.Header{Number: *uint256.NewInt(5000)}, nil)
	requested := common.Hash{0x02}
	require.Error(t, e.execDownloadedBatch(t.Context(), block, requested))
	for _, h := range []common.Hash{block.Hash(), requested} {
		bad, lastValid, validationErr := e.IsBadHeader(h)
		require.True(t, bad, h)
		require.Equal(t, common.Hash{}, lastValid)
		require.Equal(t, "domain ahead of blocks", validationErr)
	}
}

type sizeCheckingExecutionModule struct {
	execmodule.ExecutionModule
	config    *chain.Config
	genesis   *types.Header
	insertErr error
}

func (s *sizeCheckingExecutionModule) InsertBlocks(_ context.Context, blocks []*types.Block) (execmodule.ExecutionStatus, error) {
	if s.insertErr != nil {
		return 0, s.insertErr
	}
	for _, block := range blocks {
		rawBlock := types.RawBlock{Header: block.HeaderNoCopy(), Body: block.RawBody()}
		if err := rawBlock.ValidateMaxRlpSize(s.config); err != nil {
			return 0, fmt.Errorf("insert blocks: %w", err)
		}
	}
	return execmodule.ExecutionStatusSuccess, nil
}

func (s *sizeCheckingExecutionModule) CurrentHeader(context.Context) (*types.Header, error) {
	return s.genesis, nil
}

type downloadBlockReader struct {
	dbservices.FullBlockReader
	genesis *types.Header
}

func (r downloadBlockReader) HeaderByHash(_ context.Context, _ kv.Getter, hash common.Hash) (*types.Header, error) {
	if hash == r.genesis.Hash() {
		return r.genesis, nil
	}
	return nil, nil
}

type downloadChainFetcher struct {
	p2p.Fetcher
	blocks []*types.Block
}

func (f downloadChainFetcher) FetchHeadersBackwards(_ context.Context, hash common.Hash, amount uint64, _ *p2p.PeerId, _ ...p2p.FetcherOption) (p2p.FetcherResponse[[]*types.Header], error) {
	for i, candidate := range f.blocks {
		if candidate.Hash() == hash {
			headers := make([]*types.Header, 0, amount)
			for _, block := range f.blocks[max(0, i-int(amount)+1) : i+1] {
				headers = append(headers, block.HeaderNoCopy())
			}
			return p2p.FetcherResponse[[]*types.Header]{Data: headers}, nil
		}
	}
	return p2p.FetcherResponse[[]*types.Header]{}, fmt.Errorf("unknown header %s", hash)
}

func (f downloadChainFetcher) FetchBodies(_ context.Context, headers []*types.Header, _ *p2p.PeerId, _ ...p2p.FetcherOption) (p2p.FetcherResponse[[]*types.Body], error) {
	bodies := make([]*types.Body, len(headers))
	for i, header := range headers {
		bodies[i] = f.blocks[header.Number.Uint64()].Body()
	}
	return p2p.FetcherResponse[[]*types.Body]{Data: bodies}, nil
}

func newSizeCheckingDownloader(t *testing.T, config *chain.Config, blocks []*types.Block, insertErr error) *EngineBlockDownloader {
	t.Helper()
	logger := testlog.Logger(t, log.LvlCrit)
	tracker := p2p.NewPeerTracker(logger, nil)
	tracker.PeerConnected(p2p.PeerIdFromUint64(1))
	bbd := p2p.NewBackwardBlockDownloader(logger, downloadChainFetcher{blocks: blocks}, tracker, t.TempDir())
	genesis := blocks[0].HeaderNoCopy()
	exec := &sizeCheckingExecutionModule{config: config, genesis: genesis, insertErr: insertErr}
	return NewEngineBlockDownloader(
		t.Context(), logger, exec, downloadBlockReader{genesis: genesis}, mdbxtest.NewTestDB(t, dbcfg.ChainDB),
		config, ethconfig.Sync{LoopBlockLimit: 500, MaxReorgDepth: 96}, bbd,
	)
}

func oversizedTransactions() []types.Transaction {
	return []types.Transaction{types.NewTransaction(0, common.Address{1}, uint256.NewInt(0), 21_000, uint256.NewInt(1), make([]byte, params.MaxRlpBlockSize))}
}

func downloadTestChain(blockTxns ...[]types.Transaction) []*types.Block {
	blocks := []*types.Block{types.NewBlockWithHeader(&types.Header{}, nil)}
	for i, txns := range blockTxns {
		number := uint64(i + 1)
		blocks = append(blocks, types.NewBlock(&types.Header{
			Number: *uint256.NewInt(number), ParentHash: blocks[i].Hash(), Time: number,
		}, txns, nil, nil, nil, nil))
	}
	return blocks
}

func TestDownloadOversizedBlockMarksBadChain(t *testing.T) {
	config := &chain.Config{OsakaTime: common.NewUint64(2)}
	largeTxns := oversizedTransactions()
	blocks := downloadTestChain(largeTxns, largeTxns, nil, nil)
	for _, trigger := range []Trigger{NewPayloadTrigger, FcuTrigger, SegmentRecoveryTrigger} {
		t.Run(trigger.String(), func(t *testing.T) {
			e := newSizeCheckingDownloader(t, config, blocks, nil)
			req := BackwardDownloadRequest{MissingHash: blocks[3].Hash(), Trigger: trigger}
			badBlocks := blocks[2:4]
			if trigger != FcuTrigger {
				req.ValidateChainTip = blocks[4]
				badBlocks = blocks[2:]
			}
			err := e.processReq(t.Context(), req)
			require.ErrorIs(t, err, types.ErrBlockExceedsMaxRlpSize)
			for _, block := range badBlocks {
				bad, lastValid, validationErr := e.IsBadHeader(block.Hash())
				require.True(t, bad, "block %d", block.NumberU64())
				require.Equal(t, common.Hash{}, lastValid)
				require.Contains(t, validationErr, types.ErrBlockExceedsMaxRlpSize.Error())
			}
			for _, block := range blocks[:2] {
				bad, _, _ := e.IsBadHeader(block.Hash())
				require.False(t, bad, "block %d", block.NumberU64())
			}
		})
	}
}

func TestDownloadOversizedChainTipMarksOnlyTipBad(t *testing.T) {
	config := &chain.Config{OsakaTime: common.NewUint64(0)}
	blocks := downloadTestChain(nil, oversizedTransactions())
	parent, tip := blocks[1], blocks[2]
	e := newSizeCheckingDownloader(t, config, blocks, nil)
	err := e.processReq(t.Context(), BackwardDownloadRequest{
		MissingHash: parent.Hash(), Trigger: NewPayloadTrigger, ValidateChainTip: tip,
	})
	require.ErrorIs(t, err, types.ErrBlockExceedsMaxRlpSize)
	bad, lastValid, validationErr := e.IsBadHeader(tip.Hash())
	require.True(t, bad)
	require.Equal(t, common.Hash{}, lastValid)
	require.Contains(t, validationErr, types.ErrBlockExceedsMaxRlpSize.Error())
	bad, _, _ = e.IsBadHeader(parent.Hash())
	require.False(t, bad)
}

func TestDownloadInsertErrorDoesNotMarkChainBad(t *testing.T) {
	config := &chain.Config{OsakaTime: common.NewUint64(0)}
	blocks := downloadTestChain(nil, nil)
	parent, tip := blocks[1], blocks[2]
	e := newSizeCheckingDownloader(t, config, blocks, context.DeadlineExceeded)
	err := e.processReq(t.Context(), BackwardDownloadRequest{
		MissingHash: parent.Hash(), Trigger: NewPayloadTrigger, ValidateChainTip: tip,
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Zero(t, e.badHeaders.Len())
}
