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
	"testing"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/types"
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
	fcuStatus execmodule.ExecutionStatus
	fcuCalls  int
}

func (*domainAheadExecStub) ValidateChain(context.Context, common.Hash, uint64) (execmodule.ValidationResult, error) {
	return execmodule.ValidationResult{ValidationStatus: execmodule.ExecutionStatusTooFarAway}, nil
}

func (s *domainAheadExecStub) UpdateForkChoice(context.Context, common.Hash, common.Hash, common.Hash) (execmodule.ForkChoiceResult, error) {
	s.fcuCalls++
	return execmodule.ForkChoiceResult{Status: s.fcuStatus, ValidationError: "domain ahead of blocks"}, nil
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
