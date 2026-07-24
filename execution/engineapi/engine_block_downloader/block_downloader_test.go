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
	"testing"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
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

func TestClassifyNewPayloadGap(t *testing.T) {
	const limit = 96
	tests := []struct {
		name        string
		currentHead uint64
		parentNum   uint64
		want        newPayloadGapAction
	}{
		{"parent far ahead catches up", 100, 500, newPayloadForwardCatchUp},
		{"parent far behind is too deep", 500, 100, newPayloadReorgTooDeep},
		{"parent ahead by exact limit stays bounded", 100, 196, newPayloadBoundedDownload},
		{"parent ahead by one over limit catches up", 100, 197, newPayloadForwardCatchUp},
		{"parent behind by exact limit stays bounded", 196, 100, newPayloadBoundedDownload},
		{"parent behind by one over limit is too deep", 197, 100, newPayloadReorgTooDeep},
		{"parent equals head stays bounded", 100, 100, newPayloadBoundedDownload},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, classifyNewPayloadGap(tt.currentHead, tt.parentNum, limit))
		})
	}
}
