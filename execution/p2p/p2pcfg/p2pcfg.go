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

package p2pcfg

import "time"

type BlockDownload struct {
	MaxParallelBodyDownloads     int
	MaxParallelHeaderDownloads   int
	InitialHeaderFetchTimeout    time.Duration
	InitialHeaderFetchRetries    uint64
	HeaderChainBatchFetchTimeout time.Duration
	HeaderChainBatchFetchRetries uint64
	BodiesBatchFetchTimeout      time.Duration
	BodiesBatchFetchRetries      uint64
	BALsBatchFetchTimeout        time.Duration
	BALsRequestTimeout           time.Duration
}

var DefaultBlockDownload = BlockDownload{
	MaxParallelBodyDownloads:     10,
	MaxParallelHeaderDownloads:   10,
	InitialHeaderFetchTimeout:    time.Second, // note we want initial header fetch to be quick, so we terminate quickly
	InitialHeaderFetchRetries:    0,           // in case of chainLengthLimit+chainLengthCurrentHead early termination
	HeaderChainBatchFetchTimeout: 10 * time.Second,
	HeaderChainBatchFetchRetries: 1,
	BodiesBatchFetchTimeout:      time.Minute,
	BodiesBatchFetchRetries:      1,
	BALsBatchFetchTimeout:        time.Minute,
	BALsRequestTimeout:           10 * time.Second,
}
