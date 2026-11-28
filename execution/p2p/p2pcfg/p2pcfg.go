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

import (
	"cmp"
	"time"
)

type BlockDownload struct {
	BlocksBatchSize              uint64
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
	BlocksBatchSize:              500,
	MaxParallelBodyDownloads:     10,
	MaxParallelHeaderDownloads:   10,
	InitialHeaderFetchTimeout:    time.Second, // note we want initial header fetch to be quick, so we terminate quickly
	InitialHeaderFetchRetries:    0,           // in case of p2p.WithChainLengthLimit+p2p.WithChainLengthCurrentHead early termination
	HeaderChainBatchFetchTimeout: 10 * time.Second,
	HeaderChainBatchFetchRetries: 1,
	BodiesBatchFetchTimeout:      time.Minute,
	BodiesBatchFetchRetries:      1,
	BALsBatchFetchTimeout:        time.Minute,
	BALsRequestTimeout:           10 * time.Second,
}

func (b BlockDownload) WithDefaults() BlockDownload {
	d := DefaultBlockDownload
	d.BlocksBatchSize = cmp.Or(b.BlocksBatchSize, d.BlocksBatchSize)
	d.MaxParallelBodyDownloads = cmp.Or(max(b.MaxParallelBodyDownloads, 0), d.MaxParallelBodyDownloads)
	d.MaxParallelHeaderDownloads = cmp.Or(max(b.MaxParallelHeaderDownloads, 0), d.MaxParallelHeaderDownloads)
	d.InitialHeaderFetchTimeout = cmp.Or(max(b.InitialHeaderFetchTimeout, 0), d.InitialHeaderFetchTimeout)
	d.InitialHeaderFetchRetries = cmp.Or(b.InitialHeaderFetchRetries, d.InitialHeaderFetchRetries)
	d.HeaderChainBatchFetchTimeout = cmp.Or(max(b.HeaderChainBatchFetchTimeout, 0), d.HeaderChainBatchFetchTimeout)
	d.HeaderChainBatchFetchRetries = cmp.Or(b.HeaderChainBatchFetchRetries, d.HeaderChainBatchFetchRetries)
	d.BodiesBatchFetchTimeout = cmp.Or(max(b.BodiesBatchFetchTimeout, 0), d.BodiesBatchFetchTimeout)
	d.BodiesBatchFetchRetries = cmp.Or(b.BodiesBatchFetchRetries, d.BodiesBatchFetchRetries)
	d.BALsBatchFetchTimeout = cmp.Or(max(b.BALsBatchFetchTimeout, 0), d.BALsBatchFetchTimeout)
	d.BALsRequestTimeout = cmp.Or(max(b.BALsRequestTimeout, 0), d.BALsRequestTimeout)
	return d
}
