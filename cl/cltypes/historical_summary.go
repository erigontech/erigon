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

package cltypes

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

type HistoricalSummary struct {
	BlockSummaryRoot common.Hash `json:"block_summary_root"`
	StateSummaryRoot common.Hash `json:"state_summary_root"`
}

func (h *HistoricalSummary) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, h.BlockSummaryRoot[:], h.StateSummaryRoot[:])
}

func (h *HistoricalSummary) DecodeSSZ(buf []byte, _ int) error {
	return ssz2.UnmarshalSSZ(buf, 0, h.BlockSummaryRoot[:], h.StateSummaryRoot[:])
}

func (h *HistoricalSummary) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(h.BlockSummaryRoot[:], h.StateSummaryRoot[:])
}

func (*HistoricalSummary) EncodingSizeSSZ() int {
	return length.Hash * 2
}
