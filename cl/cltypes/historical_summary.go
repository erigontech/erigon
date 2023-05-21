package cltypes

import (
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/types/ssz"

	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
)

type HistoricalSummary struct {
	BlockSummaryRoot libcommon.Hash
	StateSummaryRoot libcommon.Hash
}

func (h *HistoricalSummary) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.Encode(h.BlockSummaryRoot[:], h.StateSummaryRoot[:])
}

func (h *HistoricalSummary) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < h.EncodingSizeSSZ() {
		return fmt.Errorf("[HistoricalSummary] err: %s", ssz.ErrLowBufferSize)
	}
	copy(h.BlockSummaryRoot[:], buf)
	copy(h.StateSummaryRoot[:], buf[length.Hash:])
	return nil
}

func (h *HistoricalSummary) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(h.BlockSummaryRoot[:], h.StateSummaryRoot[:])
}

func (*HistoricalSummary) EncodingSizeSSZ() int {
	return length.Hash * 2
}
