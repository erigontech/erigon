package cltypes

import (
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/types/ssz"

	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
)

type HistoricalSummary struct {
	BlockSummaryRoot libcommon.Hash
	StateSummaryRoot libcommon.Hash
}

func (h *HistoricalSummary) EncodeSSZ(buf []byte) ([]byte, error) {
	return append(buf, append(h.BlockSummaryRoot[:], h.StateSummaryRoot[:]...)...), nil
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
	return merkle_tree.ArraysRoot([][32]byte{h.BlockSummaryRoot, h.StateSummaryRoot}, 2)
}

func (*HistoricalSummary) EncodingSizeSSZ() int {
	return length.Hash * 2
}
