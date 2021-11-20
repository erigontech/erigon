package manager

import (
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/tx_analysis/minievm"
)

type report struct {
	header *types.Header
	result []minievm.Analysis
}

func newReport() *report {
	return &report{
		result: make([]minievm.Analysis, 0),
	}
}

func (r *report) reset(size int, header *types.Header) {
	r.result = make([]minievm.Analysis, size)
	r.header = header
}

func (r *report) write() {
}

func (r *report) add(tx_idx int, analysis minievm.Analysis) {
	r.result[tx_idx] = analysis
}
