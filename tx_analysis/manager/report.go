package manager

import (
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/tx_analysis/minievm"
)

type Report struct {
	header *types.Header
	result []minievm.Analysis
}

func newReport() *Report {
	return &Report{
		result: make([]minievm.Analysis, 0),
	}
}

func (r *Report) reset(size int, header *types.Header) {
	r.result = make([]minievm.Analysis, size)
	r.header = header
}

func (r *Report) Write() {
}

func (r *Report) add(tx_idx int, analysis minievm.Analysis) {
	r.result[tx_idx] = analysis
}
