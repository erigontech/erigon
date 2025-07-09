package engine_block_downloader

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types"
)

type BackwardDownloadRequest struct {
	MissingHash common.Hash
	Trigger     Trigger
	// ValidateChainTip is optional - if provided, it will be inserted after the missing hash backward download
	// is complete and will trigger an operation to validate the chain leading to it
	ValidateChainTip *types.Block
}

func (r BackwardDownloadRequest) LogArgs() []interface{} {
	args := []interface{}{"hash", r.MissingHash, "trigger", r.Trigger}
	if r.ValidateChainTip != nil {
		args = append(args, "chainTip", r.ValidateChainTip)
	}
	return args
}

type Trigger byte

const (
	NewPayloadTrigger Trigger = iota
	SegmentRecoveryTrigger
	FcuTrigger
)
