package engine_block_downloader

import (
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/execution/types"
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
		args = append(args, "chainTipNum", r.ValidateChainTip.Number().Uint64(), "chainTipHash", r.ValidateChainTip.Hash())
	}
	return args
}

type Trigger byte

func (s Trigger) String() string {
	switch s {
	case NewPayloadTrigger:
		return "NewPayloadTrigger"
	case SegmentRecoveryTrigger:
		return "SegmentRecoveryTrigger"
	case FcuTrigger:
		return "FcuTrigger"
	default:
		panic(fmt.Sprintf("unknown trigger: %d", s))
	}
}

const (
	NewPayloadTrigger Trigger = iota
	SegmentRecoveryTrigger
	FcuTrigger
)
