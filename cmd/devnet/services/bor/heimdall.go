package bor

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/consensus/bor/clerk"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/checkpoint"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/span"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdallgrpc"
	"github.com/ledgerwatch/erigon/consensus/bor/valset"
	"github.com/ledgerwatch/log/v3"
)

type Heimdall struct {
	currentSpan  *span.HeimdallSpan
	chainConfig  *chain.Config
	validatorSet *valset.ValidatorSet
	spans        map[uint64]*span.HeimdallSpan
	logger       log.Logger
	cancelFunc   context.CancelFunc
}

func NewHeimdall(chainConfig *chain.Config, logger log.Logger) *Heimdall {
	return &Heimdall{nil, chainConfig, nil, map[uint64]*span.HeimdallSpan{}, logger, nil}
}

func (h Heimdall) StateSyncEvents(ctx context.Context, fromID uint64, to int64) ([]*clerk.EventRecordWithTime, error) {
	return nil, nil
}

func (h *Heimdall) Span(ctx context.Context, spanID uint64) (*span.HeimdallSpan, error) {

	if span, ok := h.spans[spanID]; ok {
		h.currentSpan = span
		return span, nil
	}

	var nextSpan = span.Span{
		ID: spanID,
	}

	if h.currentSpan == nil || spanID == 0 {
		nextSpan.StartBlock = 1 //256
	} else {
		if spanID != h.currentSpan.ID+1 {
			return nil, fmt.Errorf("Can't initialize span: non consecutive span")
		}

		nextSpan.StartBlock = h.currentSpan.EndBlock + 1
	}

	nextSpan.EndBlock = nextSpan.StartBlock + (100 * h.chainConfig.Bor.CalculateSprint(nextSpan.StartBlock)) - 1

	// TODO we should use a subset here - see: https://wiki.polygon.technology/docs/pos/bor/

	selectedProducers := make([]valset.Validator, len(h.validatorSet.Validators))

	for i, v := range h.validatorSet.Validators {
		selectedProducers[i] = *v
	}

	h.currentSpan = &span.HeimdallSpan{
		Span:              nextSpan,
		ValidatorSet:      *h.validatorSet,
		SelectedProducers: selectedProducers,
		ChainID:           h.chainConfig.ChainID.String(),
	}

	h.spans[h.currentSpan.ID] = h.currentSpan

	return h.currentSpan, nil
}

func (h Heimdall) currentSprintLength() int {
	if h.currentSpan != nil {
		return int(h.chainConfig.Bor.CalculateSprint(h.currentSpan.StartBlock))
	}

	return int(h.chainConfig.Bor.CalculateSprint(256))
}

func (h *Heimdall) FetchCheckpoint(ctx context.Context, number int64) (*checkpoint.Checkpoint, error) {
	return nil, fmt.Errorf("TODO")
}

func (h *Heimdall) FetchCheckpointCount(ctx context.Context) (int64, error) {
	return 0, fmt.Errorf("TODO")
}

func (h *Heimdall) Close() {
}

func (h *Heimdall) NodeStarted(node devnet.Node) {
	if node.IsMiner() && node.Account() != nil {
		// TODO configurable voting power
		h.addValidator(node.Account().Address, 1000, 0)
	}
}

func (h *Heimdall) addValidator(validatorAddress libcommon.Address, votingPower int64, proposerPriority int64) {

	if h.validatorSet == nil {
		h.validatorSet = valset.NewValidatorSet([]*valset.Validator{
			{
				ID:               1,
				Address:          validatorAddress,
				VotingPower:      votingPower,
				ProposerPriority: proposerPriority,
			},
		}, h.logger)
	} else {
		h.validatorSet.UpdateWithChangeSet([]*valset.Validator{
			{
				ID:               uint64(len(h.validatorSet.Validators) + 1),
				Address:          validatorAddress,
				VotingPower:      votingPower,
				ProposerPriority: proposerPriority,
			},
		}, h.logger)
	}
}

func (h *Heimdall) Start(ctx context.Context) error {
	if h.cancelFunc != nil {
		return nil
	}

	addr := "localhost:8540"

	if cli := devnet.CliContext(ctx); cli != nil {
		if grpcAddr := cli.String("bor.heimdallgRPC"); len(grpcAddr) > 0 {
			addr = grpcAddr
		}
	}

	ctx, h.cancelFunc = context.WithCancel(ctx)
	return heimdallgrpc.StartHeimdallServer(ctx, h, addr, h.logger)
}

func (h *Heimdall) Stop() {
	if h.cancelFunc != nil {
		cancel := h.cancelFunc
		h.cancelFunc = nil
		cancel()
	}
}
