package handler

import (
	"context"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
)

func summaryCount(t *testing.T, s metrics.Summary) uint64 {
	t.Helper()
	m := &dto.Metric{}
	require.NoError(t, s.Write(m))
	return m.GetSummary().GetSampleCount()
}

func summarySum(t *testing.T, s metrics.Summary) float64 {
	t.Helper()
	m := &dto.Metric{}
	require.NoError(t, s.Write(m))
	return m.GetSummary().GetSampleSum()
}

func collectedSummary(window string) metrics.Summary {
	return metrics.GetOrCreateSummary(`caplin_block_production_payload_collected_seconds{window="` + window + `"}`)
}

type slotStartClock struct {
	eth_clock.EthereumClock
	slot  uint64
	start time.Time
}

func (c slotStartClock) GetSlotTime(slot uint64) time.Time {
	if slot == c.slot {
		return c.start
	}
	return c.EthereumClock.GetSlotTime(slot)
}

type capellaProduction struct {
	h         *ApiHandler
	engine    *execution_client.MockExecutionEngine
	baseBlock *cltypes.BeaconBlock
	baseRoot  common.Hash
	postState *state.CachingBeaconState
	payload   *cltypes.Eth1Block
}

func newCapellaProduction(t *testing.T) *capellaProduction {
	t.Helper()
	_, blocks, _, _, postState, h, _, _, _, _ := setupTestingHandler(t, clparams.CapellaVersion, log.Root(), true)
	engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]byte{1}, nil)
	h.engine = engine
	baseBlock := blocks[len(blocks)-1].Block
	baseRoot, err := baseBlock.HashSSZ()
	require.NoError(t, err)
	return &capellaProduction{
		h:         h,
		engine:    engine,
		baseBlock: baseBlock,
		baseRoot:  baseRoot,
		postState: postState,
		payload:   cltypes.NewEth1BlockFromExecutionHeader(postState.LatestExecutionPayloadHeader(), clparams.CapellaVersion, h.beaconChainCfg),
	}
}

func (p *capellaProduction) targetSlot() uint64 {
	return p.baseBlock.Slot + 1
}

func (p *capellaProduction) poll(payload *cltypes.Eth1Block, err error) *execution_client.MockExecutionEngineGetAssembledBlockCall {
	return p.engine.EXPECT().GetAssembledBlock(gomock.Any(), []byte{1}, clparams.CapellaVersion).Return(payload, nil, nil, nil, err)
}

func (p *capellaProduction) prepare() {
	p.h.preparedPayload.set(p.targetSlot(), []byte{1}, p.baseRoot, time.Now().Add(-time.Second))
}

func (p *capellaProduction) prepareWithPollingOpenFor(left time.Duration) {
	p.prepare()
	due := attestationDue(p.h.beaconChainCfg, clparams.CapellaVersion)
	start := time.Now().Add(-preparedGrabOffset(due)).Add(left)
	p.h.ethClock = slotStartClock{EthereumClock: p.h.ethClock, slot: p.targetSlot(), start: start}
}

func (p *capellaProduction) produce(ctx context.Context) error {
	_, _, err := p.h.produceBeaconBody(ctx, p.baseBlock.Slot, p.baseRoot, p.postState, p.targetSlot(), common.Bytes96{0xc0}, common.Hash{})
	return err
}

func requireCollectedOnce(t *testing.T, prepared bool, window, otherWindow string) {
	t.Helper()
	collected, other := collectedSummary(window), collectedSummary(otherWindow)
	count, sum, otherCount := summaryCount(t, collected), summarySum(t, collected), summaryCount(t, other)
	p := newCapellaProduction(t)
	p.poll(p.payload, nil)
	if prepared {
		p.prepare()
	}
	slotStart := p.h.ethClock.GetSlotTime(p.targetSlot())
	sinceSlotBefore := time.Since(slotStart).Seconds()
	require.NoError(t, p.produce(t.Context()))
	sinceSlotAfter := time.Since(slotStart).Seconds()
	require.Equal(t, count+1, summaryCount(t, collected))
	require.Equal(t, otherCount, summaryCount(t, other))
	observed := summarySum(t, collected) - sum
	// The test slot started years ago, so the float64 sum keeps no sub-microsecond precision.
	const tolerance = 1e-3
	require.GreaterOrEqual(t, observed, sinceSlotBefore-tolerance, "the collection time is the seconds since the slot start")
	require.LessOrEqual(t, observed, sinceSlotAfter+tolerance, "the collection time is the seconds since the slot start")
}

func TestProduceBeaconBodyRecordsWhenTheUnpreparedPayloadWasCollected(t *testing.T) {
	requireCollectedOnce(t, false, "unprepared", "prepared")
}

func TestProduceBeaconBodyRecordsWhenThePreparedPayloadWasCollected(t *testing.T) {
	requireCollectedOnce(t, true, "prepared", "unprepared")
}

func TestProduceBeaconBodyCountsABusyPollBeforeTheCollectedPayload(t *testing.T) {
	busy := payloadPollFailures.WithLabelValues("busy")
	before, collected := busy.GetValue(), summaryCount(t, collectedSummary("prepared"))
	p := newCapellaProduction(t)
	p.prepareWithPollingOpenFor(time.Second)
	gomock.InOrder(p.poll(nil, chainreader.ErrExecutionBusy), p.poll(p.payload, nil))
	require.NoError(t, p.produce(t.Context()))
	require.Equal(t, before+1, busy.GetValue())
	require.Equal(t, collected+1, summaryCount(t, collectedSummary("prepared")))
}

func TestProduceBeaconBodyFailedProductionRecordsNoCollectionTime(t *testing.T) {
	collected := summaryCount(t, collectedSummary("unprepared"))
	p := newCapellaProduction(t)
	p.poll(nil, chainreader.ErrExecutionBusy).AnyTimes()
	require.Error(t, p.produce(t.Context()))
	require.Equal(t, collected, summaryCount(t, collectedSummary("unprepared")))
}

func TestProduceBeaconBodyForABidRequestRecordsNoProductionMetrics(t *testing.T) {
	busy := payloadPollFailures.WithLabelValues("busy")
	before, collected := busy.GetValue(), summaryCount(t, collectedSummary("prepared"))
	p := newCapellaProduction(t)
	p.prepareWithPollingOpenFor(time.Second)
	gomock.InOrder(p.poll(nil, chainreader.ErrExecutionBusy), p.poll(p.payload, nil))
	ctx := context.WithValue(t.Context(), gloasBlockProductionOptionsKey{}, &gloasBlockProductionOptions{bidRequest: true})
	require.NoError(t, p.produce(ctx))
	require.Equal(t, before, busy.GetValue())
	require.Equal(t, collected, summaryCount(t, collectedSummary("prepared")))
}

func TestPayloadPollFailureReason(t *testing.T) {
	require.Equal(t, "busy", payloadPollFailureReason(chainreader.ErrExecutionBusy))
	require.Equal(t, "other", payloadPollFailureReason(execution_client.ErrInvalidGetPayloadResponse))
}
