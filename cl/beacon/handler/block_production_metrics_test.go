package handler

import (
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
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

func produceCapellaBody(t *testing.T, prepared bool, getErr error) (sinceSlotBefore, sinceSlotAfter float64, err error) {
	t.Helper()
	_, blocks, _, _, postState, h, _, _, _, _ := setupTestingHandler(t, clparams.CapellaVersion, log.Root(), true)
	payload := cltypes.NewEth1BlockFromExecutionHeader(postState.LatestExecutionPayloadHeader(), clparams.CapellaVersion, h.beaconChainCfg)
	if getErr != nil {
		payload = nil
	}
	engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]byte{1}, nil)
	engine.EXPECT().GetAssembledBlock(gomock.Any(), []byte{1}, clparams.CapellaVersion).Return(payload, nil, nil, nil, getErr).AnyTimes()
	h.engine = engine
	baseBlock := blocks[len(blocks)-1].Block
	baseBlockRoot, err := baseBlock.HashSSZ()
	require.NoError(t, err)
	if prepared {
		h.preparedPayload.set(baseBlock.Slot+1, []byte{1}, baseBlockRoot, time.Now().Add(-time.Second))
	}
	slotStart := h.ethClock.GetSlotTime(baseBlock.Slot + 1)
	sinceSlotBefore = time.Since(slotStart).Seconds()
	_, _, err = h.produceBeaconBody(t.Context(), baseBlock.Slot, baseBlockRoot, postState, baseBlock.Slot+1, common.Bytes96{0xc0}, common.Hash{})
	return sinceSlotBefore, time.Since(slotStart).Seconds(), err
}

func requireCollectedOnce(t *testing.T, prepared bool) {
	t.Helper()
	collected, other := payloadCollectedSeconds[prepared], payloadCollectedSeconds[!prepared]
	count, sum, otherCount := summaryCount(t, collected), summarySum(t, collected), summaryCount(t, other)
	sinceSlotBefore, sinceSlotAfter, err := produceCapellaBody(t, prepared, nil)
	require.NoError(t, err)
	require.Equal(t, count+1, summaryCount(t, collected))
	require.Equal(t, otherCount, summaryCount(t, other))
	observed := summarySum(t, collected) - sum
	// The test slot started years ago, so the float64 sum keeps no sub-microsecond precision.
	const tolerance = 1e-3
	require.GreaterOrEqual(t, observed, sinceSlotBefore-tolerance, "the collection time is the seconds since the slot start")
	require.LessOrEqual(t, observed, sinceSlotAfter+tolerance, "the collection time is the seconds since the slot start")
}

func TestProduceBeaconBodyRecordsWhenTheUnpreparedPayloadWasCollected(t *testing.T) {
	requireCollectedOnce(t, false)
}

func TestProduceBeaconBodyRecordsWhenThePreparedPayloadWasCollected(t *testing.T) {
	requireCollectedOnce(t, true)
}

func TestProduceBeaconBodyCountsAFailedPayloadPollByReason(t *testing.T) {
	busy := payloadPollFailures.WithLabelValues("busy")
	before, collected := busy.GetValue(), summaryCount(t, payloadCollectedSeconds[false])
	_, _, err := produceCapellaBody(t, false, chainreader.ErrExecutionBusy)
	require.Error(t, err)
	require.Greater(t, busy.GetValue(), before)
	require.Equal(t, collected, summaryCount(t, payloadCollectedSeconds[false]), "a failed production records no collection time")
}

func TestPayloadPollFailureReason(t *testing.T) {
	require.Equal(t, "busy", payloadPollFailureReason(chainreader.ErrExecutionBusy))
	require.Equal(t, "other", payloadPollFailureReason(execution_client.ErrInvalidGetPayloadResponse))
}
