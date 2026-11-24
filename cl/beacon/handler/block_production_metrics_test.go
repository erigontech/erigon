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

func produceCapellaBody(t *testing.T, prepared bool, getErr error) error {
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
	_, _, err = h.produceBeaconBody(t.Context(), baseBlock.Slot, baseBlockRoot, postState, baseBlock.Slot+1, common.Bytes96{0xc0}, common.Hash{})
	return err
}

func TestProduceBeaconBodyRecordsWhenTheUnpreparedPayloadWasCollected(t *testing.T) {
	unprepared, prepared := summaryCount(t, payloadCollectedSeconds[false]), summaryCount(t, payloadCollectedSeconds[true])
	require.NoError(t, produceCapellaBody(t, false, nil))
	require.Equal(t, unprepared+1, summaryCount(t, payloadCollectedSeconds[false]))
	require.Equal(t, prepared, summaryCount(t, payloadCollectedSeconds[true]))
}

func TestProduceBeaconBodyRecordsWhenThePreparedPayloadWasCollected(t *testing.T) {
	unprepared, prepared := summaryCount(t, payloadCollectedSeconds[false]), summaryCount(t, payloadCollectedSeconds[true])
	require.NoError(t, produceCapellaBody(t, true, nil))
	require.Equal(t, prepared+1, summaryCount(t, payloadCollectedSeconds[true]))
	require.Equal(t, unprepared, summaryCount(t, payloadCollectedSeconds[false]))
}

func TestProduceBeaconBodyCountsAFailedPayloadPollByReason(t *testing.T) {
	busy := payloadPollFailures.WithLabelValues("busy")
	before, collected := busy.GetValue(), summaryCount(t, payloadCollectedSeconds[false])
	require.Error(t, produceCapellaBody(t, false, chainreader.ErrExecutionBusy))
	require.Greater(t, busy.GetValue(), before)
	require.Equal(t, collected, summaryCount(t, payloadCollectedSeconds[false]), "a failed production records no collection time")
}

func TestPayloadPollFailureReason(t *testing.T) {
	require.Equal(t, "busy", payloadPollFailureReason(chainreader.ErrExecutionBusy))
	require.Equal(t, "other", payloadPollFailureReason(execution_client.ErrInvalidGetPayloadResponse))
}
