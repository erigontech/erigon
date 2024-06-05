package diagnostics_test

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/diagnostics"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/stretchr/testify/require"
)

func TestStageAdd(t *testing.T) {
	d, err := NewTestDiagnosticClient()
	require.NoError(t, err)

	//Test Set Stages List
	d.SetStagesList(stagesListMock)
	require.Equal(t, d.SyncStatistics().SyncStages, stagesListMock)

	//Test adding array of SubStages for Stage
	d.AddOrUpdateSugStages(subStagesListMock)
	require.Equal(t, d.SyncStatistics().SyncStages, updatedStagesMock)

	//Test Set Current Sync Stage Update
	d.SetCurrentSyncStage(diagnostics.CurrentSyncStage{Stage: "Snapshots"})
	require.Equal(t, d.SyncStatistics().SyncStages[0].State, diagnostics.Running)

	//Test Set Current Sync Stage Update Previous stages to completed state
	d.SetCurrentSyncStage(diagnostics.CurrentSyncStage{Stage: "BlockHashes"})
	require.Equal(t, d.SyncStatistics().SyncStages[0].State, diagnostics.Completed)
	require.Equal(t, d.SyncStatistics().SyncStages[1].State, diagnostics.Running)
	require.Equal(t, d.SyncStatistics().SyncStages[2].State, diagnostics.Queued)
	d.SetCurrentSyncStage(diagnostics.CurrentSyncStage{Stage: "SnapSendersshots"})
	require.Equal(t, d.SyncStatistics().SyncStages[0].State, diagnostics.Completed)
	require.Equal(t, d.SyncStatistics().SyncStages[1].State, diagnostics.Completed)
	require.Equal(t, d.SyncStatistics().SyncStages[2].State, diagnostics.Running)

	//Test Set Current Sync Stage To StageWhich was completed
	d.SetCurrentSyncStage(diagnostics.CurrentSyncStage{Stage: "Snapshots"})
	require.Equal(t, d.SyncStatistics().SyncStages[0].State, diagnostics.Running)
	require.Equal(t, d.SyncStatistics().SyncStages[1].State, diagnostics.Queued)
	require.Equal(t, d.SyncStatistics().SyncStages[2].State, diagnostics.Queued)

	//Test Update SubStage with different state
	d.AddOrUpdateSugStages(diagnostics.UpdateSyncSubStageList{
		List: []diagnostics.UpdateSyncSubStage{
			{
				StageId: string(stages.Snapshots),
				SubStage: diagnostics.SyncSubStage{
					ID:    "Download header-chain",
					State: diagnostics.Running,
				},
			},
		},
	})
	require.Equal(t, d.SyncStatistics().SyncStages[0].SubStages[0].State, diagnostics.Running)

	//Test set current SubStage
	d.SetCurrentSyncStage(diagnostics.CurrentSyncStage{Stage: "Snapshots"})
	d.SetCurrentSyncSubStage(diagnostics.CurrentSyncSubStage{SubStage: "Download header-chain"})
	require.Equal(t, d.SyncStatistics().SyncStages[0].SubStages[0].State, diagnostics.Running)
	require.Equal(t, d.SyncStatistics().SyncStages[0].SubStages[1].State, diagnostics.Queued)
	d.SetCurrentSyncSubStage(diagnostics.CurrentSyncSubStage{SubStage: "Download snapshots"})
	require.Equal(t, d.SyncStatistics().SyncStages[0].SubStages[0].State, diagnostics.Completed)
	require.Equal(t, d.SyncStatistics().SyncStages[0].SubStages[1].State, diagnostics.Running)

	//Test Set Current Sync SubStage To Stage Which was completed
	d.SetCurrentSyncSubStage(diagnostics.CurrentSyncSubStage{SubStage: "Download header-chain"})
	require.Equal(t, d.SyncStatistics().SyncStages[0].SubStages[0].State, diagnostics.Running)
	require.Equal(t, d.SyncStatistics().SyncStages[0].SubStages[1].State, diagnostics.Queued)

	//Test make all subStages completed after stage completed
	d.SetCurrentSyncStage(diagnostics.CurrentSyncStage{Stage: "BlockHashes"})
	require.Equal(t, d.SyncStatistics().SyncStages[0].SubStages[0].State, diagnostics.Completed)
	require.Equal(t, d.SyncStatistics().SyncStages[0].SubStages[1].State, diagnostics.Completed)
}

var (
	stagesListMock = []diagnostics.SyncStage{
		{ID: "Snapshots", State: diagnostics.Queued, SubStages: []diagnostics.SyncSubStage{}},
		{ID: "BlockHashes", State: diagnostics.Queued, SubStages: []diagnostics.SyncSubStage{}},
		{ID: "SnapSendersshots", State: diagnostics.Queued, SubStages: []diagnostics.SyncSubStage{}},
	}

	updatedStagesMock = []diagnostics.SyncStage{
		{ID: "Snapshots", State: diagnostics.Queued, SubStages: []diagnostics.SyncSubStage{
			{ID: "Download header-chain", State: diagnostics.Queued},
			{ID: "Download snapshots", State: diagnostics.Queued},
		}},
		{ID: "BlockHashes", State: diagnostics.Queued, SubStages: []diagnostics.SyncSubStage{}},
		{ID: "SnapSendersshots", State: diagnostics.Queued, SubStages: []diagnostics.SyncSubStage{}},
	}

	subStagesListMock = diagnostics.UpdateSyncSubStageList{
		List: []diagnostics.UpdateSyncSubStage{
			{
				StageId: "Snapshots",
				SubStage: diagnostics.SyncSubStage{
					ID:    "Download header-chain",
					State: diagnostics.Queued,
				},
			},
			{
				StageId: "Snapshots",
				SubStage: diagnostics.SyncSubStage{
					ID:    "Download snapshots",
					State: diagnostics.Queued,
				},
			},
		},
	}
)
