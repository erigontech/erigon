package diagnostics_test

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/diagnostics"
	"github.com/stretchr/testify/require"
)

func TestInitSyncStages(t *testing.T) {
	d, err := NewTestDiagnosticClient()
	require.NoError(t, err)

	stages := diagnostics.InitStagesFromList(nodeStages)
	d.SetStagesList(stages)
	require.Equal(t, d.GetSyncStages(), stagesListMock)

	subStages := diagnostics.InitSubStagesFromList(snapshotsSubStages)
	require.Equal(t, subStages, subStagesListMock)
	d.SetSubStagesList("Snapshots", subStages)

	require.Equal(t, d.GetSyncStages(), stagesListWithSnapshotsSubStagesMock)
}

func TestSetCurrentSyncStage(t *testing.T) {
	d, err := NewTestDiagnosticClient()
	require.NoError(t, err)

	stages := diagnostics.InitStagesFromList(nodeStages)
	d.SetStagesList(stages)
	subStages := diagnostics.InitSubStagesFromList(snapshotsSubStages)
	d.SetSubStagesList("Snapshots", subStages)

	d.SetCurrentSyncStage(diagnostics.CurrentSyncStage{Stage: "Snapshots"})
	require.Equal(t, d.GetSyncStages()[0].State, diagnostics.Running)

	d.SetCurrentSyncStage(diagnostics.CurrentSyncStage{Stage: "BlockHashes"})
	require.Equal(t, d.GetSyncStages()[0].State, diagnostics.Completed)
	require.Equal(t, d.GetSyncStages()[1].State, diagnostics.Running)

	d.SetCurrentSyncStage(diagnostics.CurrentSyncStage{Stage: "Snapshots"})
	require.Equal(t, d.GetSyncStages()[0].State, diagnostics.Running)
	require.Equal(t, d.GetSyncStages()[1].State, diagnostics.Queued)
	require.Equal(t, d.GetSyncStages()[2].State, diagnostics.Queued)
}

func TestSetCurrentSyncSubStage(t *testing.T) {
	d, err := NewTestDiagnosticClient()
	require.NoError(t, err)

	stages := diagnostics.InitStagesFromList(nodeStages)
	d.SetStagesList(stages)
	subStages := diagnostics.InitSubStagesFromList(snapshotsSubStages)
	d.SetSubStagesList("Snapshots", subStages)

	d.SetCurrentSyncStage(diagnostics.CurrentSyncStage{Stage: "Snapshots"})
	d.SetCurrentSyncSubStage(diagnostics.CurrentSyncSubStage{SubStage: "Download header-chain"})
	require.Equal(t, d.GetSyncStages()[0].SubStages[0].State, diagnostics.Running)

	d.SetCurrentSyncSubStage(diagnostics.CurrentSyncSubStage{SubStage: "Download snapshots"})
	require.Equal(t, d.GetSyncStages()[0].SubStages[0].State, diagnostics.Completed)
	require.Equal(t, d.GetSyncStages()[0].SubStages[1].State, diagnostics.Running)

	d.SetCurrentSyncSubStage(diagnostics.CurrentSyncSubStage{SubStage: "Download header-chain"})
	require.Equal(t, d.GetSyncStages()[0].SubStages[0].State, diagnostics.Completed)
	require.Equal(t, d.GetSyncStages()[0].SubStages[1].State, diagnostics.Running)
	require.Equal(t, d.GetSyncStages()[0].SubStages[2].State, diagnostics.Queued)
}

var (
	nodeStages         = []string{"Snapshots", "BlockHashes", "Senders"}
	snapshotsSubStages = []string{"Download header-chain", "Download snapshots", "Indexing", "Fill DB"}

	stagesListMock = []diagnostics.SyncStage{
		{ID: "Snapshots", State: diagnostics.Queued, SubStages: []diagnostics.SyncSubStage{}},
		{ID: "BlockHashes", State: diagnostics.Queued, SubStages: []diagnostics.SyncSubStage{}},
		{ID: "Senders", State: diagnostics.Queued, SubStages: []diagnostics.SyncSubStage{}},
	}

	subStagesListMock = []diagnostics.SyncSubStage{
		{
			ID:    "Download header-chain",
			State: diagnostics.Queued,
		},
		{
			ID:    "Download snapshots",
			State: diagnostics.Queued,
		},
		{
			ID:    "Indexing",
			State: diagnostics.Queued,
		},
		{
			ID:    "Fill DB",
			State: diagnostics.Queued,
		},
	}

	stagesListWithSnapshotsSubStagesMock = []diagnostics.SyncStage{
		{ID: "Snapshots", State: diagnostics.Queued, SubStages: []diagnostics.SyncSubStage{
			{ID: "Download header-chain", State: diagnostics.Queued},
			{ID: "Download snapshots", State: diagnostics.Queued},
			{ID: "Indexing", State: diagnostics.Queued},
			{ID: "Fill DB", State: diagnostics.Queued},
		}},
		{ID: "BlockHashes", State: diagnostics.Queued, SubStages: []diagnostics.SyncSubStage{}},
		{ID: "Senders", State: diagnostics.Queued, SubStages: []diagnostics.SyncSubStage{}},
	}
)
