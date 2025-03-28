// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package diagnostics_test

import (
	"testing"

	"github.com/erigontech/erigon-lib/diagnostics"
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

	err = d.SetCurrentSyncStage(diagnostics.CurrentSyncStage{Stage: "Snapshots"})
	require.NoError(t, err)
	require.Equal(t, diagnostics.Running, d.GetSyncStages()[0].State)

	err = d.SetCurrentSyncStage(diagnostics.CurrentSyncStage{Stage: "BlockHashes"})
	require.NoError(t, err)
	require.Equal(t, diagnostics.Completed, d.GetSyncStages()[0].State)
	require.Equal(t, diagnostics.Running, d.GetSyncStages()[1].State)

	err = d.SetCurrentSyncStage(diagnostics.CurrentSyncStage{Stage: "Snapshots"})
	require.NoError(t, err)
	require.Equal(t, diagnostics.Completed, d.GetSyncStages()[0].State)
	require.Equal(t, diagnostics.Running, d.GetSyncStages()[1].State)
	require.Equal(t, diagnostics.Queued, d.GetSyncStages()[2].State)

	//test not existed stage
	err = d.SetCurrentSyncStage(diagnostics.CurrentSyncStage{Stage: "NotExistedStage"})
	require.Error(t, err)

}

func TestSetCurrentSyncSubStage(t *testing.T) {
	d, err := NewTestDiagnosticClient()
	require.NoError(t, err)

	stages := diagnostics.InitStagesFromList(nodeStages)
	d.SetStagesList(stages)
	subStages := diagnostics.InitSubStagesFromList(snapshotsSubStages)
	d.SetSubStagesList("Snapshots", subStages)

	err = d.SetCurrentSyncStage(diagnostics.CurrentSyncStage{Stage: "Snapshots"})
	require.NoError(t, err)
	d.SetCurrentSyncSubStage(diagnostics.CurrentSyncSubStage{SubStage: "Download header-chain"})
	require.Equal(t, diagnostics.Running, d.GetSyncStages()[0].SubStages[0].State)

	d.SetCurrentSyncSubStage(diagnostics.CurrentSyncSubStage{SubStage: "Download snapshots"})
	require.Equal(t, diagnostics.Completed, d.GetSyncStages()[0].SubStages[0].State)
	require.Equal(t, diagnostics.Running, d.GetSyncStages()[0].SubStages[1].State)

	d.SetCurrentSyncSubStage(diagnostics.CurrentSyncSubStage{SubStage: "Download header-chain"})
	require.Equal(t, diagnostics.Completed, d.GetSyncStages()[0].SubStages[0].State)
	require.Equal(t, diagnostics.Running, d.GetSyncStages()[0].SubStages[1].State)
	require.Equal(t, diagnostics.Queued, d.GetSyncStages()[0].SubStages[2].State)
}

func TestGetStageState(t *testing.T) {
	d, err := NewTestDiagnosticClient()
	require.NoError(t, err)

	stages := diagnostics.InitStagesFromList(nodeStages)
	d.SetStagesList(stages)

	// Test get stage state
	for _, stageId := range nodeStages {
		state, err := d.GetStageState(stageId)
		require.NoError(t, err)
		require.Equal(t, diagnostics.Queued, state)
	}

	//Test get not existed stage state
	_, err = d.GetStageState("NotExistedStage")
	require.Error(t, err)

	//Test Snapshots Running state
	err = d.SetCurrentSyncStage(diagnostics.CurrentSyncStage{Stage: "Snapshots"})
	require.NoError(t, err)
	state, err := d.GetStageState("Snapshots")
	require.NoError(t, err)
	require.Equal(t, diagnostics.Running, state)

	//Test Snapshots Completed and BlockHashes running state
	err = d.SetCurrentSyncStage(diagnostics.CurrentSyncStage{Stage: "BlockHashes"})
	require.NoError(t, err)
	state, err = d.GetStageState("Snapshots")
	require.NoError(t, err)
	require.Equal(t, diagnostics.Completed, state)
	state, err = d.GetStageState("BlockHashes")
	require.NoError(t, err)
	require.Equal(t, diagnostics.Running, state)
}

func TestGetStageIndexes(t *testing.T) {
	d, err := NewTestDiagnosticClient()
	require.NoError(t, err)

	stages := diagnostics.InitStagesFromList(nodeStages)
	d.SetStagesList(stages)
	subStages := diagnostics.InitSubStagesFromList(snapshotsSubStages)
	d.SetSubStagesList("Snapshots", subStages)

	err = d.SetCurrentSyncStage(diagnostics.CurrentSyncStage{Stage: "Snapshots"})
	require.NoError(t, err)
	d.SetCurrentSyncSubStage(diagnostics.CurrentSyncSubStage{SubStage: "Download header-chain"})

	idxs := d.GetCurrentSyncIdxs()
	require.Equal(t, diagnostics.CurrentSyncStagesIdxs{Stage: 0, SubStage: 0}, idxs)
}

func TestStagesState(t *testing.T) {
	//Test StageState to string
	require.Equal(t, "Queued", diagnostics.StageState(0).String())
	require.Equal(t, "Running", diagnostics.StageState(1).String())
	require.Equal(t, "Completed", diagnostics.StageState(2).String())
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
