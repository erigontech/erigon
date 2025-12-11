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

package diaglib_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/diagnostics/diaglib"
)

func TestInitSyncStages(t *testing.T) {
	d, err := NewTestDiagnosticClient()
	require.NoError(t, err)

	stages := diaglib.InitStagesFromList(nodeStages)
	d.SetStagesList(stages)
	require.Equal(t, d.GetSyncStages(), stagesListMock)

	subStages := diaglib.InitSubStagesFromList(snapshotsSubStages)
	require.Equal(t, subStages, subStagesListMock)
	d.SetSubStagesList("Snapshots", subStages)

	require.Equal(t, d.GetSyncStages(), stagesListWithSnapshotsSubStagesMock)
}

func TestSetCurrentSyncStage(t *testing.T) {
	d, err := NewTestDiagnosticClient()
	require.NoError(t, err)

	stages := diaglib.InitStagesFromList(nodeStages)
	d.SetStagesList(stages)
	subStages := diaglib.InitSubStagesFromList(snapshotsSubStages)
	d.SetSubStagesList("Snapshots", subStages)

	err = d.SetCurrentSyncStage(diaglib.CurrentSyncStage{Stage: "Snapshots"})
	require.NoError(t, err)
	require.Equal(t, diaglib.Running, d.GetSyncStages()[0].State)

	err = d.SetCurrentSyncStage(diaglib.CurrentSyncStage{Stage: "BlockHashes"})
	require.NoError(t, err)
	require.Equal(t, diaglib.Completed, d.GetSyncStages()[0].State)
	require.Equal(t, diaglib.Running, d.GetSyncStages()[1].State)

	err = d.SetCurrentSyncStage(diaglib.CurrentSyncStage{Stage: "Snapshots"})
	require.NoError(t, err)
	require.Equal(t, diaglib.Completed, d.GetSyncStages()[0].State)
	require.Equal(t, diaglib.Running, d.GetSyncStages()[1].State)
	require.Equal(t, diaglib.Queued, d.GetSyncStages()[2].State)

	//test not existed stage
	err = d.SetCurrentSyncStage(diaglib.CurrentSyncStage{Stage: "NotExistedStage"})
	require.Error(t, err)

}

func TestSetCurrentSyncSubStage(t *testing.T) {
	d, err := NewTestDiagnosticClient()
	require.NoError(t, err)

	stages := diaglib.InitStagesFromList(nodeStages)
	d.SetStagesList(stages)
	subStages := diaglib.InitSubStagesFromList(snapshotsSubStages)
	d.SetSubStagesList("Snapshots", subStages)

	err = d.SetCurrentSyncStage(diaglib.CurrentSyncStage{Stage: "Snapshots"})
	require.NoError(t, err)
	d.SetCurrentSyncSubStage(diaglib.CurrentSyncSubStage{SubStage: "Download header-chain"})
	require.Equal(t, diaglib.Running, d.GetSyncStages()[0].SubStages[0].State)

	d.SetCurrentSyncSubStage(diaglib.CurrentSyncSubStage{SubStage: "Download snapshots"})
	require.Equal(t, diaglib.Completed, d.GetSyncStages()[0].SubStages[0].State)
	require.Equal(t, diaglib.Running, d.GetSyncStages()[0].SubStages[1].State)

	d.SetCurrentSyncSubStage(diaglib.CurrentSyncSubStage{SubStage: "Download header-chain"})
	require.Equal(t, diaglib.Completed, d.GetSyncStages()[0].SubStages[0].State)
	require.Equal(t, diaglib.Running, d.GetSyncStages()[0].SubStages[1].State)
	require.Equal(t, diaglib.Queued, d.GetSyncStages()[0].SubStages[2].State)
}

func TestGetStageState(t *testing.T) {
	d, err := NewTestDiagnosticClient()
	require.NoError(t, err)

	stages := diaglib.InitStagesFromList(nodeStages)
	d.SetStagesList(stages)

	// Test get stage state
	for _, stageId := range nodeStages {
		state, err := d.GetStageState(stageId)
		require.NoError(t, err)
		require.Equal(t, diaglib.Queued, state)
	}

	//Test get not existed stage state
	_, err = d.GetStageState("NotExistedStage")
	require.Error(t, err)

	//Test Snapshots Running state
	err = d.SetCurrentSyncStage(diaglib.CurrentSyncStage{Stage: "Snapshots"})
	require.NoError(t, err)
	state, err := d.GetStageState("Snapshots")
	require.NoError(t, err)
	require.Equal(t, diaglib.Running, state)

	//Test Snapshots Completed and BlockHashes running state
	err = d.SetCurrentSyncStage(diaglib.CurrentSyncStage{Stage: "BlockHashes"})
	require.NoError(t, err)
	state, err = d.GetStageState("Snapshots")
	require.NoError(t, err)
	require.Equal(t, diaglib.Completed, state)
	state, err = d.GetStageState("BlockHashes")
	require.NoError(t, err)
	require.Equal(t, diaglib.Running, state)
}

func TestGetStageIndexes(t *testing.T) {
	d, err := NewTestDiagnosticClient()
	require.NoError(t, err)

	stages := diaglib.InitStagesFromList(nodeStages)
	d.SetStagesList(stages)
	subStages := diaglib.InitSubStagesFromList(snapshotsSubStages)
	d.SetSubStagesList("Snapshots", subStages)

	err = d.SetCurrentSyncStage(diaglib.CurrentSyncStage{Stage: "Snapshots"})
	require.NoError(t, err)
	d.SetCurrentSyncSubStage(diaglib.CurrentSyncSubStage{SubStage: "Download header-chain"})

	idxs := d.GetCurrentSyncIdxs()
	require.Equal(t, diaglib.CurrentSyncStagesIdxs{Stage: 0, SubStage: 0}, idxs)
}

func TestStagesState(t *testing.T) {
	//Test StageState to string
	require.Equal(t, "Queued", diaglib.StageState(0).String())
	require.Equal(t, "Running", diaglib.StageState(1).String())
	require.Equal(t, "Completed", diaglib.StageState(2).String())
}

var (
	nodeStages         = []string{"Snapshots", "BlockHashes", "Senders"}
	snapshotsSubStages = []string{"Download header-chain", "Download snapshots", "Indexing", "Fill DB"}

	stagesListMock = []diaglib.SyncStage{
		{ID: "Snapshots", State: diaglib.Queued, SubStages: []diaglib.SyncSubStage{}},
		{ID: "BlockHashes", State: diaglib.Queued, SubStages: []diaglib.SyncSubStage{}},
		{ID: "Senders", State: diaglib.Queued, SubStages: []diaglib.SyncSubStage{}},
	}

	subStagesListMock = []diaglib.SyncSubStage{
		{
			ID:    "Download header-chain",
			State: diaglib.Queued,
		},
		{
			ID:    "Download snapshots",
			State: diaglib.Queued,
		},
		{
			ID:    "Indexing",
			State: diaglib.Queued,
		},
		{
			ID:    "Fill DB",
			State: diaglib.Queued,
		},
	}

	stagesListWithSnapshotsSubStagesMock = []diaglib.SyncStage{
		{ID: "Snapshots", State: diaglib.Queued, SubStages: []diaglib.SyncSubStage{
			{ID: "Download header-chain", State: diaglib.Queued},
			{ID: "Download snapshots", State: diaglib.Queued},
			{ID: "Indexing", State: diaglib.Queued},
			{ID: "Fill DB", State: diaglib.Queued},
		}},
		{ID: "BlockHashes", State: diaglib.Queued, SubStages: []diaglib.SyncSubStage{}},
		{ID: "Senders", State: diaglib.Queued, SubStages: []diaglib.SyncSubStage{}},
	}
)
