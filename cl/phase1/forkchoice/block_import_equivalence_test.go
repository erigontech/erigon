// Copyright 2026 The Erigon Authors
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

package forkchoice

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/ssz"
)

var updateBlockImportGolden = flag.Bool("update-block-import-golden", false, "update block import golden snapshots")

type blockImportSnapshot struct {
	JustifiedCheckpoint           solid.Checkpoint           `json:"justified_checkpoint"`
	FinalizedCheckpoint           solid.Checkpoint           `json:"finalized_checkpoint"`
	UnrealizedJustifiedCheckpoint solid.Checkpoint           `json:"unrealized_justified_checkpoint"`
	UnrealizedFinalizedCheckpoint solid.Checkpoint           `json:"unrealized_finalized_checkpoint"`
	ProposerBoostRoot             common.Hash                `json:"proposer_boost_root"`
	HighestSeen                   uint64                     `json:"highest_seen"`
	HighestSeenRoot               common.Hash                `json:"highest_seen_root"`
	HeadSet                       []common.Hash              `json:"head_set"`
	Roots                         []blockImportRootSnapshot  `json:"roots"`
	Epochs                        []blockImportEpochSnapshot `json:"epochs"`
	CurrentStateRoot              *common.Hash               `json:"current_state_root,omitempty"`
	LatestMessages                blockImportDigest          `json:"latest_messages"`
	LatestMessageCount            int                        `json:"latest_message_count"`
	EquivocatingIndices           []uint64                   `json:"equivocating_indices"`
	Head                          common.Hash                `json:"head"`
	HeadSlot                      uint64                     `json:"head_slot"`
	VoteTallyWeights              []blockImportRootWeight    `json:"vote_tally_weights"`
	ForkChoiceWeights             []blockImportRootWeight    `json:"fork_choice_weights"`
	Events                        []blockImportEvent         `json:"events"`
}

type blockImportRootSnapshot struct {
	Root                         common.Hash                     `json:"root"`
	UnrealizedJustification      *solid.Checkpoint               `json:"unrealized_justification,omitempty"`
	UnrealizedFinalization       *solid.Checkpoint               `json:"unrealized_finalization,omitempty"`
	BlockTimeliness              []bool                          `json:"block_timeliness,omitempty"`
	Eth2Root                     *common.Hash                    `json:"eth2_root,omitempty"`
	Children                     *blockImportChildren            `json:"children,omitempty"`
	Optimistic                   bool                            `json:"optimistic"`
	RandaoVector                 *blockImportDigest              `json:"randao_vector,omitempty"`
	RandaoDelta                  *blockImportRandaoDelta         `json:"randao_delta,omitempty"`
	PreverifiedSizes             *blockImportPreverifiedSizes    `json:"preverified_sizes,omitempty"`
	FinalityCheckpoints          *blockImportFinalityCheckpoints `json:"finality_checkpoints,omitempty"`
	TotalActiveBalance           *uint64                         `json:"total_active_balance,omitempty"`
	PendingConsolidations        *blockImportDigest              `json:"pending_consolidations,omitempty"`
	PendingDeposits              *blockImportDigest              `json:"pending_deposits,omitempty"`
	PendingPartialWithdrawals    *blockImportDigest              `json:"pending_partial_withdrawals,omitempty"`
	PayloadTimelinessVotes       *blockImportPayloadVoteSummary  `json:"payload_timeliness_votes,omitempty"`
	PayloadDataAvailabilityVotes *blockImportPayloadVoteSummary  `json:"payload_data_availability_votes,omitempty"`
}

type blockImportEpochSnapshot struct {
	Epoch             uint64             `json:"epoch"`
	Participation     *blockImportDigest `json:"participation,omitempty"`
	ProposerLookahead *blockImportDigest `json:"proposer_lookahead,omitempty"`
}

type blockImportChildren struct {
	ParentSlot uint64        `json:"parent_slot"`
	Roots      []common.Hash `json:"roots"`
}

type blockImportRandaoDelta struct {
	Epoch uint64      `json:"epoch"`
	Delta common.Hash `json:"delta"`
}

type blockImportPreverifiedSizes struct {
	ValidatorLength           uint64 `json:"validator_length"`
	HistoricalRootsLength     uint64 `json:"historical_roots_length"`
	HistoricalSummariesLength uint64 `json:"historical_summaries_length"`
}

type blockImportFinalityCheckpoints struct {
	Finalized         solid.Checkpoint `json:"finalized"`
	CurrentJustified  solid.Checkpoint `json:"current_justified"`
	PreviousJustified solid.Checkpoint `json:"previous_justified"`
}

type blockImportDigest struct {
	Count  int    `json:"count"`
	Bytes  int    `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type blockImportPayloadVoteSummary struct {
	True    int    `json:"true"`
	False   int    `json:"false"`
	Unvoted int    `json:"unvoted"`
	SHA256  string `json:"sha256"`
}

type blockImportRootWeight struct {
	Root   common.Hash `json:"root"`
	Weight uint64      `json:"weight"`
}

type blockImportEvent struct {
	Type beaconevents.EventTopic `json:"type"`
	Data json.RawMessage         `json:"data"`
}

type blockImportRunRecord struct {
	Slot      uint64
	BlockRoot common.Hash
	Snapshot  blockImportSnapshot
}

type blockImportGolden struct {
	Case   string                    `json:"case"`
	Mode   blockImportClockMode      `json:"mode"`
	Blocks []blockImportGoldenRecord `json:"blocks"`
}

type blockImportGoldenRecord struct {
	Slot                uint64      `json:"slot"`
	BlockRoot           common.Hash `json:"block_root"`
	SnapshotSHA256      string      `json:"snapshot_sha256"`
	LatestMessageCount  int         `json:"latest_message_count"`
	EpochFirst          bool        `json:"epoch_first"`
	RandaoVectorPresent bool        `json:"randao_vector_present"`
	ProposerBoostRoot   common.Hash `json:"proposer_boost_root"`
	Head                common.Hash `json:"head"`
	HeadSlot            uint64      `json:"head_slot"`
	EventCount          int         `json:"event_count"`
}

type blockImportClockMode string

const (
	blockImportCatchup blockImportClockMode = "catchup"
	blockImportTip     blockImportClockMode = "tip"
)

func captureBlockImportSnapshot(store *ForkChoiceStore, roots []common.Hash, epochs []uint64, currentRoot common.Hash, events []blockImportEvent) (blockImportSnapshot, error) {
	head, headSlot, err := store.GetHead(nil)
	if err != nil {
		return blockImportSnapshot{}, err
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	snapshot := blockImportSnapshot{
		JustifiedCheckpoint:           store.justifiedCheckpoint.Load().(solid.Checkpoint),
		FinalizedCheckpoint:           store.finalizedCheckpoint.Load().(solid.Checkpoint),
		UnrealizedJustifiedCheckpoint: store.unrealizedJustifiedCheckpoint.Load().(solid.Checkpoint),
		UnrealizedFinalizedCheckpoint: store.unrealizedFinalizedCheckpoint.Load().(solid.Checkpoint),
		ProposerBoostRoot:             store.proposerBoostRoot.Load().(common.Hash),
		HighestSeen:                   store.highestSeen.Load(),
		HighestSeenRoot:               store.HighestSeenRoot(),
		Head:                          head,
		HeadSlot:                      headSlot,
		Events:                        slices.Clone(events),
	}
	for root := range store.headSet {
		snapshot.HeadSet = append(snapshot.HeadSet, root)
	}
	sortBlockImportRoots(snapshot.HeadSet)

	trackedRoots := slices.Clone(roots)
	sortBlockImportRoots(trackedRoots)
	for _, root := range trackedRoots {
		rootSnapshot, err := captureBlockImportRootSnapshot(store, root)
		if err != nil {
			return blockImportSnapshot{}, err
		}
		snapshot.Roots = append(snapshot.Roots, rootSnapshot)
	}

	trackedEpochs := slices.Clone(epochs)
	slices.Sort(trackedEpochs)
	for _, epoch := range trackedEpochs {
		epochSnapshot := blockImportEpochSnapshot{Epoch: epoch}
		if participation, ok := store.participation.Peek(epoch); ok {
			epochSnapshot.Participation = digestBlockImportBytes(participation.Length(), participation.Bytes())
		}
		if lookahead, ok := store.proposerLookahead.Peek(epoch); ok {
			epochSnapshot.ProposerLookahead = digestBlockImportBytes(lookahead.Length(), lookahead.Bytes())
		}
		snapshot.Epochs = append(snapshot.Epochs, epochSnapshot)
	}

	if currentState := store.forkGraph.CurrentStateLocked(currentRoot); currentState != nil {
		root, err := currentState.HashSSZ()
		if err != nil {
			return blockImportSnapshot{}, err
		}
		stateRoot := common.Hash(root)
		snapshot.CurrentStateRoot = &stateRoot
	}

	store.latestMessages.mu.RLock()
	latestMessages := slices.Clone(store.latestMessages.latestMessages)
	store.latestMessages.mu.RUnlock()
	latestMessagesJSON, err := json.Marshal(latestMessages)
	if err != nil {
		return blockImportSnapshot{}, err
	}
	snapshot.LatestMessages = *digestBlockImportBytes(len(latestMessages), latestMessagesJSON)
	snapshot.LatestMessageCount = countLatestMessages(store)
	for byteIndex, value := range store.equivocatingIndicies {
		for bit := range uint64(8) {
			if value&(1<<bit) != 0 {
				snapshot.EquivocatingIndices = append(snapshot.EquivocatingIndices, uint64(byteIndex)*8+bit)
			}
		}
	}
	snapshot.VoteTallyWeights = blockImportWeights(store.voteTally.weightsByRoot())
	weights := store.weights
	if store.trackGloasWeights() {
		weights = store.gloasWeightTree.snapshotWeights()
	}
	snapshot.ForkChoiceWeights = blockImportWeights(weights)
	return snapshot, nil
}

func captureBlockImportRootSnapshot(store *ForkChoiceStore, root common.Hash) (blockImportRootSnapshot, error) {
	snapshot := blockImportRootSnapshot{Root: root, Optimistic: store.optimisticStore.IsOptimistic(root)}
	if value, ok := store.unrealizedJustifications.Load(root); ok {
		checkpoint := value.(solid.Checkpoint)
		snapshot.UnrealizedJustification = &checkpoint
	}
	if value, ok := store.unrealizedFinalizations.Load(root); ok {
		checkpoint := value.(solid.Checkpoint)
		snapshot.UnrealizedFinalization = &checkpoint
	}
	if value, ok := store.blockTimeliness.Load(root); ok {
		timeliness := value.([clparams.NumBlockTimelinessDeadlines]bool)
		snapshot.BlockTimeliness = append([]bool(nil), timeliness[:]...)
	}
	if value, ok := store.eth2Roots.Peek(root); ok {
		snapshot.Eth2Root = &value
	}
	if value, ok := store.childrens.Load(root); ok {
		children := value.(childrens)
		childRoots := slices.Clone(children.childrenHashes)
		sortBlockImportRoots(childRoots)
		snapshot.Children = &blockImportChildren{ParentSlot: children.parentSlot, Roots: childRoots}
	}
	if value, ok := store.randaoMixesLists.Peek(root); ok {
		snapshot.RandaoVector = digestBlockImportBytes(value.Length(), value.Bytes())
	}
	if value, ok := store.randaoDeltas.Peek(root); ok {
		snapshot.RandaoDelta = &blockImportRandaoDelta{Epoch: value.epoch, Delta: value.delta}
	}
	if value, ok := store.preverifiedSizes.Peek(root); ok {
		snapshot.PreverifiedSizes = &blockImportPreverifiedSizes{
			ValidatorLength:           value.validatorLength,
			HistoricalRootsLength:     value.historicalRootsLength,
			HistoricalSummariesLength: value.historicalSummariesLength,
		}
	}
	if value, ok := store.finalityCheckpoints.Peek(root); ok {
		snapshot.FinalityCheckpoints = &blockImportFinalityCheckpoints{
			Finalized:         value.finalizedCheckpoint,
			CurrentJustified:  value.currentJustifiedCheckpoint,
			PreviousJustified: value.previousJustifiedCheckpoint,
		}
	}
	if value, ok := store.totalActiveBalances.Peek(root); ok {
		snapshot.TotalActiveBalance = &value
	}
	if value, ok := store.pendingConsolidations.Peek(root); ok {
		digest, err := digestBlockImportList(value.Len(), value)
		if err != nil {
			return blockImportRootSnapshot{}, err
		}
		snapshot.PendingConsolidations = digest
	}
	if value, ok := store.pendingDeposits.Peek(root); ok {
		digest, err := digestBlockImportList(value.Len(), value)
		if err != nil {
			return blockImportRootSnapshot{}, err
		}
		snapshot.PendingDeposits = digest
	}
	if value, ok := store.partialWithdrawals.Peek(root); ok {
		digest, err := digestBlockImportList(value.Len(), value)
		if err != nil {
			return blockImportRootSnapshot{}, err
		}
		snapshot.PendingPartialWithdrawals = digest
	}
	if value, ok := store.payloadTimelinessVote.Load(root); ok {
		snapshot.PayloadTimelinessVotes = summarizeBlockImportPayloadVotes(value.([clparams.PtcSize]int8))
	}
	if value, ok := store.payloadDataAvailabilityVote.Load(root); ok {
		snapshot.PayloadDataAvailabilityVotes = summarizeBlockImportPayloadVotes(value.([clparams.PtcSize]int8))
	}
	return snapshot, nil
}

func digestBlockImportList(count int, value ssz.Marshaler) (*blockImportDigest, error) {
	encoded, err := value.EncodeSSZ(nil)
	if err != nil {
		return nil, err
	}
	return digestBlockImportBytes(count, encoded), nil
}

func digestBlockImportBytes(count int, value []byte) *blockImportDigest {
	digest := sha256.Sum256(value)
	return &blockImportDigest{Count: count, Bytes: len(value), SHA256: hex.EncodeToString(digest[:])}
}

func summarizeBlockImportPayloadVotes(votes [clparams.PtcSize]int8) *blockImportPayloadVoteSummary {
	encoded := make([]byte, len(votes))
	summary := &blockImportPayloadVoteSummary{}
	for index, vote := range &votes {
		encoded[index] = byte(vote)
		switch vote {
		case 1:
			summary.True++
		case -1:
			summary.False++
		default:
			summary.Unvoted++
		}
	}
	digest := sha256.Sum256(encoded)
	summary.SHA256 = hex.EncodeToString(digest[:])
	return summary
}

func blockImportWeights(weights map[common.Hash]uint64) []blockImportRootWeight {
	result := make([]blockImportRootWeight, 0, len(weights))
	for root, weight := range weights {
		result = append(result, blockImportRootWeight{Root: root, Weight: weight})
	}
	slices.SortFunc(result, func(a, b blockImportRootWeight) int { return bytes.Compare(a.Root[:], b.Root[:]) })
	return result
}

func sortBlockImportRoots(roots []common.Hash) {
	slices.SortFunc(roots, func(a, b common.Hash) int { return bytes.Compare(a[:], b[:]) })
}

func runBlockImportChain(t *testing.T, blocks []*cltypes.SignedBeaconBlock, anchor *state.CachingBeaconState, mode blockImportClockMode) []blockImportRunRecord {
	t.Helper()
	require.NotEmpty(t, blocks)
	store := newBlockTestStore(t, anchor)
	store.SetSynced(false)
	cfg := anchor.BeaconConfig()
	if mode == blockImportCatchup {
		lastSlot := blocks[len(blocks)-1].Block.Slot
		store.OnTick((anchor.GenesisTime() + (lastSlot+1)*cfg.SecondsPerSlot) * 1000)
	}

	events := make(chan *beaconevents.EventStream, 512)
	subscription := store.emitters.State().Subscribe(events)
	defer subscription.Unsubscribe()

	anchorRoot, err := anchor.BlockRoot()
	require.NoError(t, err)
	trackedRoots := []common.Hash{anchorRoot}
	trackedEpochs := []uint64{state.Epoch(anchor)}
	records := make([]blockImportRunRecord, 0, len(blocks))
	for _, block := range blocks {
		if mode == blockImportTip {
			store.OnTick((anchor.GenesisTime()+block.Block.Slot*cfg.SecondsPerSlot)*1000 + 100)
			drainBlockImportEvents(t, events)
		}
		require.NoError(t, store.OnBlock(t.Context(), block, false, true, false))
		blockRoot, err := block.Block.HashSSZ()
		require.NoError(t, err)
		trackedRoots = append(trackedRoots, blockRoot)
		epoch := block.Block.Slot / cfg.SlotsPerEpoch
		if !slices.Contains(trackedEpochs, epoch) {
			trackedEpochs = append(trackedEpochs, epoch)
		}
		importEvents := drainBlockImportEvents(t, events)
		snapshot, err := captureBlockImportSnapshot(store, trackedRoots, trackedEpochs, blockRoot, importEvents)
		require.NoError(t, err)
		records = append(records, blockImportRunRecord{Slot: block.Block.Slot, BlockRoot: blockRoot, Snapshot: snapshot})
	}
	return records
}

func drainBlockImportEvents(t *testing.T, events <-chan *beaconevents.EventStream) []blockImportEvent {
	t.Helper()
	result := make([]blockImportEvent, 0, len(events))
	for len(events) > 0 {
		event := <-events
		data, err := json.Marshal(event.Data)
		require.NoError(t, err)
		result = append(result, blockImportEvent{Type: event.Event, Data: data})
	}
	return result
}

func makeBlockImportGolden(t *testing.T, caseName string, mode blockImportClockMode, slotsPerEpoch uint64, records []blockImportRunRecord) blockImportGolden {
	t.Helper()
	golden := blockImportGolden{Case: caseName, Mode: mode, Blocks: make([]blockImportGoldenRecord, 0, len(records))}
	for recordIndex := range records {
		record := &records[recordIndex]
		encoded, err := json.Marshal(record.Snapshot)
		require.NoError(t, err)
		epochFirst := record.Slot%slotsPerEpoch == 0
		randaoVectorPresent := false
		for rootIndex := range record.Snapshot.Roots {
			root := &record.Snapshot.Roots[rootIndex]
			if root.Root == record.BlockRoot {
				randaoVectorPresent = root.RandaoVector != nil
				break
			}
		}
		golden.Blocks = append(golden.Blocks, blockImportGoldenRecord{
			Slot:                record.Slot,
			BlockRoot:           record.BlockRoot,
			SnapshotSHA256:      digestBlockImportBytes(1, encoded).SHA256,
			LatestMessageCount:  record.Snapshot.LatestMessageCount,
			EpochFirst:          epochFirst,
			RandaoVectorPresent: randaoVectorPresent,
			ProposerBoostRoot:   record.Snapshot.ProposerBoostRoot,
			Head:                record.Snapshot.Head,
			HeadSlot:            record.Snapshot.HeadSlot,
			EventCount:          len(record.Snapshot.Events),
		})
	}
	return golden
}

func TestBlockImportMatchesGoldenSnapshots(t *testing.T) {
	cases := []struct {
		name string
		load func() ([]*cltypes.SignedBeaconBlock, *state.CachingBeaconState, *state.CachingBeaconState)
	}{
		{name: "phase0", load: tests.GetPhase0Random},
		{name: "bellatrix", load: tests.GetBellatrixRandom},
		{name: "capella", load: tests.GetCapellaRandom},
		{name: "electra", load: tests.GetElectraRandom},
		{name: "gloas", load: func() ([]*cltypes.SignedBeaconBlock, *state.CachingBeaconState, *state.CachingBeaconState) {
			fixture := tests.GetGloasFixture("attestation", false, 0)
			return fixture.Blocks, fixture.PreState, fixture.PostState
		}},
	}
	for _, testCase := range cases {
		for _, mode := range []blockImportClockMode{blockImportCatchup, blockImportTip} {
			t.Run(testCase.name+"/"+string(mode), func(t *testing.T) {
				blocks, anchor, _ := testCase.load()
				records := runBlockImportChain(t, blocks, anchor, mode)
				actual := makeBlockImportGolden(t, testCase.name, mode, anchor.BeaconConfig().SlotsPerEpoch, records)
				path := filepath.Join("testdata", "block_import", testCase.name+"_"+string(mode)+".json")
				if *updateBlockImportGolden {
					encoded, err := json.MarshalIndent(actual, "", "  ")
					require.NoError(t, err)
					require.NoError(t, os.WriteFile(path, append(encoded, '\n'), 0o644))
				}
				expectedJSON, err := os.ReadFile(path)
				require.NoError(t, err)
				var expected blockImportGolden
				require.NoError(t, json.Unmarshal(expectedJSON, &expected))
				compareBlockImportGolden(t, expected, actual, records)
				checkBlockImportGoldenCoverage(t, mode, actual)
			})
		}
	}
}

func compareBlockImportGolden(t *testing.T, expected, actual blockImportGolden, records []blockImportRunRecord) {
	t.Helper()
	require.Equal(t, expected.Case, actual.Case)
	require.Equal(t, expected.Mode, actual.Mode)
	require.Len(t, actual.Blocks, len(expected.Blocks))
	for index := range expected.Blocks {
		if expected.Blocks[index] == actual.Blocks[index] {
			continue
		}
		snapshot, err := json.MarshalIndent(records[index].Snapshot, "", "  ")
		require.NoError(t, err)
		t.Fatalf("block %d (slot %d, root %s) snapshot mismatch\nexpected orientation: %+v\nactual orientation:   %+v\nactual snapshot:\n%s", index, actual.Blocks[index].Slot, actual.Blocks[index].BlockRoot, expected.Blocks[index], actual.Blocks[index], snapshot)
	}
}

func checkBlockImportGoldenCoverage(t *testing.T, mode blockImportClockMode, golden blockImportGolden) {
	t.Helper()
	latestMessages := 0
	epochFirst := 0
	randaoVectors := 0
	proposerBoosts := 0
	for blockIndex := range golden.Blocks {
		block := &golden.Blocks[blockIndex]
		if block.LatestMessageCount > 0 {
			latestMessages++
		}
		if block.EpochFirst {
			epochFirst++
			if block.RandaoVectorPresent {
				randaoVectors++
			}
		}
		if block.ProposerBoostRoot != (common.Hash{}) {
			proposerBoosts++
		}
	}
	require.Positive(t, latestMessages)
	require.Equal(t, epochFirst, randaoVectors)
	if mode == blockImportTip {
		require.Positive(t, proposerBoosts)
	}
	t.Logf("latest_messages=%d epoch_first=%d randao_vectors=%d proposer_boosts=%d", latestMessages, epochFirst, randaoVectors, proposerBoosts)
}
