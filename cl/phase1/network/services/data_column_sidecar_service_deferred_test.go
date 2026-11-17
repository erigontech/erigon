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

package services

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/das"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	gossipmock "github.com/erigontech/erigon/cl/phase1/network/gossip/mock_services"
	serviceinterface "github.com/erigontech/erigon/cl/phase1/network/services/service_interface"
	"github.com/erigontech/erigon/common"
)

func (t *dataColumnSidecarTestSuite) TestGloasInvalidSidecarOutcome() {
	t.activateGloas()
	t.mockSyncedData.EXPECT().Syncing().Return(false).AnyTimes()
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	service := t.dataColumnSidecarService.(*dataColumnSidecarService)

	for _, tc := range []struct {
		name    string
		prepare func() *cltypes.DataColumnSidecar
		invalid bool
	}{
		{"slot mismatch", func() *cltypes.DataColumnSidecar {
			t.importBlock(testBlockRoot, createMockGloasBlock(testSlot+1))
			return createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot)
		}, true},
		{"column over max blobs", func() *cltypes.DataColumnSidecar {
			t.importBlock(testBlockRoot, createMockGloasBlock(testSlot))
			t.beaconConfig.BlobSchedule = []clparams.BlobParameters{{Epoch: testEpoch, MaxBlobsPerBlock: 1}}
			sidecar := createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot)
			sidecar.Column.Append(&cltypes.Cell{})
			sidecar.KzgProofs.Append(&cltypes.KZGProof{})
			return sidecar
		}, true},
		{"verify_data_column_sidecar fails", func() *cltypes.DataColumnSidecar {
			t.importBlock(testBlockRoot, createMockGloasBlock(testSlot))
			verifyDataColumnSidecarWithCommitments = func(*cltypes.DataColumnSidecar, *solid.ListSSZ[*cltypes.KZGCommitment]) bool { return false }
			return createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot)
		}, true},
		{"kzg proofs fail", func() *cltypes.DataColumnSidecar {
			t.importBlock(testBlockRoot, createMockGloasBlock(testSlot))
			verifyDataColumnSidecarWithCommitments = func(*cltypes.DataColumnSidecar, *solid.ListSSZ[*cltypes.KZGCommitment]) bool { return true }
			verifyDataColumnSidecarKZGProofsWithCommitments = func(*cltypes.DataColumnSidecar, *solid.ListSSZ[*cltypes.KZGCommitment]) bool { return false }
			return createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot)
		}, true},
		{"missing bid", func() *cltypes.DataColumnSidecar {
			block := createMockGloasBlock(testSlot)
			block.Block.Body.SignedExecutionPayloadBid = nil
			t.importBlock(testBlockRoot, block)
			return createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot)
		}, false},
	} {
		t.Run(tc.name, func() {
			defer t.resetVerifiers()
			sidecar := tc.prepare()
			err := service.processGloasMessage(t.T().Context(), nil, sidecar, false)
			t.Error(err)
			t.Equal(tc.invalid, errors.Is(err, errInvalidGloasSidecar))
			t.NotErrorIs(err, ErrIgnore)
		})
	}
}

func (t *dataColumnSidecarTestSuite) resetVerifiers() {
	verifyDataColumnSidecarWithCommitments = das.VerifyDataColumnSidecarWithCommitments
	verifyDataColumnSidecarKZGProofsWithCommitments = das.VerifyDataColumnSidecarKZGProofsWithCommitments
}

var (
	otherBlockRoot = common.Hash{0x22}
	goneBlockRoot  = common.Hash{0x33}
)

type verifierCounts struct{ commitments, kzg int }

type storedColumn struct {
	root  common.Hash
	index int64
}

func peerN(i int) peer.ID { return peer.ID(fmt.Sprintf("peer-%d", i)) }

// deferredCandidate builds a sidecar whose content depends on variant. Its KZG check passes only if valid.
func deferredCandidate(index uint64, root common.Hash, variant int, valid bool) *cltypes.DataColumnSidecar {
	sidecar := createMockGloasDataColumnSidecar(testSlot, index, root)
	proof := &cltypes.KZGProof{}
	proof[0] = byte(variant)
	proof[1] = byte(variant >> 8)
	if valid {
		proof[2] = 1
	}
	sidecar.KzgProofs.Set(0, proof)
	return sidecar
}

// setupDeferred prepares a Gloas service whose sidecars on even columns share subnet 0 and on odd columns subnet 1.
func (t *dataColumnSidecarTestSuite) setupDeferred() (*dataColumnSidecarService, *verifierCounts, *[]storedColumn, *gossipmock.MockGossip) {
	t.activateGloas()
	t.mockSyncedData.EXPECT().Syncing().Return(false).AnyTimes()
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	computeSubnetForDataColumnSidecar = func(index uint64) uint64 { return index % 2 }

	counts := &verifierCounts{}
	verifyDataColumnSidecarWithCommitments = func(*cltypes.DataColumnSidecar, *solid.ListSSZ[*cltypes.KZGCommitment]) bool {
		counts.commitments++
		return true
	}
	verifyDataColumnSidecarKZGProofsWithCommitments = func(sidecar *cltypes.DataColumnSidecar, _ *solid.ListSSZ[*cltypes.KZGCommitment]) bool {
		counts.kzg++
		return sidecar.KzgProofs.Get(0)[2] == 1
	}

	stored := &[]storedColumn{}
	t.mockColumnSidecarStorage.EXPECT().WriteColumnSidecars(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, root common.Hash, index int64, _ *cltypes.DataColumnSidecar) error {
			*stored = append(*stored, storedColumn{root, index})
			return nil
		}).AnyTimes()
	t.mockPeerDas.EXPECT().TryScheduleRecover(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	service := t.dataColumnSidecarService.(*dataColumnSidecarService)
	gossip := gossipmock.NewMockGossip(t.gomockCtrl)
	service.gossipManager = gossip
	t.importBlock(testBlockRoot, createMockGloasBlock(testSlot))
	return service, counts, stored, gossip
}

func (t *dataColumnSidecarTestSuite) admit(service *dataColumnSidecarService, from peer.ID, sidecar *cltypes.DataColumnSidecar) {
	service.scheduleSidecarForLaterProcessing(serviceinterface.WithGossipPeer(t.T().Context(), from), sidecar)
}

func (t *dataColumnSidecarTestSuite) isQueued(service *dataColumnSidecarService, sidecar *cltypes.DataColumnSidecar) bool {
	root, err := sidecar.HashSSZ()
	t.Require().NoError(err)
	_, ok := service.pendingGloasSidecars.jobs.Load(common.Hash(root))
	return ok
}

func (t *dataColumnSidecarTestSuite) backdate(service *dataColumnSidecarService, sidecar *cltypes.DataColumnSidecar, age time.Duration) {
	root, err := sidecar.HashSSZ()
	t.Require().NoError(err)
	job, ok := service.pendingGloasSidecars.jobs.Load(common.Hash(root))
	t.Require().True(ok)
	job.(*pendingJob[pendingGloasSidecar]).creationTime = time.Now().Add(-age)
}

func (t *dataColumnSidecarTestSuite) quotaUsed(service *dataColumnSidecarService, from peer.ID, subnet uint64) int {
	return service.pendingQuota.used[pendingSidecarSource{peer: from, subnet: subnet}]
}

func TestPendingGloasSidecarQuotaDerivedFromConfig(t *testing.T) {
	mainnet := clparams.MainnetBeaconConfig
	minimal := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&minimal)

	require.Equal(t, 3, pendingGloasSidecarQuota(&mainnet))
	require.Equal(t, 5, pendingGloasSidecarQuota(&minimal))
}

func (t *dataColumnSidecarTestSuite) TestGloasPendingQuotaIsPerPeerAndSubnet() {
	service, _, _, _ := t.setupDeferred()
	quota := service.pendingQuota.limit
	t.Equal(3, quota)

	for i := range quota {
		t.admit(service, peerN(1), deferredCandidate(0, testBlockRoot, i, false))
	}
	overQuota := deferredCandidate(0, testBlockRoot, quota, false)
	t.admit(service, peerN(1), overQuota)
	t.False(t.isQueued(service, overQuota))
	t.Equal(int32(quota), service.pendingGloasSidecars.count.Load())

	otherPeer := deferredCandidate(0, testBlockRoot, quota+1, false)
	t.admit(service, peerN(2), otherPeer)
	t.True(t.isQueued(service, otherPeer))

	otherSubnet := deferredCandidate(1, testBlockRoot, quota+2, false)
	t.admit(service, peerN(1), otherSubnet)
	t.True(t.isQueued(service, otherSubnet))
}

func (t *dataColumnSidecarTestSuite) TestGloasPendingQuotaIgnoresDuplicates() {
	service, _, _, _ := t.setupDeferred()
	sidecar := deferredCandidate(0, testBlockRoot, 1, false)
	t.admit(service, peerN(1), sidecar)

	t.admit(service, peerN(1), sidecar)
	t.admit(service, peerN(2), sidecar)

	t.Equal(int32(1), service.pendingGloasSidecars.count.Load())
	t.Equal(1, t.quotaUsed(service, peerN(1), 0))
	t.Equal(map[pendingSidecarSource]int{{peer: peerN(1), subnet: 0}: 1}, service.pendingQuota.used)
}

func (t *dataColumnSidecarTestSuite) TestGloasPendingQuotaReleasedWhenQueueIsFull() {
	service, _, _, _ := t.setupDeferred()
	service.pendingGloasSidecars.capacity = 1
	t.admit(service, peerN(1), deferredCandidate(0, testBlockRoot, 1, false))

	t.admit(service, peerN(2), deferredCandidate(0, testBlockRoot, 2, false))

	t.Equal(map[pendingSidecarSource]int{{peer: peerN(1), subnet: 0}: 1}, service.pendingQuota.used)
}

func (t *dataColumnSidecarTestSuite) TestGloasPendingQuotaReleasedOnExpiry() {
	service, _, _, _ := t.setupDeferred()
	delete(t.mockForkChoice.Blocks, testBlockRoot)
	delete(t.mockForkChoice.Headers, testBlockRoot)
	quota := service.pendingQuota.limit
	var candidates []*cltypes.DataColumnSidecar
	for i := range quota {
		candidates = append(candidates, deferredCandidate(0, testBlockRoot, i, false))
		t.admit(service, peerN(1), candidates[i])
		t.backdate(service, candidates[i], pendingGloasSidecarExpiry+time.Second)
	}

	service.pendingGloasSidecars.processPending(t.T().Context())

	t.Zero(service.pendingGloasSidecars.count.Load())
	t.Empty(service.pendingQuota.used)
	readmitted := deferredCandidate(0, testBlockRoot, quota, false)
	t.admit(service, peerN(1), readmitted)
	t.True(t.isQueued(service, readmitted))
}

func (t *dataColumnSidecarTestSuite) TestGloasPendingQuotaReleasedAfterProcessing() {
	service, _, stored, _ := t.setupDeferred()
	delete(t.mockForkChoice.Blocks, testBlockRoot)
	delete(t.mockForkChoice.Headers, testBlockRoot)
	t.admit(service, peerN(1), deferredCandidate(0, testBlockRoot, 1, true))
	t.Equal(1, t.quotaUsed(service, peerN(1), 0))
	t.importBlock(testBlockRoot, createMockGloasBlock(testSlot))

	service.pendingGloasSidecars.processPending(t.T().Context())

	t.Len(*stored, 1)
	t.Empty(service.pendingQuota.used)
}

// blockVanishesBetweenLookups serves the block of root to the queue's lookup but not to the later validation lookups.
type blockVanishesBetweenLookups struct {
	forkchoice.ForkChoiceStorage
	root  common.Hash
	reads int
}

func (s *blockVanishesBetweenLookups) GetBlock(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	if root == s.root {
		s.reads++
		if s.reads%3 != 1 {
			return nil, false
		}
	}
	return s.ForkChoiceStorage.GetBlock(root)
}

func (t *dataColumnSidecarTestSuite) TestGloasDeferredVerificationsAreBudgetedPerPass() {
	service, counts, _, gossip := t.setupDeferred()
	gossip.EXPECT().BanPeer(gomock.Any()).AnyTimes()
	for i := range maxPendingGloasSidecars {
		t.admit(service, peerN(i), deferredCandidate(0, testBlockRoot, i, false))
	}
	t.Require().Equal(int32(maxPendingGloasSidecars), service.pendingGloasSidecars.count.Load())

	passes := maxPendingGloasSidecars / maxPendingGloasVerificationsPerPass
	for pass := 1; pass <= passes; pass++ {
		before := counts.kzg
		service.pendingGloasSidecars.processPending(t.T().Context())
		t.Equal(maxPendingGloasVerificationsPerPass, counts.kzg-before, "pass %d", pass)
	}

	t.Zero(service.pendingGloasSidecars.count.Load())
}

func (t *dataColumnSidecarTestSuite) TestGloasDeferredBudgetDoesNotStarveAnOlderValidCandidate() {
	service, _, stored, gossip := t.setupDeferred()
	gossip.EXPECT().BanPeer(gomock.Any()).AnyTimes()
	valid := deferredCandidate(0, testBlockRoot, 0, true)
	t.admit(service, peerN(0), valid)
	t.backdate(service, valid, time.Second)

	for i := 1; i < maxPendingGloasSidecars; i++ {
		t.admit(service, peerN(i), deferredCandidate(1, testBlockRoot, i, false))
	}

	service.pendingGloasSidecars.processPending(t.T().Context())

	t.Equal([]storedColumn{{testBlockRoot, 0}}, *stored)
}

func (t *dataColumnSidecarTestSuite) TestGloasDeferredBudgetReachesANewerCandidateOfAnotherBlock() {
	service, _, stored, gossip := t.setupDeferred()
	gossip.EXPECT().BanPeer(gomock.Any()).AnyTimes()
	t.importBlock(otherBlockRoot, createMockGloasBlock(testSlot))
	invalid := maxPendingGloasSidecars - 1
	for i := range invalid {
		sidecar := deferredCandidate(0, testBlockRoot, i, false)
		t.admit(service, peerN(i), sidecar)
		t.backdate(service, sidecar, time.Second)
	}
	t.admit(service, peerN(invalid), deferredCandidate(0, otherBlockRoot, invalid, true))

	for range maxPendingGloasSidecars / maxPendingGloasVerificationsPerPass {
		service.pendingGloasSidecars.processPending(t.T().Context())
	}

	t.Equal([]storedColumn{{otherBlockRoot, 0}}, *stored)
}

func (t *dataColumnSidecarTestSuite) TestGloasDeferredBudgetIsNotSpentOnCommitmentMismatch() {
	service, counts, stored, gossip := t.setupDeferred()
	gossip.EXPECT().BanPeer(gomock.Any()).AnyTimes()
	t.importBlock(otherBlockRoot, createMockGloasBlock(testSlot))
	verifyDataColumnSidecarWithCommitments = func(sidecar *cltypes.DataColumnSidecar, _ *solid.ListSSZ[*cltypes.KZGCommitment]) bool {
		return sidecar.BeaconBlockRoot != testBlockRoot
	}
	const mismatched = 2 * maxPendingGloasVerificationsPerPass
	for i := range mismatched {
		sidecar := deferredCandidate(0, testBlockRoot, i, false)
		t.admit(service, peerN(i), sidecar)
		t.backdate(service, sidecar, time.Second)
	}
	t.admit(service, peerN(mismatched), deferredCandidate(0, otherBlockRoot, 0, true))

	service.pendingGloasSidecars.processPending(t.T().Context())

	t.Equal(1, counts.kzg)
	t.Equal([]storedColumn{{otherBlockRoot, 0}}, *stored)
}

func (t *dataColumnSidecarTestSuite) TestGloasDeferredBansAPeerOncePerPass() {
	service, _, stored, gossip := t.setupDeferred()
	gossip.EXPECT().BanPeer(peerN(1)).Times(1)
	for i := range 3 {
		t.admit(service, peerN(1), deferredCandidate(0, testBlockRoot, i, false))
	}

	service.pendingGloasSidecars.processPending(t.T().Context())

	t.Empty(*stored)
	t.Zero(service.pendingGloasSidecars.count.Load())
}

func (t *dataColumnSidecarTestSuite) TestGloasDeferredBudgetIsNotSpentOnSeenOrVanishedCandidates() {
	service, counts, stored, _ := t.setupDeferred()
	t.importBlock(otherBlockRoot, createMockGloasBlock(testSlot))
	t.importBlock(goneBlockRoot, createMockGloasBlock(testSlot))
	service.forkChoice = &blockVanishesBetweenLookups{ForkChoiceStorage: t.mockForkChoice, root: goneBlockRoot}
	service.seenGloas.claim(seenGloasSidecarKey{beaconBlockRoot: testBlockRoot, index: 0})

	const spoilers = 2 * maxPendingGloasVerificationsPerPass
	for i := range spoilers {
		seen := deferredCandidate(0, testBlockRoot, i, false)
		t.admit(service, peerN(i), seen)
		t.backdate(service, seen, 2*time.Second)
		vanishing := deferredCandidate(0, goneBlockRoot, i, false)
		t.admit(service, peerN(spoilers+i), vanishing)
		t.backdate(service, vanishing, time.Second)
	}
	t.admit(service, peerN(2*spoilers), deferredCandidate(0, otherBlockRoot, 0, true))

	service.pendingGloasSidecars.processPending(t.T().Context())

	t.Equal(1, counts.kzg)
	t.Equal([]storedColumn{{otherBlockRoot, 0}}, *stored)
	t.Equal(int32(spoilers), service.pendingGloasSidecars.count.Load())
}

func (t *dataColumnSidecarTestSuite) TestGloasDeferredValidCandidateIsStoredAfterAnInvalidOneForTheSameColumn() {
	service, _, stored, gossip := t.setupDeferred()
	gossip.EXPECT().BanPeer(peerN(1)).Times(1)
	invalid := deferredCandidate(0, testBlockRoot, 1, false)
	t.admit(service, peerN(1), invalid)
	t.backdate(service, invalid, time.Second)
	t.admit(service, peerN(2), deferredCandidate(0, testBlockRoot, 2, true))

	service.pendingGloasSidecars.processPending(t.T().Context())

	t.Equal([]storedColumn{{testBlockRoot, 0}}, *stored)
	t.Zero(service.pendingGloasSidecars.count.Load())
}

// assertDeferredRejectBans queues a candidate whose block is unknown, lets prepare make the candidate invalid against
// the imported block, and expects one ban of its forwarding peer.
func (t *dataColumnSidecarTestSuite) assertDeferredRejectBans(blockSlot uint64, prepare func()) {
	service, _, stored, gossip := t.setupDeferred()
	delete(t.mockForkChoice.Blocks, testBlockRoot)
	delete(t.mockForkChoice.Headers, testBlockRoot)
	t.admit(service, peerN(1), deferredCandidate(0, testBlockRoot, 1, false))
	prepare()
	t.importBlock(testBlockRoot, createMockGloasBlock(blockSlot))
	gossip.EXPECT().BanPeer(peerN(1)).Times(1)

	service.pendingGloasSidecars.processPending(t.T().Context())

	t.Empty(*stored)
	t.Zero(service.pendingGloasSidecars.count.Load())
	t.Empty(service.pendingQuota.used)
}

func (t *dataColumnSidecarTestSuite) TestGloasDeferredSlotMismatchBansTheForwardingPeer() {
	t.assertDeferredRejectBans(testSlot+1, func() {})
}

func (t *dataColumnSidecarTestSuite) TestGloasDeferredOversizedColumnBansTheForwardingPeer() {
	t.assertDeferredRejectBans(testSlot, func() {
		t.beaconConfig.BlobSchedule = []clparams.BlobParameters{{Epoch: testEpoch, MaxBlobsPerBlock: 0}}
	})
}

func (t *dataColumnSidecarTestSuite) TestGloasDeferredInvalidSidecarBansTheForwardingPeer() {
	t.assertDeferredRejectBans(testSlot, func() {
		verifyDataColumnSidecarWithCommitments = func(*cltypes.DataColumnSidecar, *solid.ListSSZ[*cltypes.KZGCommitment]) bool { return false }
	})
}

func (t *dataColumnSidecarTestSuite) TestGloasDeferredInvalidKZGProofsBanTheForwardingPeer() {
	t.assertDeferredRejectBans(testSlot, func() {})
}

func (t *dataColumnSidecarTestSuite) TestGloasDeferredDoesNotBanForIgnoreOrLocalErrors() {
	service, _, _, _ := t.setupDeferred()
	delete(t.mockForkChoice.Blocks, testBlockRoot)
	delete(t.mockForkChoice.Headers, testBlockRoot)
	seen := deferredCandidate(0, testBlockRoot, 1, false)
	t.admit(service, peerN(1), seen)
	service.seenGloas.claim(seenGloasSidecarKey{beaconBlockRoot: testBlockRoot, index: 0})
	missingBid := deferredCandidate(1, testBlockRoot, 2, false)
	t.admit(service, peerN(2), missingBid)
	unimportable := deferredCandidate(2, otherBlockRoot, 3, false)
	t.admit(service, peerN(3), unimportable)
	block := createMockGloasBlock(testSlot)
	block.Block.Body.SignedExecutionPayloadBid = nil
	t.importBlock(testBlockRoot, block)
	t.mockForkChoice.FinalizedCheckpointVal = solid.Checkpoint{Epoch: testEpoch + 1}

	service.pendingGloasSidecars.processPending(t.T().Context())

	t.Zero(service.pendingGloasSidecars.count.Load())
}

func (t *dataColumnSidecarTestSuite) TestGloasDeferredRejectWithoutForwardingPeerDoesNotBan() {
	service, _, _, _ := t.setupDeferred()
	service.scheduleSidecarForLaterProcessing(t.T().Context(), deferredCandidate(0, testBlockRoot, 1, false))
	t.Require().Equal(int32(1), service.pendingGloasSidecars.count.Load())

	service.pendingGloasSidecars.processPending(t.T().Context())

	t.Zero(service.pendingGloasSidecars.count.Load())
}

func (t *dataColumnSidecarTestSuite) TestGloasDeferredBanKeepsTheBadPeersOtherCandidates() {
	service, _, stored, gossip := t.setupDeferred()
	gossip.EXPECT().BanPeer(peerN(1)).Times(1)
	invalid := deferredCandidate(0, testBlockRoot, 1, false)
	t.admit(service, peerN(1), invalid)
	t.backdate(service, invalid, time.Second)
	t.admit(service, peerN(1), deferredCandidate(1, testBlockRoot, 2, true))

	service.pendingGloasSidecars.processPending(t.T().Context())

	t.Equal([]storedColumn{{testBlockRoot, 1}}, *stored)
}

func (t *dataColumnSidecarTestSuite) TestGloasInvalidBlockRejectsTheSidecar() {
	t.activateGloas()
	t.mockSyncedData.EXPECT().Syncing().Return(false).AnyTimes()
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	service := t.dataColumnSidecarService.(*dataColumnSidecarService)
	t.mockForkChoice.InvalidBlocks = map[common.Hash]struct{}{testBlockRoot: {}}

	err := service.processGloasMessage(t.T().Context(), nil, createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot), false)

	t.ErrorIs(err, errInvalidGloasSidecar)
	t.NotErrorIs(err, ErrIgnore)
}

func (t *dataColumnSidecarTestSuite) TestGloasDeferredInvalidBlockBansTheForwardingPeer() {
	service, counts, stored, gossip := t.setupDeferred()
	delete(t.mockForkChoice.Blocks, testBlockRoot)
	delete(t.mockForkChoice.Headers, testBlockRoot)
	t.admit(service, peerN(1), deferredCandidate(0, testBlockRoot, 1, true))
	t.admit(service, peerN(1), deferredCandidate(1, testBlockRoot, 2, true))
	t.mockForkChoice.InvalidBlocks = map[common.Hash]struct{}{testBlockRoot: {}}
	gossip.EXPECT().BanPeer(peerN(1)).Times(1)

	service.pendingGloasSidecars.processPending(t.T().Context())

	t.Zero(counts.kzg)
	t.Empty(*stored)
	t.Zero(service.pendingGloasSidecars.count.Load())
	t.Empty(service.pendingQuota.used)
}
