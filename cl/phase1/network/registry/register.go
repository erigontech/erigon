package registry

import (
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/network/gossip"
	"github.com/erigontech/erigon/cl/phase1/network/services"
)

func RegisterGossipServices(
	gm *gossip.GossipManager,
	blockService services.BlockService,
	attesterSlashingService services.AttesterSlashingService,
	blobService services.BlobSidecarsService,
	dataColumnSidecarService services.DataColumnSidecarService,
	syncCommitteeMessagesService services.SyncCommitteeMessagesService,
	syncContributionService services.SyncContributionService,
	aggregateAndProofService services.AggregateAndProofService,
	attestationService services.AttestationService,
	voluntaryExitService services.VoluntaryExitService,
	blsToExecutionChangeService services.BLSToExecutionChangeService,
	proposerSlashingService services.ProposerSlashingService,
) {
	// register services
	gossip.RegisterGossipService(gm, blockService, gossip.WithRateLimiterByPeer(1, 2))
	gossip.RegisterGossipService(gm, syncContributionService, gossip.WithRateLimiterByPeer(8, 16))
	gossip.RegisterGossipService(gm, aggregateAndProofService, gossip.WithRateLimiterByPeer(8, 16))
	gossip.RegisterGossipService(gm, syncCommitteeMessagesService, gossip.WithRateLimiterByPeer(8, 16))
	gossip.RegisterGossipService(gm, attesterSlashingService, gossip.WithRateLimiterByPeer(2, 8))
	gossip.RegisterGossipService(gm, voluntaryExitService, gossip.WithRateLimiterByPeer(2, 8))
	gossip.RegisterGossipService(gm, blsToExecutionChangeService, gossip.WithRateLimiterByPeer(2, 8))
	gossip.RegisterGossipService(gm, proposerSlashingService, gossip.WithRateLimiterByPeer(2, 8))
	gossip.RegisterGossipService(gm, attestationService, gossip.WithGlobalTimeBasedRateLimiter(6*time.Second, 250))
	gossip.RegisterGossipService(gm, blobService, gossip.WithBeginVersion(clparams.DenebVersion), gossip.WithEndVersion(clparams.FuluVersion), gossip.WithGlobalTimeBasedRateLimiter(6*time.Second, 32))
	// fulu
	gossip.RegisterGossipService(gm, dataColumnSidecarService, gossip.WithBeginVersion(clparams.FuluVersion), gossip.WithRateLimiterByPeer(32, 64))
}
