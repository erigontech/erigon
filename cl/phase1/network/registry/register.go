package registry

import (
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/network/gossip"
	"github.com/erigontech/erigon/cl/phase1/network/services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

func RegisterGossipServices(
	gm *gossip.GossipManager,
	forkChoiceReader forkchoice.ForkChoiceStorageReader,
	ethClock eth_clock.EthereumClock,
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
	waitReady := WithHeadSlotReady(forkChoiceReader, ethClock)

	// register services
	gossip.RegisterGossipService(gm, blockService, WithRateLimiterByPeer(1, 2))
	gossip.RegisterGossipService(gm, syncContributionService, waitReady, WithRateLimiterByPeer(8, 16))
	gossip.RegisterGossipService(gm, aggregateAndProofService, waitReady, WithRateLimiterByPeer(8, 16))
	gossip.RegisterGossipService(gm, syncCommitteeMessagesService, waitReady, WithRateLimiterByPeer(8, 16))
	gossip.RegisterGossipService(gm, attesterSlashingService, waitReady, WithRateLimiterByPeer(2, 8))
	gossip.RegisterGossipService(gm, voluntaryExitService, waitReady, WithRateLimiterByPeer(2, 8))
	gossip.RegisterGossipService(gm, blsToExecutionChangeService, waitReady, WithRateLimiterByPeer(2, 8))
	gossip.RegisterGossipService(gm, proposerSlashingService, waitReady, WithRateLimiterByPeer(2, 8))
	gossip.RegisterGossipService(gm, attestationService, waitReady, WithGlobalTimeBasedRateLimiter(6*time.Second, 250))
	gossip.RegisterGossipService(gm, blobService, WithEndVersion(clparams.FuluVersion), WithGlobalTimeBasedRateLimiter(6*time.Second, 32))
	// fulu
	gossip.RegisterGossipService(gm, dataColumnSidecarService, WithBeginVersion(clparams.FuluVersion), WithRateLimiterByPeer(32, 64))
}

func WithHeadSlotReady(forkChoiceReader forkchoice.ForkChoiceStorageReader, ethClock eth_clock.EthereumClock) gossip.ConditionFunc {
	return func(pid peer.ID, msg *pubsub.Message, curVersion clparams.StateVersion) bool {
		return forkChoiceReader.HighestSeen()+8 >= ethClock.GetCurrentSlot()
	}
}

// WithBeginVersion returns a condition that checks if the current version is greater than or equal to the begin version
func WithBeginVersion(beginVersion clparams.StateVersion) gossip.ConditionFunc {
	return func(pid peer.ID, msg *pubsub.Message, curVersion clparams.StateVersion) bool {
		return curVersion >= beginVersion
	}
}

// WithEndVersion returns a condition that checks if the current version is less than the end version
func WithEndVersion(endVersion clparams.StateVersion) gossip.ConditionFunc {
	return func(pid peer.ID, msg *pubsub.Message, curVersion clparams.StateVersion) bool {
		return curVersion < endVersion
	}
}

// WithGlobalTimeBasedRateLimiter returns a condition that checks if the message can be processed based on the time based rate limiter
func WithGlobalTimeBasedRateLimiter(duration time.Duration, maxRequests int) gossip.ConditionFunc {
	limiter := newTimeBasedRateLimiter(duration, maxRequests)
	return func(pid peer.ID, msg *pubsub.Message, curVersion clparams.StateVersion) bool {
		return limiter.tryAcquire()
	}
}

// WithRateLimiterByPeer returns a condition that checks if the message can be processed based on the token bucket rate limiter
func WithRateLimiterByPeer(ratePerSecond float64, burst int) gossip.ConditionFunc {
	limiter := newTokenBucketRateLimiter(ratePerSecond, burst)
	return func(pid peer.ID, msg *pubsub.Message, curVersion clparams.StateVersion) bool {
		return limiter.acquire(pid.String())
	}
}
