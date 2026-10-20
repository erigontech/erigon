package registry

import (
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/network/gossip"
	"github.com/erigontech/erigon/cl/phase1/network/services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common/log/v3"
)

// RegisterGossipServices registers all the gossip services with the given gossip manager.
// Put it in a separate file to avoid circular dependency because it depends on many big packages.
func RegisterGossipServices(
	gm *gossip.GossipManager,
	forkChoiceReader forkchoice.ForkChoiceStorageReader,
	ethClock eth_clock.EthereumClock,
	activeValidatorCount func() uint64,
	// services
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
	executionPayloadService services.ExecutionPayloadService,
	payloadAttestationService services.PayloadAttestationService,
	proposerPreferencesService services.ProposerPreferencesService,
	executionPayloadBidService services.ExecutionPayloadBidService,
) {
	waitReady := withHeadSlotReady(forkChoiceReader, ethClock)

	var subscribed, expired int
	add := func(s, e int) {
		subscribed += s
		expired += e
	}

	// register services
	add(gossip.RegisterGossipService(gm, blockService, withRateLimiterByPeer(1, 2)))
	add(gossip.RegisterGossipService(gm, syncContributionService, waitReady, withRateLimiterByPeer(8, 16)))
	add(gossip.RegisterGossipService(gm, aggregateAndProofService, waitReady, withRateLimiterByPeer(8, 16)))
	add(gossip.RegisterGossipService(gm, syncCommitteeMessagesService, waitReady, withRateLimiterByPeer(8, 16)))
	add(gossip.RegisterGossipService(gm, attesterSlashingService, waitReady, withRateLimiterByPeer(2, 8)))
	add(gossip.RegisterGossipService(gm, voluntaryExitService, waitReady, withRateLimiterByPeer(2, 8)))
	add(gossip.RegisterGossipService(gm, blsToExecutionChangeService, waitReady, withRateLimiterByPeer(2, 8)))
	add(gossip.RegisterGossipService(gm, proposerSlashingService, waitReady, withRateLimiterByPeer(2, 8)))
	attestationWindow := 6 * time.Second
	add(gossip.RegisterGossipService(gm, attestationService, waitReady, withGlobalTimeBasedRateLimiter(attestationWindow, func() int {
		return max(250, int(gm.ExpectedAttestations(attestationWindow, activeValidatorCount())*3/2))
	})))
	add(gossip.RegisterGossipService(gm, blobService, withEndVersion(clparams.FuluVersion), withGlobalTimeBasedRateLimiter(6*time.Second, func() int { return 32 })))
	// fulu
	add(gossip.RegisterGossipService(gm, dataColumnSidecarService, withBeginVersion(clparams.FuluVersion), withRateLimiterByPeer(32, 64)))
	// gloas
	add(gossip.RegisterGossipService(gm, executionPayloadService, waitReady, withBeginVersion(clparams.GloasVersion), withRateLimiterByPeer(2, 4)))
	add(gossip.RegisterGossipService(gm, payloadAttestationService, waitReady, withBeginVersion(clparams.GloasVersion), withPayloadAttestationRateLimiter()))
	add(gossip.RegisterGossipService(gm, proposerPreferencesService, waitReady, withRateLimiterByPeer(2, 4)))
	add(gossip.RegisterGossipService(gm, executionPayloadBidService, waitReady, withBeginVersion(clparams.GloasVersion), withRateLimiterByPeer(8, 16)))

	log.Info("[GossipManager] Registered services", "subscribed", subscribed, "expired", expired)
}

func withHeadSlotReady(forkChoiceReader forkchoice.ForkChoiceStorageReader, ethClock eth_clock.EthereumClock) gossip.ConditionFunc {
	return func(pid peer.ID, msg *pubsub.Message, curVersion clparams.StateVersion) bool {
		return forkChoiceReader.HighestSeen()+8 >= ethClock.GetCurrentSlot()
	}
}

// withBeginVersion returns a condition that checks if the current version is greater than or equal to the begin version
func withBeginVersion(beginVersion clparams.StateVersion) gossip.ConditionFunc {
	return func(pid peer.ID, msg *pubsub.Message, curVersion clparams.StateVersion) bool {
		return curVersion >= beginVersion
	}
}

// withEndVersion returns a condition that checks if the current version is less than the end version
func withEndVersion(endVersion clparams.StateVersion) gossip.ConditionFunc {
	return func(pid peer.ID, msg *pubsub.Message, curVersion clparams.StateVersion) bool {
		return curVersion < endVersion
	}
}

// withGlobalTimeBasedRateLimiter returns a condition that checks if the message can be processed based on the time based rate limiter
func withGlobalTimeBasedRateLimiter(duration time.Duration, maxRequests func() int) gossip.ConditionFunc {
	limiter := newTimeBasedRateLimiter(duration, maxRequests)
	return func(pid peer.ID, msg *pubsub.Message, curVersion clparams.StateVersion) bool {
		return limiter.tryAcquire()
	}
}

// withPayloadAttestationRateLimiter lets one peer forward a whole PTC's votes at once: the votes
// are due at the same point in the slot, and a vote ignored here is also dropped from every other
// peer, because gossipsub has already marked it as seen.
func withPayloadAttestationRateLimiter() gossip.ConditionFunc {
	return withRateLimiterByPeer(float64(clparams.MaxPtcSize), int(clparams.MaxPtcSize))
}

// withRateLimiterByPeer returns a condition that checks if the message can be processed based on the token bucket rate limiter
func withRateLimiterByPeer(ratePerSecond float64, burst int) gossip.ConditionFunc {
	limiter := newTokenBucketRateLimiter(ratePerSecond, burst)
	return func(pid peer.ID, msg *pubsub.Message, curVersion clparams.StateVersion) bool {
		return limiter.acquire(pid.String())
	}
}
