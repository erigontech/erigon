package p2p

import (
	"encoding/hex"
	"math"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/gossip"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	// overlay parameters
	gossipSubDhi  = 12 // topic stable mesh high watermark
	gossipSubDout = 1  // topic stable mesh target out degree. // Dout must be set below Dlo, and must not exceed D / 2.

	// gossip parameters
	gossipSubMcacheLen    = 6   // number of windows to retain full messages in cache for `IWANT` responses
	gossipSubMcacheGossip = 3   // number of windows to gossip about
	gossipSubSeenTTL      = 550 // number of heartbeat intervals to retain message IDs
	// heartbeat interval
	gossipSubHeartbeatInterval = 700 * time.Millisecond // frequency of heartbeat, milliseconds
)

// determines the decay rate from the provided time period till
// gossip.DecayToZero. Ex: ( 1 -> 0.01)
func scoreDecay(totalDurationDecay time.Duration, beaconConfig *clparams.BeaconChainConfig) float64 {
	oneSlotDuration := time.Duration(beaconConfig.SecondsPerSlot) * time.Second
	numOfTimes := totalDurationDecay / oneSlotDuration
	return math.Pow(gossip.DecayToZero, 1/float64(numOfTimes))
}

func (p *p2pManager) pubsubOptions(beaconConfig *clparams.BeaconChainConfig) ([]pubsub.Option, error) {
	forkDigests, err := p.scheduledForkDigests(beaconConfig)
	if err != nil {
		return nil, err
	}
	oneSlotDuration := time.Duration(beaconConfig.SecondsPerSlot) * time.Second
	oneEpochDuration := time.Duration(beaconConfig.SlotsPerEpoch) * oneSlotDuration

	thresholds := &pubsub.PeerScoreThresholds{
		GossipThreshold:             -4000,
		PublishThreshold:            -8000,
		GraylistThreshold:           -16000,
		AcceptPXThreshold:           100,
		OpportunisticGraftThreshold: 5,
	}
	scoreParams := &pubsub.PeerScoreParams{
		Topics:        make(map[string]*pubsub.TopicScoreParams),
		TopicScoreCap: 32.72,
		AppSpecificScore: func(p peer.ID) float64 {
			return 0
		},
		AppSpecificWeight:           1,
		IPColocationFactorWeight:    -35.11,
		IPColocationFactorThreshold: 10,
		IPColocationFactorWhitelist: nil,
		BehaviourPenaltyWeight:      -15.92,
		BehaviourPenaltyThreshold:   6,
		BehaviourPenaltyDecay:       scoreDecay(10*oneEpochDuration, beaconConfig), // 10 epochs
		DecayInterval:               oneSlotDuration,
		DecayToZero:                 gossip.DecayToZero,
		RetainScore:                 100 * oneEpochDuration, // Retain for 100 epochs
	}
	pubsubQueueSize := 600
	psOpts := []pubsub.Option{
		pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
		pubsub.WithMessageIdFn(p.msgId),
		pubsub.WithNoAuthor(),
		pubsub.WithPeerOutboundQueueSize(pubsubQueueSize),
		pubsub.WithMaxMessageSize(int(p.cfg.NetworkConfig.MaxGossipMessageSize())),
		pubsub.WithValidateQueueSize(pubsubQueueSize),
		pubsub.WithPeerScore(scoreParams, thresholds),
		pubsub.WithGossipSubParams(pubsubGossipParam()),
		pubsub.WithSubscriptionFilter(gossipSubscriptionFilter(forkDigests, gossipTopicNames(p.cfg.NetworkConfig, beaconConfig))),
	}
	return psOpts, nil
}

// maxSubscriptionsPerRPC is above what an honest peer sends for all subnets of two fork digests.
const maxSubscriptionsPerRPC = 500

// gossipTopicNames lists every gossip topic name. Gossipsub keeps each topic a peer subscribes to until it
// disconnects, so the accepted set has to be finite. A topic missing here can be neither joined nor seen from peers.
func gossipTopicNames(networkConfig *clparams.NetworkConfig, beaconConfig *clparams.BeaconChainConfig) []string {
	fixedNames := []string{
		gossip.TopicNameBeaconBlock,
		gossip.TopicNameBeaconAggregateAndProof,
		gossip.TopicNameVoluntaryExit,
		gossip.TopicNameProposerSlashing,
		gossip.TopicNameAttesterSlashing,
		gossip.TopicNameBlsToExecutionChange,
		gossip.TopicNameSyncCommitteeContributionAndProof,
		gossip.TopicNameLightClientFinalityUpdate,
		gossip.TopicNameLightClientOptimisticUpdate,
		gossip.TopicNameExecutionPayload,
		gossip.TopicNameExecutionPayloadBid,
		gossip.TopicNamePayloadAttestation,
		gossip.TopicNameProposerPreferences,
	}
	blobSubnets := max(beaconConfig.BlobSidecarSubnetCount, beaconConfig.BlobSidecarSubnetCountElectra)
	names := make([]string, 0, uint64(len(fixedNames))+networkConfig.AttestationSubnetCount+beaconConfig.SyncCommitteeSubnetCount+blobSubnets+beaconConfig.DataColumnSidecarSubnetCount)
	names = append(names, fixedNames...)
	for i := range networkConfig.AttestationSubnetCount {
		names = append(names, gossip.TopicNameBeaconAttestation(i))
	}
	for i := range beaconConfig.SyncCommitteeSubnetCount {
		names = append(names, gossip.TopicNameSyncCommittee(int(i)))
	}
	for i := range blobSubnets {
		names = append(names, gossip.TopicNameBlobSidecar(i))
	}
	for i := range beaconConfig.DataColumnSidecarSubnetCount {
		names = append(names, gossip.TopicNameDataColumnSidecar(i))
	}
	return names
}

// scheduledForkDigests returns the digest of every fork and blob parameter change in the schedule.
func (p *p2pManager) scheduledForkDigests(beaconConfig *clparams.BeaconChainConfig) ([]string, error) {
	epochs := make([]uint64, 0, len(beaconConfig.ForkVersionSchedule)+len(beaconConfig.BlobSchedule))
	for _, fork := range beaconConfig.ForkVersionSchedule {
		if fork.Epoch != beaconConfig.FarFutureEpoch {
			epochs = append(epochs, fork.Epoch)
		}
	}
	for _, blobParameters := range beaconConfig.BlobSchedule {
		epochs = append(epochs, blobParameters.Epoch)
	}
	digests := make([]string, 0, len(epochs))
	for _, epoch := range epochs {
		digest, err := p.ethClock.ComputeForkDigest(epoch)
		if err != nil {
			return nil, err
		}
		digests = append(digests, hex.EncodeToString(digest[:]))
	}
	return digests, nil
}

// gossipSubscriptionFilter accepts only gossip topics of the scheduled forks and refuses oversized batches.
func gossipSubscriptionFilter(forkDigests, topicNames []string) pubsub.SubscriptionFilter {
	topics := make([]string, 0, len(forkDigests)*len(topicNames))
	for _, digest := range forkDigests {
		for _, name := range topicNames {
			topics = append(topics, "/eth2/"+digest+"/"+name+"/"+gossip.SSZSnappyCodec)
		}
	}
	return pubsub.WrapLimitSubscriptionFilter(pubsub.NewAllowlistSubscriptionFilter(topics...), maxSubscriptionsPerRPC)
}

// creates a custom gossipsub parameter set.
func pubsubGossipParam() pubsub.GossipSubParams {
	gParams := pubsub.DefaultGossipSubParams()
	gParams.Dlo = gossip.GossipSubDlo
	gParams.Dhi = gossipSubDhi
	gParams.D = gossip.GossipSubD
	gParams.Dout = gossipSubDout

	gParams.HeartbeatInterval = gossipSubHeartbeatInterval
	gParams.HistoryLength = gossipSubMcacheLen
	gParams.HistoryGossip = gossipSubMcacheGossip
	return gParams
}
