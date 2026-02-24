package gossip

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/common/log/v3"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

const (
	// overlay parameters
	gossipSubD    = 4 // topic stable mesh target count
	gossipSubDlo  = 2 // topic stable mesh low watermark
	gossipSubDhi  = 6 // topic stable mesh high watermark
	gossipSubDout = 1 // topic stable mesh target out degree. // Dout must be set below Dlo, and must not exceed D / 2.

	// gossip parameters
	gossipSubMcacheLen    = 6   // number of windows to retain full messages in cache for `IWANT` responses
	gossipSubMcacheGossip = 3   // number of windows to gossip about
	gossipSubSeenTTL      = 550 // number of heartbeat intervals to retain message IDs
	// heartbeat interval
	gossipSubHeartbeatInterval = 700 * time.Millisecond // frequency of heartbeat, milliseconds

	// decayToZero specifies the terminal value that we will use when decaying
	// a value.
	decayToZero = 0.01
)

const (
	// beaconBlockWeight specifies the scoring weight that we apply to
	// our beacon block topic.
	beaconBlockWeight = 0.8
	// executionPayloadWeight specifies the scoring weight that we apply to
	// our execution payload topic. Similar to beacon block as it's one per slot.
	executionPayloadWeight = 0.8
	// aggregateWeight specifies the scoring weight that we apply to
	// our aggregate topic.
	aggregateWeight = 0.5
	// syncContributionWeight specifies the scoring weight that we apply to
	// our sync contribution topic.
	syncContributionWeight = 0.2
	// attestationTotalWeight specifies the scoring weight that we apply to
	// our attestation subnet topic.
	attestationTotalWeight = 1
	// syncCommitteesTotalWeight specifies the scoring weight that we apply to
	// our sync subnet topic.
	syncCommitteesTotalWeight = 0.4
	// attesterSlashingWeight specifies the scoring weight that we apply to
	// our attester slashing topic.
	attesterSlashingWeight = 0.05
	// proposerSlashingWeight specifies the scoring weight that we apply to
	// our proposer slashing topic.
	proposerSlashingWeight = 0.05
	// voluntaryExitWeight specifies the scoring weight that we apply to
	// our voluntary exit topic.
	voluntaryExitWeight = 0.05
	// blsToExecutionChangeWeight specifies the scoring weight that we apply to
	// our bls to execution topic.
	blsToExecutionChangeWeight = 0.05

	// maxInMeshScore describes the max score a peer can attain from being in the mesh.
	maxInMeshScore = 10
	// maxFirstDeliveryScore describes the max score a peer can obtain from first deliveries.
	maxFirstDeliveryScore = 40

	// dampeningFactor reduces the amount by which the various thresholds and caps are created.
	dampeningFactor = 90
)

func (g *GossipManager) topicScoreParams(topic string) *pubsub.TopicScoreParams {
	switch {
	case strings.Contains(topic, gossip.TopicNameBeaconBlock) || gossip.IsTopicBlobSidecar(topic):
		return g.defaultBlockTopicParams()
	case strings.Contains(topic, gossip.TopicNameExecutionPayload):
		return g.defaultExecutionPayloadTopicParams()
	case strings.Contains(topic, gossip.TopicNameVoluntaryExit):
		return g.defaultVoluntaryExitTopicParams()
	case gossip.IsTopicBeaconAttestation(topic):
		return g.defaultAggregateSubnetTopicParams()
	case gossip.IsTopicSyncCommittee(topic):
		return g.defaultSyncSubnetTopicParams(g.activeIndicies)

	default:
		return nil
	}
}

// Based on the prysm parameters.
// https://gist.github.com/blacktemplar/5c1862cb3f0e32a1a7fb0b25e79e6e2c
func (g *GossipManager) defaultBlockTopicParams() *pubsub.TopicScoreParams {
	blocksPerEpoch := g.beaconConfig.SlotsPerEpoch
	meshWeight := float64(0)
	return &pubsub.TopicScoreParams{
		TopicWeight:                     beaconBlockWeight,
		TimeInMeshWeight:                maxInMeshScore / g.inMeshCap(),
		TimeInMeshQuantum:               g.oneSlotDuration(),
		TimeInMeshCap:                   g.inMeshCap(),
		FirstMessageDeliveriesWeight:    1,
		FirstMessageDeliveriesDecay:     g.scoreDecay(20 * g.oneEpochDuration()),
		FirstMessageDeliveriesCap:       23,
		MeshMessageDeliveriesWeight:     meshWeight,
		MeshMessageDeliveriesDecay:      g.scoreDecay(5 * g.oneEpochDuration()),
		MeshMessageDeliveriesCap:        float64(blocksPerEpoch * 5),
		MeshMessageDeliveriesThreshold:  float64(blocksPerEpoch*5) / 10,
		MeshMessageDeliveriesWindow:     2 * time.Second,
		MeshMessageDeliveriesActivation: 4 * g.oneEpochDuration(),
		MeshFailurePenaltyWeight:        meshWeight,
		MeshFailurePenaltyDecay:         g.scoreDecay(5 * g.oneEpochDuration()),
		InvalidMessageDeliveriesWeight:  -140.4475,
		InvalidMessageDeliveriesDecay:   g.scoreDecay(50 * g.oneEpochDuration()),
	}
}

// defaultExecutionPayloadTopicParams returns scoring parameters for the execution_payload topic.
// Similar to beacon_block since execution payloads are one per slot in GLOAS (EIP-7732).
func (g *GossipManager) defaultExecutionPayloadTopicParams() *pubsub.TopicScoreParams {
	blocksPerEpoch := g.beaconConfig.SlotsPerEpoch
	meshWeight := float64(0)
	return &pubsub.TopicScoreParams{
		TopicWeight:                     executionPayloadWeight,
		TimeInMeshWeight:                maxInMeshScore / g.inMeshCap(),
		TimeInMeshQuantum:               g.oneSlotDuration(),
		TimeInMeshCap:                   g.inMeshCap(),
		FirstMessageDeliveriesWeight:    1,
		FirstMessageDeliveriesDecay:     g.scoreDecay(20 * g.oneEpochDuration()),
		FirstMessageDeliveriesCap:       23,
		MeshMessageDeliveriesWeight:     meshWeight,
		MeshMessageDeliveriesDecay:      g.scoreDecay(5 * g.oneEpochDuration()),
		MeshMessageDeliveriesCap:        float64(blocksPerEpoch * 5),
		MeshMessageDeliveriesThreshold:  float64(blocksPerEpoch*5) / 10,
		MeshMessageDeliveriesWindow:     2 * time.Second,
		MeshMessageDeliveriesActivation: 4 * g.oneEpochDuration(),
		MeshFailurePenaltyWeight:        meshWeight,
		MeshFailurePenaltyDecay:         g.scoreDecay(5 * g.oneEpochDuration()),
		InvalidMessageDeliveriesWeight:  -140.4475,
		InvalidMessageDeliveriesDecay:   g.scoreDecay(50 * g.oneEpochDuration()),
	}
}

func (g *GossipManager) defaultVoluntaryExitTopicParams() *pubsub.TopicScoreParams {
	return &pubsub.TopicScoreParams{
		TopicWeight:                     voluntaryExitWeight,
		TimeInMeshWeight:                maxInMeshScore / g.inMeshCap(),
		TimeInMeshQuantum:               g.oneSlotDuration(),
		TimeInMeshCap:                   g.inMeshCap(),
		FirstMessageDeliveriesWeight:    2,
		FirstMessageDeliveriesDecay:     g.scoreDecay(100 * g.oneEpochDuration()),
		FirstMessageDeliveriesCap:       5,
		MeshMessageDeliveriesWeight:     0,
		MeshMessageDeliveriesDecay:      0,
		MeshMessageDeliveriesCap:        0,
		MeshMessageDeliveriesThreshold:  0,
		MeshMessageDeliveriesWindow:     0,
		MeshMessageDeliveriesActivation: 0,
		MeshFailurePenaltyWeight:        0,
		MeshFailurePenaltyDecay:         0,
		InvalidMessageDeliveriesWeight:  -2000,
		InvalidMessageDeliveriesDecay:   g.scoreDecay(50 * g.oneEpochDuration()),
	}
}

func (g *GossipManager) defaultSyncSubnetTopicParams(activeValidators uint64) *pubsub.TopicScoreParams {
	subnetCount := g.beaconConfig.SyncCommitteeSubnetCount
	// Get weight for each specific subnet.
	topicWeight := syncCommitteesTotalWeight / float64(subnetCount)
	syncComSize := g.beaconConfig.SyncCommitteeSize
	// Set the max as the sync committee size
	if activeValidators > syncComSize {
		activeValidators = syncComSize
	}
	subnetWeight := activeValidators / subnetCount
	if subnetWeight == 0 {
		log.Warn("Subnet weight is 0, skipping initializing topic scoring")
		return nil
	}
	firstDecayDuration := 1 * g.oneEpochDuration()
	meshDecayDuration := 4 * g.oneEpochDuration()

	rate := subnetWeight * 2 / gossipSubD
	if rate == 0 {
		log.Warn("rate is 0, skipping initializing topic scoring")
		return nil
	}
	// Determine expected first deliveries based on the message rate.
	firstMessageCap, err := decayLimit(g.scoreDecay(firstDecayDuration), float64(rate))
	if err != nil {
		log.Warn("Skipping initializing topic scoring")
		return nil
	}
	firstMessageWeight := maxFirstDeliveryScore / firstMessageCap
	// Determine expected mesh deliveries based on message rate applied with a dampening factor.
	meshThreshold, err := decayThreshold(g.scoreDecay(meshDecayDuration), float64(subnetWeight)/dampeningFactor)
	if err != nil {
		log.Warn("Skipping initializing topic scoring")
		return nil
	}
	meshCap := 4 * meshThreshold

	return &pubsub.TopicScoreParams{
		TopicWeight:                     topicWeight,
		TimeInMeshWeight:                maxInMeshScore / g.inMeshCap(),
		TimeInMeshQuantum:               g.oneSlotDuration(),
		TimeInMeshCap:                   g.inMeshCap(),
		FirstMessageDeliveriesWeight:    firstMessageWeight,
		FirstMessageDeliveriesDecay:     g.scoreDecay(firstDecayDuration),
		FirstMessageDeliveriesCap:       firstMessageCap,
		MeshMessageDeliveriesWeight:     0,
		MeshMessageDeliveriesDecay:      g.scoreDecay(meshDecayDuration),
		MeshMessageDeliveriesCap:        meshCap,
		MeshMessageDeliveriesThreshold:  meshThreshold,
		MeshMessageDeliveriesWindow:     2 * time.Second,
		MeshMessageDeliveriesActivation: g.oneEpochDuration(),
		MeshFailurePenaltyWeight:        0,
		MeshFailurePenaltyDecay:         g.scoreDecay(meshDecayDuration),
		InvalidMessageDeliveriesWeight:  -maxScore() / topicWeight,
		InvalidMessageDeliveriesDecay:   g.scoreDecay(50 * g.oneEpochDuration()),
	}
}

// decayLimit provides the value till which a decay process will
// limit till provided with an expected growth rate.
func decayLimit(decayRate, rate float64) (float64, error) {
	if 1 <= decayRate {
		return 0, fmt.Errorf("got an invalid decayLimit rate: %f", decayRate)
	}
	return rate / (1 - decayRate), nil
}

func (g *GossipManager) committeeCountPerSlot() uint64 {
	activeValidatorCount := g.activeIndicies
	cfg := g.beaconConfig
	var committeesPerSlot = activeValidatorCount / cfg.SlotsPerEpoch / cfg.TargetCommitteeSize

	if committeesPerSlot > cfg.MaxCommitteesPerSlot {
		return cfg.MaxCommitteesPerSlot
	}
	if committeesPerSlot == 0 {
		return 1
	}

	return committeesPerSlot
}

// maxScore attainable by a peer.
func maxScore() float64 {
	totalWeight := beaconBlockWeight + aggregateWeight + syncContributionWeight +
		attestationTotalWeight + syncCommitteesTotalWeight + attesterSlashingWeight +
		proposerSlashingWeight + voluntaryExitWeight + blsToExecutionChangeWeight
	return (maxInMeshScore + maxFirstDeliveryScore) * totalWeight
}

// is used to determine the threshold from the decay limit with
// a provided growth rate. This applies the decay rate to a
// computed limit.
func decayThreshold(decayRate, rate float64) (float64, error) {
	d, err := decayLimit(decayRate, rate)
	if err != nil {
		return 0, err
	}
	return d * decayRate, nil
}

func (g *GossipManager) defaultAggregateSubnetTopicParams() *pubsub.TopicScoreParams {
	subnetCount := g.networkConfig.AttestationSubnetCount
	// Get weight for each specific subnet.
	topicWeight := float64(attestationTotalWeight) / float64(subnetCount)
	subnetWeight := g.activeIndicies / subnetCount
	if subnetWeight == 0 {
		log.Warn("Subnet weight is 0, skipping initializing topic scoring", "activeValidatorCount", g.activeIndicies)
		return nil
	}
	// Determine the amount of validators expected in a subnet in a single slot.
	numPerSlot := time.Duration(subnetWeight / g.beaconConfig.SlotsPerEpoch)
	if numPerSlot == 0 {
		log.Trace("numPerSlot is 0, skipping initializing topic scoring")
		return nil
	}
	comsPerSlot := g.committeeCountPerSlot()
	exceedsThreshold := comsPerSlot >= 2*subnetCount/g.beaconConfig.SlotsPerEpoch
	firstDecayDuration := 1 * g.oneEpochDuration()
	meshDecayDuration := 4 * g.oneEpochDuration()
	if exceedsThreshold {
		firstDecayDuration = 4 * g.oneEpochDuration()
		meshDecayDuration = 16 * g.oneEpochDuration()
	}
	rate := numPerSlot * 2 / gossipSubD
	if rate == 0 {
		log.Trace("rate is 0, skipping initializing topic scoring")
		return nil
	}
	// Determine expected first deliveries based on the message rate.
	firstMessageCap, err := decayLimit(g.scoreDecay(firstDecayDuration), float64(rate))
	if err != nil {
		log.Trace("skipping initializing topic scoring", "err", err)
		return nil
	}
	firstMessageWeight := float64(maxFirstDeliveryScore) / firstMessageCap
	// Determine expected mesh deliveries based on message rate applied with a dampening factor.
	meshThreshold, err := decayThreshold(g.scoreDecay(meshDecayDuration), float64(numPerSlot)/float64(dampeningFactor))
	if err != nil {
		log.Trace("skipping initializing topic scoring", "err", err)
		return nil
	}
	meshCap := 4 * meshThreshold

	return &pubsub.TopicScoreParams{
		TopicWeight:                     topicWeight,
		TimeInMeshWeight:                maxInMeshScore / g.inMeshCap(),
		TimeInMeshQuantum:               g.oneSlotDuration(),
		TimeInMeshCap:                   g.inMeshCap(),
		FirstMessageDeliveriesWeight:    firstMessageWeight,
		FirstMessageDeliveriesDecay:     g.scoreDecay(firstDecayDuration),
		FirstMessageDeliveriesCap:       firstMessageCap,
		MeshMessageDeliveriesDecay:      g.scoreDecay(meshDecayDuration),
		MeshMessageDeliveriesCap:        meshCap,
		MeshMessageDeliveriesThreshold:  meshThreshold,
		MeshMessageDeliveriesWindow:     2 * time.Second,
		MeshMessageDeliveriesActivation: 1 * g.oneEpochDuration(),
		MeshFailurePenaltyDecay:         g.scoreDecay(meshDecayDuration),
		InvalidMessageDeliveriesWeight:  -maxScore() / topicWeight,
		InvalidMessageDeliveriesDecay:   g.scoreDecay(50 * g.oneEpochDuration()),
	}
}

func (g *GossipManager) oneSlotDuration() time.Duration {
	return time.Duration(g.beaconConfig.SecondsPerSlot) * time.Second
}

func (g *GossipManager) oneEpochDuration() time.Duration {
	return g.oneSlotDuration() * time.Duration(g.beaconConfig.SlotsPerEpoch)
}

// the cap for `inMesh` time scoring.
func (g *GossipManager) inMeshCap() float64 {
	return float64((3600 * time.Second) / g.oneSlotDuration())
}

// determines the decay rate from the provided time period till
// the decayToZero value. Ex: ( 1 -> 0.01)
func (g *GossipManager) scoreDecay(totalDurationDecay time.Duration) float64 {
	numOfTimes := totalDurationDecay / g.oneSlotDuration()
	return math.Pow(decayToZero, 1/float64(numOfTimes))
}
