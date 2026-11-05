package gossip

import (
	"fmt"
	"math"
	"time"

	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/common/log/v3"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
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
	// executionPayloadBidWeight specifies the scoring weight that we apply to
	// our execution payload bid topic. [New in Gloas:EIP7732]
	executionPayloadBidWeight = 0.5
	// payloadAttestationWeight specifies the scoring weight that we apply to
	// our payload attestation message topic. [New in Gloas:EIP7732]
	payloadAttestationWeight = 0.5
	// proposerPreferencesWeight specifies the scoring weight that we apply to
	// our proposer preferences topic. [New in Gloas:EIP7732]
	proposerPreferencesWeight = 0.05

	// maxInMeshScore describes the max score a peer can attain from being in the mesh.
	maxInMeshScore = 10
	// maxFirstDeliveryScore describes the max score a peer can obtain from first deliveries.
	maxFirstDeliveryScore = 40

	// dampeningFactor reduces the amount by which the various thresholds and caps are created.
	dampeningFactor = 90
)

func (g *GossipManager) topicScoreParams(topic string) *pubsub.TopicScoreParams {
	switch {
	case topic == gossip.TopicNameBeaconBlock || gossip.IsTopicBlobSidecar(topic) || gossip.IsTopicDataColumnSidecar(topic):
		return g.defaultBlockTopicParams()
	case topic == gossip.TopicNameBeaconAggregateAndProof:
		aggregatorsPerSlot := g.committeeCountPerSlot() * g.beaconConfig.TargetAggregatorsPerCommittee
		return g.rateBasedTopicParams(aggregateWeight, aggregatorsPerSlot)
	case topic == gossip.TopicNameSyncCommitteeContributionAndProof:
		aggregatorsPerSlot := g.beaconConfig.SyncCommitteeSubnetCount * g.beaconConfig.TargetAggregatorsPerSyncSubcommittee
		return g.rateBasedTopicParams(syncContributionWeight, aggregatorsPerSlot)
	case topic == gossip.TopicNameAttesterSlashing:
		return g.staticTopicParams(attesterSlashingWeight, 36, 1)
	case topic == gossip.TopicNameProposerSlashing:
		return g.staticTopicParams(proposerSlashingWeight, 36, 1)
	case topic == gossip.TopicNameBlsToExecutionChange:
		return g.staticTopicParams(blsToExecutionChangeWeight, 2, 5)
	case topic == gossip.TopicNameExecutionPayload:
		return g.defaultExecutionPayloadTopicParams()
	case topic == gossip.TopicNameExecutionPayloadBid:
		return g.defaultExecutionPayloadBidTopicParams()
	case topic == gossip.TopicNamePayloadAttestation:
		return g.defaultPayloadAttestationTopicParams()
	case topic == gossip.TopicNameProposerPreferences:
		return g.staticTopicParams(proposerPreferencesWeight, 2, 5)
	case topic == gossip.TopicNameVoluntaryExit:
		return g.staticTopicParams(voluntaryExitWeight, 2, 5)
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

// defaultExecutionPayloadBidTopicParams returns scoring parameters for the execution_payload_bid topic.
// Builders may submit multiple bids per slot; scored similarly to blocks but with lower weight. [New in Gloas:EIP7732]
func (g *GossipManager) defaultExecutionPayloadBidTopicParams() *pubsub.TopicScoreParams {
	blocksPerEpoch := g.beaconConfig.SlotsPerEpoch
	meshWeight := float64(0)
	return &pubsub.TopicScoreParams{
		TopicWeight:                     executionPayloadBidWeight,
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

// defaultPayloadAttestationTopicParams returns scoring parameters for the payload_attestation_message topic.
// PTC validators attest to payload timeliness once per slot. [New in Gloas:EIP7732]
func (g *GossipManager) defaultPayloadAttestationTopicParams() *pubsub.TopicScoreParams {
	blocksPerEpoch := g.beaconConfig.SlotsPerEpoch
	meshWeight := float64(0)
	return &pubsub.TopicScoreParams{
		TopicWeight:                     payloadAttestationWeight,
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

// staticTopicParams uses fixed first-delivery weights and caps for low-rate topics.
// It also rewards time in the mesh and penalizes invalid messages, while leaving
// mesh delivery rate scoring disabled.
func (g *GossipManager) staticTopicParams(topicWeight, firstMessageWeight, firstMessageCap float64) *pubsub.TopicScoreParams {
	return &pubsub.TopicScoreParams{
		TopicWeight:                    topicWeight,
		TimeInMeshWeight:               maxInMeshScore / g.inMeshCap(),
		TimeInMeshQuantum:              g.oneSlotDuration(),
		TimeInMeshCap:                  g.inMeshCap(),
		FirstMessageDeliveriesWeight:   firstMessageWeight,
		FirstMessageDeliveriesDecay:    g.scoreDecay(100 * g.oneEpochDuration()),
		FirstMessageDeliveriesCap:      firstMessageCap,
		InvalidMessageDeliveriesWeight: -2000,
		InvalidMessageDeliveriesDecay:  g.scoreDecay(50 * g.oneEpochDuration()),
	}
}

// rateBasedTopicParams calculates first-delivery weights and caps from the expected
// message rate; mesh delivery rate scoring stays disabled. A zero rate retains
// invalid-message penalties and disables rewards for first deliveries and time in the mesh.
func (g *GossipManager) rateBasedTopicParams(topicWeight float64, messagesPerSlot uint64) *pubsub.TopicScoreParams {
	// Gossipsub requires a nonzero TimeInMeshQuantum even when TimeInMeshWeight is zero.
	params := &pubsub.TopicScoreParams{
		TopicWeight:                    topicWeight,
		TimeInMeshQuantum:              g.oneSlotDuration(),
		InvalidMessageDeliveriesWeight: -maxScore() / topicWeight,
		InvalidMessageDeliveriesDecay:  g.scoreDecay(50 * g.oneEpochDuration()),
	}
	rate := float64(messagesPerSlot) * 2 / gossip.GossipSubD
	if rate == 0 {
		return params
	}
	firstMessageDecay := g.scoreDecay(g.oneEpochDuration())
	firstMessageCap, err := decayLimit(firstMessageDecay, rate)
	if err != nil {
		log.Warn("Skipping initializing topic scoring", "err", err)
		return nil
	}
	params.TimeInMeshWeight = maxInMeshScore / g.inMeshCap()
	params.TimeInMeshCap = g.inMeshCap()
	params.FirstMessageDeliveriesWeight = maxFirstDeliveryScore / firstMessageCap
	params.FirstMessageDeliveriesDecay = firstMessageDecay
	params.FirstMessageDeliveriesCap = firstMessageCap
	return params
}

func (g *GossipManager) defaultSyncSubnetTopicParams(activeValidators uint64) *pubsub.TopicScoreParams {
	subnetCount := g.beaconConfig.SyncCommitteeSubnetCount
	// Get weight for each specific subnet.
	topicWeight := syncCommitteesTotalWeight / float64(subnetCount)
	params := g.rateBasedTopicParams(topicWeight, 0)
	syncComSize := g.beaconConfig.SyncCommitteeSize
	// Set the max as the sync committee size
	if activeValidators > syncComSize {
		activeValidators = syncComSize
	}
	subnetWeight := activeValidators / subnetCount
	if subnetWeight == 0 {
		log.Debug("Skipping sync subnet delivery scoring: subnet weight is 0")
		return params
	}
	firstDecayDuration := 1 * g.oneEpochDuration()
	meshDecayDuration := 4 * g.oneEpochDuration()

	rate := float64(subnetWeight) * 2 / gossip.GossipSubD
	// Determine expected first deliveries based on the message rate.
	firstMessageCap, err := decayLimit(g.scoreDecay(firstDecayDuration), rate)
	if err != nil {
		log.Warn("Skipping sync subnet delivery scoring")
		return params
	}
	firstMessageWeight := maxFirstDeliveryScore / firstMessageCap
	// Determine expected mesh deliveries based on message rate applied with a dampening factor.
	meshThreshold, err := decayThreshold(g.scoreDecay(meshDecayDuration), float64(subnetWeight)/dampeningFactor)
	if err != nil {
		log.Warn("Skipping sync subnet delivery scoring")
		return params
	}
	meshCap := 4 * meshThreshold

	params.TimeInMeshWeight = maxInMeshScore / g.inMeshCap()
	params.TimeInMeshCap = g.inMeshCap()
	params.FirstMessageDeliveriesWeight = firstMessageWeight
	params.FirstMessageDeliveriesDecay = g.scoreDecay(firstDecayDuration)
	params.FirstMessageDeliveriesCap = firstMessageCap
	params.MeshMessageDeliveriesDecay = g.scoreDecay(meshDecayDuration)
	params.MeshMessageDeliveriesCap = meshCap
	params.MeshMessageDeliveriesThreshold = meshThreshold
	params.MeshMessageDeliveriesWindow = 2 * time.Second
	params.MeshMessageDeliveriesActivation = g.oneEpochDuration()
	params.MeshFailurePenaltyDecay = g.scoreDecay(meshDecayDuration)
	return params
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
	committeesPerSlot := activeValidatorCount / cfg.SlotsPerEpoch / cfg.TargetCommitteeSize

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
		proposerSlashingWeight + voluntaryExitWeight + blsToExecutionChangeWeight +
		executionPayloadWeight + executionPayloadBidWeight +
		payloadAttestationWeight + proposerPreferencesWeight
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
	params := g.rateBasedTopicParams(topicWeight, 0)
	subnetWeight := g.activeIndicies / subnetCount
	if subnetWeight == 0 {
		log.Debug("Skipping attestation subnet delivery scoring: subnet weight is 0", "activeValidatorCount", g.activeIndicies)
		return params
	}
	// Determine the amount of validators expected in a subnet in a single slot.
	numPerSlot := time.Duration(subnetWeight / g.beaconConfig.SlotsPerEpoch)
	if numPerSlot == 0 {
		log.Trace("Skipping attestation subnet delivery scoring: numPerSlot is 0")
		return params
	}
	comsPerSlot := g.committeeCountPerSlot()
	exceedsThreshold := comsPerSlot >= 2*subnetCount/g.beaconConfig.SlotsPerEpoch
	firstDecayDuration := 1 * g.oneEpochDuration()
	meshDecayDuration := 4 * g.oneEpochDuration()
	if exceedsThreshold {
		firstDecayDuration = 4 * g.oneEpochDuration()
		meshDecayDuration = 16 * g.oneEpochDuration()
	}
	rate := float64(numPerSlot) * 2 / gossip.GossipSubD
	// Determine expected first deliveries based on the message rate.
	firstMessageCap, err := decayLimit(g.scoreDecay(firstDecayDuration), rate)
	if err != nil {
		log.Trace("Skipping attestation subnet delivery scoring", "err", err)
		return params
	}
	firstMessageWeight := float64(maxFirstDeliveryScore) / firstMessageCap
	// Determine expected mesh deliveries based on message rate applied with a dampening factor.
	meshThreshold, err := decayThreshold(g.scoreDecay(meshDecayDuration), float64(numPerSlot)/float64(dampeningFactor))
	if err != nil {
		log.Trace("Skipping attestation subnet delivery scoring", "err", err)
		return params
	}
	meshCap := 4 * meshThreshold

	params.TimeInMeshWeight = maxInMeshScore / g.inMeshCap()
	params.TimeInMeshCap = g.inMeshCap()
	params.FirstMessageDeliveriesWeight = firstMessageWeight
	params.FirstMessageDeliveriesDecay = g.scoreDecay(firstDecayDuration)
	params.FirstMessageDeliveriesCap = firstMessageCap
	params.MeshMessageDeliveriesDecay = g.scoreDecay(meshDecayDuration)
	params.MeshMessageDeliveriesCap = meshCap
	params.MeshMessageDeliveriesThreshold = meshThreshold
	params.MeshMessageDeliveriesWindow = 2 * time.Second
	params.MeshMessageDeliveriesActivation = g.oneEpochDuration()
	params.MeshFailurePenaltyDecay = g.scoreDecay(meshDecayDuration)
	return params
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
// the gossip.DecayToZero value. Ex: ( 1 -> 0.01)
func (g *GossipManager) scoreDecay(totalDurationDecay time.Duration) float64 {
	numOfTimes := totalDurationDecay / g.oneSlotDuration()
	return math.Pow(gossip.DecayToZero, 1/float64(numOfTimes))
}
