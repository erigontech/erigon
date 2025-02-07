package monitor

import (
	"sort"
	"sync"
	"time"

	"github.com/erigontech/erigon-lib/metrics"
)

var (
	// VALIDATOR METRICS

	// metricAttestHit is the number of attestations that hit for those validators we observe within current_epoch-2
	metricAttestHit = metrics.GetOrCreateCounter("validator_attestation_hit")
	// metricAttestMiss is the number of attestations that miss for those validators we observe within current_epoch-2
	metricAttestMiss = metrics.GetOrCreateCounter("validator_attestation_miss")
	// metricProposerHit is the number of proposals that hit for those validators we observe in previous slot
	metricProposerHit = metrics.GetOrCreateCounter("validator_proposal_hit")
	// metricProposerMiss is the number of proposals that miss for those validators we observe in previous slot
	metricProposerMiss = metrics.GetOrCreateCounter("validator_proposal_miss")
	// aggregateAndProofSignatures is the sum of signatures in all the aggregates in the recent slot
	aggregateAndProofSignatures = metrics.GetOrCreateGauge("aggregate_and_proof_signatures")

	// Block processing metrics
	fullBlockProcessingTime        = metrics.GetOrCreateGauge("full_block_processing_time")
	attestationBlockProcessingTime = metrics.GetOrCreateGauge("attestation_block_processing_time")
	batchVerificationThroughput    = metrics.GetOrCreateGauge("aggregation_per_signature")
	blobVerificationTime           = metrics.GetOrCreateGauge("blob_verification_time")
	executionTime                  = metrics.GetOrCreateGauge("execution_time")
	executionClientInsertingBlocks = metrics.GetOrCreateGauge("execution_client_insert_blocks_time")
	executionClientValidateChain   = metrics.GetOrCreateGauge("execution_client_validate_chain_time")

	// Epoch processing metrics
	EpochProcessingTime                     = metrics.GetOrCreateGauge("epoch_processing_time")
	ProcessJustificationBitsAndFinalityTime = metrics.GetOrCreateGauge("process_justification_bits_and_finality_time")
	ProcessInactivityScoresTime             = metrics.GetOrCreateGauge("process_inactivity_ccores_time")
	ProcessRewardsAndPenaltiesTime          = metrics.GetOrCreateGauge("process_rewards_and_penalties_time")
	ProcessRegistryUpdatesTime              = metrics.GetOrCreateGauge("process_registry_updates_time")
	ProcessSlashingsTime                    = metrics.GetOrCreateGauge("process_slashings_time")
	ProcessEffectiveBalanceUpdatesTime      = metrics.GetOrCreateGauge("process_effective_balance_updates_time")
	ProcessHistoricalRootsUpdateTime        = metrics.GetOrCreateGauge("process_historical_roots_update_time")
	ProcessParticipationFlagUpdatesTime     = metrics.GetOrCreateGauge("process_participation_flag_updates_time")
	ProcessSyncCommitteeUpdateTime          = metrics.GetOrCreateGauge("process_sync_committee_update_time")
	ProcessPendingDepositsTime              = metrics.GetOrCreateGauge("process_pending_deposits_time")

	// Network metrics
	gossipTopicsMetricCounterPrefix = "gossip_topics_seen"
	gossipMetricsMap                = sync.Map{}
	aggregateQuality50Per           = metrics.GetOrCreateGauge("aggregate_quality_50")
	aggregateQuality25Per           = metrics.GetOrCreateGauge("aggregate_quality_25")
	aggregateQuality75Per           = metrics.GetOrCreateGauge("aggregate_quality_75")
	aggregateQualityMin             = metrics.GetOrCreateGauge("aggregate_quality_min")
	aggregateQualityMax             = metrics.GetOrCreateGauge("aggregate_quality_max")
	blockImportingLatency           = metrics.GetOrCreateGauge("block_importing_latency")

	// Beacon chain metrics
	committeeSize         = metrics.GetOrCreateGauge("committee_size")
	activeValidatorsCount = metrics.GetOrCreateGauge("active_validators_count")
	currentSlot           = metrics.GetOrCreateGauge("current_slot")
	currentEpoch          = metrics.GetOrCreateGauge("current_epoch")
	aggregateAttestation  = metrics.GetOrCreateGauge("aggregate_attestation")

	// Libp2p metrics
	totalInBytes  = metrics.GetOrCreateGauge("total_in_bytes")
	totalOutBytes = metrics.GetOrCreateGauge("total_out_bytes")

	// Snapshot metrics
	frozenBlocks = metrics.GetOrCreateGauge("frozen_blocks")
	frozenBlobs  = metrics.GetOrCreateGauge("frozen_blobs")
)

type batchVerificationThroughputMetric struct {
	totalVerified      uint64
	currentAverageSecs float64
	mu                 sync.Mutex
}

type aggregateQualityMetric struct {
	qualities []float64
	mu        sync.Mutex
}

func (a *aggregateQualityMetric) observe(participationCount int, totalCount int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	newPercentage := float64(participationCount) / float64(totalCount)
	a.qualities = append(a.qualities, newPercentage)
	if len(a.qualities) <= 40 {
		return
	}
	sort.Float64s(a.qualities)
	aggregateQuality50Per.Set(a.qualities[len(a.qualities)/2])
	aggregateQuality25Per.Set(a.qualities[len(a.qualities)/4])
	aggregateQuality75Per.Set(a.qualities[(len(a.qualities)*3)/4])
	aggregateQualityMin.Set(a.qualities[0])
	aggregateQualityMax.Set(a.qualities[len(a.qualities)-1])

	a.qualities = a.qualities[:0]

}

var (
	batchVerificationThroughputMetricStruct = &batchVerificationThroughputMetric{}
	aggregateQualityMetricStruct            = &aggregateQualityMetric{}
)

func (b *batchVerificationThroughputMetric) observe(t time.Duration, totalSigs int) float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	elapsedInMillisecsPerSig := float64(t.Microseconds()) / 1000 / float64(totalSigs)
	if b.totalVerified == 0 {
		b.currentAverageSecs = elapsedInMillisecsPerSig
	} else {
		b.currentAverageSecs = (b.currentAverageSecs*float64(b.totalVerified) + elapsedInMillisecsPerSig) / float64(b.totalVerified+1)
	}
	b.totalVerified++
	ret := b.currentAverageSecs
	if b.totalVerified > 1000 {
		b.currentAverageSecs = 0
		b.totalVerified = 0
	}
	return ret
}

func microToMilli(micros int64) float64 {
	return float64(micros) / 1000
}

// ObserveNumberOfAggregateSignatures sets the average processing time for each attestation in aggregate
func ObserveNumberOfAggregateSignatures(signatures int) {
	aggregateAndProofSignatures.Add(float64(signatures))
}

type TimeMeasure struct {
	start  time.Time
	metric metrics.Gauge
}

func (m TimeMeasure) End() {
	m.metric.Set(float64(time.Since(m.start).Microseconds()))
}

func ObserveElaspedTime(m metrics.Gauge) TimeMeasure {
	return TimeMeasure{start: time.Now(), metric: m}
}

// ObserveAggregateAttestation sets the time it took add new attestation to aggregateAndProof
func ObserveAggregateAttestation(startTime time.Time) {
	aggregateAttestation.Set(float64(time.Since(startTime).Microseconds()))
}

// ObserveAttestHit increments the attestation hit metric
func ObserveAttestationBlockProcessingTime(startTime time.Time) {
	attestationBlockProcessingTime.Set(microToMilli(time.Since(startTime).Microseconds()))
}

// ObserveFullBlockProcessingTime increments the full block processing time metric
func ObserveFullBlockProcessingTime(startTime time.Time) {
	fullBlockProcessingTime.Set(microToMilli(time.Since(startTime).Microseconds()))
}

// ObserveBatchVerificationThroughput increments the batch verification throughput metric
func ObserveBatchVerificationThroughput(d time.Duration, totalSigs int) {
	batchVerificationThroughput.Set(batchVerificationThroughputMetricStruct.observe(d, totalSigs))
}

// ObserveGossipTopicSeen increments the gossip topic seen metric
func ObserveGossipTopicSeen(topic string, l int) {
	var metric metrics.Counter
	metricI, ok := gossipMetricsMap.LoadOrStore(topic, metrics.GetOrCreateCounter(gossipTopicsMetricCounterPrefix+"_"+topic))
	if ok {
		metric = metricI.(metrics.Counter)
	} else {
		metric = metrics.GetOrCreateCounter(gossipTopicsMetricCounterPrefix + "_" + topic)
		gossipMetricsMap.Store(topic, metric)
	}
	metric.Add(float64(l))
}

func ObserveAggregateQuality(participationCount int, totalCount int) {
	aggregateQualityMetricStruct.observe(participationCount, totalCount)
}

func ObserveCommitteeSize(size float64) {
	committeeSize.Set(size)
}

func ObserveActiveValidatorsCount(count int) {
	activeValidatorsCount.Set(float64(count))
}

func ObserveCurrentSlot(slot uint64) {
	if currentSlot.GetValueUint64() != slot {
		aggregateAndProofSignatures.Set(0)
	}
	currentSlot.Set(float64(slot))
}

func ObserveCurrentEpoch(epoch uint64) {
	currentEpoch.Set(float64(epoch))
}

func ObserveFrozenBlocks(count int) {
	frozenBlocks.Set(float64(count))
}

func ObserveFrozenBlobs(count int) {
	frozenBlobs.Set(float64(count))
}

func ObserveTotalInBytes(count int64) {
	totalInBytes.Set(float64(count))
}

func ObserveTotalOutBytes(count int64) {
	totalOutBytes.Set(float64(count))
}

func ObserveBlockImportingLatency(latency time.Time) {
	blockImportingLatency.Set(microToMilli(time.Since(latency).Microseconds()))
}

func ObserveBlobVerificationTime(startTime time.Time) {
	blobVerificationTime.Set(microToMilli(time.Since(startTime).Microseconds()))
}

func ObserveNewPayloadTime(startTime time.Time) {
	executionTime.Set(microToMilli(time.Since(startTime).Microseconds()))
}

func ObserveExecutionClientInsertingBlocks(startTime time.Time) {
	executionClientInsertingBlocks.Set(microToMilli(time.Since(startTime).Microseconds()))
}

func ObserveExecutionClientValidateChain(startTime time.Time) {
	executionClientValidateChain.Set(microToMilli(time.Since(startTime).Microseconds()))
}
