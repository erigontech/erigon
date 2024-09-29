package monitor

import (
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

	// Verification metrics
	fullBlockProcessingTime        = metrics.GetOrCreateHistogram("full_block_processing_time")
	attestationBlockProcessingTime = metrics.GetOrCreateHistogram("attestation_block_processing_time")
	batchVerificationThroughput    = metrics.GetOrCreateSummary("aggregation_per_signature")
)

type batchVerificationThroughputMetric struct {
	totalVerified      uint64
	currentAverageSecs float64
	mu                 sync.Mutex
}

var batchVerificationThroughputMetricStruct = &batchVerificationThroughputMetric{}

func (b *batchVerificationThroughputMetric) observe(t time.Duration, totalSigs int) float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	elapsedInMillisecs := float64(t.Microseconds() / 1000)
	if b.totalVerified == 0 {
		b.currentAverageSecs = elapsedInMillisecs
	} else {
		b.currentAverageSecs = (b.currentAverageSecs*float64(b.totalVerified) + elapsedInMillisecs) / float64(b.totalVerified+uint64(totalSigs))
	}
	b.totalVerified += uint64(totalSigs)
	return b.currentAverageSecs
}

// ObserveAttestHit increments the attestation hit metric
func ObserveAttestationBlockProcessingTime(d time.Duration) {
	attestationBlockProcessingTime.Observe(d.Seconds())
}

// ObserveFullBlockProcessingTime increments the full block processing time metric
func ObserveFullBlockProcessingTime(d time.Duration) {
	fullBlockProcessingTime.Observe(d.Seconds())
}

// ObserveBatchVerificationThroughput increments the batch verification throughput metric
func ObserveBatchVerificationThroughput(d time.Duration, totalSigs int) {
	batchVerificationThroughput.Observe(batchVerificationThroughputMetricStruct.observe(d, totalSigs))
}
