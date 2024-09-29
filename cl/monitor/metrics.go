package monitor

import "github.com/erigontech/erigon-lib/metrics"

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
)
