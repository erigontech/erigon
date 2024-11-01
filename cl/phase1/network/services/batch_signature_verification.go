package services

import (
	"context"
	"errors"
	"time"

	"github.com/Giulio2002/bls"
	sentinel "github.com/erigontech/erigon-lib/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/monitor"
)

const (
	batchSignatureVerificationThreshold = 300
	reservedSize                        = 512
)

var (
	batchCheckInterval          = 500 * time.Millisecond
	blsVerifyMultipleSignatures = bls.VerifyMultipleSignatures
)

type BatchSignatureVerifier struct {
	sentinel                   sentinel.SentinelClient
	attVerifyAndExecute        chan *AggregateVerificationData
	aggregateProofVerify       chan *AggregateVerificationData
	blsToExecutionChangeVerify chan *AggregateVerificationData
	voluntaryExitVerify        chan *AggregateVerificationData
	ctx                        context.Context
}

var ErrInvalidBlsSignature = errors.New("invalid bls signature")

// each AggregateVerification request has sentinel.SentinelClient and *sentinel.GossipData
// to make sure that we can validate it separately and in case of failure we ban corresponding
// GossipData.Peer or simply run F and publish GossipData in case signature verification succeeds.
type AggregateVerificationData struct {
	Signatures [][]byte
	SignRoots  [][]byte
	Pks        [][]byte
	F          func()
	GossipData *sentinel.GossipData
}

func NewBatchSignatureVerifier(ctx context.Context, sentinel sentinel.SentinelClient) *BatchSignatureVerifier {
	return &BatchSignatureVerifier{
		ctx:                        ctx,
		sentinel:                   sentinel,
		attVerifyAndExecute:        make(chan *AggregateVerificationData, 1024),
		aggregateProofVerify:       make(chan *AggregateVerificationData, 1024),
		blsToExecutionChangeVerify: make(chan *AggregateVerificationData, 1024),
		voluntaryExitVerify:        make(chan *AggregateVerificationData, 1024),
	}
}

// AsyncVerifyAttestation schedules new verification
func (b *BatchSignatureVerifier) AsyncVerifyAttestation(data *AggregateVerificationData) {
	b.attVerifyAndExecute <- data
}

func (b *BatchSignatureVerifier) AsyncVerifyAggregateProof(data *AggregateVerificationData) {
	b.aggregateProofVerify <- data
}

func (b *BatchSignatureVerifier) AsyncVerifyBlsToExecutionChange(data *AggregateVerificationData) {
	b.blsToExecutionChangeVerify <- data
}

func (b *BatchSignatureVerifier) AsyncVerifyVoluntaryExit(data *AggregateVerificationData) {
	b.voluntaryExitVerify <- data
}

func (b *BatchSignatureVerifier) ImmediateVerification(data *AggregateVerificationData) error {
	return b.processSignatureVerification([]*AggregateVerificationData{data})
}

func (b *BatchSignatureVerifier) Start() {
	// separate goroutines for each type of verification
	go b.start(b.attVerifyAndExecute)
	go b.start(b.aggregateProofVerify)
	go b.start(b.blsToExecutionChangeVerify)
	go b.start(b.voluntaryExitVerify)
}

// When receiving AggregateVerificationData, we simply collect all the signature verification data
// and verify them together - running all the final functions afterwards
func (b *BatchSignatureVerifier) start(incoming chan *AggregateVerificationData) {
	ticker := time.NewTicker(batchCheckInterval)
	defer ticker.Stop()
	aggregateVerificationData := make([]*AggregateVerificationData, 0, reservedSize)
	for {
		select {
		case <-b.ctx.Done():
			return
		case verification := <-incoming:
			aggregateVerificationData = append(aggregateVerificationData, verification)
			if len(aggregateVerificationData) >= batchSignatureVerificationThreshold {
				b.processSignatureVerification(aggregateVerificationData)
				ticker.Reset(batchCheckInterval)
				// clear the slice
				aggregateVerificationData = make([]*AggregateVerificationData, 0, reservedSize)
			}
		case <-ticker.C:
			if len(aggregateVerificationData) == 0 {
				continue
			}
			b.processSignatureVerification(aggregateVerificationData)
			// clear the slice
			aggregateVerificationData = make([]*AggregateVerificationData, 0, reservedSize)
		}
	}
}

// processSignatureVerification Runs signature verification for all the signatures altogether, if it
// succeeds we publish all accumulated gossip data. If verification fails, start verifying each AggregateVerificationData one by
// one, publish corresponding gossip data if verification succeeds, if not ban the corresponding peer that sent it.
func (b *BatchSignatureVerifier) processSignatureVerification(aggregateVerificationData []*AggregateVerificationData) error {
	signatures, signRoots, pks, fns :=
		make([][]byte, 0, reservedSize),
		make([][]byte, 0, reservedSize),
		make([][]byte, 0, reservedSize),
		make([]func(), 0, reservedSize)

	for _, v := range aggregateVerificationData {
		signatures, signRoots, pks, fns =
			append(signatures, v.Signatures...),
			append(signRoots, v.SignRoots...),
			append(pks, v.Pks...),
			append(fns, v.F)
	}
	if err := b.runBatchVerification(signatures, signRoots, pks, fns); err != nil {
		b.handleIncorrectSignatures(aggregateVerificationData)
		return err
	}

	// Everything went well, run corresponding Fs and send all the gossip data to the network
	for _, v := range aggregateVerificationData {
		v.F()
		if b.sentinel != nil && v.GossipData != nil {
			if _, err := b.sentinel.PublishGossip(b.ctx, v.GossipData); err != nil {
				log.Debug("failed to publish gossip", "err", err)
				return err
			}
		}
	}
	return nil
}

// we could locate failing signature with binary search but for now let's choose simplicity over optimisation.
func (b *BatchSignatureVerifier) handleIncorrectSignatures(aggregateVerificationData []*AggregateVerificationData) {
	for _, v := range aggregateVerificationData {
		valid, err := blsVerifyMultipleSignatures(v.Signatures, v.SignRoots, v.Pks)
		if err != nil {
			log.Crit("[BatchVerifier] signature verification failed with the error: " + err.Error())
			if b.sentinel != nil && v.GossipData != nil && v.GossipData.Peer != nil {
				b.sentinel.BanPeer(b.ctx, v.GossipData.Peer)
			}
			continue
		}

		if !valid {
			if v.GossipData == nil {
				continue
			}
			log.Debug("[BatchVerifier] received invalid signature on the gossip", "topic", v.GossipData.Name)
			if b.sentinel != nil && v.GossipData != nil && v.GossipData.Peer != nil {
				b.sentinel.BanPeer(b.ctx, v.GossipData.Peer)
			}
			continue
		}

		// run corresponding function and publish the gossip into the network
		v.F()
		if b.sentinel != nil && v.GossipData != nil {
			if _, err := b.sentinel.PublishGossip(b.ctx, v.GossipData); err != nil {
				log.Debug("failed to publish gossip", "err", err)
			}
		}
	}
}

func (b *BatchSignatureVerifier) runBatchVerification(signatures [][]byte, signRoots [][]byte, pks [][]byte, fns []func()) error {
	start := time.Now()
	valid, err := blsVerifyMultipleSignatures(signatures, signRoots, pks)
	if err != nil {
		return errors.New("batch signature verification failed with the error: " + err.Error())
	}
	monitor.ObserveBatchVerificationThroughput(time.Since(start), len(signatures))

	if !valid {
		return ErrInvalidBlsSignature
	}

	return nil
}
