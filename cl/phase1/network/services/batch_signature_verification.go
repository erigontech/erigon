package services

import (
	"context"
	"errors"
	"time"

	"github.com/Giulio2002/bls"
	sentinel "github.com/erigontech/erigon-lib/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon-lib/log/v3"
)

const (
	BatchSignatureVerificationThreshold = 100
)

var (
	batchCheckInterval          = 50 * time.Millisecond
	blsVerifyMultipleSignatures = bls.VerifyMultipleSignatures
)

type BatchVerifier struct {
	sentinel         sentinel.SentinelClient
	verifyAndExecute chan *AggregateVerificationData
	ctx              context.Context
	size             uint64
}

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

func NewBatchVerifier(ctx context.Context, sentinel sentinel.SentinelClient) *BatchVerifier {
	return &BatchVerifier{
		ctx:              ctx,
		sentinel:         sentinel,
		verifyAndExecute: make(chan *AggregateVerificationData, 128),
	}
}

// AddVerification schedules new verification
func (b *BatchVerifier) AddVerification(aggregateVerificationData *AggregateVerificationData) {
	b.verifyAndExecute <- aggregateVerificationData
}

// When receiving AggregateVerificationData, we simply collect all the signature verification data
// and verify them together - running all the final functions afterwards
func (b *BatchVerifier) Start() {
	ticker := time.NewTicker(batchCheckInterval)
	aggregateVerificationData := make([]*AggregateVerificationData, 0, 128)
	for {
		select {
		case <-b.ctx.Done():
			return
		case verification := <-b.verifyAndExecute:
			b.size += uint64(len(verification.Signatures))
			aggregateVerificationData = append(aggregateVerificationData, verification)
			if b.size > BatchSignatureVerificationThreshold {
				b.processSignatureVerification(aggregateVerificationData)
				aggregateVerificationData = make([]*AggregateVerificationData, 0, 128)
				ticker.Reset(batchCheckInterval)
			}
		case <-ticker.C:
			if len(aggregateVerificationData) != 0 {
				b.processSignatureVerification(aggregateVerificationData)
				aggregateVerificationData = make([]*AggregateVerificationData, 0, 128)
				ticker.Reset(batchCheckInterval)
			}
		}
	}
}

// processSignatureVerification Runs signature verification for all the signatures altogether, if it
// succeeds we publish all accumulated gossip data. If verification fails, start verifying each AggregateVerificationData one by
// one, publish corresponding gossip data if verification succeeds, if not ban the corresponding peer that sent it.
func (b *BatchVerifier) processSignatureVerification(aggregateVerificationData []*AggregateVerificationData) {
	signatures, signRoots, pks, fns :=
		make([][]byte, 0, 128),
		make([][]byte, 0, 128),
		make([][]byte, 0, 128),
		make([]func(), 0, 64)

	for _, v := range aggregateVerificationData {
		signatures, signRoots, pks, fns =
			append(signatures, v.Signatures...),
			append(signRoots, v.SignRoots...),
			append(pks, v.Pks...),
			append(fns, v.F)
	}
	if err := b.runBatchVerification(signatures, signRoots, pks, fns); err != nil {
		b.handleIncorrectSignatures(aggregateVerificationData)
		log.Warn(err.Error())
		return
	}

	// Everything went well, run corresponding Fs and send all the gossip data to the network
	for _, v := range aggregateVerificationData {
		v.F()
		if b.sentinel != nil && v.GossipData != nil {
			if _, err := b.sentinel.PublishGossip(b.ctx, v.GossipData); err != nil {
				log.Warn("failed publish gossip", "err", err)
			}
		}
	}
}

// we could locate failing signature with binary search but for now let's choose simplicity over optimisation.
func (b *BatchVerifier) handleIncorrectSignatures(aggregateVerificationData []*AggregateVerificationData) {
	for _, v := range aggregateVerificationData {
		valid, err := blsVerifyMultipleSignatures(v.Signatures, v.SignRoots, v.Pks)
		if err != nil {
			log.Warn("attestation_service signature verification failed with the error: " + err.Error())
			if b.sentinel != nil && v.GossipData != nil && v.GossipData.Peer != nil {
				b.sentinel.BanPeer(b.ctx, v.GossipData.Peer)
			}
			continue
		}

		if !valid {
			log.Warn("attestation_service signature verification failed")
			if b.sentinel != nil && v.GossipData != nil && v.GossipData.Peer != nil {
				b.sentinel.BanPeer(b.ctx, v.GossipData.Peer)
			}
			continue
		}

		// run corresponding function and publish the gossip into the network
		v.F()

		if b.sentinel != nil && v.GossipData != nil {
			if _, err := b.sentinel.PublishGossip(b.ctx, v.GossipData); err != nil {
				log.Warn("failed publish gossip", "err", err)
			}
		}
	}
}

func (b *BatchVerifier) runBatchVerification(signatures [][]byte, signRoots [][]byte, pks [][]byte, fns []func()) error {
	valid, err := blsVerifyMultipleSignatures(signatures, signRoots, pks)
	if err != nil {
		return errors.New("attestation_service batch signature verification failed with the error: " + err.Error())
	}

	if !valid {
		return errors.New("attestation_service signature verification failed")
	}

	return nil
}
