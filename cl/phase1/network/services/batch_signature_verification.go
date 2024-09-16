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
	batchSignatureVerificationThreshold = 300
)

var (
	batchCheckInterval          = 500 * time.Millisecond
	blsVerifyMultipleSignatures = bls.VerifyMultipleSignatures
)

type BatchSignatureVerifier struct {
	sentinel                    sentinel.SentinelClient
	verifyAndExecute            chan *AggregateVerificationData
	verifyAndExecuteAggregation chan *AggregateVerificationData
	ctx                         context.Context
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

func NewBatchSignatureVerifier(ctx context.Context, sentinel sentinel.SentinelClient) *BatchSignatureVerifier {
	return &BatchSignatureVerifier{
		ctx:      ctx,
		sentinel: sentinel,
		// buffer should be large enough to avoid http call blocking and timeout
		verifyAndExecute:            make(chan *AggregateVerificationData, 1<<16),
		verifyAndExecuteAggregation: make(chan *AggregateVerificationData, 1<<14),
	}
}

// AddVerification schedules new verification
func (b *BatchSignatureVerifier) AddVerification(aggregateVerificationData *AggregateVerificationData) {
	b.verifyAndExecute <- aggregateVerificationData
}

func (b *BatchSignatureVerifier) AddAggregateVerification(AggregateVerificationData *AggregateVerificationData) {
	b.verifyAndExecuteAggregation <- AggregateVerificationData
}

func (b *BatchSignatureVerifier) Start() {
	go b.start(b.verifyAndExecute)
	go b.start(b.verifyAndExecuteAggregation)
}

// When receiving AggregateVerificationData, we simply collect all the signature verification data
// and verify them together - running all the final functions afterwards
func (b *BatchSignatureVerifier) start(inputCh <-chan *AggregateVerificationData) {
	ticker := time.NewTicker(batchCheckInterval)
	defer ticker.Stop()
	aggregateVerificationData := make([]*AggregateVerificationData, 0, batchSignatureVerificationThreshold)
	for {
		select {
		case <-b.ctx.Done():
			return
		case verification := <-inputCh:
			aggregateVerificationData = append(aggregateVerificationData, verification)
			if len(aggregateVerificationData) >= batchSignatureVerificationThreshold {
				b.processSignatureVerification(aggregateVerificationData)
				ticker.Reset(batchCheckInterval)
				// clear the slice
				aggregateVerificationData = aggregateVerificationData[:0]
			}
		case <-ticker.C:
			if len(aggregateVerificationData) == 0 {
				continue
			}
			b.processSignatureVerification(aggregateVerificationData)
			// clear the slice
			aggregateVerificationData = aggregateVerificationData[:0]
		}
	}
}

// processSignatureVerification Runs signature verification for all the signatures altogether, if it
// succeeds we publish all accumulated gossip data. If verification fails, start verifying each AggregateVerificationData one by
// one, publish corresponding gossip data if verification succeeds, if not ban the corresponding peer that sent it.
func (b *BatchSignatureVerifier) processSignatureVerification(aggregateVerificationData []*AggregateVerificationData) {
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
func (b *BatchSignatureVerifier) handleIncorrectSignatures(aggregateVerificationData []*AggregateVerificationData) {
	for _, v := range aggregateVerificationData {
		valid, err := blsVerifyMultipleSignatures(v.Signatures, v.SignRoots, v.Pks)
		if err != nil {
			log.Warn("signature verification failed with the error: " + err.Error())
			if b.sentinel != nil && v.GossipData != nil && v.GossipData.Peer != nil {
				b.sentinel.BanPeer(b.ctx, v.GossipData.Peer)
			}
			continue
		}

		if !valid {
			log.Warn("batch invalid signature")
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

func (b *BatchSignatureVerifier) runBatchVerification(signatures [][]byte, signRoots [][]byte, pks [][]byte, fns []func()) error {
	valid, err := blsVerifyMultipleSignatures(signatures, signRoots, pks)
	if err != nil {
		return errors.New("batch signature verification failed with the error: " + err.Error())
	}

	if !valid {
		return errors.New("batch invalid signature")
	}

	return nil
}
