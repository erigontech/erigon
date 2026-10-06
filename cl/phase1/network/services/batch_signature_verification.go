package services

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

const (
	batchSignatureVerificationThreshold = 50
	reservedSize                        = 512
	batchCheckInterval                  = 50 * time.Millisecond
)

var blsVerifyMultipleSignatures = bls.VerifyMultipleSignatures

type BatchSignatureVerifier struct {
	sentinel                   sentinelproto.SentinelClient
	attVerifyAndExecute        chan *AggregateVerificationData
	aggregateProofVerify       chan *AggregateVerificationData
	blsToExecutionChangeVerify chan *AggregateVerificationData
	syncContributionVerify     chan *AggregateVerificationData
	syncCommitteeMessage       chan *AggregateVerificationData
	voluntaryExitVerify        chan *AggregateVerificationData
	ctx                        context.Context
}

var ErrInvalidBlsSignature = errors.New("invalid bls signature")

type AggregateVerificationData struct {
	Signatures  [][]byte
	SignRoots   [][]byte
	Pks         [][]byte
	F           func()
	SendingPeer *sentinelproto.Peer
	result      chan error
}

func NewBatchSignatureVerifier(ctx context.Context, sentinel sentinelproto.SentinelClient) *BatchSignatureVerifier {
	return &BatchSignatureVerifier{
		ctx:                        ctx,
		sentinel:                   sentinel,
		attVerifyAndExecute:        make(chan *AggregateVerificationData, 1024),
		aggregateProofVerify:       make(chan *AggregateVerificationData, 1024),
		blsToExecutionChangeVerify: make(chan *AggregateVerificationData, 1024),
		syncContributionVerify:     make(chan *AggregateVerificationData, 1024),
		syncCommitteeMessage:       make(chan *AggregateVerificationData, 1024),
		voluntaryExitVerify:        make(chan *AggregateVerificationData, 1024),
	}
}

func (b *BatchSignatureVerifier) VerifyAttestation(ctx context.Context, data *AggregateVerificationData) error {
	return b.verifyAndWait(ctx, b.attVerifyAndExecute, data)
}

func (b *BatchSignatureVerifier) VerifyAggregateProof(ctx context.Context, data *AggregateVerificationData) error {
	return b.verifyAndWait(ctx, b.aggregateProofVerify, data)
}

func (b *BatchSignatureVerifier) VerifyBlsToExecutionChange(ctx context.Context, data *AggregateVerificationData) error {
	return b.verifyAndWait(ctx, b.blsToExecutionChangeVerify, data)
}

func (b *BatchSignatureVerifier) AsyncVerifySyncContribution(data *AggregateVerificationData) {
	b.syncContributionVerify <- data
}

func (b *BatchSignatureVerifier) AsyncVerifySyncCommitteeMessage(data *AggregateVerificationData) {
	b.syncCommitteeMessage <- data
}

func (b *BatchSignatureVerifier) VerifyVoluntaryExit(ctx context.Context, data *AggregateVerificationData) error {
	return b.verifyAndWait(ctx, b.voluntaryExitVerify, data)
}

// verifyAndWait blocks until the entry's batch is verified so libp2p forwards gossip only after signature checks.
func (b *BatchSignatureVerifier) verifyAndWait(ctx context.Context, queue chan<- *AggregateVerificationData, data *AggregateVerificationData) error {
	data.result = make(chan error, 1)
	select {
	case queue <- data:
	case <-ctx.Done():
		return fmt.Errorf("%w: signature verification canceled: %w", ErrIgnore, ctx.Err())
	case <-b.ctx.Done():
		return fmt.Errorf("%w: batch signature verifier stopped: %w", ErrIgnore, b.ctx.Err())
	}

	select {
	case err := <-data.result:
		return err
	case <-ctx.Done():
		return fmt.Errorf("%w: signature verification canceled: %w", ErrIgnore, ctx.Err())
	case <-b.ctx.Done():
		return fmt.Errorf("%w: batch signature verifier stopped: %w", ErrIgnore, b.ctx.Err())
	}
}

func (b *BatchSignatureVerifier) ImmediateVerification(data *AggregateVerificationData) error {
	return b.processSignatureVerification([]*AggregateVerificationData{data})
}

func (b *BatchSignatureVerifier) Start() {
	// separate goroutines for each type of verification
	go b.start(b.attVerifyAndExecute)
	go b.start(b.aggregateProofVerify)
	go b.start(b.blsToExecutionChangeVerify)
	go b.start(b.syncContributionVerify)
	go b.start(b.syncCommitteeMessage)
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
				// Failing signatures are already reprocessed and their senders banned
				// inside processSignatureVerification; the error here is diagnostic only.
				if err := b.processSignatureVerification(aggregateVerificationData); err != nil {
					log.Debug("[BatchVerifier] batch signature verification failed", "err", err)
				}
				ticker.Reset(batchCheckInterval)
				// clear the slice
				aggregateVerificationData = make([]*AggregateVerificationData, 0, reservedSize)
			}
		case <-ticker.C:
			if len(aggregateVerificationData) == 0 {
				continue
			}
			if err := b.processSignatureVerification(aggregateVerificationData); err != nil {
				log.Debug("[BatchVerifier] batch signature verification failed", "err", err)
			}
			// clear the slice
			aggregateVerificationData = make([]*AggregateVerificationData, 0, reservedSize)
		}
	}
}

func (b *BatchSignatureVerifier) processSignatureVerification(aggregateVerificationData []*AggregateVerificationData) error {
	signatures, signRoots, pks :=
		make([][]byte, 0, reservedSize),
		make([][]byte, 0, reservedSize),
		make([][]byte, 0, reservedSize)

	for _, v := range aggregateVerificationData {
		signatures, signRoots, pks =
			append(signatures, v.Signatures...),
			append(signRoots, v.SignRoots...),
			append(pks, v.Pks...)
	}
	if err := b.runBatchVerification(signatures, signRoots, pks); err != nil {
		b.handleIncorrectSignatures(aggregateVerificationData)
		return err
	}

	for _, v := range aggregateVerificationData {
		v.report(nil)
	}
	for _, v := range aggregateVerificationData {
		v.F()
	}
	return nil
}

// we could locate failing signature with binary search but for now let's choose simplicity over optimisation.
func (b *BatchSignatureVerifier) handleIncorrectSignatures(aggregateVerificationData []*AggregateVerificationData) {
	alreadyBanned := false
	for _, v := range aggregateVerificationData {
		valid, err := blsVerifyMultipleSignatures(v.Signatures, v.SignRoots, v.Pks)
		if err != nil {
			log.Crit("[BatchVerifier] signature verification failed with the error: " + err.Error())
			v.report(err)
			if b.sentinel != nil && v.SendingPeer != nil {
				if _, err := b.sentinel.BanPeer(b.ctx, v.SendingPeer); err != nil {
					log.Debug("[BatchVerifier] failed to ban peer", "peer", v.SendingPeer.Pid, "err", err)
				}
			}
			continue
		}

		if !valid {
			v.report(ErrInvalidBlsSignature)
			if v.SendingPeer == nil || alreadyBanned {
				continue
			}
			log.Debug("[BatchVerifier] received invalid signature on the gossip", "peer", v.SendingPeer.Pid)
			if b.sentinel != nil && v.SendingPeer != nil {
				if _, err := b.sentinel.BanPeer(b.ctx, v.SendingPeer); err != nil {
					log.Debug("[BatchVerifier] failed to ban peer", "peer", v.SendingPeer.Pid, "err", err)
				}
				alreadyBanned = true
			}
			continue
		}

		v.report(nil)
		v.F()
	}
}

func (v *AggregateVerificationData) report(err error) {
	if v.result != nil {
		v.result <- err
	}
}

func (b *BatchSignatureVerifier) runBatchVerification(signatures, signRoots, pks [][]byte) error {
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
