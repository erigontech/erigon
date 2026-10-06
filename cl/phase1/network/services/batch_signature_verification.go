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
	callbacks, err := b.processSignatureVerification([]*AggregateVerificationData{data})
	for _, callback := range callbacks {
		callback()
	}
	return err
}

func (b *BatchSignatureVerifier) Start() {
	b.startVerifier(b.attVerifyAndExecute)
	b.startVerifier(b.aggregateProofVerify)
	b.startVerifier(b.blsToExecutionChangeVerify)
	b.startVerifier(b.syncContributionVerify)
	b.startVerifier(b.syncCommitteeMessage)
	b.startVerifier(b.voluntaryExitVerify)
}

func (b *BatchSignatureVerifier) startVerifier(incoming chan *AggregateVerificationData) {
	callbacks := make(chan func(), cap(incoming))
	go b.runCallbacks(callbacks)
	go b.start(incoming, callbacks)
}

// When receiving AggregateVerificationData, we simply collect all the signature verification data
// and verify them together - running all the final functions afterwards
func (b *BatchSignatureVerifier) start(incoming chan *AggregateVerificationData, callbacks chan<- func()) {
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
				if !b.processBatch(aggregateVerificationData, callbacks) {
					return
				}
				ticker.Reset(batchCheckInterval)
				// clear the slice
				aggregateVerificationData = make([]*AggregateVerificationData, 0, reservedSize)
			}
		case <-ticker.C:
			if len(aggregateVerificationData) == 0 {
				continue
			}
			if !b.processBatch(aggregateVerificationData, callbacks) {
				return
			}
			// clear the slice
			aggregateVerificationData = make([]*AggregateVerificationData, 0, reservedSize)
		}
	}
}

func (b *BatchSignatureVerifier) processBatch(aggregateVerificationData []*AggregateVerificationData, callbacks chan<- func()) bool {
	fns, err := b.processSignatureVerification(aggregateVerificationData)
	if err != nil {
		log.Debug("[BatchVerifier] batch signature verification failed", "err", err)
	}
	for _, callback := range fns {
		select {
		case callbacks <- callback:
		case <-b.ctx.Done():
			return false
		}
	}
	return true
}

func (b *BatchSignatureVerifier) runCallbacks(callbacks <-chan func()) {
	for {
		select {
		case <-b.ctx.Done():
			return
		case callback := <-callbacks:
			callback()
		}
	}
}

func (b *BatchSignatureVerifier) processSignatureVerification(aggregateVerificationData []*AggregateVerificationData) ([]func(), error) {
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
	if err := b.runBatchVerification(signatures, signRoots, pks); err != nil {
		return b.handleIncorrectSignatures(aggregateVerificationData), err
	}

	for _, v := range aggregateVerificationData {
		v.report(nil)
	}
	return fns, nil
}

// we could locate failing signature with binary search but for now let's choose simplicity over optimisation.
func (b *BatchSignatureVerifier) handleIncorrectSignatures(aggregateVerificationData []*AggregateVerificationData) []func() {
	alreadyBanned := false
	callbacks := make([]func(), 0, len(aggregateVerificationData))
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
		callbacks = append(callbacks, v.F)
	}
	return callbacks
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
