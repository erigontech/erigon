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
	peerBanQueueSize                    = 256
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
	peerBanQueue               chan *sentinelproto.Peer
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
	waiterDone  <-chan struct{}
}

func NewBatchSignatureVerifier(ctx context.Context, sentinel sentinelproto.SentinelClient) *BatchSignatureVerifier {
	verifier := &BatchSignatureVerifier{
		ctx:                        ctx,
		sentinel:                   sentinel,
		attVerifyAndExecute:        make(chan *AggregateVerificationData, 1024),
		aggregateProofVerify:       make(chan *AggregateVerificationData, 1024),
		blsToExecutionChangeVerify: make(chan *AggregateVerificationData, 1024),
		syncContributionVerify:     make(chan *AggregateVerificationData, 1024),
		syncCommitteeMessage:       make(chan *AggregateVerificationData, 1024),
		voluntaryExitVerify:        make(chan *AggregateVerificationData, 1024),
	}
	if sentinel != nil {
		verifier.peerBanQueue = make(chan *sentinelproto.Peer, peerBanQueueSize)
	}
	return verifier
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
	data.result = make(chan error)
	waiterDone := make(chan struct{})
	data.waiterDone = waiterDone
	defer close(waiterDone)
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
	if b.peerBanQueue != nil {
		go b.runPeerBans()
	}
	b.startVerifier(b.attVerifyAndExecute)
	b.startVerifier(b.aggregateProofVerify)
	b.startVerifier(b.blsToExecutionChangeVerify)
	b.startVerifier(b.syncContributionVerify)
	b.startVerifier(b.syncCommitteeMessage)
	b.startVerifier(b.voluntaryExitVerify)
}

func (b *BatchSignatureVerifier) runPeerBans() {
	for {
		select {
		case <-b.ctx.Done():
			return
		case peerToBan := <-b.peerBanQueue:
			if _, err := b.sentinel.BanPeer(b.ctx, peerToBan); err != nil {
				log.Debug("[BatchVerifier] failed to ban peer", "peer", peerToBan.Pid, "err", err)
			}
		}
	}
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
	callbacks := make([]func(), 0, len(aggregateVerificationData))
	var peerToBan *sentinelproto.Peer
	for _, v := range aggregateVerificationData {
		valid, err := blsVerifyMultipleSignatures(v.Signatures, v.SignRoots, v.Pks)
		if err != nil {
			log.Debug("[BatchVerifier] signature verification failed", "err", err)
			reported := v.report(err)
			if peerToBan == nil && !reported {
				peerToBan = v.SendingPeer
			}
			continue
		}

		if !valid {
			reported := v.report(ErrInvalidBlsSignature)
			if peerToBan == nil && !reported && v.SendingPeer != nil {
				peerToBan = v.SendingPeer
				log.Debug("[BatchVerifier] received invalid signature on the gossip", "peer", peerToBan.Pid)
			}
			continue
		}

		v.report(nil)
		callbacks = append(callbacks, v.F)
	}
	if b.peerBanQueue != nil && peerToBan != nil {
		select {
		case b.peerBanQueue <- peerToBan:
		default:
			log.Debug("[BatchVerifier] peer ban queue full, dropping ban", "peer", peerToBan.Pid)
		}
	}
	return callbacks
}

func (v *AggregateVerificationData) report(err error) bool {
	if v.result == nil {
		return false
	}
	select {
	case v.result <- err:
		return true
	case <-v.waiterDone:
		return false
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
