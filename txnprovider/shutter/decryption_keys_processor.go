// Copyright 2025 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

//go:build !abigen

package shutter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/erigontech/erigon/txnprovider/shutter/shuttercfg"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/eth/ethconfig/estimate"
	shuttercrypto "github.com/erigontech/erigon/txnprovider/shutter/internal/crypto"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/proto"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

type DecryptionKeysProcessor struct {
	logger            log.Logger
	config            shuttercfg.Config
	encryptedTxnsPool *EncryptedTxnsPool
	decryptedTxnsPool *DecryptedTxnsPool
	blockListener     *BlockListener
	slotCalculator    SlotCalculator
	txnParseCtxMu     sync.Mutex
	txnParseCtx       *txpool.TxnParseContext
	queue             chan *proto.DecryptionKeys
	processed         mapset.Set[ProcessedMark]
}

func NewDecryptionKeysProcessor(
	logger log.Logger,
	config shuttercfg.Config,
	encryptedTxnsPool *EncryptedTxnsPool,
	decryptedTxnsPool *DecryptedTxnsPool,
	blockListener *BlockListener,
	slotCalculator SlotCalculator,
) *DecryptionKeysProcessor {
	return &DecryptionKeysProcessor{
		logger:            logger,
		config:            config,
		encryptedTxnsPool: encryptedTxnsPool,
		decryptedTxnsPool: decryptedTxnsPool,
		blockListener:     blockListener,
		slotCalculator:    slotCalculator,
		txnParseCtx:       txpool.NewTxnParseContext(*config.ChainId).ChainIDRequired(),
		queue:             make(chan *proto.DecryptionKeys),
		processed:         mapset.NewSet[ProcessedMark](),
	}
}

func (dkp *DecryptionKeysProcessor) Enqueue(msg *proto.DecryptionKeys) {
	dkp.queue <- msg
}

func (dkp *DecryptionKeysProcessor) Run(ctx context.Context) error {
	defer dkp.logger.Info("decryption keys processor stopped")
	dkp.logger.Info("running decryption keys processor")

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		err := dkp.processKeys(ctx)
		if err != nil {
			return fmt.Errorf("decryption keys processing loop: %w", err)
		}
		return nil
	})

	eg.Go(func() error {
		err := dkp.cleanupLoop(ctx)
		if err != nil {
			return fmt.Errorf("decryption keys processor cleanup loop: %w", err)
		}
		return nil
	})

	return eg.Wait()
}

func (dkp *DecryptionKeysProcessor) processKeys(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-dkp.queue:
			err := dkp.process(msg)
			if err != nil {
				return err
			}
		}
	}
}

func (dkp *DecryptionKeysProcessor) process(msg *proto.DecryptionKeys) error {
	processingTimeStart := time.Now()
	slot := msg.GetGnosis().Slot
	eonIndex := EonIndex(msg.Eon)
	dkp.logger.Debug(
		"processing decryption keys message",
		"instanceId", msg.InstanceId,
		"eonIndex", eonIndex,
		"slot", slot,
		"txnIndex", msg.GetGnosis().TxPointer,
		"keys", len(msg.Keys),
	)

	keys := msg.Keys[1:] // skip placeholder (we can safely do this because msg has already been validated)
	from := TxnIndex(msg.GetGnosis().TxPointer)
	to := from + TxnIndex(len(keys)) // [from,to)
	processedMark := ProcessedMark{Slot: slot, Eon: eonIndex, From: from, To: to}
	if dkp.processed.Contains(processedMark) {
		dkp.logger.Debug(
			"skipping decryption keys message - already processed",
			"slot", processedMark.Slot,
			"eonIndex", processedMark.Eon,
			"from", processedMark.From,
			"to", processedMark.To,
		)
		return nil
	}

	encryptedTxns, err := dkp.encryptedTxnsPool.Txns(eonIndex, from, to, dkp.config.EncryptedGasLimit)
	if err != nil {
		return err
	}

	txnIndexToKey := make(map[TxnIndex]*proto.Key, len(keys))
	for i, key := range keys {
		txnIndexToKey[from+TxnIndex(i)] = key
	}

	var eg errgroup.Group
	eg.SetLimit(estimate.AlmostAllCPUs())
	txns := make([]types.Transaction, len(encryptedTxns))
	var totalGasLimit atomic.Uint64
	var totalBytes atomic.Int64
	for i, encryptedTxn := range encryptedTxns {
		eg.Go(func() error {
			txn, err := dkp.decryptTxn(txnIndexToKey, encryptedTxn)
			if err != nil {
				dkp.logger.Debug(
					"failed to decrypt transaction - skipping",
					"slot", slot,
					"eonIndex", eonIndex,
					"txnIndex", encryptedTxn.TxnIndex,
					"err", err,
				)
				// we do not return err here since as per protocol we skip bad decryption
				// we also do not want to interrupt other decryption goroutines
				return nil
			}

			txns[i] = txn
			totalGasLimit.Add(encryptedTxn.GasLimit.Uint64())
			// note this is rlp encoding size and so it doesn't reflect 1:1 mem size occupied by the go struct,
			// but it gives us somewhat of an estimate - this is probably ok for our metrics for now
			txnSize := txn.EncodingSize()
			totalBytes.Add(int64(txnSize))
			decryptedTxnSizeBytes.Observe(float64(txnSize))
			return nil
		})
	}

	err = eg.Wait()
	if err != nil {
		return err
	}

	// txns that didn't pass decryption and other checks will be left nil -> filter those out
	filteredTxns := make([]types.Transaction, 0, len(txns))
	for _, txn := range txns {
		if txn == nil {
			continue
		}

		filteredTxns = append(filteredTxns, txn)
	}

	decryptionMark := DecryptionMark{Slot: slot, Eon: eonIndex}
	txnBatch := TxnBatch{Transactions: filteredTxns, TotalGasLimit: totalGasLimit.Load(), TotalBytes: totalBytes.Load()}
	dkp.decryptedTxnsPool.AddDecryptedTxns(decryptionMark, txnBatch)
	dkp.processed.Add(processedMark)

	failures := len(txns) - len(filteredTxns)
	if failures > 0 {
		dkp.logger.Debug("decryption failures for decryption keys", "slot", slot, "eonIndex", eonIndex, "count", failures)
	}

	decryptionKeysCount.Observe(float64(len(keys)))
	decryptionKeysGas.Observe(float64(totalGasLimit.Load()))
	decryptionSuccesses.Add(float64(len(filteredTxns)))
	decryptionFailures.Add(float64(failures))
	decryptionKeysProcessingTimeSecs.ObserveDuration(processingTimeStart)
	slotStartTime := time.Unix(int64(dkp.slotCalculator.CalcSlotStartTimestamp(slot)), 0)
	decryptionKeysSlotDelaySecs.ObserveDuration(slotStartTime)
	return nil
}

func (dkp *DecryptionKeysProcessor) decryptTxn(keys map[TxnIndex]*proto.Key, sub EncryptedTxnSubmission) (types.Transaction, error) {
	dkp.logger.Debug("decrypting txn", "txnIndex", sub.TxnIndex)
	key, ok := keys[sub.TxnIndex]
	if !ok {
		return nil, fmt.Errorf("key not found for txn index %d", sub.TxnIndex)
	}

	epochSecretKey, err := EpochSecretKeyFromBytes(key.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to decode epoch secret key during txn decryption: %w", err)
	}

	submissionIdentityPreimage := sub.IdentityPreimageBytes()
	if !bytes.Equal(key.IdentityPreimage, submissionIdentityPreimage) {
		return nil, identityPreimageMismatchErr(key.IdentityPreimage, submissionIdentityPreimage)
	}

	encryptedMessage := new(shuttercrypto.EncryptedMessage)
	err = encryptedMessage.Unmarshal(sub.EncryptedTransaction)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal encrypted message: %w", err)
	}

	decryptedMessage, err := encryptedMessage.Decrypt(epochSecretKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt message: %w", err)
	}

	txnSlot, sender, err := dkp.threadSafeParseTxn(decryptedMessage)
	if err != nil {
		return nil, fmt.Errorf("failed to parse decrypted transaction: %w", err)
	}

	txn, err := types.DecodeWrappedTransaction(txnSlot.Rlp)
	if err != nil {
		return nil, fmt.Errorf("failed to decode transaction: %w", err)
	}

	if txn.Type() == types.BlobTxType {
		return nil, errors.New("blob txns not allowed in shutter")
	}

	if subGasLimit := sub.GasLimit.Uint64(); txn.GetGasLimit() != subGasLimit {
		return nil, fmt.Errorf("txn gas limit mismatch: txn=%d, encryptedTxnSubmission=%d", txn.GetGasLimit(), subGasLimit)
	}

	txn.SetSender(sender)
	return txn, nil
}

// threadSafeParseTxn is needed because txnParseCtx.ParseTransaction is not thread safe
func (dkp *DecryptionKeysProcessor) threadSafeParseTxn(rlp []byte) (*txpool.TxnSlot, common.Address, error) {
	dkp.txnParseCtxMu.Lock()
	defer dkp.txnParseCtxMu.Unlock()

	var txnSlot txpool.TxnSlot
	var sender common.Address
	_, err := dkp.txnParseCtx.ParseTransaction(rlp, 0, &txnSlot, sender[:], true, true, nil)
	if err != nil {
		return nil, common.Address{}, err
	}

	return &txnSlot, sender, nil
}

func (dkp *DecryptionKeysProcessor) cleanupLoop(ctx context.Context) error {
	blockEventC := make(chan BlockEvent)
	unregister := dkp.blockListener.RegisterObserver(func(event BlockEvent) {
		select {
		case <-ctx.Done():
		case blockEventC <- event:
		}
	})
	defer unregister()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case blockEvent := <-blockEventC:
			err := dkp.processBlockEventCleanup(blockEvent)
			if err != nil {
				return err
			}
		}
	}
}

func (dkp *DecryptionKeysProcessor) processBlockEventCleanup(blockEvent BlockEvent) error {
	slot, err := dkp.slotCalculator.CalcSlot(blockEvent.LatestBlockTime)
	if err != nil {
		return err
	}

	// decryptedTxnsPool is not re-org aware since it is slot based - we can clean it up straight away
	decryptedMarkDeletions, decryptedTxnsDeletions := dkp.decryptedTxnsPool.DeleteDecryptedTxnsUpToSlot(slot)
	if decryptedMarkDeletions > 0 {
		dkp.logger.Debug(
			"cleaned up decrypted txns up to",
			"slot", slot,
			"decryptedMarkDeletions", decryptedMarkDeletions,
			"decryptedTxnsDeletions", decryptedTxnsDeletions,
		)
	}

	// encryptedTxnPool on the other hand may be sensitive to re-orgs - clean it up with some delay
	var cleanUpToSlot uint64
	if slot > dkp.config.ReorgDepthAwareness {
		cleanUpToSlot = slot - dkp.config.ReorgDepthAwareness
	}

	var cleanUpMarks []ProcessedMark
	dkp.processed.Each(func(mark ProcessedMark) bool {
		if mark.Slot <= cleanUpToSlot {
			cleanUpMarks = append(cleanUpMarks, mark)
		}
		return false // continue, want to check all
	})

	for _, mark := range cleanUpMarks {
		dkp.processed.Remove(mark)
		dkp.encryptedTxnsPool.DeleteUpTo(mark.Eon, mark.To+1)
	}

	return nil
}

func identityPreimageMismatchErr(keyIpBytes, submissionIpBytes []byte) error {
	err := errors.New("identity preimage mismatch")

	keyIp, ipErr := IdentityPreimageFromBytes(keyIpBytes)
	if ipErr != nil {
		err = fmt.Errorf("%w: keyIp=%s", err, ipErr)
	} else {
		err = fmt.Errorf("%w: keyIp=%s", err, keyIp)
	}

	submissionIp, ipErr := IdentityPreimageFromBytes(submissionIpBytes)
	if ipErr != nil {
		err = fmt.Errorf("%w: submissionIp=%s", err, ipErr)
	} else {
		err = fmt.Errorf("%w: submissionIp=%s", err, submissionIp)
	}

	return err
}

type ProcessedMark struct {
	Slot uint64
	Eon  EonIndex
	From TxnIndex
	To   TxnIndex
}
