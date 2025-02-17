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
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/btree"
	"golang.org/x/sync/errgroup"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/accounts/abi/bind"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/contracts"
)

type EncryptedTxnsPool struct {
	logger            log.Logger
	config            Config
	sequencerContract *contracts.Sequencer
	blockListener     BlockListener
	mu                sync.RWMutex
	submissions       *btree.BTreeG[EncryptedTxnSubmission]
}

func NewEncryptedTxnsPool(logger log.Logger, config Config, cb bind.ContractBackend, bl BlockListener) *EncryptedTxnsPool {
	sequencerContractAddress := libcommon.HexToAddress(config.SequencerContractAddress)
	sequencerContract, err := contracts.NewSequencer(sequencerContractAddress, cb)
	if err != nil {
		panic(fmt.Errorf("failed to create shutter sequencer contract: %w", err))
	}

	return &EncryptedTxnsPool{
		logger:            logger,
		config:            config,
		sequencerContract: sequencerContract,
		blockListener:     bl,
		submissions:       btree.NewG[EncryptedTxnSubmission](32, EncryptedTxnSubmissionLess),
	}
}

func (etp *EncryptedTxnsPool) Run(ctx context.Context) error {
	defer etp.logger.Info("encrypted txns pool stopped")
	etp.logger.Info("running encrypted txns pool")

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error { return etp.watchSubmissions(ctx) })
	eg.Go(func() error { return etp.loadSubmissionsOnInit(ctx) })
	return eg.Wait()
}

func (etp *EncryptedTxnsPool) Txns(eon EonIndex, from, to TxnIndex, gasLimit uint64) ([]EncryptedTxnSubmission, error) {
	if from > to {
		return nil, fmt.Errorf("invalid encrypted txns requests range: %d >= %d", from, to)
	}

	etp.mu.RLock()
	defer etp.mu.RUnlock()

	fromKey := EncryptedTxnSubmission{EonIndex: eon, TxnIndex: from}
	toKey := EncryptedTxnSubmission{EonIndex: eon, TxnIndex: to}
	count := to - from
	txns := make([]EncryptedTxnSubmission, 0, count)

	var accumulatedGasLimit uint64
	var idxOffset TxnIndex
	var err error
	etp.submissions.AscendRange(fromKey, toKey, func(item EncryptedTxnSubmission) bool {
		accumulatedGasLimit += item.GasLimit.Uint64()
		if accumulatedGasLimit > gasLimit {
			etp.logger.Warn(
				"exceeded gas limit when reading encrypted txns",
				"eonIndex", item.EonIndex,
				"txnIndex", item.TxnIndex,
				"from", from,
				"to", to,
				"gasLimit", gasLimit,
				"accumulatedGasLimit", accumulatedGasLimit,
			)
			return false // break
		}

		nextTxnIndex := from + idxOffset
		if item.TxnIndex < nextTxnIndex {
			// this should never happen - highlights bug in the logic somewhere
			err = fmt.Errorf("unexpected item txn index lt next txn index: %d < %d", item.TxnIndex, nextTxnIndex)
			return false // break
		}

		if item.TxnIndex > nextTxnIndex {
			etp.logger.Warn(
				"encrypted txn gap when reading encrypted txns",
				"gapFrom", nextTxnIndex,
				"gapTo", item.TxnIndex,
				"eonIndex", item.EonIndex,
				"from", from,
				"to", to,
			)
			idxOffset += item.TxnIndex - nextTxnIndex + 1
			return true // continue
		}

		txns = append(txns, item)
		return true // continue
	})

	return txns, err
}

func (etp *EncryptedTxnsPool) watchSubmissions(ctx context.Context) error {
	submissionEventC := make(chan *contracts.SequencerTransactionSubmitted)
	submissionEventSub, err := etp.sequencerContract.WatchTransactionSubmitted(&bind.WatchOpts{}, submissionEventC)
	if err != nil {
		return fmt.Errorf("failed to subscribe to sequencer TransactionSubmitted event: %w", err)
	}

	defer submissionEventSub.Unsubscribe()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-submissionEventSub.Err():
			return err
		case event := <-submissionEventC:
			err := etp.handleEncryptedTxnSubmissionEvent(event)
			if err != nil {
				return fmt.Errorf("failed to handle encrypted txn submission event: %w", err)
			}
		}
	}
}

func (etp *EncryptedTxnsPool) handleEncryptedTxnSubmissionEvent(event *contracts.SequencerTransactionSubmitted) error {
	etp.mu.Lock()
	defer etp.mu.Unlock()

	encryptedTxnSubmission := EncryptedTxnSubmissionFromLogEvent(event)
	etp.logger.Debug(
		"received encrypted txn submission event",
		"eonIndex", encryptedTxnSubmission.EonIndex,
		"txnIndex", encryptedTxnSubmission.TxnIndex,
		"unwind", event.Raw.Removed,
	)

	if event.Raw.Removed {
		etp.submissions.Delete(encryptedTxnSubmission)
		return nil
	}

	//
	// TODO fill gaps
	//
	etp.submissions.ReplaceOrInsert(encryptedTxnSubmission)
	if etp.submissions.Len() > etp.config.MaxPooledEncryptedTxns {
		etp.submissions.DeleteMin()
	}

	return nil
}

func (etp *EncryptedTxnsPool) loadSubmissionsOnInit(ctx context.Context) error {
	blockEventC := make(chan BlockEvent)
	unregister := etp.blockListener.RegisterObserver(func(blockEvent BlockEvent) {
		select {
		case <-ctx.Done():
			return
		case blockEventC <- blockEvent:
			// no-op
		}
	})

	defer unregister()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case blockEvent := <-blockEventC:
			if blockEvent.Unwind {
				continue
			}

			var start uint64
			if blockEvent.LatestBlockNum > etp.config.EncryptedTxnsLookBackLimit {
				start = blockEvent.LatestBlockNum - etp.config.EncryptedTxnsLookBackLimit
			}

			var end uint64
			if blockEvent.LatestBlockNum > blockEvent.BlocksBatchLen {
				// shift the end back, logs for blocks in the batch would have been delivered
				// via log events, so there is no need to fetch those
				end = blockEvent.LatestBlockNum - blockEvent.BlocksBatchLen
			}

			etp.logger.Debug("loading submissions on init", "start", start, "end", end)
			err := etp.loadSubmissions(start, end, alwaysContinueSubmissionsContinuer)
			if err != nil {
				return fmt.Errorf("failed to load submissions on init: %w", err)
			}

			return nil // we are done
		}
	}
}

func (etp *EncryptedTxnsPool) loadSubmissions(start, end uint64, cont submissionsContinuer) error {
	startTime := time.Now()
	defer etp.logger.Debug("loadSubmissions timing", "start", start, "end", end, "duration", time.Since(startTime))
	opts := bind.FilterOpts{
		Start: start,
		End:   &end,
	}

	submissionsIter, err := etp.sequencerContract.FilterTransactionSubmitted(&opts)
	if err != nil {
		return fmt.Errorf("failed to filter submissions from sequencer contract: %w", err)
	}

	defer func() {
		err := submissionsIter.Close()
		if err != nil {
			etp.logger.Error("failed to close submissions iterator", "err", err)
		}
	}()

	etp.mu.Lock()
	defer etp.mu.Unlock()
	for submissionsIter.Next() {
		if !cont(submissionsIter.Event) {
			return nil
		}

		encryptedTxnSubmission := EncryptedTxnSubmissionFromLogEvent(submissionsIter.Event)
		etp.submissions.ReplaceOrInsert(encryptedTxnSubmission)
	}

	return nil
}

type submissionsContinuer func(*contracts.SequencerTransactionSubmitted) bool

func alwaysContinueSubmissionsContinuer(*contracts.SequencerTransactionSubmitted) bool {
	return true
}

func stopAtTxnIndexSubmissionsContinuer(txnIndex uint64) submissionsContinuer {
	return func(event *contracts.SequencerTransactionSubmitted) bool {
		return event.TxIndex >= txnIndex
	}
}
