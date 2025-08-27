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
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/proto"
	"github.com/erigontech/erigon/txnprovider/shutter/shuttercfg"
)

var _ txnprovider.TxnProvider = (*Pool)(nil)

type Pool struct {
	logger                  log.Logger
	config                  shuttercfg.Config
	baseTxnProvider         txnprovider.TxnProvider
	blockListener           *BlockListener
	blockTracker            *BlockTracker
	eonTracker              EonTracker
	decryptionKeysListener  *DecryptionKeysListener
	decryptionKeysProcessor *DecryptionKeysProcessor
	encryptedTxnsPool       *EncryptedTxnsPool
	decryptedTxnsPool       *DecryptedTxnsPool
	slotCalculator          SlotCalculator
	stopped                 atomic.Bool
}

func NewPool(
	logger log.Logger,
	config shuttercfg.Config,
	baseTxnProvider txnprovider.TxnProvider,
	contractBackend bind.ContractBackend,
	stateChangesClient stateChangesClient,
	currentBlockNumReader currentBlockNumReader,
	opts ...Option,
) *Pool {
	logger = logger.New("component", "shutter")
	flatOpts := options{}
	for _, opt := range opts {
		opt(&flatOpts)
	}

	blockListener := NewBlockListener(logger, stateChangesClient)
	blockTracker := NewBlockTracker(logger, blockListener, currentBlockNumReader)
	eonTracker := NewKsmEonTracker(logger, config, blockListener, contractBackend)

	var slotCalculator SlotCalculator
	if flatOpts.slotCalculator != nil {
		slotCalculator = flatOpts.slotCalculator
	} else {
		slotCalculator = NewBeaconChainSlotCalculator(config.BeaconChainGenesisTimestamp, config.SecondsPerSlot)
	}

	decryptionKeysValidator := NewDecryptionKeysExtendedValidator(logger, config, slotCalculator, eonTracker)
	var decryptionKeysSource DecryptionKeysSource
	if flatOpts.decryptionKeysSourceFactory != nil {
		decryptionKeysSource = flatOpts.decryptionKeysSourceFactory(decryptionKeysValidator)
	} else {
		decryptionKeysSource = NewPubSubDecryptionKeysSource(logger, config.P2pConfig, decryptionKeysValidator)
	}

	decryptionKeysListener := NewDecryptionKeysListener(logger, config, decryptionKeysSource)
	encryptedTxnsPool := NewEncryptedTxnsPool(logger, config, contractBackend, blockListener)
	decryptedTxnsPool := NewDecryptedTxnsPool()
	decryptionKeysProcessor := NewDecryptionKeysProcessor(
		logger,
		config,
		encryptedTxnsPool,
		decryptedTxnsPool,
		blockListener,
		slotCalculator,
	)
	return &Pool{
		logger:                  logger,
		config:                  config,
		blockListener:           blockListener,
		blockTracker:            blockTracker,
		eonTracker:              eonTracker,
		baseTxnProvider:         baseTxnProvider,
		decryptionKeysListener:  decryptionKeysListener,
		decryptionKeysProcessor: decryptionKeysProcessor,
		encryptedTxnsPool:       encryptedTxnsPool,
		decryptedTxnsPool:       decryptedTxnsPool,
		slotCalculator:          slotCalculator,
	}
}

func (p *Pool) Run(ctx context.Context) error {
	defer func() {
		p.stopped.Store(true)
		p.logger.Info("pool stopped")
	}()

	p.logger.Info("running pool")
	unregisterDkpObserver := p.decryptionKeysListener.RegisterObserver(func(msg *proto.DecryptionKeys) {
		p.decryptionKeysProcessor.Enqueue(msg)
	})
	defer unregisterDkpObserver()

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		err := p.blockListener.Run(ctx)
		if err != nil {
			return fmt.Errorf("block listener issue: %w", err)
		}
		return nil
	})

	eg.Go(func() error {
		err := p.blockTracker.Run(ctx)
		if err != nil {
			return fmt.Errorf("block tracker issue: %w", err)
		}
		return nil
	})

	eg.Go(func() error {
		err := p.eonTracker.Run(ctx)
		if err != nil {
			return fmt.Errorf("eon tracker issue: %w", err)
		}
		return nil
	})

	eg.Go(func() error {
		err := p.decryptionKeysListener.Run(ctx)
		if err != nil {
			return fmt.Errorf("decryption keys listener issue: %w", err)
		}
		return nil
	})

	eg.Go(func() error {
		err := p.decryptionKeysProcessor.Run(ctx)
		if err != nil {
			return fmt.Errorf("decryption keys processor issue: %w", err)
		}
		return nil
	})

	eg.Go(func() error {
		err := p.encryptedTxnsPool.Run(ctx)
		if err != nil {
			return fmt.Errorf("encrypted txns pool issue: %w", err)
		}
		return nil
	})

	return eg.Wait()
}

func (p *Pool) ProvideTxns(ctx context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	if p.stopped.Load() {
		p.logger.Error("cannot provide shutter transactions - pool stopped")
		return p.baseTxnProvider.ProvideTxns(ctx, opts...)
	}

	provideOpts := txnprovider.ApplyProvideOptions(opts...)
	blockTime := provideOpts.BlockTime
	if blockTime == 0 {
		return nil, errors.New("block time option is required by the shutter provider")
	}

	parentBlockNum := provideOpts.ParentBlockNum
	parentBlockWaitTimeout := time.Second * time.Duration(p.slotCalculator.SecondsPerSlot())
	parentBlockWaitCtx, parentBlockWaitCtxCancel := context.WithTimeout(ctx, parentBlockWaitTimeout)
	defer parentBlockWaitCtxCancel()
	parentBlockWaitStart := time.Now()
	err := p.blockTracker.Wait(parentBlockWaitCtx, parentBlockNum)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			parentBlockWaitSecs.ObserveDuration(parentBlockWaitStart)
			parentBlockMissed.Inc()
		}

		return nil, fmt.Errorf("issue while waiting for parent block %d: %w", parentBlockNum, err)
	}

	parentBlockWaitSecs.ObserveDuration(parentBlockWaitStart)
	parentBlockOnTime.Inc()

	eon, ok := p.eonTracker.EonByBlockNum(parentBlockNum)
	if !ok {
		p.logger.Warn("unknown eon for block num, falling back to base txn provider", "blockNum", parentBlockNum)
		return p.baseTxnProvider.ProvideTxns(ctx, opts...)
	}

	slot, err := p.slotCalculator.CalcSlot(blockTime)
	if err != nil {
		return nil, err
	}

	blockNum := parentBlockNum + 1
	decryptionMark := DecryptionMark{Slot: slot, Eon: eon.Index}
	slotStartTime := p.slotCalculator.CalcSlotStartTimestamp(slot)
	cutoffTime := time.Unix(int64(slotStartTime), 0).Add(p.config.MaxDecryptionKeysDelay)
	if time.Now().Before(cutoffTime) {
		decryptionMarkWaitTimeout := time.Until(cutoffTime)
		// enforce the max cap for malicious inputs with slot times far ahead in the future
		decryptionMarkWaitTimeout = min(decryptionMarkWaitTimeout, p.config.MaxDecryptionKeysDelay)
		p.logger.Debug(
			"waiting for decryption keys",
			"slot", slot,
			"blockNum", blockNum,
			"eon", eon.Index,
			"slotStart", slotStartTime,
			"cutoffTime", cutoffTime.Unix(),
			"timeout", decryptionMarkWaitTimeout,
		)

		decryptionMarkWaitCtx, decryptionMarkWaitCtxCancel := context.WithTimeout(ctx, decryptionMarkWaitTimeout)
		defer decryptionMarkWaitCtxCancel()
		decryptionMarkWaitStart := time.Now()
		err = p.decryptedTxnsPool.Wait(decryptionMarkWaitCtx, decryptionMark)
		if err != nil {
			decryptionMarkWaitSecs.ObserveDuration(decryptionMarkWaitStart)
			decryptionMarkMissed.Inc()

			if errors.Is(err, context.DeadlineExceeded) {
				p.logger.Warn(
					"decryption mark wait timeout, falling back to secondary txn provider",
					"slot", slot,
					"blockNum", blockNum,
					"eon", eon.Index,
					"slotStart", slotStartTime,
					"cutoffTime", cutoffTime.Unix(),
					"timeout", decryptionMarkWaitTimeout,
				)

				return p.baseTxnProvider.ProvideTxns(ctx, opts...)
			}

			return nil, err
		}

		decryptionMarkWaitSecs.ObserveDuration(decryptionMarkWaitStart)
		decryptionMarkOnTime.Inc()
	}

	decryptedTxns, ok := p.decryptedTxnsPool.DecryptedTxns(decryptionMark)
	if !ok {
		p.logger.Warn(
			"decryption keys missing, falling back to base txn provider",
			"slot", slot,
			"eon", eon.Index,
			"slotStart", slotStartTime,
			"cutoffTime", cutoffTime.Unix(),
			"blockNum", blockNum,
		)

		decryptionMarkMissed.Inc()
		return p.baseTxnProvider.ProvideTxns(ctx, opts...)
	}

	availableGas := provideOpts.GasTarget
	txnsIdFilter := provideOpts.TxnIdsFilter
	txns := make([]types.Transaction, 0, len(decryptedTxns.Transactions))
	decryptedTxnsGas := uint64(0)
	for _, txn := range decryptedTxns.Transactions {
		if txnsIdFilter != nil && txnsIdFilter.Contains(txn.Hash()) {
			continue
		}
		if txn.GetGasLimit() > availableGas {
			continue
		}
		availableGas -= txn.GetGasLimit()
		decryptedTxnsGas += txn.GetGasLimit()
		txns = append(txns, txn)
		if txnsIdFilter != nil {
			txnsIdFilter.Add(txn.Hash())
		}
	}

	p.logger.Debug("providing decrypted txns", "count", len(txns), "gas", decryptedTxnsGas)
	opts = append(opts, txnprovider.WithGasTarget(availableGas)) // overrides option
	additionalTxns, err := p.baseTxnProvider.ProvideTxns(ctx, opts...)
	if err != nil {
		return nil, err
	}

	p.logger.Debug("providing additional public txns", "count", len(additionalTxns))
	return append(txns, additionalTxns...), nil
}

func (p *Pool) AllEncryptedTxns() []EncryptedTxnSubmission {
	return p.encryptedTxnsPool.AllSubmissions()
}

func (p *Pool) AllDecryptedTxns() []types.Transaction {
	return p.decryptedTxnsPool.AllDecryptedTxns()
}

type Option func(opts *options)

func WithSlotCalculator(slotCalculator SlotCalculator) Option {
	return func(opts *options) {
		opts.slotCalculator = slotCalculator
	}
}

func WithDecryptionKeysSourceFactory(factory DecryptionKeysSourceFactory) Option {
	return func(opts *options) {
		opts.decryptionKeysSourceFactory = factory
	}
}

type options struct {
	slotCalculator              SlotCalculator
	decryptionKeysSourceFactory DecryptionKeysSourceFactory
}
