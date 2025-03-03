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
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/accounts/abi/bind"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/txnprovider"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/proto"
)

var _ txnprovider.TxnProvider = (*Pool)(nil)

type Pool struct {
	logger                  log.Logger
	config                  Config
	secondaryTxnProvider    txnprovider.TxnProvider
	blockListener           BlockListener
	blockTracker            BlockTracker
	eonTracker              EonTracker
	decryptionKeysListener  DecryptionKeysListener
	decryptionKeysProcessor DecryptionKeysProcessor
	encryptedTxnsPool       *EncryptedTxnsPool
	decryptedTxnsPool       *DecryptedTxnsPool
	slotCalculator          SlotCalculator
}

func NewPool(
	logger log.Logger,
	config Config,
	secondaryTxnProvider txnprovider.TxnProvider,
	contractBackend bind.ContractBackend,
	stateChangesClient stateChangesClient,
) *Pool {
	logger = logger.New("component", "shutter")
	slotCalculator := NewBeaconChainSlotCalculator(config.BeaconChainGenesisTimestamp, config.SecondsPerSlot)
	blockListener := NewBlockListener(logger, stateChangesClient)
	blockTracker := NewBlockTracker(logger, blockListener)
	eonTracker := NewKsmEonTracker(logger, config, blockListener, contractBackend)
	decryptionKeysValidator := NewDecryptionKeysExtendedValidator(logger, config, slotCalculator, eonTracker)
	decryptionKeysListener := NewDecryptionKeysListener(logger, config, decryptionKeysValidator)
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
		secondaryTxnProvider:    secondaryTxnProvider,
		decryptionKeysListener:  decryptionKeysListener,
		decryptionKeysProcessor: decryptionKeysProcessor,
		encryptedTxnsPool:       encryptedTxnsPool,
		decryptedTxnsPool:       decryptedTxnsPool,
		slotCalculator:          slotCalculator,
	}
}

func (p Pool) Run(ctx context.Context) error {
	defer p.logger.Info("pool stopped")
	p.logger.Info("running pool")

	unregisterDkpObserver := p.decryptionKeysListener.RegisterObserver(func(msg *proto.DecryptionKeys) {
		p.decryptionKeysProcessor.Enqueue(msg)
	})
	defer unregisterDkpObserver()

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error { return p.blockListener.Run(ctx) })
	eg.Go(func() error { return p.blockTracker.Run(ctx) })
	eg.Go(func() error { return p.eonTracker.Run(ctx) })
	eg.Go(func() error { return p.decryptionKeysListener.Run(ctx) })
	eg.Go(func() error { return p.decryptionKeysProcessor.Run(ctx) })
	eg.Go(func() error { return p.encryptedTxnsPool.Run(ctx) })
	return eg.Wait()
}

func (p Pool) ProvideTxns(ctx context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
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
		return nil, fmt.Errorf("unknown eon for block num %d", parentBlockNum)
	}

	slot, err := p.slotCalculator.CalcSlot(blockTime)
	if err != nil {
		return nil, err
	}

	decryptionMark := DecryptionMark{Slot: slot, Eon: eon.Index}
	slotAge := p.slotCalculator.CalcSlotAge(slot)
	decryptionMarkWaitTimeout := p.config.MaxDecryptionKeysDelay - slotAge
	decryptionMarkWaitCtx, decryptionMarkWaitCtxCancel := context.WithTimeout(ctx, decryptionMarkWaitTimeout)
	defer decryptionMarkWaitCtxCancel()
	decryptionMarkWaitStart := time.Now()
	err = p.decryptedTxnsPool.Wait(decryptionMarkWaitCtx, decryptionMark)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			p.logger.Warn(
				"decryption mark wait timeout, falling back to secondary txn provider",
				"slot", slot,
				"age", slotAge,
				"timeout", decryptionMarkWaitTimeout,
			)

			decryptionMarkWaitSecs.ObserveDuration(decryptionMarkWaitStart)
			decryptionMarkMissed.Inc()
			// Note: specs say to produce empty block in case decryption keys do not arrive on time.
			// However, upon discussion with Shutter and Nethermind it was agreed that this is not
			// practical at this point in time as it can hurt validator rewards across the network,
			// and also it doesn't in any way prevent any cheating from happening.
			// To properly address cheating, we need a mechanism for slashing which is a future
			// work stream item for the Shutter team. For now, we follow what Nethermind does
			// and fallback to the public devp2p mempool - any changes to this should be
			// co-ordinated with them.
			return p.secondaryTxnProvider.ProvideTxns(ctx, opts...)
		}

		return nil, err
	}

	decryptionMarkWaitSecs.ObserveDuration(decryptionMarkWaitStart)
	decryptionMarkOnTime.Inc()
	return p.provide(ctx, decryptionMark, opts...)
}

func (p Pool) provide(ctx context.Context, mark DecryptionMark, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	decryptedTxns, ok := p.decryptedTxnsPool.DecryptedTxns(mark)
	if !ok {
		return nil, fmt.Errorf("unexpected missing decrypted txns for mark: slot=%d, eon=%d", mark.Slot, mark.Eon)
	}

	decryptedTxnsGas := decryptedTxns.TotalGasLimit
	provideOpts := txnprovider.ApplyProvideOptions(opts...)
	totalGasTarget := provideOpts.GasTarget
	if decryptedTxnsGas > totalGasTarget {
		// note this should never happen because EncryptedGasLimit must always be <= gasLimit for a block
		return nil, fmt.Errorf("decrypted txns gas gt target: %d > %d", decryptedTxnsGas, totalGasTarget)
	}

	p.logger.Debug("providing decrypted txns", "count", len(decryptedTxns.Transactions), "gas", decryptedTxnsGas)
	if decryptedTxnsGas == totalGasTarget {
		return decryptedTxns.Transactions, nil
	}

	remGasTarget := totalGasTarget - decryptedTxnsGas
	opts = append(opts, txnprovider.WithGasTarget(remGasTarget)) // overrides option
	additionalTxns, err := p.secondaryTxnProvider.ProvideTxns(ctx, opts...)
	if err != nil {
		return nil, err
	}

	p.logger.Debug("providing additional public txns", "count", len(additionalTxns))
	return append(decryptedTxns.Transactions, additionalTxns...), nil
}
