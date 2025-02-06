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
	eonTracker              EonTracker
	decryptionKeysListener  DecryptionKeysListener
	decryptionKeysProcessor DecryptionKeysProcessor
	encryptedTxnsPool       EncryptedTxnsPool
	decryptedTxnsPool       DecryptedTxnsPool
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
	eonTracker := NewKsmEonTracker(config, blockListener, contractBackend)
	decryptionKeysListener := NewDecryptionKeysListener(logger, config, slotCalculator, eonTracker)
	decryptionKeysProcessor := NewDecryptionKeysProcessor(logger)
	encryptedTxnsPool := NewEncryptedTxnsPool(logger, config, contractBackend)
	return &Pool{
		logger:                  logger,
		config:                  config,
		secondaryTxnProvider:    secondaryTxnProvider,
		blockListener:           blockListener,
		eonTracker:              eonTracker,
		decryptionKeysListener:  decryptionKeysListener,
		decryptionKeysProcessor: decryptionKeysProcessor,
		encryptedTxnsPool:       encryptedTxnsPool,
		slotCalculator:          slotCalculator,
	}
}

func (p Pool) Run(ctx context.Context) error {
	defer func() { p.logger.Info("pool stopped") }()
	p.logger.Info("running pool")

	unregisterDkpObserver := p.decryptionKeysListener.RegisterObserver(func(msg *proto.DecryptionKeys) {
		p.decryptionKeysProcessor.Enqueue(msg)
	})

	defer unregisterDkpObserver()
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error { return p.blockListener.Run(ctx) })
	eg.Go(func() error { return p.eonTracker.Run(ctx) })
	eg.Go(func() error { return p.decryptionKeysListener.Run(ctx) })
	eg.Go(func() error { return p.decryptionKeysProcessor.Run(ctx) })
	eg.Go(func() error { return p.encryptedTxnsPool.Run(ctx) })
	return eg.Wait()
}

func (p Pool) ProvideTxns(ctx context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	provideOpts := txnprovider.ApplyProvideOptions(opts...)
	blockTimestamp := provideOpts.BlockTimestamp
	if blockTimestamp == 0 {
		return nil, errors.New("block timestamp option is required by the shutter provider")
	}

	slot, err := p.slotCalculator.CalcSlot(blockTimestamp)
	if err != nil {
		return nil, err
	}

	seenKeys := p.decryptedTxnsPool.SeenDecryptionKeys(slot)
	if seenKeys {
		return p.provide(ctx, slot, opts...)
	}

	slotAge := p.slotCalculator.CalcSlotAge(slot)
	keysWaitTime := p.config.MaxDecryptionKeysDelay - slotAge
	ctx, cancel := context.WithTimeout(ctx, keysWaitTime)
	defer cancel()
	err = p.decryptedTxnsPool.WaitForSlot(ctx, slot)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			p.logger.Warn(
				"decryption keys wait timeout, falling back to secondary txn provider",
				"slot", slot,
				"age", slotAge,
			)

			return p.secondaryTxnProvider.ProvideTxns(ctx, opts...)
		}

		return nil, err
	}

	return p.provide(ctx, slot, opts...)
}

func (p Pool) provide(ctx context.Context, slot uint64, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	decryptedTxns, err := p.decryptedTxnsPool.DecryptedTxns(slot)
	if err != nil {
		return nil, err
	}

	decryptedTxnsGas := decryptedTxns.TotalGas
	provideOpts := txnprovider.ApplyProvideOptions(opts...)
	totalGasTarget := provideOpts.GasTarget
	if decryptedTxnsGas > totalGasTarget {
		// note this should never happen because EncryptedGasLimit must always be <= gasLimit for a block
		return nil, fmt.Errorf("decrypted txns gas gt target: %d > %d", decryptedTxnsGas, totalGasTarget)
	}

	if decryptedTxnsGas == totalGasTarget {
		return decryptedTxns.Transactions, nil
	}

	remGasTarget := totalGasTarget - decryptedTxnsGas
	opts = append(opts, txnprovider.WithGasTarget(remGasTarget)) // overrides option
	additionalTxns, err := p.secondaryTxnProvider.ProvideTxns(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return append(decryptedTxns.Transactions, additionalTxns...), nil
}
