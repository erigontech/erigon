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
	"math/big"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sync/errgroup"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/accounts/abi/bind"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/contracts"
)

type EonTracker interface {
	Run(ctx context.Context) error
	CurrentEon() (Eon, bool)
	RecentEon(index EonIndex) (Eon, bool)
}

type EonIndex uint64

type Eon struct {
	Index           EonIndex
	ActivationBlock uint64
	Key             []byte
	Threshold       uint64
	Members         []libcommon.Address
}

type KsmEonTracker struct {
	logger               log.Logger
	blockListener        BlockListener
	contractBackend      bind.ContractBackend
	ksmContract          *contracts.KeyperSetManager
	keyBroadcastContract *contracts.KeyBroadcastContract
	currentEon           atomic.Pointer[Eon]
	recentEons           *lru.Cache[EonIndex, Eon]
	lastCleanupBlockNum  uint64
}

func NewKsmEonTracker(config Config, blockListener BlockListener, contractBackend bind.ContractBackend) *KsmEonTracker {
	ksmContractAddr := libcommon.HexToAddress(config.KeyperSetManagerContractAddress)
	ksmContract, err := contracts.NewKeyperSetManager(ksmContractAddr, contractBackend)
	if err != nil {
		panic(fmt.Errorf("failed to create KeyperSetManager: %w", err))
	}

	keyBroadcastContractAddr := libcommon.HexToAddress(config.KeyBroadcastContractAddress)
	keyBroadcastContract, err := contracts.NewKeyBroadcastContract(keyBroadcastContractAddr, contractBackend)
	if err != nil {
		panic(fmt.Errorf("failed to create KeyBroadcastContract: %w", err))
	}

	recentEons, err := lru.New[EonIndex, Eon](config.MaxRecentEons)
	if err != nil {
		panic(fmt.Errorf("failed to create recentEons LRU cache: %w", err))
	}

	return &KsmEonTracker{
		blockListener:        blockListener,
		contractBackend:      contractBackend,
		ksmContract:          ksmContract,
		keyBroadcastContract: keyBroadcastContract,
		recentEons:           recentEons,
	}
}

func (et *KsmEonTracker) Run(ctx context.Context) error {
	et.logger.Info("running eon tracker")

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error { return et.trackCurrentEon(ctx) })
	eg.Go(func() error { return et.trackFutureEons(ctx) })
	return nil
}

func (et *KsmEonTracker) CurrentEon() (Eon, bool) {
	eon := et.currentEon.Load()
	if eon == nil {
		return Eon{}, false
	}

	return *eon, true
}

func (et *KsmEonTracker) RecentEon(index EonIndex) (Eon, bool) {
	return et.recentEons.Peek(index)
}

func (et *KsmEonTracker) trackCurrentEon(ctx context.Context) error {
	blockEventC := make(chan BlockEvent)
	unregisterBlockEventObserver := et.blockListener.RegisterObserver(func(blockEvent BlockEvent) {
		select {
		case <-ctx.Done(): // no-op
		case blockEventC <- blockEvent:
		}
	})

	defer unregisterBlockEventObserver()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case blockEvent := <-blockEventC:
			eon, err := et.readEonAtNewBlockEvent(blockEvent.BlockNum)
			if err != nil {
				return err
			}

			et.currentEon.Store(&eon)
			et.maybeCleanRecentEons(blockEvent.BlockNum)
		}
	}
}

func (et *KsmEonTracker) readEonAtNewBlockEvent(blockNum uint64) (Eon, error) {
	var err error
	var cached bool
	startTime := time.Now()
	defer func() {
		if err != nil {
			return
		}

		et.logger.Debug("readEonAtNewBlockEvent timing", "blockNum", blockNum, "cached", cached, "duration", time.Since(startTime))
	}()

	callOpts := &bind.CallOpts{BlockNumber: new(big.Int).SetUint64(blockNum)}
	eonIndex, err := et.ksmContract.GetKeyperSetIndexByBlock(callOpts, blockNum)
	if err != nil {
		return Eon{}, err
	}
	if eon, ok := et.recentEons.Get(EonIndex(eonIndex)); ok {
		cached = true
		return eon, nil
	}

	keyperSetAddress, err := et.ksmContract.GetKeyperSetAddress(&bind.CallOpts{}, eonIndex)
	if err != nil {
		return Eon{}, err
	}

	keyperSet, err := contracts.NewKeyperSet(keyperSetAddress, et.contractBackend)
	if err != nil {
		return Eon{}, err
	}

	threshold, err := keyperSet.GetThreshold(callOpts)
	if err != nil {
		return Eon{}, err
	}

	members, err := keyperSet.GetMembers(callOpts)
	if err != nil {
		return Eon{}, err
	}

	key, err := et.keyBroadcastContract.GetEonKey(callOpts, eonIndex)
	if err != nil {
		return Eon{}, err
	}

	activationBlock, err := et.ksmContract.GetKeyperSetActivationBlock(callOpts, eonIndex)
	if err != nil {
		return Eon{}, err
	}
	if activationBlock < blockNum {
		return Eon{}, fmt.Errorf("unexpected invalid activation block: %d < %d", activationBlock, blockNum)
	}

	finalized, err := keyperSet.IsFinalized(callOpts)
	if err != nil {
		return Eon{}, err
	}
	if !finalized {
		return Eon{}, fmt.Errorf("unexpected KeyperSet is not finalized: eon=%d, address=%s", eonIndex, keyperSetAddress)
	}

	eon := Eon{
		Index:           EonIndex(eonIndex),
		ActivationBlock: activationBlock,
		Key:             key,
		Threshold:       threshold,
		Members:         members,
	}

	return eon, nil
}

func (et *KsmEonTracker) maybeCleanRecentEons(blockNum uint64) {
	cleanupThreshold := uint64(128) // wait this many blocks before cleaning old eons
	if blockNum < cleanupThreshold {
		// protects from underflow
		return
	}

	if et.lastCleanupBlockNum == 0 {
		// first time after startup
		et.lastCleanupBlockNum = blockNum
		return
	}

	cleanUpTo := blockNum - cleanupThreshold
	if cleanUpTo <= et.lastCleanupBlockNum {
		// not enough blocks have passed since last cleanup
		return
	}

	currentEon := et.currentEon.Load()
	var cleanedEons []EonIndex
	for _, eon := range et.recentEons.Values() {
		if eon.Index < currentEon.Index && eon.ActivationBlock < cleanUpTo {
			cleanedEons = append(cleanedEons, eon.Index)
			et.recentEons.Remove(eon.Index)
		}
	}
	if len(cleanedEons) > 0 {
		et.logger.Debug(
			"cleaned recent eons",
			"cleaned", cleanedEons,
			"cleanUpTo", cleanUpTo,
			"lastCleanupBlockNum", et.lastCleanupBlockNum,
			"currentEon", currentEon.Index,
		)
	}

	et.lastCleanupBlockNum = cleanUpTo
}

func (et *KsmEonTracker) trackFutureEons(ctx context.Context) error {
	keyperSetAddedEventC := make(chan *contracts.KeyperSetManagerKeyperSetAdded)
	keyperSetAddedEventSub, err := et.ksmContract.WatchKeyperSetAdded(&bind.WatchOpts{}, keyperSetAddedEventC)
	if err != nil {
		return err
	}

	defer keyperSetAddedEventSub.Unsubscribe()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-keyperSetAddedEventSub.Err():
			return err
		case event := <-keyperSetAddedEventC:
			eonIndex := EonIndex(event.Eon)
			if event.Raw.Removed {
				et.recentEons.Remove(eonIndex)
				continue
			}

			eon, err := et.readEonAtKeyperSetAddedEvent(event)
			if err != nil {
				return err
			}

			et.recentEons.Add(eonIndex, eon)
		}
	}
}

func (et *KsmEonTracker) readEonAtKeyperSetAddedEvent(event *contracts.KeyperSetManagerKeyperSetAdded) (Eon, error) {
	callOpts := &bind.CallOpts{
		BlockNumber: new(big.Int).SetUint64(event.Raw.BlockNumber),
	}

	eonIndex := event.Eon
	key, err := et.keyBroadcastContract.GetEonKey(callOpts, eonIndex)
	if err != nil {
		return Eon{}, err
	}

	eon := Eon{
		Index:           EonIndex(eonIndex),
		ActivationBlock: event.ActivationBlock,
		Key:             key,
		Threshold:       event.Threshold,
		Members:         event.Members,
	}

	return eon, nil
}
