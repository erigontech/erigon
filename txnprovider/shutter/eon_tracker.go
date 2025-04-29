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
	"sync"
	"time"

	"github.com/erigontech/erigon/txnprovider/shutter/shuttercfg"
	"github.com/google/btree"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/contracts"
)

type EonTracker interface {
	Run(ctx context.Context) error
	CurrentEon() (Eon, bool)
	RecentEon(index EonIndex) (Eon, bool)
	EonByBlockNum(blockNum uint64) (Eon, bool)
}

type KsmEonTracker struct {
	logger               log.Logger
	blockListener        *BlockListener
	contractBackend      bind.ContractBackend
	ksmContract          *contracts.KeyperSetManager
	keyBroadcastContract *contracts.KeyBroadcastContract
	mu                   sync.RWMutex
	currentEon           *Eon
	recentEons           *btree.BTreeG[Eon]
	cleanupThreshold     uint64
	lastCleanupBlockNum  uint64
}

func NewKsmEonTracker(logger log.Logger, config shuttercfg.Config, bl *BlockListener, cb bind.ContractBackend) *KsmEonTracker {
	ksmContractAddr := common.HexToAddress(config.KeyperSetManagerContractAddress)
	ksmContract, err := contracts.NewKeyperSetManager(ksmContractAddr, cb)
	if err != nil {
		panic(fmt.Errorf("failed to create KeyperSetManager: %w", err))
	}

	keyBroadcastContractAddr := common.HexToAddress(config.KeyBroadcastContractAddress)
	keyBroadcastContract, err := contracts.NewKeyBroadcastContract(keyBroadcastContractAddr, cb)
	if err != nil {
		panic(fmt.Errorf("failed to create KeyBroadcastContract: %w", err))
	}

	return &KsmEonTracker{
		logger:               logger,
		blockListener:        bl,
		contractBackend:      cb,
		ksmContract:          ksmContract,
		keyBroadcastContract: keyBroadcastContract,
		cleanupThreshold:     config.ReorgDepthAwareness,
		recentEons:           btree.NewG[Eon](2, EonLess),
	}
}

func (et *KsmEonTracker) Run(ctx context.Context) error {
	defer et.logger.Info("eon tracker stopped")
	et.logger.Info("running eon tracker")

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		err := et.trackCurrentEon(ctx)
		if err != nil {
			return fmt.Errorf("track current eon: %w", err)
		}
		return nil
	})

	eg.Go(func() error {
		err := et.trackFutureEons(ctx)
		if err != nil {
			return fmt.Errorf("track future eons: %w", err)
		}
		return nil
	})

	return eg.Wait()
}

func (et *KsmEonTracker) CurrentEon() (Eon, bool) {
	et.mu.RLock()
	defer et.mu.RUnlock()

	if et.currentEon == nil {
		return Eon{}, false
	}

	return *et.currentEon, true
}

func (et *KsmEonTracker) RecentEon(index EonIndex) (Eon, bool) {
	et.mu.RLock()
	defer et.mu.RUnlock()

	return et.recentEon(index)
}

func (et *KsmEonTracker) EonByBlockNum(blockNum uint64) (Eon, bool) {
	et.mu.RLock()
	defer et.mu.RUnlock()

	var eon Eon
	var found bool
	et.recentEons.Descend(func(e Eon) bool {
		if blockNum >= e.ActivationBlock {
			eon, found = e, true
			return false // stop
		}
		return true // continue
	})
	return eon, found
}

func (et *KsmEonTracker) recentEon(index EonIndex) (Eon, bool) {
	return et.recentEons.Get(Eon{Index: index})
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
			err := et.handleBlockEvent(blockEvent)
			if err != nil {
				return err
			}
		}
	}
}

func (et *KsmEonTracker) handleBlockEvent(blockEvent BlockEvent) error {
	et.mu.Lock()
	defer et.mu.Unlock()

	blockNum := blockEvent.LatestBlockNum
	eon, ok, err := et.readEonAtNewBlockEvent(blockNum)
	if err != nil {
		return fmt.Errorf("read eon at new block event: %w", err)
	}
	if !ok {
		et.logger.Warn("no eon at", "blockNum", blockNum)
		return nil
	}

	if et.currentEon == nil || et.currentEon.Index != eon.Index {
		et.logger.Debug("current eon at block", "blockNum", blockNum, "eonIndex", eon.Index)
	}

	et.currentEon = &eon
	et.recentEons.ReplaceOrInsert(eon)
	et.maybeCleanup(blockNum)
	return nil
}

func (et *KsmEonTracker) readEonAtNewBlockEvent(blockNum uint64) (Eon, bool, error) {
	var err error
	var cached bool
	startTime := time.Now()
	defer func() {
		if err != nil {
			return
		}

		et.logger.Trace("readEonAtNewBlockEvent timing", "blockNum", blockNum, "cached", cached, "duration", time.Since(startTime))
	}()

	callOpts := &bind.CallOpts{BlockNumber: new(big.Int).SetUint64(blockNum)}
	if et.currentEon == nil {
		numKeyperSets, err := et.ksmContract.GetNumKeyperSets(callOpts)
		if err != nil {
			return Eon{}, false, fmt.Errorf("get num keyper sets: %w", err)
		}
		// wait until we have at least one keyper set added and past activation block
		// to avoid execution reverts for GetKeyperSetIndexByBlock
		if numKeyperSets == 0 {
			et.logger.Warn("no keyper sets found", "blockNum", blockNum)
			return Eon{}, false, nil
		}

		activationBlock, err := et.ksmContract.GetKeyperSetActivationBlock(callOpts, 0)
		if err != nil {
			return Eon{}, false, fmt.Errorf("initial get keyper set activation block: %w", err)
		}
		if blockNum < activationBlock {
			et.logger.Warn("initial keyper set not yet activated", "blockNum", blockNum, "activationAt", activationBlock)
			return Eon{}, false, nil
		}
	}

	eonIndex, err := et.ksmContract.GetKeyperSetIndexByBlock(callOpts, blockNum)
	if err != nil {
		return Eon{}, false, fmt.Errorf("get keyper set index by block: %w", err)
	}

	if eon, ok := et.recentEon(EonIndex(eonIndex)); ok {
		cached = true
		return eon, true, nil
	}

	keyperSetAddress, err := et.ksmContract.GetKeyperSetAddress(&bind.CallOpts{}, eonIndex)
	if err != nil {
		return Eon{}, false, fmt.Errorf("get keyper set address: %w", err)
	}

	keyperSet, err := contracts.NewKeyperSet(keyperSetAddress, et.contractBackend)
	if err != nil {
		return Eon{}, false, fmt.Errorf("new keyper set: %w", err)
	}

	threshold, err := keyperSet.GetThreshold(callOpts)
	if err != nil {
		return Eon{}, false, fmt.Errorf("keyper set get threshold: %w", err)
	}

	members, err := keyperSet.GetMembers(callOpts)
	if err != nil {
		return Eon{}, false, fmt.Errorf("keyper set get members: %w", err)
	}

	key, err := et.keyBroadcastContract.GetEonKey(callOpts, eonIndex)
	if err != nil {
		return Eon{}, false, fmt.Errorf("get eon key: %w", err)
	}
	if len(key) == 0 {
		et.logger.Warn("empty eon key at", "blockNum", blockNum, "eonIndex", eonIndex)
		return Eon{}, false, nil
	}

	activationBlock, err := et.ksmContract.GetKeyperSetActivationBlock(callOpts, eonIndex)
	if err != nil {
		return Eon{}, false, fmt.Errorf("get keyper set activation block: %w", err)
	}
	if activationBlock > blockNum {
		return Eon{}, false, fmt.Errorf("unexpected invalid activation block: %d > %d", activationBlock, blockNum)
	}

	finalized, err := keyperSet.IsFinalized(callOpts)
	if err != nil {
		return Eon{}, false, fmt.Errorf("keyper set is finalized: %w", err)
	}
	if !finalized {
		return Eon{}, false, fmt.Errorf("unexpected keyper set is not finalized: eon=%d, address=%s", eonIndex, keyperSetAddress)
	}

	eon := Eon{
		Index:           EonIndex(eonIndex),
		ActivationBlock: activationBlock,
		Key:             key,
		Threshold:       threshold,
		Members:         members,
	}

	return eon, true, nil
}

func (et *KsmEonTracker) maybeCleanup(blockNum uint64) {
	if blockNum < et.cleanupThreshold {
		// protects from underflow
		return
	}

	if et.lastCleanupBlockNum == 0 {
		// first time after startup
		et.lastCleanupBlockNum = blockNum
		return
	}

	cleanUpTo := blockNum - et.cleanupThreshold
	if cleanUpTo <= et.lastCleanupBlockNum {
		// not enough blocks have passed since last cleanup
		return
	}

	var cleanedEons []Eon
	et.recentEons.Ascend(func(eon Eon) bool {
		if eon.Index < et.currentEon.Index && eon.ActivationBlock < cleanUpTo {
			cleanedEons = append(cleanedEons, eon)
			return true // continue
		}
		return false // stop
	})

	for _, eon := range cleanedEons {
		et.recentEons.Delete(eon)
	}

	if len(cleanedEons) > 0 {
		et.logger.Debug(
			"cleaned recent eons",
			"cleaned", cleanedEons,
			"cleanUpTo", cleanUpTo,
			"lastCleanupBlockNum", et.lastCleanupBlockNum,
			"currentEon", et.currentEon.Index,
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
			err := et.handleKeyperSetAddedEvent(event)
			if err != nil {
				return err
			}
		}
	}
}

func (et *KsmEonTracker) handleKeyperSetAddedEvent(event *contracts.KeyperSetManagerKeyperSetAdded) error {
	et.mu.Lock()
	defer et.mu.Unlock()

	et.logger.Debug("keyper set added event", "eon", event.Eon, "removed", event.Raw.Removed)
	eonIndex := EonIndex(event.Eon)
	if event.Raw.Removed {
		et.recentEons.Delete(Eon{Index: eonIndex})
		return nil
	}

	eon, ok, err := et.readEonAtKeyperSetAddedEvent(event)
	if err != nil {
		return fmt.Errorf("read eon at keyper set added event: %w", err)
	}
	if !ok {
		et.logger.Warn("no eon at keyper set added event", "eon", event.Eon)
		return nil
	}

	et.recentEons.ReplaceOrInsert(eon)
	return nil
}

func (et *KsmEonTracker) readEonAtKeyperSetAddedEvent(event *contracts.KeyperSetManagerKeyperSetAdded) (Eon, bool, error) {
	callOpts := &bind.CallOpts{
		BlockNumber: new(big.Int).SetUint64(event.Raw.BlockNumber),
	}

	eonIndex := event.Eon
	key, err := et.keyBroadcastContract.GetEonKey(callOpts, eonIndex)
	if err != nil {
		return Eon{}, false, fmt.Errorf("get eon key: %w", err)
	}
	if len(key) == 0 {
		//
		// TODO: this needs to actually go into a "future cache"
		//       & add listening for eon key broadcasts
		//       then match and add to recent eons
		//       would also need to handle this cache in cleanup flow
		//       then remove the `eon, ok` return values and stick to just `error` here
		//
		et.logger.Warn(
			"empty eon key for keyper set added event",
			"blockNum", event.Raw.BlockNumber,
			"eonIndex", eonIndex,
			"activationBlock", event.ActivationBlock,
		)
		return Eon{}, false, nil
	}

	eon := Eon{
		Index:           EonIndex(eonIndex),
		ActivationBlock: event.ActivationBlock,
		Key:             key,
		Threshold:       event.Threshold,
		Members:         event.Members,
	}

	return eon, true, nil
}
