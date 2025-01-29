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

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/accounts/abi/bind"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/contracts"
)

type EonTracker struct {
	logger               log.Logger
	config               Config
	blockListener        BlockListener
	contractBackend      bind.ContractBackend
	ksmContract          *contracts.KeyperSetManager
	keyBroadcastContract *contracts.KeyBroadcastContract
	currentEon           atomic.Pointer[Eon]
}

func NewEonTracker(config Config, blockListener BlockListener, contractBackend bind.ContractBackend) *EonTracker {
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

	return &EonTracker{
		config:               config,
		blockListener:        blockListener,
		contractBackend:      contractBackend,
		ksmContract:          ksmContract,
		keyBroadcastContract: keyBroadcastContract,
	}
}

func (et *EonTracker) Run(ctx context.Context) error {
	et.logger.Info("running eon tracker")

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
			eon, err := et.readEon(blockEvent.BlockNum)
			if err != nil {
				return err
			}

			et.currentEon.Store(&eon)
		}
	}
}

func (et *EonTracker) CurrentEon() (Eon, bool) {
	eon := et.currentEon.Load()
	if eon == nil {
		return Eon{}, false
	}

	return *eon, true
}

func (et *EonTracker) readEon(blockNum uint64) (Eon, error) {
	callOpts := &bind.CallOpts{BlockNumber: new(big.Int).SetUint64(blockNum)}
	eonIndex, err := et.ksmContract.GetKeyperSetIndexByBlock(callOpts, blockNum)
	if err != nil {
		return Eon{}, fmt.Errorf("failed to get KeyperSetIndexByBlock: %w", err)
	}

	keyperSetAddress, err := et.ksmContract.GetKeyperSetAddress(&bind.CallOpts{}, eonIndex)
	if err != nil {
		return Eon{}, fmt.Errorf("failed to get KeyperSetAddress: %w", err)
	}

	keyperSet, err := contracts.NewKeyperSet(keyperSetAddress, et.contractBackend)
	if err != nil {
		return Eon{}, fmt.Errorf("failed to create KeyperSet: %w", err)
	}

	threshold, err := keyperSet.GetThreshold(callOpts)
	if err != nil {
		return Eon{}, fmt.Errorf("failed to get KeyperSet threshold: %w", err)
	}

	members, err := keyperSet.GetMembers(callOpts)
	if err != nil {
		return Eon{}, fmt.Errorf("failed to get KeyperSet members: %w", err)
	}

	key, err := et.keyBroadcastContract.GetEonKey(callOpts, eonIndex)
	if err != nil {
		return Eon{}, fmt.Errorf("failed to get EonKey: %w", err)
	}

	activationBlock, err := et.ksmContract.GetKeyperSetActivationBlock(callOpts, eonIndex)
	if err != nil {
		return Eon{}, fmt.Errorf("failed to get KeyperSet activation block: %w", err)
	}
	if activationBlock < blockNum {
		return Eon{}, fmt.Errorf("unexpected invalid activation block: %d < %d", activationBlock, blockNum)
	}

	finalized, err := keyperSet.IsFinalized(callOpts)
	if err != nil {
		return Eon{}, fmt.Errorf("failed to get KeyperSet finalized: %w", err)
	}
	if !finalized {
		return Eon{}, fmt.Errorf("unexpected KeyperSet is not finalized: eon=%d, address=%s", eonIndex, keyperSetAddress)
	}

	eon := Eon{
		Index:           eonIndex,
		ActivationBlock: activationBlock,
		Key:             key,
		Threshold:       threshold,
		Members:         members,
	}

	return eon, nil
}

type Eon struct {
	Index           uint64
	ActivationBlock uint64
	Key             []byte
	Threshold       uint64
	Members         []libcommon.Address
}
