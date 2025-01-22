package shutter

import (
	"context"
	"fmt"
	"math/big"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/accounts/abi/bind"
	"github.com/erigontech/erigon/contracts"
	shuttercontracts "github.com/erigontech/erigon/txnprovider/shutter/internal/contracts"
)

type EonPool struct {
	config Config
}

func NewEonPool(config Config) EonPool {
	return EonPool{
		config: config,
	}
}

func (ep EonPool) Run(ctx context.Context) error {
	//
	// TODO listen to new events for new eons and pool them
	//      pool is like a lru cache - keeps them pooled based on activation block
	//      may need a tree implementation that allows Seek semantic
	//
	return nil
}

func (ep EonPool) Eon(blockNum uint64) (Eon, error) {
	//
	// TODO - check if we have it in the pool first, if not then fallback
	//
	backend := contracts.Backend{}
	addr := libcommon.HexToAddress(ep.config.KeyperSetManagerContractAddress)
	ksm, err := shuttercontracts.NewKeyperSetManager(addr, backend)
	if err != nil {
		return Eon{}, fmt.Errorf("failed to create KeyperSetManager: %w", err)
	}

	callOpts := &bind.CallOpts{BlockNumber: new(big.Int).SetUint64(blockNum)}
	eonIndex, err := ksm.GetKeyperSetIndexByBlock(callOpts, blockNum)
	if err != nil {
		return Eon{}, fmt.Errorf("failed to get KeyperSetIndexByBlock: %w", err)
	}

	keyperSetAddress, err := ksm.GetKeyperSetAddress(&bind.CallOpts{}, eonIndex)
	if err != nil {
		return Eon{}, fmt.Errorf("failed to get KeyperSetAddress: %w", err)
	}

	keyperSet, err := shuttercontracts.NewKeyperSet(keyperSetAddress, backend)
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

	eon := Eon{
		Index:     eonIndex,
		Threshold: threshold,
		Members:   members,
	}

	return eon, nil
}

type Eon struct {
	Index     uint64
	Threshold uint64
	Members   []libcommon.Address
}
