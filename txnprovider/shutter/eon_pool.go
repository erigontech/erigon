package shutter

import (
	"context"
	"fmt"
	"math/big"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/accounts/abi/bind"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/contracts"
)

type EonPool struct {
	config               Config
	contractBackend      bind.ContractBackend
	ksmContract          *contracts.KeyperSetManager
	keyBroadcastContract *contracts.KeyBroadcastContract
}

func NewEonPool(config Config, contractBackend bind.ContractBackend) EonPool {
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

	return EonPool{
		config:               config,
		contractBackend:      contractBackend,
		ksmContract:          ksmContract,
		keyBroadcastContract: keyBroadcastContract,
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
	// TODO - check if we have it in the pool first, if not then fallback (fallback should make sure to wait until blockNum has been observed)
	//

	callOpts := &bind.CallOpts{BlockNumber: new(big.Int).SetUint64(blockNum)}
	eonIndex, err := ep.ksmContract.GetKeyperSetIndexByBlock(callOpts, blockNum)
	if err != nil {
		return Eon{}, fmt.Errorf("failed to get KeyperSetIndexByBlock: %w", err)
	}

	keyperSetAddress, err := ep.ksmContract.GetKeyperSetAddress(&bind.CallOpts{}, eonIndex)
	if err != nil {
		return Eon{}, fmt.Errorf("failed to get KeyperSetAddress: %w", err)
	}

	keyperSet, err := contracts.NewKeyperSet(keyperSetAddress, ep.contractBackend)
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

	key, err := ep.keyBroadcastContract.GetEonKey(callOpts, eonIndex)
	if err != nil {
		return Eon{}, fmt.Errorf("failed to get EonKey: %w", err)
	}

	eon := Eon{
		Index:     eonIndex,
		Key:       key,
		Threshold: threshold,
		Members:   members,
	}

	return eon, nil
}

type Eon struct {
	Index     uint64
	Key       []byte
	Threshold uint64
	Members   []libcommon.Address
}
