package stagedsync

import (
	"bytes"
	"context"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/stagedsync/otscontracts"
)

// This is a Prober that detects if an address contains a contract which implements ERC165 interface.
//
// It follows the detection mechanism described in the official specification: https://eips.ethereum.org/EIPS/eip-165
type ERC165Prober struct {
	abi                       *abi.ABI
	supportsInterface165      *[]byte
	supportsInterfaceFFFFFFFF *[]byte
}

func NewERC165Prober() (Prober, error) {
	// ERC165
	aERC165, err := abi.JSON(bytes.NewReader(otscontracts.ERC165))
	if err != nil {
		return nil, err
	}

	// Caches predefined supportsInterface() packed calls
	siEIP165, err := aERC165.Pack("supportsInterface", [4]byte{0x01, 0xff, 0xc9, 0xa7})
	if err != nil {
		return nil, err
	}
	siFFFFFFFF, err := aERC165.Pack("supportsInterface", [4]byte{0xff, 0xff, 0xff, 0xff})
	if err != nil {
		return nil, err
	}

	return &ERC165Prober{
		abi:                       &aERC165,
		supportsInterface165:      &siEIP165,
		supportsInterfaceFFFFFFFF: &siFFFFFFFF,
	}, nil
}

func (p *ERC165Prober) Probe(ctx context.Context, evm *vm.EVM, header *types.Header, chainConfig *chain.Config, ibs *state.IntraBlockState, blockNum uint64, addr common.Address, _, _ []byte) (*roaring64.Bitmap, error) {
	// supportsInterface(0x01ffc9a7) -> EIP165 interface
	res, err := probeContractWithArgs(ctx, evm, header, chainConfig, ibs, addr, p.abi, p.supportsInterface165, "supportsInterface")
	if err != nil {
		return nil, err
	}
	if res == nil || !res[0].(bool) {
		return nil, nil
	}

	// supportsInterface(0xffffffff) -> MUST return false according to EIP165
	res, err = probeContractWithArgs(ctx, evm, header, chainConfig, ibs, addr, p.abi, p.supportsInterfaceFFFFFFFF, "supportsInterface")
	if err != nil {
		return nil, err
	}
	if res == nil || res[0].(bool) {
		return nil, nil
	}

	return roaring64.BitmapOf(kv.ADDR_ATTR_ERC165), nil
}
