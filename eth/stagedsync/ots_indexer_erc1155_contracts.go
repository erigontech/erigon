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

// This is a Prober that detects if an address contains a contract which implements ERC1155 interface.
//
// It assumes ERC165 detection was already done and it passes the criteria.
type ERC1155Prober struct {
	abi                   *abi.ABI
	supportsInterface1155 *[]byte
}

func NewERC1155Prober() (Prober, error) {
	a, err := abi.JSON(bytes.NewReader(otscontracts.ERC165))
	if err != nil {
		return nil, err
	}

	// Caches predefined supportsInterface() packed calls
	siEIP1155, err := a.Pack("supportsInterface", [4]byte{0xd9, 0xb6, 0x7a, 0x26})
	if err != nil {
		return nil, err
	}

	return &ERC1155Prober{
		abi:                   &a,
		supportsInterface1155: &siEIP1155,
	}, nil
}

func (p *ERC1155Prober) Probe(ctx context.Context, evm *vm.EVM, header *types.Header, chainConfig *chain.Config, ibs *state.IntraBlockState, blockNum uint64, addr common.Address, _, _ []byte) (*roaring64.Bitmap, error) {
	// supportsInterface(0xd9b67a26) -> ERC1155 interface
	res, err := probeContractWithArgs(ctx, evm, header, chainConfig, ibs, addr, p.abi, p.supportsInterface1155, "supportsInterface")
	if err != nil {
		return nil, err
	}
	if res == nil || !res[0].(bool) {
		return nil, nil
	}

	return roaring64.BitmapOf(kv.ADDR_ATTR_ERC1155), nil
}
