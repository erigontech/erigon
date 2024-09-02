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

// This is a Prober that detects if an address contains a contract which implements ERC721 interface.
//
// It assumes ERC165 detection was already done and it passes the criteria.
type ERC721Prober struct {
	abi                    *abi.ABI
	supportsInterface721   *[]byte
	supportsInterface721MD *[]byte
}

// TODO: support 721 and 721MD simultaneously
func NewERC721Prober() (Prober, error) {
	a, err := abi.JSON(bytes.NewReader(otscontracts.ERC165))
	if err != nil {
		return nil, err
	}

	// Caches predefined supportsInterface() packed calls
	siEIP721, err := a.Pack("supportsInterface", [4]byte{0x80, 0xac, 0x58, 0xcd})
	if err != nil {
		return nil, err
	}
	si721MD, err := a.Pack("supportsInterface", [4]byte{0x5b, 0x5e, 0x13, 0x9f})
	if err != nil {
		return nil, err
	}

	return &ERC721Prober{
		abi:                    &a,
		supportsInterface721:   &siEIP721,
		supportsInterface721MD: &si721MD,
	}, nil
}

func (p *ERC721Prober) Probe(ctx context.Context, evm *vm.EVM, header *types.Header, chainConfig *chain.Config, ibs *state.IntraBlockState, blockNum uint64, addr common.Address, _, _ []byte) (*roaring64.Bitmap, error) {
	bm := roaring64.NewBitmap()

	// supportsInterface(0x80ac58cd) -> ERC721 interface
	res, err := probeContractWithArgs(ctx, evm, header, chainConfig, ibs, addr, p.abi, p.supportsInterface721, "supportsInterface")
	if err != nil {
		return nil, err
	}
	if res == nil || !res[0].(bool) {
		return nil, nil
	}
	bm.Add(kv.ADDR_ATTR_ERC721)

	// supportsInterface(0x5b5e139f) -> ERC721 Metadata
	res, err = probeContractWithArgs(ctx, evm, header, chainConfig, ibs, addr, p.abi, p.supportsInterface721MD, "supportsInterface")
	if err != nil {
		return nil, err
	}
	if res != nil && res[0].(bool) {
		bm.Add(kv.ADDR_ATTR_ERC721_MD)
	}

	return bm, nil
}
