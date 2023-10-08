package stagedsync

import (
	"bytes"
	"context"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
)

// This is a Prober that detects if an address contains a contract which implements an ERC1167 minimal proxy
// contract.
//
// It matches the bytecode describe in the specification: https://eips.ethereum.org/EIPS/eip-1167
type ERC1167Prober struct {
}

func NewERC1167Prober() (Prober, error) {
	return &ERC1167Prober{}, nil
}

var minimalProxyTemplate = hexutility.Hex2Bytes("363d3d373d3d3d363d73bebebebebebebebebebebebebebebebebebebebe5af43d82803e903d91602b57fd5bf3")

// TODO: implement support for ERC1167 push optimizations
func (i *ERC1167Prober) Probe(ctx context.Context, evm *vm.EVM, header *types.Header, chainConfig *chain.Config, ibs *state.IntraBlockState, blockNum uint64, addr common.Address, _, _ []byte) (*roaring64.Bitmap, error) {
	code := ibs.GetCode(addr)
	if len(code) != len(minimalProxyTemplate) {
		return nil, nil
	}

	if !bytes.HasPrefix(code, minimalProxyTemplate[:10]) {
		return nil, nil
	}
	if !bytes.HasSuffix(code, minimalProxyTemplate[30:]) {
		return nil, nil
	}

	return roaring64.BitmapOf(kv.ADDR_ATTR_ERC1167), nil
}
