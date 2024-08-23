// Copyright 2024 The Erigon Authors
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

package jsonrpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/polygon/bor/borcfg"

	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/crypto"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/polygon/bor/valset"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/rpchelper"
)

const (
	checkpointInterval = 1024 // Number of blocks after which vote snapshots are saved to db
)

var (
	extraVanity = 32 // Fixed number of extra-data prefix bytes reserved for signer vanity
	extraSeal   = 65 // Fixed number of extra-data suffix bytes reserved for signer seal
)

var (
	// errUnknownBlock is returned when the list of signers is requested for a block
	// that is not part of the local blockchain.
	errUnknownBlock = errors.New("unknown block")

	// errMissingSignature is returned if a block's extra-data section doesn't seem
	// to contain a 65 byte secp256k1 signature.
	errMissingSignature = errors.New("extra-data 65 byte signature suffix missing")

	// errOutOfRangeChain is returned if an authorization list is attempted to
	// be modified via out-of-range or non-contiguous headers.
	errOutOfRangeChain = errors.New("out of range or non-contiguous chain")

	// errMissingVanity is returned if a block's extra-data section is shorter than
	// 32 bytes, which is required to store the signer vanity.
	errMissingVanity = errors.New("extra-data 32 byte vanity prefix missing")
)

// getHeaderByNumber returns a block's header given a block number ignoring the block's transaction and uncle list (may be faster).
// derived from erigon_getHeaderByNumber implementation (see ./erigon_block.go)
func getHeaderByNumber(ctx context.Context, number rpc.BlockNumber, api *BorImpl, tx kv.Tx) (*types.Header, error) {
	// Pending block is only known by the miner
	if number == rpc.PendingBlockNumber {
		block := api.pendingBlock()
		if block == nil {
			return nil, nil
		}
		return block.Header(), nil
	}

	blockNum, _, _, err := rpchelper.GetBlockNumber(ctx, rpc.BlockNumberOrHashWithNumber(number), tx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	}

	header, err := api._blockReader.HeaderByNumber(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, fmt.Errorf("block header not found: %d", blockNum)
	}

	return header, nil
}

// getHeaderByHash returns a block's header given a block's hash.
// derived from erigon_getHeaderByHash implementation (see ./erigon_block.go)
func getHeaderByHash(ctx context.Context, api *BorImpl, tx kv.Tx, hash common.Hash) (*types.Header, error) {
	header, err := api._blockReader.HeaderByHash(ctx, tx, hash)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, fmt.Errorf("block header not found: %s", hash.String())
	}

	return header, nil
}

// ecrecover extracts the Ethereum account address from a signed header.
func ecrecover(header *types.Header, c *borcfg.BorConfig) (common.Address, error) {
	// Retrieve the signature from the header extra-data
	if len(header.Extra) < extraSeal {
		return common.Address{}, errMissingSignature
	}
	signature := header.Extra[len(header.Extra)-extraSeal:]

	// Recover the public key and the Ethereum address
	pubkey, err := crypto.Ecrecover(bor.SealHash(header, c).Bytes(), signature)
	if err != nil {
		return common.Address{}, err
	}
	var signer common.Address
	copy(signer[:], crypto.Keccak256(pubkey[1:])[12:])

	return signer, nil
}

// validatorContains checks for a validator in given validator set
func validatorContains(a []*valset.Validator, x *valset.Validator) (*valset.Validator, bool) {
	for _, n := range a {
		if bytes.Equal(n.Address.Bytes(), x.Address.Bytes()) {
			return n, true
		}
	}
	return nil, false
}

type ValidatorSet = valset.ValidatorSet

// getUpdatedValidatorSet applies changes to a validator set and returns a new validator set
func getUpdatedValidatorSet(oldValidatorSet *ValidatorSet, newVals []*valset.Validator) *ValidatorSet {
	v := oldValidatorSet
	oldVals := v.Validators

	changes := make([]*valset.Validator, 0, len(oldVals))
	for _, ov := range oldVals {
		if f, ok := validatorContains(newVals, ov); ok {
			ov.VotingPower = f.VotingPower
		} else {
			ov.VotingPower = 0
		}

		changes = append(changes, ov)
	}

	for _, nv := range newVals {
		if _, ok := validatorContains(changes, nv); !ok {
			changes = append(changes, nv)
		}
	}

	v.UpdateWithChangeSet(changes)
	return v
}

// author returns the Ethereum address recovered
// from the signature in the header's extra-data section.
func author(api *BorImpl, tx kv.Tx, header *types.Header) (common.Address, error) {
	borEngine, _ := api.bor()
	return ecrecover(header, borEngine.Config())
}

func rankMapDifficulties(values map[common.Address]uint64) []difficultiesKV {
	ss := make([]difficultiesKV, 0, len(values))
	for k, v := range values {
		ss = append(ss, difficultiesKV{k, v})
	}

	sort.Slice(ss, func(i, j int) bool {
		return ss[i].Difficulty > ss[j].Difficulty
	})

	return ss
}
