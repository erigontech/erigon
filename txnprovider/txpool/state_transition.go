// Copyright 2021 The Erigon Authors
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

package txpool

import (
	"context"
	"fmt"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

func (p *TxPool) chainTransition(ctx context.Context, tx kv.Tx, from, to *remoteproto.ExecutionView) ([]*remoteproto.StateChange, error) {
	oldHash, newHash := common.Hash(gointerfaces.ConvertH256ToHash(from.BlockHash)), common.Hash(gointerfaces.ConvertH256ToHash(to.BlockHash))
	oldHeight, newHeight := from.BlockHeight, to.BlockHeight
	var unwound, mined []*remoteproto.StateChange
	for oldHash != newHash {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		unwind := oldHeight >= newHeight
		hash, height := newHash, newHeight
		if unwind {
			hash, height = oldHash, oldHeight
		}
		if height == 0 {
			return nil, fmt.Errorf("state transition has different genesis blocks")
		}
		block, err := p.transitionBlock(ctx, tx, hash, height)
		if err != nil {
			return nil, err
		}
		txs, err := types.MarshalTransactionsBinary(block.Transactions())
		if err != nil {
			return nil, err
		}
		change := &remoteproto.StateChange{BlockHash: gointerfaces.ConvertHashToH256(hash), BlockHeight: height, Txs: txs}
		if unwind {
			change.Direction = remoteproto.Direction_UNWIND
			unwound = append(unwound, change)
			oldHash, oldHeight = block.ParentHash(), height-1
		} else {
			mined = append(mined, change)
			newHash, newHeight = block.ParentHash(), height-1
		}
	}
	slices.Reverse(mined)
	return append(unwound, mined...), nil
}

func (p *TxPool) transitionBlock(ctx context.Context, tx kv.Tx, hash common.Hash, height uint64) (*types.Block, error) {
	block, _, err := rawdb.ReadBlockWithSenders(tx, hash, height)
	if err != nil {
		return nil, err
	}
	if block == nil && p.ethBackend != nil {
		reply, err := p.ethBackend.Block(ctx, &remoteproto.BlockRequest{BlockHash: gointerfaces.ConvertHashToH256(hash), BlockHeight: height})
		if err != nil {
			return nil, err
		}
		if len(reply.BlockRlp) != 0 {
			block = new(types.Block)
			if err := rlp.DecodeBytes(reply.BlockRlp, block); err != nil {
				return nil, err
			}
		}
	}
	if block == nil || block.Hash() != hash || block.NumberU64() != height {
		return nil, fmt.Errorf("state transition block unavailable: %s at %d", hash, height)
	}
	return block, nil
}

type blockAccounts struct {
	kvcache.CacheView
	values map[common.Address][]byte
}

func (v blockAccounts) Get(key []byte) ([]byte, error) {
	value, ok := v.values[common.BytesToAddress(key)]
	if !ok {
		return nil, fmt.Errorf("account was not included in the block refresh: %x", key)
	}
	return value, nil
}

func (p *TxPool) readBlockAccounts(view kvcache.CacheView, changes *remoteproto.StateChangeBatch, unwind, blobs, mined TxnSlots) (kvcache.CacheView, error) {
	addresses := make(map[common.Address]struct{})
	if changes.StateChangesIncomplete {
		for address := range p.senders.senderIDs {
			addresses[address] = struct{}{}
		}
	} else {
		for _, block := range changes.ChangeBatch {
			for _, change := range block.Changes {
				address := common.Address(gointerfaces.ConvertH160toAddress(change.Address))
				if _, ok := p.senders.getID(address); ok {
					addresses[address] = struct{}{}
				}
			}
		}
	}
	for _, mt := range p.queued.best.ms {
		addresses[p.senders.senderID2Addr[mt.TxnSlot.SenderID]] = struct{}{}
	}
	for address := range p.frameAccountsToRefresh {
		addresses[address] = struct{}{}
	}
	for _, mt := range p.all.senderIDFrameTxn {
		addresses[p.senders.senderID2Addr[mt.TxnSlot.SenderID]] = struct{}{}
		if validation := mt.TxnSlot.frameValidation; validation != nil {
			addresses[validation.Payer] = struct{}{}
		}
	}
	for _, batch := range []TxnSlots{unwind, blobs, mined} {
		for i := range batch.Txns {
			addresses[batch.Senders.AddressAt(i)] = struct{}{}
		}
	}
	keys := make([][]byte, 0, len(addresses))
	for address := range addresses {
		keys = append(keys, address[:])
	}
	var values [][]byte
	if batch, ok := view.(interface {
		GetMany([][]byte) ([][]byte, error)
	}); ok {
		var err error
		values, err = batch.GetMany(keys)
		if err != nil {
			return nil, err
		}
	} else {
		values = make([][]byte, len(keys))
		for i, key := range keys {
			value, err := view.Get(key)
			if err != nil {
				return nil, err
			}
			values[i] = value
		}
	}
	result := blockAccounts{CacheView: view, values: make(map[common.Address][]byte, len(keys))}
	for i, key := range keys {
		if len(values[i]) > 0 {
			var account accounts.Account
			if err := accounts.DeserialiseV3(&account, values[i]); err != nil {
				return nil, err
			}
		}
		result.values[common.BytesToAddress(key)] = values[i]
	}
	return result, nil
}
