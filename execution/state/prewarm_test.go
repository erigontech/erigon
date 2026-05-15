// Copyright 2026 The Erigon Authors
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

package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestPrewarmBlockStateCacheFromBAL_NilGuards(t *testing.T) {
	// Each nil/empty arg must early-return without panicking.
	PrewarmBlockStateCacheFromBAL(nil, nil, nil)
	bsc := NewBlockStateCache()
	PrewarmBlockStateCacheFromBAL(bsc, nil, nil)
	PrewarmBlockStateCacheFromBAL(bsc, types.BlockAccessList{}, cache.NewDefaultStateCache())
}

func TestPrewarmBlockStateCacheFromBAL_AccountHitPopulatesBSC(t *testing.T) {
	bsc := NewBlockStateCache()
	sc := cache.NewDefaultStateCache()

	var addr common.Address
	addr[19] = 0x42
	internedAddr := accounts.InternAddress(addr)

	src := &accounts.Account{Nonce: 7, Balance: *uint256.NewInt(12345)}
	encoded := accounts.SerialiseV3(src)
	sc.Put(kv.AccountsDomain, addr[:], encoded)

	bal := types.BlockAccessList{
		&types.AccountChanges{Address: internedAddr},
	}
	PrewarmBlockStateCacheFromBAL(bsc, bal, sc)

	got, ok := bsc.GetCommittedAccount(internedAddr)
	require.True(t, ok, "BlockStateCache must hit after prewarm")
	require.NotNil(t, got)
	require.Equal(t, uint64(7), got.Nonce)
	require.Equal(t, *uint256.NewInt(12345), got.Balance)
}

func TestPrewarmBlockStateCacheFromBAL_AccountMissIsSkipped(t *testing.T) {
	bsc := NewBlockStateCache()
	sc := cache.NewDefaultStateCache()

	var addr common.Address
	addr[19] = 0x42
	internedAddr := accounts.InternAddress(addr)

	bal := types.BlockAccessList{
		&types.AccountChanges{Address: internedAddr},
	}
	PrewarmBlockStateCacheFromBAL(bsc, bal, sc)

	_, ok := bsc.GetCommittedAccount(internedAddr)
	require.False(t, ok, "cache.StateCache miss must leave BlockStateCache empty so the lazy fallthrough handles it")
}

func TestPrewarmBlockStateCacheFromBAL_NegativeAccountPropagated(t *testing.T) {
	bsc := NewBlockStateCache()
	sc := cache.NewDefaultStateCache()

	var addr common.Address
	addr[19] = 0x42
	internedAddr := accounts.InternAddress(addr)

	// Empty bytes are the negative-cache encoding (account doesn't exist).
	sc.Put(kv.AccountsDomain, addr[:], nil)

	bal := types.BlockAccessList{
		&types.AccountChanges{Address: internedAddr},
	}
	PrewarmBlockStateCacheFromBAL(bsc, bal, sc)

	got, ok := bsc.GetCommittedAccount(internedAddr)
	require.True(t, ok, "negative cache entry must surface in BlockStateCache")
	require.Nil(t, got, "PutCommittedAccount(nil) is the negative-cache contract")
}

func TestPrewarmBlockStateCacheFromBAL_StorageHitPopulatesBSC(t *testing.T) {
	bsc := NewBlockStateCache()
	sc := cache.NewDefaultStateCache()

	var addr common.Address
	addr[19] = 0x42
	internedAddr := accounts.InternAddress(addr)

	var slotHash common.Hash
	slotHash[31] = 0x10
	internedSlot := accounts.InternKey(slotHash)

	storageKey := make([]byte, 52)
	copy(storageKey, addr[:])
	copy(storageKey[20:], slotHash[:])
	value := []byte{0xab, 0xcd, 0xef}
	sc.Put(kv.StorageDomain, storageKey, value)

	bal := types.BlockAccessList{
		&types.AccountChanges{
			Address:      internedAddr,
			StorageReads: []accounts.StorageKey{internedSlot},
		},
	}
	PrewarmBlockStateCacheFromBAL(bsc, bal, sc)

	got, ok := bsc.GetCommittedStorage(internedAddr, internedSlot)
	require.True(t, ok, "BlockStateCache storage must hit after prewarm")
	require.Equal(t, value, got)
}
