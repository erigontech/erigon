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

package commitment

import (
	"bytes"
	"context"
	"encoding/hex"
	"math"
	"math/rand"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common/length"
)

// go test -trimpath -v -fuzz=Fuzz_ProcessUpdate -fuzztime=300s ./erigon/execution/commitment

func Fuzz_ProcessUpdate(f *testing.F) {
	ctx := context.Background()
	ha, _ := hex.DecodeString("13ccfe8074645cab4cb42b423625e055f0293c87")
	hb, _ := hex.DecodeString("73f822e709a0016bfaed8b5e81b5f86de31d6895")

	f.Add(uint64(2), ha, uint64(1235105), hb)

	f.Fuzz(func(t *testing.T, balanceA uint64, accountA []byte, balanceB uint64, accountB []byte) {
		if len(accountA) == 0 || len(accountA) > length.Addr || len(accountB) == 0 || len(accountB) > length.Addr {
			t.Skip()
		}
		t.Logf("fuzzing %d keys\n", 2)

		builder := NewUpdateBuilder().
			Balance(hex.EncodeToString(accountA), balanceA).
			Balance(hex.EncodeToString(accountB), balanceB)

		ms := NewMockState(t)
		ms2 := NewMockState(t)
		hph := NewHexPatriciaHashed(length.Addr, ms)
		hphAnother := NewHexPatriciaHashed(length.Addr, ms2)

		hph.SetTrace(false)
		hphAnother.SetTrace(false)

		plainKeys, updates := builder.Build()
		err := ms.applyPlainUpdates(plainKeys, updates)
		require.NoError(t, err)
		err = ms2.applyPlainUpdates(plainKeys, updates)
		require.NoError(t, err)

		upds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, nil, nil)
		rootHashDirect, err := hph.Process(ctx, upds, "")
		require.NoError(t, err)
		require.Len(t, rootHashDirect, length.Hash, "invalid root hash length")
		upds.Close()

		anotherUpds := WrapKeyUpdates(t, ModeUpdate, KeyToHexNibbleHash, nil, nil)
		rootHashUpdate, err := hphAnother.Process(ctx, anotherUpds, "")
		require.NoError(t, err)
		require.Len(t, rootHashUpdate, length.Hash, "invalid root hash length")
		require.Equal(t, rootHashDirect, rootHashUpdate, "storage-based and update-based rootHash mismatch")
	})
}

// go test -trimpath -v -fuzz=Fuzz_ProcessUpdates_ArbitraryUpdateCount2 -fuzztime=300s ./commitment

func Fuzz_ProcessUpdates_ArbitraryUpdateCount2(f *testing.F) {
	//ha, _ := hex.DecodeString("0008852883b2850c7a48f4b0eea3ccc4c04e6cb6025e9e8f7db2589c7dae81517c514790cfd6f668903161349e")
	ctx := context.Background()
	f.Add(uint16(10_000), uint32(1), uint32(2))

	f.Fuzz(func(t *testing.T, keysCount uint16, ks, us uint32) {
		keysSeed := rand.New(rand.NewSource(int64(ks)))
		updateSeed := rand.New(rand.NewSource(int64(us)))

		t.Logf("fuzzing %d keys keysSeed=%d updateSeed=%d", keysCount, ks, us)

		plainKeys := make([][]byte, keysCount)
		updates := make([]Update, keysCount)

		for k := uint16(0); k < keysCount; k++ {

			aux := make([]byte, 32)

			flg := UpdateFlags(updateSeed.Intn(int(CodeUpdate | DeleteUpdate | StorageUpdate | NonceUpdate | BalanceUpdate)))
			if flg&BalanceUpdate != 0 {
				updates[k].Flags |= BalanceUpdate
				bn := uint256.NewInt(math.MaxUint64)
				bn.AddUint64(bn, updateSeed.Uint64())
				updates[k].Balance.Set(bn)
			}
			if flg&NonceUpdate != 0 {
				updates[k].Flags |= NonceUpdate
				updates[k].Nonce = updateSeed.Uint64()
			}
			if flg&CodeUpdate != 0 {
				updates[k].Flags |= CodeUpdate
				updateSeed.Read(aux)
				copy(updates[k].CodeHash[:], aux)
			}
			if flg&DeleteUpdate != 0 {
				updates[k].Flags |= DeleteUpdate
			}
			kl := length.Addr
			if flg&StorageUpdate != 0 {
				kl += length.Hash

				updates[k].Reset()
				updates[k].Flags |= StorageUpdate

				sz := 1 + updateSeed.Intn(len(aux)-1)
				updateSeed.Read(aux[:sz])

				copy(updates[k].Storage[:], aux[:sz])
				updates[k].StorageLen = sz
			}

			plainKeys[k] = make([]byte, kl)
			keysSeed.Read(plainKeys[k][:])
		}

		ms := NewMockState(t)
		ms2 := NewMockState(t)
		hph := NewHexPatriciaHashed(length.Addr, ms)
		hphAnother := NewHexPatriciaHashed(length.Addr, ms2)

		trace := false
		hph.SetTrace(trace)
		hphAnother.SetTrace(trace)

		for i := 0; i < len(plainKeys); i++ {
			err := ms.applyPlainUpdates(plainKeys[i:i+1], updates[i:i+1])
			require.NoError(t, err)

			updsDirect := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys[i:i+1], updates[i:i+1])
			rootHashDirect, err := hph.Process(ctx, updsDirect, "")
			updsDirect.Close()
			require.NoError(t, err)
			require.Len(t, rootHashDirect, length.Hash, "invalid root hash length")

			err = ms2.applyPlainUpdates(plainKeys[i:i+1], updates[i:i+1])
			require.NoError(t, err)

			upds := WrapKeyUpdates(t, ModeUpdate, KeyToHexNibbleHash, plainKeys[i:i+1], updates[i:i+1])
			rootHashAnother, err := hphAnother.Process(ctx, upds, "")
			upds.Close()
			require.NoError(t, err)
			require.Len(t, rootHashAnother, length.Hash, "invalid root hash length")
			if !bytes.Equal(rootHashDirect, rootHashAnother) {
				t.Logf("rootHashDirect=%x rootHashUpdates=%x", rootHashDirect, rootHashAnother)
				t.Logf("Update %d/%d %x", i+1, len(plainKeys), plainKeys[i])
				t.Logf("%s", updates[i].String())
			}
			require.Equal(t, rootHashDirect, rootHashAnother, "storage-based and update-based rootHash mismatch")
		}

	})
}

func Fuzz_HexPatriciaHashed_ReviewKeys(f *testing.F) {
	ctx := context.Background()
	var (
		keysCount uint64 = 100000
		seed      int64  = 1234123415
	)

	f.Add(keysCount, seed)

	f.Fuzz(func(t *testing.T, kc uint64, seed int64) {
		if kc > 10e9 {
			return
		}

		rnd := rand.New(rand.NewSource(seed))
		builder := NewUpdateBuilder()

		// generate updates
		for i := 0; i < int(kc); i++ {
			key := make([]byte, length.Addr)

			for j := 0; j < len(key); j++ {
				key[j] = byte(rnd.Intn(256))
			}
			addr := hex.EncodeToString(key)
			builder.Balance(addr, rnd.Uint64())
			builder.Nonce(addr, uint64(i))
			builder.CodeHash(addr, hex.EncodeToString(append(key, make([]byte, 12)...)))
		}
		t.Logf("keys count: %d", kc)

		ms := NewMockState(t)
		hph := NewHexPatriciaHashed(length.Addr, ms)

		plainKeys, updates := builder.Build()
		if err := ms.applyPlainUpdates(plainKeys, updates); err != nil {
			t.Fatal(err)
		}

		upds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
		defer upds.Close()

		rootHash, err := hph.Process(ctx, upds, "")
		require.NoError(t, err)
		require.Lenf(t, rootHash, length.Hash, "invalid root hash length")
	})
}
