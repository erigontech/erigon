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

//go:build !nofuzz

package commitment

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/sha3"

	"github.com/erigontech/erigon-lib/common/length"
)

// go test -trimpath -v -fuzz=Fuzz_ProcessUpdate$ -fuzztime=300s ./commitment

func Fuzz_ProcessUpdate(f *testing.F) {
	ctx := context.Background()
	ha, _ := hex.DecodeString("13ccfe8074645cab4cb42b423625e055f0293c87")
	hb, _ := hex.DecodeString("73f822e709a0016bfaed8b5e81b5f86de31d6895")

	f.Add(uint64(2), ha, uint64(1235105), hb)

	f.Fuzz(func(t *testing.T, balanceA uint64, accountA []byte, balanceB uint64, accountB []byte) {
		if len(accountA) == 0 || len(accountA) > 20 || len(accountB) == 0 || len(accountB) > 20 {
			t.Skip()
		}

		builder := NewUpdateBuilder().
			Balance(hex.EncodeToString(accountA), balanceA).
			Balance(hex.EncodeToString(accountB), balanceB)

		ms := NewMockState(t)
		ms2 := NewMockState(t)
		hph := NewHexPatriciaHashed(20, ms, ms.TempDir())
		hphAnother := NewHexPatriciaHashed(20, ms2, ms2.TempDir())

		hph.SetTrace(false)
		hphAnother.SetTrace(false)

		plainKeys, updates := builder.Build()
		if err := ms.applyPlainUpdates(plainKeys, updates); err != nil {
			t.Fatal(err)
		}
		if err := ms2.applyPlainUpdates(plainKeys, updates); err != nil {
			t.Fatal(err)
		}

		rootHash, err := hph.ProcessKeys(ctx, plainKeys, "")
		if err != nil {
			t.Fatal(err)
		}

		//ms.applyBranchNodeUpdates(branchNodeUpdates)
		if len(rootHash) != 32 {
			t.Fatalf("invalid root hash length: expected 32 bytes, got %v", len(rootHash))
		}

		rootHashAnother, err := hphAnother.ProcessKeys(ctx, plainKeys, "")
		if err != nil {
			t.Fatal(err)
		}
		//ms2.applyBranchNodeUpdates(branchNodeUpdates)

		if len(rootHashAnother) > 32 {
			t.Fatalf("invalid root hash length: expected 32 bytes, got %v", len(rootHash))
		}
		if !bytes.Equal(rootHash, rootHashAnother) {
			t.Fatalf("invalid second root hash with same updates: [%v] != [%v]", hex.EncodeToString(rootHash), hex.EncodeToString(rootHashAnother))
		}
	})
}

// go test -trimpath -v -fuzz=Fuzz_ProcessUpdates_ArbitraryUpdateCount -fuzztime=300s ./commitment

func Fuzz_ProcessUpdates_ArbitraryUpdateCount(f *testing.F) {
	ha, _ := hex.DecodeString("0008852883b2850c7a48f4b0eea3ccc4c04e6cb6025e9e8f7db2589c7dae81517c514790cfd6f668903161349e")
	ctx := context.Background()
	f.Add(ha)

	f.Fuzz(func(t *testing.T, build []byte) {
		if len(build) < 12 {
			t.Skip()
		}
		i := 0
		keysCount := binary.BigEndian.Uint32(build[i : i+4])
		i += 4
		ks := binary.BigEndian.Uint32(build[i : i+4])
		keysSeed := rand.New(rand.NewSource(int64(ks)))
		i += 4
		us := binary.BigEndian.Uint32(build[i : i+4])
		updateSeed := rand.New(rand.NewSource(int64(us)))

		t.Logf("fuzzing %d keys keysSeed=%d updateSeed=%d", keysCount, ks, us)

		builder := NewUpdateBuilder()
		for k := uint32(0); k < keysCount; k++ {
			var key [length.Addr]byte
			n, err := keysSeed.Read(key[:])
			pkey := hex.EncodeToString(key[:])
			require.NoError(t, err)
			require.EqualValues(t, length.Addr, n)

			aux := make([]byte, 32)

			flg := UpdateFlags(updateSeed.Intn(int(CodeUpdate | DeleteUpdate | StorageUpdate | NonceUpdate | BalanceUpdate)))
			switch {
			case flg&BalanceUpdate != 0:
				builder.Balance(pkey, updateSeed.Uint64()).Nonce(pkey, updateSeed.Uint64())
				continue
			case flg&CodeUpdate != 0:
				keccak := sha3.NewLegacyKeccak256().(keccakState)
				var s [8]byte
				n, err := updateSeed.Read(s[:])
				require.NoError(t, err)
				require.EqualValues(t, len(s), n)
				keccak.Write(s[:])
				keccak.Read(aux)

				builder.CodeHash(pkey, hex.EncodeToString(aux))
				continue
			case flg&StorageUpdate != 0:
				sz := updateSeed.Intn(length.Hash)
				n, err = updateSeed.Read(aux[:sz])
				require.NoError(t, err)
				require.EqualValues(t, sz, n)

				loc := make([]byte, updateSeed.Intn(length.Hash-1)+1)
				keysSeed.Read(loc)
				builder.Storage(pkey, hex.EncodeToString(loc), hex.EncodeToString(aux[:sz]))
				continue
			case flg&DeleteUpdate != 0:
				continue
			default:
				continue
			}
		}

		ms := NewMockState(t)
		ms2 := NewMockState(t)
		hph := NewHexPatriciaHashed(20, ms, ms.TempDir())
		hphAnother := NewHexPatriciaHashed(20, ms2, ms2.TempDir())

		plainKeys, updates := builder.Build()

		hph.SetTrace(false)
		hphAnother.SetTrace(false)

		err := ms.applyPlainUpdates(plainKeys, updates)
		require.NoError(t, err)

		rootHashReview, err := hph.ProcessKeys(ctx, plainKeys, "")
		require.NoError(t, err)

		//ms.applyBranchNodeUpdates(branchNodeUpdates)
		require.Len(t, rootHashReview, length.Hash, "invalid root hash length")

		err = ms2.applyPlainUpdates(plainKeys, updates)
		require.NoError(t, err)

		rootHashAnother, err := hphAnother.ProcessKeys(ctx, plainKeys, "")
		require.NoError(t, err)
		//ms2.applyBranchNodeUpdates(branchUpdatesAnother)

		require.Len(t, rootHashAnother, length.Hash, "invalid root hash length")
		require.EqualValues(t, rootHashReview, rootHashAnother, "storage-based and update-based rootHash mismatch")
	})
}

func Fuzz_HexPatriciaHashed_ReviewKeys(f *testing.F) {
	ctx := context.Background()
	var (
		keysCount uint64 = 100
		seed      int64  = 1234123415
	)

	f.Add(keysCount, seed)

	f.Fuzz(func(t *testing.T, keysCount uint64, seed int64) {
		if keysCount > 10e9 {
			return
		}

		rnd := rand.New(rand.NewSource(seed))
		builder := NewUpdateBuilder()

		// generate updates
		for i := 0; i < int(keysCount); i++ {
			key := make([]byte, length.Addr)

			for j := 0; j < len(key); j++ {
				key[j] = byte(rnd.Intn(256))
			}
			builder.Balance(hex.EncodeToString(key), rnd.Uint64())
		}

		ms := NewMockState(t)
		hph := NewHexPatriciaHashed(length.Addr, ms, ms.TempDir())

		hph.SetTrace(false)

		plainKeys, updates := builder.Build()
		if err := ms.applyPlainUpdates(plainKeys, updates); err != nil {
			t.Fatal(err)
		}

		rootHash, err := hph.ProcessKeys(ctx, plainKeys, "")
		require.NoError(t, err)

		//ms.applyBranchNodeUpdates(branchNodeUpdates)
		require.Lenf(t, rootHash, length.Hash, "invalid root hash length")
	})
}
