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

package accounts

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"runtime"
	"sync"
	"testing"
	"time"
	"unique"
	"weak"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

func TestEmptyAccount(t *testing.T) {
	t.Parallel()
	a := Account{
		Nonce:       100,
		Balance:     uint256.Int{},
		Root:        empty.RootHash, // extAccount doesn't have Root value
		CodeHash:    EmptyCodeHash,  // extAccount doesn't have CodeHash value
		Incarnation: 5,
	}

	encodedAccount := SerialiseV3(&a)

	decodedAcc := Account{}
	if err := DeserialiseV3(&decodedAcc, encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	isIncarnationEqual(t, a.Incarnation, decodedAcc.Incarnation)
}

func TestEmptyAccount2(t *testing.T) {
	t.Parallel()
	emptyAcc := Account{}

	encodedAccount := SerialiseV3(&emptyAcc)

	decodedAcc := Account{}
	if err := DeserialiseV3(&decodedAcc, encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	isIncarnationEqual(t, emptyAcc.Incarnation, decodedAcc.Incarnation)
}

// fails if run package tests
// account_test.go:57: cant decode the account malformed RLP for Account(c064): prefixLength(1) + dataLength(0) != sliceLength(2) �d
func TestEmptyAccount_BufferStrangeBehaviour(t *testing.T) {
	t.Parallel()
	a := Account{}

	encodedAccount := SerialiseV3(&a)

	decodedAcc := Account{}
	if err := DeserialiseV3(&decodedAcc, encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	isIncarnationEqual(t, a.Incarnation, decodedAcc.Incarnation)
}

func TestDeserialiseV3CodeHash(t *testing.T) {
	t.Parallel()
	balances := []uint256.Int{{}, *uint256.NewInt(1), *uint256.NewInt(1e18), *new(uint256.Int).Lsh(uint256.NewInt(1), 200)}
	nonces := []uint64{0, 1, 255, 1 << 40}
	codeHashes := []CodeHash{EmptyCodeHash, InternCodeHash(crypto.Keccak256Hash([]byte{1, 2, 3}))}
	incarnations := []uint64{0, 7}

	for _, nonce := range nonces {
		for i := range balances {
			for _, ch := range codeHashes {
				for _, inc := range incarnations {
					a := Account{Nonce: nonce, Balance: balances[i], CodeHash: ch, Incarnation: inc}
					enc := SerialiseV3(&a)

					var full Account
					if err := DeserialiseV3(&full, enc); err != nil {
						t.Fatal(err)
					}
					got := DeserialiseV3CodeHash(enc)
					if full.CodeHash.IsEmpty() {
						if got != nil {
							t.Fatalf("empty codeHash must extract as nil, got %x (acc %+v)", got, a)
						}
					} else {
						want := full.CodeHash.Value()
						if !bytes.Equal(got, want[:]) {
							t.Fatalf("extracted %x, want %x (acc %+v)", got, want, a)
						}
					}
				}
			}
		}
	}
}

func TestDeserialiseV3CodeHashMalformed(t *testing.T) {
	t.Parallel()
	a := Account{
		Nonce:       255,
		Balance:     *uint256.NewInt(1e18),
		CodeHash:    InternCodeHash(crypto.Keccak256Hash([]byte{1, 2, 3})),
		Incarnation: 4,
	}
	enc := SerialiseV3(&a)
	// [1+nonce][1+balance][1+codeHash]... — the codeHash field is complete at:
	codeHashEnd := 1 + int(enc[0]) + 1
	codeHashEnd += int(enc[codeHashEnd-1]) + 1
	codeHashEnd += int(enc[codeHashEnd-1])
	// Any truncation cutting into (or before) the codeHash must yield nil,
	// never an out-of-bounds read; beyond it the codeHash is extractable.
	for cut := 0; cut <= len(enc); cut++ {
		got := DeserialiseV3CodeHash(enc[:cut])
		if cut < codeHashEnd && got != nil {
			t.Fatalf("cut=%d (codeHash complete at %d): expected nil, got %x", cut, codeHashEnd, got)
		}
		if cut >= codeHashEnd && got == nil {
			t.Fatalf("cut=%d (codeHash complete at %d): expected hash, got nil", cut, codeHashEnd)
		}
	}
	if got := DeserialiseV3CodeHash(nil); got != nil {
		t.Fatalf("nil input: expected nil, got %x", got)
	}
	// A record claiming a non-32-byte codeHash is malformed for extraction.
	odd := append([]byte{0, 0, 31}, make([]byte, 31)...)
	if got := DeserialiseV3CodeHash(odd); got != nil {
		t.Fatalf("non-32-byte codeHash field: expected nil, got %x", got)
	}
	// Non-canonical records spelling out the no-code sentinels (canonical
	// SerialiseV3 writes length 0 instead) must extract as nil, matching
	// CodeHash.IsEmpty.
	for _, sentinel := range [][]byte{make([]byte, 32), empty.CodeHash[:]} {
		rec := append([]byte{0, 0, 32}, sentinel...)
		rec = append(rec, 0)
		if got := DeserialiseV3CodeHash(rec); got != nil {
			t.Fatalf("sentinel codeHash %x: expected nil, got %x", sentinel, got)
		}
	}
}

func TestAccountEncodeWithCode(t *testing.T) {
	t.Parallel()
	a := Account{
		Nonce:       2,
		Balance:     *uint256.NewInt(0).SetUint64(1000),
		Root:        common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
		CodeHash:    InternCodeHash(crypto.Keccak256Hash([]byte{1, 2, 3})),
		Incarnation: 4,
	}

	encodedAccount := SerialiseV3(&a)

	decodedAcc := Account{}
	if err := DeserialiseV3(&decodedAcc, encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	isIncarnationEqual(t, a.Incarnation, decodedAcc.Incarnation)
}

func TestAccountEncodeWithCodeWithStorageSizeHack(t *testing.T) {
	t.Parallel()
	a := Account{
		Nonce:       2,
		Balance:     *uint256.NewInt(0).SetUint64(1000),
		Root:        common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
		CodeHash:    InternCodeHash(crypto.Keccak256Hash([]byte{1, 2, 3})),
		Incarnation: 5,
	}

	encodedAccount := SerialiseV3(&a)

	decodedAcc := Account{}
	if err := DeserialiseV3(&decodedAcc, encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	isIncarnationEqual(t, a.Incarnation, decodedAcc.Incarnation)
}

func TestAccountEncodeWithoutCode(t *testing.T) {
	t.Parallel()
	a := Account{
		Nonce:       2,
		Balance:     *uint256.NewInt(0).SetUint64(1000),
		Root:        empty.RootHash, // extAccount doesn't have Root value
		CodeHash:    EmptyCodeHash,  // extAccount doesn't have CodeHash value
		Incarnation: 5,
	}

	encodedAccount := SerialiseV3(&a)

	decodedAcc := Account{}
	if err := DeserialiseV3(&decodedAcc, encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	isIncarnationEqual(t, a.Incarnation, decodedAcc.Incarnation)
}

func TestEncodeAccountWithEmptyBalanceNonNilContractAndNotZeroIncarnation(t *testing.T) {
	t.Parallel()
	a := Account{
		Nonce:       0,
		Balance:     uint256.Int{},
		Root:        common.HexToHash("123"),
		CodeHash:    InternCodeHash(common.HexToHash("123")),
		Incarnation: 1,
	}
	encodedAccount := SerialiseV3(&a)

	decodedAcc := Account{}
	if err := DeserialiseV3(&decodedAcc, encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	isIncarnationEqual(t, a.Incarnation, decodedAcc.Incarnation)
}

func TestEncodeAccountWithEmptyBalanceAndNotZeroIncarnation(t *testing.T) {
	t.Parallel()
	a := Account{
		Nonce:       0,
		Balance:     uint256.Int{},
		Incarnation: 1,
	}
	encodedAccount := SerialiseV3(&a)

	decodedAccount := Account{}
	if err := DeserialiseV3(&decodedAccount, encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	if a.Incarnation != decodedAccount.Incarnation {
		t.FailNow()
	}
	if a.Balance.Cmp(&decodedAccount.Balance) != 0 {
		t.FailNow()
	}
	if a.Nonce != decodedAccount.Nonce {
		t.FailNow()
	}
}

func TestIncarnationForEmptyAccount(t *testing.T) {
	t.Parallel()
	a := Account{
		Nonce:       100,
		Balance:     uint256.Int{},
		Root:        empty.RootHash,
		CodeHash:    EmptyCodeHash,
		Incarnation: 4,
	}

	encodedAccount := SerialiseV3(&a)

	decodedAcc := Account{}
	if err := DeserialiseV3(&decodedAcc, encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	isIncarnationEqual(t, a.Incarnation, decodedAcc.Incarnation)
}

func TestEmptyIncarnationForEmptyAccount2(t *testing.T) {
	t.Parallel()
	a := Account{}

	encodedAccount := SerialiseV3(&a)

	decodedAcc := Account{}
	if err := DeserialiseV3(&decodedAcc, encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	isIncarnationEqual(t, a.Incarnation, decodedAcc.Incarnation)
}

func TestIncarnationWithNonEmptyAccount(t *testing.T) {
	t.Parallel()
	a := Account{
		Nonce:       2,
		Balance:     *uint256.NewInt(0).SetUint64(1000),
		Root:        common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
		CodeHash:    InternCodeHash(crypto.Keccak256Hash([]byte{1, 2, 3})),
		Incarnation: 4,
	}

	encodedAccount := SerialiseV3(&a)

	decodedAcc := Account{}
	if err := DeserialiseV3(&decodedAcc, encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	isIncarnationEqual(t, a.Incarnation, decodedAcc.Incarnation)
}

func TestIncarnationWithNoIncarnation(t *testing.T) {
	t.Parallel()
	a := Account{
		Nonce:       2,
		Balance:     *uint256.NewInt(0).SetUint64(1000),
		Root:        common.HexToHash("0000000000000000000000000000000000000000000000000000000000000021"),
		CodeHash:    InternCodeHash(crypto.Keccak256Hash([]byte{1, 2, 3})),
		Incarnation: 0,
	}

	encodedAccount := SerialiseV3(&a)

	decodedAcc := Account{}
	if err := DeserialiseV3(&decodedAcc, encodedAccount); err != nil {
		t.Fatal("Can't decode the incarnation", err, encodedAccount)
	}

	isIncarnationEqual(t, a.Incarnation, decodedAcc.Incarnation)
}

func TestIncarnationWithInvalidEncodedAccount(t *testing.T) {
	failingSlice := []byte{1, 12}

	if incarnation, err := DecodeIncarnationFromStorage(failingSlice); err == nil {
		t.Fatal("decoded the incarnation", incarnation, failingSlice)
	}
}

func isIncarnationEqual(t *testing.T, initialIncarnation uint64, decodedIncarnation uint64) {
	t.Helper()
	if initialIncarnation != decodedIncarnation {
		t.Fatal("Can't decode the incarnation", initialIncarnation, decodedIncarnation)
	}
}

func TestAccProofResultMarshalFastJSONTo(t *testing.T) {
	proof := func(n int) []hexutil.Bytes {
		nodes := make([]hexutil.Bytes, n)
		for i := range nodes {
			nodes[i] = bytes.Repeat([]byte{byte(i + 1)}, 532)
		}
		return nodes
	}
	maxU256 := (*hexutil.U256)(new(uint256.Int).SetAllOne())
	for name, result := range map[string]*AccProofResult{
		"zero":          {},
		"no storage":    {Address: common.HexToAddress("0x01"), AccountProof: proof(3), Balance: maxU256, Nonce: hexutil.Uint64(^uint64(0))},
		"empty storage": {AccountProof: []hexutil.Bytes{}, Balance: new(hexutil.U256), StorageProof: []StorProofResult{}},
		"slots": {
			AccountProof: proof(8), Balance: (*hexutil.U256)(uint256.NewInt(1 << 40)), CodeHash: common.HexToHash("0xc0de"), StorageHash: common.HexToHash("0x5707"),
			StorageProof: []StorProofResult{
				{Key: "0x01", Value: maxU256, Proof: proof(5)},
				{Key: `"quoted"`, Value: new(hexutil.U256), Proof: []hexutil.Bytes{}},
				{Key: "0x03", Value: (*hexutil.U256)(uint256.NewInt(0x10)), Proof: proof(1)},
				{},
			},
		},
		"large": {AccountProof: proof(2 * jsonstream.FlushThreshold / 532), Balance: new(hexutil.U256)},
	} {
		t.Run(name, func(t *testing.T) {
			want, err := json.Marshal(result)
			require.NoError(t, err)
			s := jsonstream.Get(nil)
			defer jsonstream.Put(s)
			require.NoError(t, result.MarshalFastJSONTo(s))
			require.Equal(t, string(want), string(s.Buffer()))
		})
	}
}

func TestAccountCopyCopiesAllFields(t *testing.T) {
	t.Parallel()
	a := Account{
		Nonce:           1,
		Balance:         *uint256.NewInt(2),
		Root:            common.Hash{3},
		CodeHash:        InternCodeHash(common.Hash{4}),
		Incarnation:     5,
		PrevIncarnation: 6,
	}
	var b Account
	b.Copy(&a)
	require.Equal(t, a, b)
	require.Equal(t, a, *a.SelfCopy())
}

// Values that share an intern-table slot must each come back as their own canonical handle.
func TestInternSlotCollision(t *testing.T) {
	k1, k2 := common.Hash{1}, common.Hash{1}
	k2[10] = 7
	a1, a2 := common.Address{1}, common.Address{2}
	require.Same(t, keySlot(k1), keySlot(k2))
	require.Same(t, addressSlot(a1), addressSlot(a2))
	for range 2 {
		for _, k := range []common.Hash{k1, k2} {
			require.Equal(t, StorageKey(unique.Make(k)), InternKey(k))
			require.Equal(t, k, InternKey(k).Value())
		}
		for _, a := range []common.Address{a1, a2} {
			require.True(t, Address{addressInterner.intern(a)} == InternAddress(a))
			require.Equal(t, a, InternAddress(a).Value())
		}
	}
}

// Precompiles and small scalar storage slots differ only in their last bytes; they must not share a slot.
func TestInternSlotsSpreadLowValues(t *testing.T) {
	require.NotSame(t, addressSlot(common.BytesToAddress([]byte{1})), addressSlot(common.BytesToAddress([]byte{2})))
	require.NotSame(t, keySlot(common.BytesToHash([]byte{1})), keySlot(common.BytesToHash([]byte{2})))
}

func TestInternAddressColdAddressesDoNotPayPerAddressWeakRegistration(t *testing.T) {
	var n uint32 = 1 << 30
	allocs := testing.AllocsPerRun(640, func() {
		var a common.Address
		binary.BigEndian.PutUint32(a[:4], n)
		n++
		InternAddress(a)
	})
	t.Logf("allocs per cold address: %v", allocs)
	require.Less(t, allocs, 2.0)
}

func coldAddress(prefix, i uint32) common.Address {
	var a common.Address
	binary.BigEndian.PutUint32(a[:4], prefix)
	binary.BigEndian.PutUint32(a[4:8], i)
	return a
}

// An address is canonical only while a handle holds its slab; once all are gone its index entry must go too.
func TestSlabInternerForgetsValuesOfDeadSlabs(t *testing.T) {
	in := newSlabInterner[common.Address]()
	probe := coldAddress(2<<28, slabLen)
	for i := range uint32(3 * slabLen) {
		in.intern(coldAddress(2<<28, i))
	}
	require.Eventually(t, func() bool {
		runtime.GC()
		_, ok := in.index.Load(probe)
		return !ok
	}, 10*time.Second, 10*time.Millisecond)
}

func TestSlabInternerKeepsHeldValuesCanonical(t *testing.T) {
	in := newSlabInterner[common.Address]()
	first := coldAddress(3<<28, 0)
	held := in.intern(first)
	for i := uint32(1); i < 2*slabLen; i++ {
		in.intern(coldAddress(3<<28, i))
	}
	for range 5 {
		runtime.GC()
		require.Same(t, held, in.intern(first))
		require.Equal(t, first, *held)
	}
}

func TestSlabInternerConcurrentCallersShareHandle(t *testing.T) {
	in := newSlabInterner[common.Address]()
	const callers = 8
	var wg sync.WaitGroup
	results := make([][]*common.Address, callers)
	for c := range callers {
		wg.Go(func() {
			got := make([]*common.Address, 4*slabLen)
			for i := range got {
				got[i] = in.intern(coldAddress(4<<28, uint32(i)))
			}
			results[c] = got
		})
	}
	wg.Wait()
	for c := 1; c < callers; c++ {
		for i := range results[c] {
			require.Same(t, results[0][i], results[c][i])
		}
	}
}

func TestForgetSlabKeepsEntryTakenOverByNewerSlab(t *testing.T) {
	in := newSlabInterner[common.Address]()
	a := coldAddress(6<<28, 0)
	held := in.intern(a)
	other := new(slab[common.Address])
	values := new(slabValues[common.Address])
	values.values[0] = a
	values.n.Store(1)
	forgetSlab(deadSlab[common.Address]{in.index, weak.Make(other), values})
	runtime.KeepAlive(other)
	require.Same(t, held, in.intern(a))
}

// A fresh process must not reserve the index for every handle addressTable can hold: its untouched pages fault in
// during the first blocks.
func TestAddressIndexIsNotReservedUpFront(t *testing.T) {
	require.Less(t, addressInterner.index.Stats().Capacity, addressTableSize/4)
}
