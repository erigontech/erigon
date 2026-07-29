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

package commitment

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
)

// pbinTestCorpus collects plain-key updates in the two shapes the engine
// accepts and derives the leaf set they must produce, so a Process run can be
// diffed against the reference tree over the same entries.
type pbinTestCorpus struct {
	plainKeys [][]byte
	updates   []Update
}

func (c *pbinTestCorpus) account(addr []byte, nonce, balance uint64, codeHash common.Hash) *pbinTestCorpus {
	u := Update{Flags: NonceUpdate | BalanceUpdate | CodeUpdate, Nonce: nonce, CodeHash: codeHash}
	u.Balance.SetUint64(balance)
	c.plainKeys = append(c.plainKeys, bytes.Clone(addr))
	c.updates = append(c.updates, u)
	return c
}

func (c *pbinTestCorpus) storage(addr, slot []byte, value ...byte) *pbinTestCorpus {
	u := Update{Flags: StorageUpdate, StorageLen: int8(len(value))}
	copy(u.Storage[:], value)
	c.plainKeys = append(c.plainKeys, append(bytes.Clone(addr), slot...))
	c.updates = append(c.updates, u)
	return c
}

// entries is the leaf set the corpus stands for. An account is two leaves, so
// this is also where the fan-out is stated independently of the engine.
func (c *pbinTestCorpus) entries(t *testing.T) []pbinOracleEntry {
	t.Helper()
	entries := make([]pbinOracleEntry, 0, len(c.plainKeys))
	for i, plainKey := range c.plainKeys {
		u := &c.updates[i]
		switch len(plainKey) {
		case length.Addr:
			basic, err := pbinEncodeBasicData(u.Nonce, &u.Balance, 0)
			require.NoError(t, err)
			code := pbinCodeHashValue(u.CodeHash)
			entries = append(entries,
				pbinOracleEntry{key: pbinTreeKeyAccount(plainKey, pbinBasicDataLeafKey), value: basic[:]},
				pbinOracleEntry{key: pbinTreeKeyAccount(plainKey, pbinCodeHashLeafKey), value: code[:]})
		case length.Addr + length.Hash:
			value := pbinEncodeStorageValue(u.Storage[:u.StorageLen])
			entries = append(entries, pbinOracleEntry{
				key:   pbinTreeKeyStorage(plainKey[:length.Addr], plainKey[length.Addr:]),
				value: value[:],
			})
		default:
			t.Fatalf("plain key of %d bytes is neither an account nor a storage key", len(plainKey))
		}
	}
	return entries
}

func (c *pbinTestCorpus) oracleRoot(t *testing.T) []byte {
	t.Helper()
	root := pbinOracleRoot(c.entries(t))
	return root[:]
}

// process applies the corpus to state, then runs it through the engine the way
// the domain layer would: ModeDirect, so every value comes back through the
// context rather than the update stream.
func (c *pbinTestCorpus) process(t *testing.T) (*PBinPatriciaHashed, []byte) {
	t.Helper()
	pph, ms := pbinTestEngine(t)
	require.NoError(t, ms.applyPlainUpdates(c.plainKeys, c.updates))
	return pph, pbinTestProcess(t, pph, c.plainKeys, c.updates)
}

func pbinTestProcess(t *testing.T, pph *PBinPatriciaHashed, plainKeys [][]byte, updates []Update) []byte {
	t.Helper()
	upd := WrapKeyUpdates(t, ModeDirect, pbinKeyHasher(), plainKeys, updates)
	root, err := pph.Process(context.Background(), upd, "", nil, WarmupConfig{})
	require.NoError(t, err)
	return root
}

// TestPBinRootHashEmptyEngine guards H11 at the engine boundary: an empty
// EIP-8297 tree is 32 zero bytes (eip:208), not the empty-MPT root the rest of
// erigon reaches for.
func TestPBinRootHashEmptyEngine(t *testing.T) {
	t.Parallel()

	pph, _ := pbinTestEngine(t)
	root, err := pph.RootHash()
	require.NoError(t, err)
	require.Equal(t, make([]byte, length.Hash), root)
	require.NotEqual(t, empty.RootHash[:], root)
}

// TestPBinProcessSingleKeyRootIsLeaf pins eip:133-135: with one entry the root
// is the leaf itself, not a branch wrapping it.
func TestPBinProcessSingleKeyRootIsLeaf(t *testing.T) {
	t.Parallel()

	addr, slot := pbinOracleAddr(1), pbinOracleSlot(1000)
	corpus := new(pbinTestCorpus).storage(addr, slot, 0x01, 0x02)

	pph, root := corpus.process(t)
	require.Equal(t, pbinNodeLeaf, pph.grid.root.kind)

	value := pbinEncodeStorageValue([]byte{0x01, 0x02})
	want := pbinTestKeccak(t, []byte{0x00}, pbinTreeKeyStorage(addr, slot), value[:])
	require.Equal(t, want, root)
	require.Equal(t, corpus.oracleRoot(t), root)
}

// TestPBinProcessTwoKeysRootIsBranch is the other half: a second entry turns the
// root into a branch over the two leaf hashes.
func TestPBinProcessTwoKeysRootIsBranch(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(2)
	a, b := pbinOracleSlot(256), pbinOracleSlot(257)
	corpus := new(pbinTestCorpus).storage(addr, a, 0xAA).storage(addr, b, 0xBB)

	pph, root := corpus.process(t)
	require.Equal(t, pbinNodeBranch, pph.grid.root.kind)

	// The two sub-indices differ only in their low bit, so the branch prefix is
	// every bit of the key but the last and slot 256 takes the left side.
	left := pbinEncodeStorageValue([]byte{0xAA})
	right := pbinEncodeStorageValue([]byte{0xBB})
	leftHash := pbinTestKeccak(t, []byte{0x00}, pbinTreeKeyStorage(addr, a), left[:])
	rightHash := pbinTestKeccak(t, []byte{0x00}, pbinTreeKeyStorage(addr, b), right[:])
	prefix := pbinOracleBytesToBits(pbinTreeKeyStorage(addr, a))[:pbinMaxPathBits-1]
	want := pbinTestKeccak(t, []byte{0x01}, pbinOracleEncodeBitPrefix(prefix), leftHash, rightHash)

	require.Equal(t, want, root)
	require.Equal(t, corpus.oracleRoot(t), root)
}

// TestPBinProcessMatchesOracle is the M0 gate: for every corpus shape the engine
// must reproduce the reference tree's root.
func TestPBinProcessMatchesOracle(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		corpus *pbinTestCorpus
	}{
		{
			name:   "one account",
			corpus: new(pbinTestCorpus).account(pbinOracleAddr(1), 3, 7, common.Hash{0xC0, 0xDE}),
		},
		{
			name: "accounts only",
			corpus: new(pbinTestCorpus).
				account(pbinOracleAddr(1), 1, 100, common.Hash{0x01}).
				account(pbinOracleAddr(2), 0, 0, common.Hash{}).
				account(pbinOracleAddr(3), 1<<40, 1<<62, empty.CodeHash),
		},
		{
			name: "storage zone only",
			corpus: new(pbinTestCorpus).
				storage(pbinOracleAddr(4), pbinOracleSlot(64), 0x01).
				storage(pbinOracleAddr(4), pbinOracleSlot(255), 0x02, 0x03).
				storage(pbinOracleAddr(4), pbinOracleSlot(256), 0x04).
				storage(pbinOracleAddr(4), pbinOracleSlot(1000), bytes.Repeat([]byte{0xEE}, 32)...),
		},
		{
			name: "header zone slots",
			corpus: new(pbinTestCorpus).
				storage(pbinOracleAddr(5), pbinOracleSlot(0), 0x01).
				storage(pbinOracleAddr(5), pbinOracleSlot(1), 0x02).
				storage(pbinOracleAddr(5), pbinOracleSlot(63), 0x03),
		},
		{
			name: "one account across both zones",
			corpus: new(pbinTestCorpus).
				account(pbinOracleAddr(6), 9, 1234, common.Hash{0xAB}).
				storage(pbinOracleAddr(6), pbinOracleSlot(0), 0x01).
				storage(pbinOracleAddr(6), pbinOracleSlot(63), 0x02).
				storage(pbinOracleAddr(6), pbinOracleSlot(64), 0x03).
				storage(pbinOracleAddr(6), pbinOracleSlot(65), 0x04).
				storage(pbinOracleAddr(6), pbinOracleSlot(1000), 0x05),
		},
		{
			name:   "mixed accounts and storage",
			corpus: pbinTestMixedCorpus(),
		},
		{
			name:   "deep shared prefix",
			corpus: pbinTestDeepSharedPrefixCorpus(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, root := tc.corpus.process(t)
			require.Equal(t, tc.corpus.oracleRoot(t), root)
		})
	}
}

func pbinTestMixedCorpus() *pbinTestCorpus {
	c := new(pbinTestCorpus)
	for i := uint64(1); i <= 6; i++ {
		addr := pbinOracleAddr(i)
		c.account(addr, i, i*1000, common.Hash{byte(i)})
		for _, slot := range []uint64{0, 5, 63, 64, 255, 256, 1000, 1 << 20} {
			c.storage(addr, pbinOracleSlot(slot), byte(i), byte(slot))
		}
	}
	return c
}

// pbinTestDeepSharedPrefixCorpus reuses the mined cluster, so the descent walks
// far past the root before diverging (guards H1's corpus side).
func pbinTestDeepSharedPrefixCorpus() *pbinTestCorpus {
	c := new(pbinTestCorpus)
	for i, addr := range pbinOracleMinedAddrs() {
		c.account(addr, uint64(i), uint64(i)*7, common.Hash{byte(i)})
	}
	return c
}

// TestPBinProcessAccountFansOutToCodeHash pins the sibling leaf: one account
// update produces both BASIC_DATA and CODE_HASH, written during the same stem
// visit so the shared keyHasher stays a one-key function.
func TestPBinProcessAccountFansOutToCodeHash(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(11)
	codeHash := common.Hash{0xC0, 0xDE, 0xFF}
	corpus := new(pbinTestCorpus).account(addr, 5, 999, codeHash)

	pph, root := corpus.process(t)
	require.Equal(t, pbinNodeBranch, pph.grid.root.kind, "two leaves under one stem make a branch")
	require.Equal(t, corpus.oracleRoot(t), root)

	basic, err := pbinEncodeBasicData(5, &corpus.updates[0].Balance, 0)
	require.NoError(t, err)
	basicOnly := pbinOracleRoot([]pbinOracleEntry{
		{key: pbinTreeKeyAccount(addr, pbinBasicDataLeafKey), value: basic[:]},
	})
	require.NotEqual(t, basicOnly[:], root, "dropping the CODE_HASH leaf must change the root")

	code := pbinCodeHashValue(codeHash)
	require.Equal(t, codeHash[:], code[:])
}

// TestPBinProcessRejectsStreamDelete guards H13: EIP-8297 never removes an
// entry, so a delete arriving on the update stream is an error rather than a
// silently applied removal.
func TestPBinProcessRejectsStreamDelete(t *testing.T) {
	t.Parallel()

	pph, ms := pbinTestEngine(t)
	plainKeys := [][]byte{pbinOracleAddr(1)}
	updates := []Update{{Flags: DeleteUpdate}}
	require.NoError(t, ms.applyPlainUpdates(plainKeys, updates))

	upd := WrapKeyUpdates(t, ModeUpdate, pbinKeyHasher(), plainKeys, updates)
	_, err := pph.Process(context.Background(), upd, "", nil, WarmupConfig{})
	require.ErrorIs(t, err, errPBinDeleteUnsupported)
}

// TestPBinProcessMissingStateIsAbsent is H13's other half: a context read for a
// key with no state reports DeleteUpdate, which means "no leaf here" and must
// not be mistaken for a removal.
func TestPBinProcessMissingStateIsAbsent(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(21)
	present := new(pbinTestCorpus).
		storage(addr, pbinOracleSlot(256), 0x01).
		storage(addr, pbinOracleSlot(257), 0x02)

	touched := new(pbinTestCorpus).
		storage(addr, pbinOracleSlot(256), 0x01).
		storage(addr, pbinOracleSlot(257), 0x02).
		storage(addr, pbinOracleSlot(258), 0x03).
		account(pbinOracleAddr(22), 1, 2, common.Hash{0x03})

	pph, ms := pbinTestEngine(t)
	require.NoError(t, ms.applyPlainUpdates(present.plainKeys, present.updates))

	root := pbinTestProcess(t, pph, touched.plainKeys, touched.updates)
	require.Equal(t, present.oracleRoot(t), root, "keys with no state contribute no leaf")
}

// TestPBinProcessRepeatedKeyKeepsOneLeaf checks a stem touched twice in one run
// still holds a single leaf, so the second visit updates rather than splits.
func TestPBinProcessRepeatedKeyKeepsOneLeaf(t *testing.T) {
	t.Parallel()

	addr, slot := pbinOracleAddr(31), pbinOracleSlot(1000)
	pph, ms := pbinTestEngine(t)

	first := new(pbinTestCorpus).storage(addr, slot, 0x01)
	require.NoError(t, ms.applyPlainUpdates(first.plainKeys, first.updates))
	require.Equal(t, first.oracleRoot(t), pbinTestProcess(t, pph, first.plainKeys, first.updates))

	second := new(pbinTestCorpus).storage(addr, slot, 0x02)
	require.NoError(t, ms.applyPlainUpdates(second.plainKeys, second.updates))
	root := pbinTestProcess(t, pph, second.plainKeys, second.updates)

	require.Equal(t, pbinNodeLeaf, pph.grid.root.kind)
	require.Equal(t, second.oracleRoot(t), root)
}

// TestPBinProcessEmptyUpdatesKeepsEmptyRoot checks the drive loop over nothing:
// the root stays the empty-tree constant instead of picking up a shape.
func TestPBinProcessEmptyUpdatesKeepsEmptyRoot(t *testing.T) {
	t.Parallel()

	pph, _ := pbinTestEngine(t)
	root := pbinTestProcess(t, pph, nil, nil)
	require.Equal(t, make([]byte, length.Hash), root)
}
