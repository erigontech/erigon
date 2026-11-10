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
	"bytes"
	"math/big"
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type fixedTemporalTx struct {
	kv.TemporalTx
	val []byte
}

func (g fixedTemporalTx) GetLatest(name kv.Domain, k []byte, _ kv.GetLatestOptions) ([]byte, kv.Step, error) {
	return g.val, 0, nil
}

func (g fixedTemporalTx) GetLatestValSize(name kv.Domain, k []byte) (int, bool, error) {
	return len(g.val), len(g.val) > 0, nil
}

func (g fixedTemporalTx) HasPrefix(name kv.Domain, prefix []byte) ([]byte, []byte, bool, error) {
	return nil, nil, false, nil
}
func (g fixedTemporalTx) StepsInFiles(entitySet ...kv.Domain) kv.Step { return 0 }

type histMockTx struct {
	kv.TemporalTx
	val []byte
}

func (m histMockTx) GetAsOf(domain kv.Domain, key []byte, ts uint64) ([]byte, bool, error) {
	return m.val, true, nil
}

func TestStateReader_ReadMethods_Allocs(t *testing.T) {
	var acc accounts.Account
	acc.Nonce = 1
	accEnc := accounts.SerialiseV3(&acc) // valid encoding so ReadAccountData hits the deserialize path

	r := NewReaderV3(execctx.NewTemporalTxStateGetter(fixedTemporalTx{val: accEnc}))
	addr := accounts.InternAddress(common.Address{0x11})
	key := accounts.InternKey(common.Hash{0x22})
	hr := NewHistoryReaderV3(histMockTx{val: accEnc}, 0)

	addrValue := addr.Value()
	c3 := NewCachedReader3(stubCacheView{string(addrValue[:]): accEnc, storageCacheKey(addr, key): make([]byte, 32)}, nil)

	for _, tc := range []struct {
		name string
		want float64
		fn   func()
	}{
		{"ReaderV3.ReadAccountStorage", 0, func() { _, _, _ = r.ReadAccountStorage(addr, key) }},
		{"ReaderV3.ReadAccountData", 1, func() { _, _ = r.ReadAccountData(addr) }}, // 1: returns *accounts.Account
		{"ReaderV3.ReadAccountCode", 0, func() { _, _ = r.ReadAccountCode(addr) }},
		{"ReaderV3.ReadAccountCodeSize", 0, func() { _, _ = r.ReadAccountCodeSize(addr) }},
		{"ReaderV3.ReadAccountDataForDebug", 1, func() { _, _ = r.ReadAccountDataForDebug(addr) }}, // 1: returns *accounts.Account
		{"ReaderV3.ReadAccountIncarnation", 0, func() { _, _ = r.ReadAccountIncarnation(addr) }},
		{"ReaderV3.HasAccount", 0, func() { _, _ = r.HasAccount(addrValue) }}, // 0: answers from the encoded length

		{"HistoryReaderV3.ReadAccountStorage", 0, func() { _, _, _ = hr.ReadAccountStorage(addr, key) }},
		{"HistoryReaderV3.ReadAccountCode", 0, func() { _, _ = hr.ReadAccountCode(addr) }},
		{"HistoryReaderV3.ReadAccountCodeSize", 0, func() { _, _ = hr.ReadAccountCodeSize(addr) }},
		{"HistoryReaderV3.ReadAccountData", 1, func() { _, _ = hr.ReadAccountData(addr) }},                 // 1: returns *accounts.Account
		{"HistoryReaderV3.ReadAccountDataForDebug", 1, func() { _, _ = hr.ReadAccountDataForDebug(addr) }}, // 1: returns *accounts.Account
		{"HistoryReaderV3.HasAccount", 0, func() { _, _ = hr.HasAccount(addrValue) }},                      // 0: answers from the encoded length

		{"CachedReader3.ReadAccountData", 1, func() { _, _ = c3.ReadAccountData(addr) }}, // 1: returns *accounts.Account
		{"CachedReader3.HasAccount", 0, func() { _, _ = c3.HasAccount(addrValue) }},      // 0: answers from the encoded length
		{"CachedReader3.ReadAccountStorage (cache hit)", 0, func() { _, _, _ = c3.ReadAccountStorage(addr, key) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allocs := testing.AllocsPerRun(100, tc.fn)
			require.Equal(t, tc.want, allocs, "%s: alloc count changed", tc.name)
		})
	}
}

// HasAccount must answer the same question ReadAccountData's nil check answered, both ways.
func TestStateReader_HasAccount_Existence(t *testing.T) {
	var acc accounts.Account
	acc.Nonce = 1
	accEnc := accounts.SerialiseV3(&acc)
	present := accounts.InternAddress(common.Address{0x11})
	absent := accounts.InternAddress(common.Address{0x22})
	presentValue := present.Value()

	for name, r := range map[string]StateReader{
		"CachedReader3":   NewCachedReader3(stubCacheView{string(presentValue[:]): accEnc}, nil),
		"ReaderV3":        NewReaderV3(execctx.NewTemporalTxStateGetter(addrTemporalTx{present: presentValue, val: accEnc})),
		"HistoryReaderV3": NewHistoryReaderV3(addrHistTx{present: presentValue, val: accEnc}, 0),
	} {
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				addr accounts.Address
				want bool
			}{{present, true}, {absent, false}} {
				got, err := r.HasAccount(tc.addr.Value())
				require.NoError(t, err)
				require.Equal(t, tc.want, got)

				data, err := r.ReadAccountData(tc.addr)
				require.NoError(t, err)
				require.Equal(t, tc.want, data != nil, "HasAccount must agree with ReadAccountData")
			}
		})
	}
}

// addrTemporalTx answers only for one address, so an absent account reads empty.
type addrTemporalTx struct {
	kv.TemporalTx
	present common.Address
	val     []byte
}

func (g addrTemporalTx) GetLatest(_ kv.Domain, k []byte, _ kv.GetLatestOptions) ([]byte, kv.Step, error) {
	if bytes.Equal(k, g.present[:]) {
		return g.val, 0, nil
	}
	return nil, 0, nil
}

func (g addrTemporalTx) GetLatestValSize(_ kv.Domain, k []byte) (int, bool, error) {
	if bytes.Equal(k, g.present[:]) {
		return len(g.val), true, nil
	}
	return 0, false, nil
}
func (g addrTemporalTx) StepsInFiles(...kv.Domain) kv.Step { return 0 }

type addrHistTx struct {
	kv.TemporalTx
	present common.Address
	val     []byte
}

func (m addrHistTx) GetAsOf(_ kv.Domain, key []byte, _ uint64) ([]byte, bool, error) {
	if bytes.Equal(key, m.present[:]) {
		return m.val, true, nil
	}
	return nil, false, nil
}

// returnReadList pools the list, so leaving Vals populated keeps every value the
// transaction read — bytecode included — alive for as long as the pool holds it.
// Not parallel: it inspects an object it has just handed back to the pool.
func TestReturnReadListUnpinsWhatTheTxnRead(t *testing.T) {
	lists := readListPool.Get().(ReadLists)
	tbl := lists[kv.AccountsDomain.String()]
	tbl.Push("k1", make([]byte, 4096))
	tbl.Push("k2", make([]byte, 4096))
	require.Equal(t, 2, tbl.Len())

	returnReadList(lists)

	require.Zero(t, tbl.Len())
	for i, v := range tbl.Vals[:cap(tbl.Vals)] {
		require.Nil(t, v, "Vals[%d] still pins %d bytes the txn read", i, len(v))
	}
	for i, k := range tbl.Keys[:cap(tbl.Keys)] {
		require.Empty(t, k, "Keys[%d] still pins a key", i)
	}
}

// stubCacheView answers from a map keyed by the composite key.
type stubCacheView map[string][]byte

func storageCacheKey(a accounts.Address, k accounts.StorageKey) string {
	av, kv := a.Value(), k.Value()
	return string(av[:]) + string(kv[:])
}

func (s stubCacheView) Get(k []byte) ([]byte, error)     { return s[string(k)], nil }
func (s stubCacheView) GetCode(k []byte) ([]byte, error) { return s[string(k)], nil }

// The composite key a storage read builds lives on the reader, so consecutive reads of
// different slots must not bleed into each other.
func TestCachedReader3StorageKeyIsReused(t *testing.T) {
	addr1 := accounts.InternAddress(common.HexToAddress("0x01"))
	addr2 := accounts.InternAddress(common.HexToAddress("0x02"))
	slot1 := accounts.InternKey(common.HexToHash("0x0a"))
	slot2 := accounts.InternKey(common.HexToHash("0x0b"))

	view := stubCacheView{
		storageCacheKey(addr1, slot1): {0x11},
		storageCacheKey(addr2, slot2): {0x22},
	}
	r := NewCachedReader3(view, nil)

	for _, tc := range []struct {
		addr accounts.Address
		slot accounts.StorageKey
		want uint64
	}{
		{addr1, slot1, 0x11},
		{addr2, slot2, 0x22},
		{addr1, slot2, 0},
		{addr1, slot1, 0x11},
	} {
		v, _, err := r.ReadAccountStorage(tc.addr, tc.slot)
		require.NoError(t, err)
		require.Equal(t, tc.want, v.Uint64())
	}
}

// stubPutDel records the composite keys it is handed, as a string, which is what the real
// domains do when they keep one.
type stubPutDel struct {
	kv.TemporalPutDel
	keys []string
}

func (s *stubPutDel) DomainPut(_ kv.Domain, k, _ []byte, _ uint64, _ []byte) error {
	if s.keys == nil {
		return nil
	}
	s.keys = append(s.keys, string(k))
	return nil
}

func (s *stubPutDel) DomainDel(_ kv.Domain, k []byte, _ uint64, _ []byte) error {
	s.keys = append(s.keys, string(k))
	return nil
}

// The writer builds its storage key once and refills it, so writing different slots must
// still address different keys and must not allocate.
func TestWriterStorageKeyIsReused(t *testing.T) {
	put := &stubPutDel{keys: make([]string, 0, 2)}
	w := NewWriter(put, nil, 1)
	addr := accounts.InternAddress(common.HexToAddress("0x01"))
	slot1 := accounts.InternKey(common.HexToHash("0x0a"))
	slot2 := accounts.InternKey(common.HexToHash("0x0b"))

	require.NoError(t, w.WriteAccountStorage(addr, 0, slot1, uint256.Int{}, *uint256.NewInt(1)))
	require.NoError(t, w.WriteAccountStorage(addr, 0, slot2, uint256.Int{}, *uint256.NewInt(2)))
	require.Len(t, put.keys, 2)
	require.NotEqual(t, put.keys[0], put.keys[1], "each slot must address its own key")

	// Writing slot1 again must address slot1, not whatever the buffer last held.
	require.NoError(t, w.WriteAccountStorage(addr, 0, slot1, uint256.Int{}, *uint256.NewInt(3)))
	require.Equal(t, put.keys[0], put.keys[2])

	// One allocation remains and it is not the key: uint256.Int.Bytes() allocates the
	// trimmed value, which escapes into DomainPut. Reinstating a per-call key makes it two.
	quiet := NewWriter(&stubPutDel{}, nil, 1)
	allocs := testing.AllocsPerRun(100, func() {
		_ = quiet.WriteAccountStorage(addr, 0, slot1, uint256.Int{}, *uint256.NewInt(1))
	})
	require.Equal(t, float64(1), allocs, "the composite key must not allocate")
}

// WriteSet.Apply builds one storage key for the whole call rather than one per slot, so its
// allocations must not scale twice per slot. The domains keep a string copy of each key, so
// one allocation per slot is expected and stays; a reinstated per-slot make would double it.
func TestWriteSetApplyKeyDoesNotScalePerSlot(t *testing.T) {
	_, tx, domains := NewTestRwTx(t)
	addr := accounts.InternAddress(common.HexToAddress("0x01"))

	applyAllocs := func(slots int) float64 {
		writes := &WriteSet{
			storage: map[accounts.Address]map[accounts.StorageKey]*VersionedWrite[uint256.Int]{addr: {}},
		}
		for i := range slots {
			key := accounts.InternKey(common.BigToHash(big.NewInt(int64(i + 1))))
			writes.storage[addr][key] = &VersionedWrite[uint256.Int]{
				WriteHeader: WriteHeader{Address: addr, Key: key, Path: StoragePath},
				Val:         *uint256.NewInt(uint64(i + 1)),
			}
		}
		return testing.AllocsPerRun(5, func() {
			require.NoError(t, ApplyWrites(writes, domains, tx, 1, 1, nil, &chain.Rules{}, false))
		})
	}

	few, many := applyAllocs(2), applyAllocs(16)
	perSlot := (many - few) / 14
	// Measured 2.21 with the shared key and 3.21 with a per-slot make: the extra
	// allocation is exactly the key, so the bound sits between the two.
	require.Less(t, perSlot, 2.7,
		"allocations per storage slot (%v) must not include a composite key", perSlot)
}

// ApplyWrites keeps the account fields it collects by value, so writing nonce, incarnation and
// code hash next to a balance must not add an allocation per field.
func TestApplyWritesAccountFieldsDoNotAllocatePerField(t *testing.T) {
	_, tx, domains := NewTestRwTx(t)
	const n = 16
	addrs := make([]accounts.Address, n)
	for i := range addrs {
		addrs[i] = accounts.InternAddress(common.BigToAddress(big.NewInt(int64(i + 1))))
	}
	codeHash := accounts.InternCodeHash(common.HexToHash("0xc0de"))

	applyAllocs := func(allFields bool) float64 {
		writes := &WriteSet{}
		for i, addr := range addrs {
			writes.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath}, Val: *uint256.NewInt(uint64(i + 1))})
			if allFields {
				writes.SetNonce(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath}, Val: uint64(i + 1)})
				writes.SetIncarnation(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: IncarnationPath}, Val: 1})
				writes.SetCodeHash(addr, &VersionedWrite[accounts.CodeHash]{WriteHeader: WriteHeader{Address: addr, Path: CodeHashPath}, Val: codeHash})
			}
		}
		return testing.AllocsPerRun(5, func() {
			require.NoError(t, ApplyWrites(writes, domains, tx, 1, 1, nil, &chain.Rules{}, false))
		})
	}

	perAccount := (applyAllocs(true) - applyAllocs(false)) / n
	require.Less(t, perAccount, 1.5,
		"three more account fields cost %v allocations per account; holding them by value costs none", perAccount)
}

// storageLatestReads counts the storage reads ApplyWrites sends to the database.
type storageLatestReads struct {
	kv.TemporalTx
	count int
}

func (s *storageLatestReads) GetLatest(name kv.Domain, k []byte, opts kv.GetLatestOptions) ([]byte, kv.Step, error) {
	if name == kv.StorageDomain {
		s.count++
	}
	return s.TemporalTx.GetLatest(name, k, opts)
}

func storageFoldView(addr accounts.Address, key accounts.StorageKey, vw *VersionedWrite[uint256.Int]) WriteSetView {
	vw.WriteHeader.Address, vw.WriteHeader.Key, vw.WriteHeader.Path = addr, key, StoragePath
	ws := &WriteSet{}
	ws.SetStorage(addr, key, vw)
	return NewVersionMapWriteView(ws, NewVersionMap(nil), 0)
}

// A storage write recorded by a tx already holds the value the tx read, so the fold
// must take the slot's previous value from it and not read the state again.
func TestApplyWritesFoldTakesPrevFromRecordedOrigin(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0x01"))
	key := accounts.InternKey(common.HexToHash("0x02"))
	addrValue, keyValue := addr.Value(), key.Value()
	composite := slices.Concat(addrValue[:], keyValue[:])

	for _, tc := range []struct {
		name   string
		status valueStatus
		orig   uint64
		val    uint64
		want   []byte
	}{
		{"deleted", ValueDeleted, 5, 0, []byte{}},
		{"created", ValueCreated, 0, 7, []byte{7}},
		{"changed", ValueChanged, 5, 7, []byte{7}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, tx, domains := NewTestRwTx(t)
			reads := &storageLatestReads{TemporalTx: tx}
			changes := &changeset.StateChangeSet{}
			domains.SetChangesetAccumulator(changes)
			view := storageFoldView(addr, key, &VersionedWrite[uint256.Int]{
				WriteHeader: WriteHeader{valStatus: tc.status},
				Val:         *uint256.NewInt(tc.val),
				orig:        *uint256.NewInt(tc.orig),
			})

			require.NoError(t, ApplyWrites(view, domains, reads, 1, 1, nil, &chain.Rules{}, false))

			require.Zero(t, reads.count, "the recorded origin is the previous value")
			got, _, ok := domains.GetLatestFromMemory(kv.StorageDomain, composite)
			require.True(t, ok, "the write reaches the shared domains")
			require.True(t, bytes.Equal(tc.want, got), "got %x want %x", got, tc.want)
			diff := changes.Diffs[kv.StorageDomain].GetDiffSet()
			require.Len(t, diff, 1)
			require.True(t, bytes.Equal(uint256.NewInt(tc.orig).Bytes(), diff[0].Value), "the changeset keeps the origin as prev, got %x", diff[0].Value)
		})
	}
}

// A write without a recorded origin (zero origin, default status) must still read the
// previous value: a zero prev would turn a real delete into a no-op.
func TestApplyWritesFoldReadsPrevWithoutRecordedOrigin(t *testing.T) {
	_, tx, domains := NewTestRwTx(t)
	addr := accounts.InternAddress(common.HexToAddress("0x01"))
	key := accounts.InternKey(common.HexToHash("0x02"))
	addrValue, keyValue := addr.Value(), key.Value()
	composite := slices.Concat(addrValue[:], keyValue[:])
	require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, composite, []byte{9}, 1, nil))

	view := storageFoldView(addr, key, &VersionedWrite[uint256.Int]{})
	require.NoError(t, ApplyWrites(view, domains, tx, 1, 2, nil, &chain.Rules{}, false))

	got, _, ok := domains.GetLatestFromMemory(kv.StorageDomain, composite)
	require.True(t, ok)
	require.Empty(t, got, "the delete must be applied")
}

// A slot cleared by a creation earlier in the block must not take its prev value from
// a later tx's origin, which can predate the clear.
func TestApplyWritesFoldReadsPrevAfterAnEarlierCreate(t *testing.T) {
	_, tx, domains := NewTestRwTx(t)
	addr := accounts.InternAddress(common.HexToAddress("0x01"))
	key := accounts.InternKey(common.HexToHash("0x02"))
	addrValue, keyValue := addr.Value(), key.Value()
	composite := slices.Concat(addrValue[:], keyValue[:])
	require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, composite, []byte{5}, 1, nil))
	require.NoError(t, domains.DomainDelPrefix(kv.StorageDomain, tx, addrValue[:], 2))
	changes := &changeset.StateChangeSet{}
	domains.SetChangesetAccumulator(changes)

	vm := NewVersionMap(nil)
	vm.WriteCreateContract(addr, Version{TxIndex: 0}, true, true)
	ws := &WriteSet{}
	ws.SetStorage(addr, key, &VersionedWrite[uint256.Int]{
		WriteHeader: WriteHeader{Address: addr, Key: key, Path: StoragePath, valStatus: ValueChanged},
		Val:         *uint256.NewInt(7),
		orig:        *uint256.NewInt(5),
	})

	require.NoError(t, ApplyWrites(NewVersionMapWriteView(ws, vm, 1), domains, tx, 1, 3, nil, &chain.Rules{}, false))

	diff := changes.Diffs[kv.StorageDomain].GetDiffSet()
	require.Len(t, diff, 1)
	require.Empty(t, diff[0].Value, "the creation cleared the slot, so its prev value is empty")
}

// A slot cleared by a creation or destruct in an uncommitted prior block must take an empty
// prev value: the block may still be in the exec window or already folded into the overlay.
func TestApplyWritesFoldPrevAfterCreationInUncommittedPriorBlock(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0x01"))
	key := accounts.InternKey(common.HexToHash("0x02"))
	addrValue, keyValue := addr.Value(), key.Value()
	composite := slices.Concat(addrValue[:], keyValue[:])
	rules := &chain.Rules{ChainID: uint256.NewInt(1)}

	for _, tc := range []struct {
		name          string
		destructFirst bool
		inExecWindow  bool
	}{
		{name: "created, block in exec window", inExecWindow: true},
		{name: "created, block folded into the overlay"},
		{name: "destructed and re-created, block in exec window", destructFirst: true, inExecWindow: true},
		{name: "destructed and re-created, block folded into the overlay", destructFirst: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, tx, domains := NewTestRwTx(t)
			acc := accounts.NewAccount()
			if tc.destructFirst {
				acc.Nonce = 1
			}
			addrKey := addrValue[:]
			require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, addrKey, accounts.SerialiseV3(&acc), 1, nil))
			require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, composite, []byte{5}, 1, nil))

			reader := NewReaderV3(domains.AsGetter(tx))
			block1 := NewVersionMap(nil)
			var block1Folds []func() error
			runBlock1Tx := func(txIdx int, txNum uint64, body func(*IntraBlockState) error) {
				ibs := NewWithVersionMap(reader, block1)
				t.Cleanup(ibs.Close)
				ibs.SetTxContext(1, txIdx)
				require.NoError(t, body(ibs))
				writes := finalizedWrites(t, ibs, rules)
				block1.FlushVersionedWrites(writes, true)
				block1.SealUpTo(txIdx)
				block1Folds = append(block1Folds, func() error {
					return ApplyWrites(NewVersionMapWriteView(writes, block1, txIdx), domains, tx, 1, txNum, nil, rules, false)
				})
			}
			createdIdx := 0
			if tc.destructFirst {
				runBlock1Tx(0, 2, func(ibs *IntraBlockState) error {
					_, err := ibs.Selfdestruct(addr, false)
					return err
				})
				createdIdx = 1
			}
			runBlock1Tx(createdIdx, 3, func(ibs *IntraBlockState) error {
				if err := ibs.CreateAccount(addr, true); err != nil {
					return err
				}
				return ibs.SetNonce(addr, 1, tracing.NonceChangeNewContract)
			})
			foldBlock1 := func() {
				for _, fold := range block1Folds {
					require.NoError(t, fold())
				}
			}

			execWindow := NewPrevBlockList()
			if tc.inExecWindow {
				execWindow.PushHead(1, 3, block1)
			} else {
				foldBlock1()
			}
			block2Reader := NewPrevBlockReader(reader, execWindow)
			block2Reader.SetBlock(2)

			block2 := NewVersionMap(nil)
			ibs := NewWithVersionMap(block2Reader, block2)
			t.Cleanup(ibs.Close)
			ibs.SetTxContext(2, 0)
			require.NoError(t, ibs.SetState(addr, key, *uint256.NewInt(7)))
			writes := finalizedWrites(t, ibs, rules)

			if tc.inExecWindow {
				foldBlock1()
				execWindow.RemoveTail()
			}
			changes := &changeset.StateChangeSet{}
			domains.SetChangesetAccumulator(changes)
			require.NoError(t, ApplyWrites(NewVersionMapWriteView(writes, block2, 0), domains, tx, 2, 4, nil, rules, false))

			diff := changes.Diffs[kv.StorageDomain].GetDiffSet()
			require.Len(t, diff, 1)
			require.Empty(t, diff[0].Value, "block 1 cleared the slot, so its prev value is empty")

			require.NoError(t, domains.Flush(t.Context(), tx))
			var unwindDiffs [kv.DomainLen][]kv.DomainEntryDiff
			for idx, d := range changes.Diffs {
				unwindDiffs[idx] = d.GetDiffSet()
			}
			require.NoError(t, tx.Unwind(t.Context(), 4, &unwindDiffs))
			got, _, err := tx.GetLatest(kv.StorageDomain, composite, kv.GetLatestOptions{})
			require.NoError(t, err)
			require.Empty(t, got, "the unwind must restore the cleared slot, got %x", got)
		})
	}
}
