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

package execctx_test

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
)

var updateFlushGolden = flag.Bool("update", false, "rewrite flush golden files")

const flushGoldenDir = "testdata/flush_golden"

type flushGoldenRow struct {
	table string
	key   []byte
	value []byte
}

func TestFlushGolden(t *testing.T) {
	for _, name := range []string{"forward", "discard", "unwind", "merge", "reflush"} {
		t.Run(name, func(t *testing.T) {
			rows := buildFlushGoldenBatch(t, name)

			got := formatFlushGolden(rows)
			goldenPath := filepath.Join(flushGoldenDir, name+".txt")
			if *updateFlushGolden {
				require.NoError(t, os.MkdirAll(flushGoldenDir, 0o755))
				require.NoError(t, os.WriteFile(goldenPath, []byte(got), 0o644))
				return
			}

			wantBytes, err := os.ReadFile(goldenPath)
			require.NoError(t, err)
			want := string(wantBytes)
			if line, wantLine, gotLine, ok := firstFlushGoldenDifference(want, got); ok {
				t.Fatalf("golden mismatch at line %d: want %q, got %q", line, wantLine, gotLine)
			}
		})
	}
}

func buildFlushGoldenBatch(t *testing.T, name string) []flushGoldenRow {
	t.Helper()
	db := newTestDb(t, flushGoldenStepSize(name))
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	t.Cleanup(func() { domains.Close() })

	switch name {
	case "forward":
		buildForward(t, domains, tx, "forward")
	case "discard":
		domains.DiscardWrites(kv.AccountsDomain)
		domains.DiscardWrites(kv.StorageDomain)
		domains.DiscardWrites(kv.CodeDomain)
		buildForward(t, domains, tx, "discard")
	case "unwind":
		buildUnwind(t, domains, tx)
	case "merge":
		buildMerge(t, domains, tx)
	case "reflush":
		buildReflush(t, domains, tx)
	default:
		t.Fatalf("unknown golden case %q", name)
	}

	require.NoError(t, domains.Flush(t.Context(), tx))
	rows := dumpFlushGoldenRows(t, tx)
	if name == "unwind" {
		assertUnwindRows(t, rows)
	}
	return rows
}

func flushGoldenStepSize(name string) uint64 {
	if name == "unwind" {
		return 1
	}
	return 4
}

func buildForward(t *testing.T, domains *execctx.SharedDomains, tx kv.TemporalRwTx, label string) {
	t.Helper()
	accountA := flushGoldenAddress(0x11)
	accountB := flushGoldenAddress(0x22)
	accountC := flushGoldenAddress(0x33)
	accountD := flushGoldenAddress(0x44)
	accountE := flushGoldenAddress(0x55)
	accountF := flushGoldenAddress(0x66)
	storageB := flushGoldenStorageKey(accountB, 0x01)
	storageF := flushGoldenStorageKey(accountF, 0x02)
	commitmentKey := []byte{0xa0, 0x01}

	executeGoldenBlock(t, domains, tx, label, 1, func() {
		flushGoldenPut(t, domains, tx, kv.AccountsDomain, accountA, []byte{0xa1, 0x01}, 1)
		flushGoldenPut(t, domains, tx, kv.AccountsDomain, accountB, []byte{0xb1, 0x01}, 1)
		flushGoldenPut(t, domains, tx, kv.AccountsDomain, accountD, []byte{0xd1, 0x01}, 1)
		flushGoldenPut(t, domains, tx, kv.AccountsDomain, accountF, []byte{0xf1, 0x01}, 1)
		flushGoldenPut(t, domains, tx, kv.StorageDomain, storageB, []byte{0xb2, 0x01}, 1)
		flushGoldenPut(t, domains, tx, kv.StorageDomain, storageF, []byte{0xf2, 0x01}, 1)
		flushGoldenPut(t, domains, tx, kv.CodeDomain, accountB, []byte{0xc2, 0x01}, 1)
		flushGoldenPut(t, domains, tx, kv.CodeDomain, accountF, []byte{0xc6, 0x01}, 1)
		flushGoldenPut(t, domains, tx, kv.CommitmentDomain, commitmentKey, []byte{0xcc, 0x01}, 1)
	})
	executeGoldenBlock(t, domains, tx, label, 2, func() {
		flushGoldenPut(t, domains, tx, kv.AccountsDomain, accountA, []byte{0xa1, 0x02}, 2)
		flushGoldenPut(t, domains, tx, kv.AccountsDomain, accountC, []byte{0xc1, 0x02}, 2)
		flushGoldenPut(t, domains, tx, kv.AccountsDomain, accountC, []byte{0xc1, 0x03}, 2)
		require.NoError(t, rawtemporaldb.AppendReceiptMetadata(domains.AsPutDel(tx), 2, 20, 200, 2))
		require.NoError(t, rawtemporaldb.AppendReceiptMetadata(domains.AsPutDel(tx), 3, 21, 201, 2))
	})
	executeGoldenBlock(t, domains, tx, label, 3, func() {
		flushGoldenDel(t, domains, tx, kv.AccountsDomain, accountD, 3)
		flushGoldenPut(t, domains, tx, kv.AccountsDomain, accountD, []byte{0xd1, 0x03}, 3)
		flushGoldenDel(t, domains, tx, kv.AccountsDomain, accountE, 3)
	})
	executeGoldenBlock(t, domains, tx, label, 4, func() {
		flushGoldenPut(t, domains, tx, kv.AccountsDomain, accountF, []byte{0xf1, 0x04}, 4)
		flushGoldenPut(t, domains, tx, kv.StorageDomain, storageF, []byte{0xf2, 0x04}, 4)
		flushGoldenPut(t, domains, tx, kv.CommitmentDomain, commitmentKey, []byte{0xcc, 0x04}, 4)
	})
	executeGoldenBlock(t, domains, tx, label, 5, func() {
		flushGoldenPut(t, domains, tx, kv.AccountsDomain, accountA, []byte{0xa1, 0x05}, 5)
		flushGoldenPut(t, domains, tx, kv.CodeDomain, accountF, []byte{0xc6, 0x05}, 5)
	})
	executeGoldenBlock(t, domains, tx, label, 6, func() {
		flushGoldenDel(t, domains, tx, kv.AccountsDomain, accountA, 6)
		flushGoldenPut(t, domains, tx, kv.CommitmentDomain, commitmentKey, []byte{0xcc, 0x06}, 6)
	})
	executeGoldenBlock(t, domains, tx, label, 7, func() {
		flushGoldenPut(t, domains, tx, kv.StorageDomain, storageF, []byte{0xf2, 0x07}, 7)
	})
	executeGoldenBlock(t, domains, tx, label, 8, func() {
		flushGoldenPut(t, domains, tx, kv.AccountsDomain, accountF, []byte{0xf1, 0x08}, 8)
		flushGoldenPut(t, domains, tx, kv.CodeDomain, accountF, []byte{0xc6, 0x08}, 8)
	})
	executeGoldenBlock(t, domains, tx, label, 9, func() {
		flushGoldenPut(t, domains, tx, kv.CommitmentDomain, commitmentKey, []byte{0xcc, 0x09}, 9)
	})
	executeGoldenBlock(t, domains, tx, label, 10, func() {
		flushGoldenDel(t, domains, tx, kv.AccountsDomain, accountB, 10)
	})
	executeGoldenBlock(t, domains, tx, label, 11, func() {
		flushGoldenPut(t, domains, tx, kv.AccountsDomain, accountF, []byte{0xf1, 0x0b}, 11)
	})
	executeGoldenBlock(t, domains, tx, label, 12, func() {
		flushGoldenPut(t, domains, tx, kv.CommitmentDomain, commitmentKey, []byte{0xcc, 0x0c}, 12)
	})
	multiAccount := flushGoldenAddress(0x77)
	multiStorage := flushGoldenStorageKey(multiAccount, 0x03)
	executeGoldenTxns(t, domains, tx, label, 13, []uint64{13, 14, 15}, func(txNum uint64) {
		switch txNum {
		case 13, 14:
			flushGoldenPut(t, domains, tx, kv.AccountsDomain, multiAccount, []byte{0x77, byte(txNum)}, txNum)
			flushGoldenPut(t, domains, tx, kv.StorageDomain, multiStorage, []byte{0x87, byte(txNum)}, txNum)
		case 15:
			flushGoldenPut(t, domains, tx, kv.CommitmentDomain, []byte{0xa0, 0x0d}, []byte{0xcd, 0x0f}, txNum)
		}
	})
	crossAccount := flushGoldenAddress(0x78)
	crossStorage := flushGoldenStorageKey(crossAccount, 0x04)
	executeGoldenTxns(t, domains, tx, label, 16, []uint64{15, 16}, func(txNum uint64) {
		flushGoldenPut(t, domains, tx, kv.AccountsDomain, crossAccount, []byte{0x78, byte(txNum)}, txNum)
		flushGoldenPut(t, domains, tx, kv.StorageDomain, crossStorage, []byte{0x88, byte(txNum)}, txNum)
	})
}

func buildUnwind(t *testing.T, domains *execctx.SharedDomains, tx kv.TemporalRwTx) {
	t.Helper()
	baseAccount := flushGoldenAddress(0x71)
	oldAccount5 := flushGoldenAddress(0x72)
	oldAccount6 := flushGoldenAddress(0x73)
	newAccount5 := flushGoldenAddress(0x74)
	newAccount6 := flushGoldenAddress(0x75)
	baseStorage := flushGoldenStorageKey(baseAccount, 0x11)
	oldStorage5 := flushGoldenStorageKey(oldAccount5, 0x12)
	oldStorage6 := flushGoldenStorageKey(oldAccount6, 0x13)
	newStorage6 := flushGoldenStorageKey(newAccount6, 0x13)
	codeKey := flushGoldenAddress(0x76)
	commitmentKey := []byte{0xb0, 0x01}
	perBlock := make(map[uint64]*changeset.StateChangeSet)

	perBlock[1] = executeGoldenBlock(t, domains, tx, "unwind", 1, func() {
		flushGoldenPut(t, domains, tx, kv.AccountsDomain, baseAccount, []byte{0x71, 0x01}, 1)
		flushGoldenPut(t, domains, tx, kv.StorageDomain, baseStorage, []byte{0x81, 0x01}, 1)
		flushGoldenPut(t, domains, tx, kv.CodeDomain, codeKey, []byte{0x91, 0x01}, 1)
		flushGoldenPut(t, domains, tx, kv.CommitmentDomain, commitmentKey, []byte{0xa1, 0x01}, 1)
	})
	perBlock[2] = executeGoldenBlock(t, domains, tx, "unwind", 2, func() {
		flushGoldenPut(t, domains, tx, kv.AccountsDomain, baseAccount, []byte{0x71, 0x02}, 2)
	})
	perBlock[3] = executeGoldenBlock(t, domains, tx, "unwind", 3, func() {
		flushGoldenPut(t, domains, tx, kv.StorageDomain, baseStorage, []byte{0x81, 0x03}, 3)
	})
	perBlock[4] = executeGoldenBlock(t, domains, tx, "unwind", 4, func() {
		flushGoldenPut(t, domains, tx, kv.CommitmentDomain, commitmentKey, []byte{0xa1, 0x04}, 4)
	})
	perBlock[5] = executeGoldenBlock(t, domains, tx, "unwind", 5, func() {
		flushGoldenPut(t, domains, tx, kv.AccountsDomain, oldAccount5, []byte{0x72, 0x05}, 5)
		flushGoldenPut(t, domains, tx, kv.StorageDomain, oldStorage5, []byte{0x82, 0x05}, 5)
	})
	perBlock[6] = executeGoldenBlock(t, domains, tx, "unwind", 6, func() {
		flushGoldenPut(t, domains, tx, kv.AccountsDomain, oldAccount6, []byte{0x73, 0x06}, 6)
		flushGoldenPut(t, domains, tx, kv.StorageDomain, oldStorage6, []byte{0x83, 0x06}, 6)
		flushGoldenPut(t, domains, tx, kv.CodeDomain, oldAccount6, []byte{0x93, 0x06}, 6)
	})

	var unwindDiffs [kv.DomainLen][]kv.DomainEntryDiff
	for block := uint64(6); block >= 5; block-- {
		for domain, diff := range perBlock[block].Diffs {
			entries := diff.GetDiffSet()
			if unwindDiffs[domain] == nil {
				unwindDiffs[domain] = entries
				continue
			}
			unwindDiffs[domain] = changeset.MergeDiffSets(unwindDiffs[domain], entries)
		}
	}
	domains.Unwind(5, &unwindDiffs)
	domains.SetTxNum(5)

	executeGoldenBlock(t, domains, tx, "unwind-reorg", 5, func() {
		flushGoldenPut(t, domains, tx, kv.AccountsDomain, newAccount5, []byte{0x74, 0x15}, 5)
		flushGoldenPut(t, domains, tx, kv.StorageDomain, newAccount5, []byte{0x84, 0x15}, 5)
	})
	executeGoldenBlock(t, domains, tx, "unwind-reorg", 6, func() {
		flushGoldenPut(t, domains, tx, kv.AccountsDomain, newAccount6, []byte{0x75, 0x16}, 6)
		flushGoldenPut(t, domains, tx, kv.StorageDomain, newStorage6, []byte{0x85, 0x16}, 6)
	})
}

func buildReflush(t *testing.T, domains *execctx.SharedDomains, tx kv.TemporalRwTx) {
	t.Helper()
	account := flushGoldenAddress(0x91)
	storage := flushGoldenStorageKey(account, 0x05)

	domains.SetTxNum(1)
	flushGoldenPut(t, domains, tx, kv.AccountsDomain, account, []byte{0x91, 0x01}, 1)
	flushGoldenPut(t, domains, tx, kv.StorageDomain, storage, []byte{0xa1, 0x01}, 1)
	require.NoError(t, domains.Flush(t.Context(), tx))

	changes := &changeset.StateChangeSet{}
	domains.SetChangesetAccumulator(changes)
	domains.SetTxNum(5)
	flushGoldenPut(t, domains, tx, kv.AccountsDomain, account, []byte{0x91, 0x05}, 5)
	flushGoldenPut(t, domains, tx, kv.StorageDomain, storage, []byte{0xa1, 0x05}, 5)
	domains.SavePastChangesetAccumulator(flushGoldenHash("reflush", 5), 5, changes)
	domains.SetChangesetAccumulator(nil)
	require.NoError(t, domains.Flush(t.Context(), tx))

	var unwindDiffs [kv.DomainLen][]kv.DomainEntryDiff
	for domain, diff := range changes.Diffs {
		unwindDiffs[domain] = diff.GetDiffSet()
	}
	domains.Unwind(4, &unwindDiffs)
	domains.SetTxNum(4)
	flushGoldenPut(t, domains, tx, kv.AccountsDomain, account, []byte{0x91, 0x04}, 4)
	flushGoldenPut(t, domains, tx, kv.StorageDomain, storage, []byte{0xa1, 0x04}, 4)
	require.NoError(t, domains.Flush(t.Context(), tx))
}

func buildMerge(t *testing.T, receiver *execctx.SharedDomains, tx kv.TemporalRwTx) {
	t.Helper()
	key := flushGoldenAddress(0x81)
	receiverOnly := flushGoldenAddress(0x82)
	forkOnly := flushGoldenAddress(0x83)
	receiverCommitment := []byte{0xc0, 0x01}

	executeGoldenBlock(t, receiver, tx, "merge-receiver", 1, func() {
		flushGoldenPut(t, receiver, tx, kv.AccountsDomain, key, []byte{0x81, 0x01}, 1)
		flushGoldenPut(t, receiver, tx, kv.AccountsDomain, receiverOnly, []byte{0x82, 0x01}, 1)
		flushGoldenPut(t, receiver, tx, kv.CommitmentDomain, receiverCommitment, []byte{0xc1, 0x01}, 1)
	})
	executeGoldenBlock(t, receiver, tx, "merge-receiver", 2, func() {
		flushGoldenPut(t, receiver, tx, kv.AccountsDomain, key, []byte{0x81, 0x02}, 2)
	})
	executeGoldenBlock(t, receiver, tx, "merge-receiver", 3, func() {
		flushGoldenPut(t, receiver, tx, kv.CommitmentDomain, receiverCommitment, []byte{0xc1, 0x03}, 3)
	})

	fork, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	fork.SetParent(receiver)

	executeGoldenBlock(t, fork, tx, "merge-fork", 4, func() {
		parentValue, _, getErr := fork.GetLatest(kv.AccountsDomain, tx, key)
		require.NoError(t, getErr)
		require.Equal(t, []byte{0x81, 0x02}, parentValue)
		flushGoldenPut(t, fork, tx, kv.AccountsDomain, key, []byte{0x81, 0x04}, 4)
		flushGoldenPut(t, fork, tx, kv.AccountsDomain, forkOnly, []byte{0x83, 0x04}, 4)
	})
	executeGoldenBlock(t, fork, tx, "merge-fork", 5, func() {
		flushGoldenPut(t, fork, tx, kv.CommitmentDomain, receiverCommitment, []byte{0xc1, 0x05}, 5)
	})

	receiver.SetChangesetAccumulator(nil)
	fork.SetChangesetAccumulator(nil)
	require.NoError(t, receiver.Merge(t.Context(), 3, fork, 5))
	value, ok, err := receiver.GetAsOf(kv.AccountsDomain, key, 3)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte{0x81, 0x02}, value)
	t.Logf("merged GetAsOf at ts=3 for shared key: value=%x ok=%t", value, ok)
	fork.Close()
}

func executeGoldenBlock(t *testing.T, domains *execctx.SharedDomains, tx kv.TemporalRwTx, label string, block uint64, write func()) *changeset.StateChangeSet {
	t.Helper()
	changes := &changeset.StateChangeSet{}
	domains.SetChangesetAccumulator(changes)
	domains.SetTxNum(block)
	write()
	domains.SavePastChangesetAccumulator(flushGoldenHash(label, block), block, changes)
	domains.SetChangesetAccumulator(nil)
	return changes
}

func executeGoldenTxns(t *testing.T, domains *execctx.SharedDomains, tx kv.TemporalRwTx, label string, block uint64, txNums []uint64, write func(uint64)) *changeset.StateChangeSet {
	t.Helper()
	changes := &changeset.StateChangeSet{}
	domains.SetChangesetAccumulator(changes)
	for _, txNum := range txNums {
		domains.SetTxNum(txNum)
		write(txNum)
	}
	domains.SavePastChangesetAccumulator(flushGoldenHash(label, block), block, changes)
	domains.SetChangesetAccumulator(nil)
	return changes
}

func flushGoldenPut(t *testing.T, domains *execctx.SharedDomains, tx kv.TemporalRwTx, domain kv.Domain, key, value []byte, txNum uint64) {
	t.Helper()
	prev, _, err := domains.GetLatest(domain, tx, key)
	require.NoError(t, err)
	require.NoError(t, domains.DomainPut(domain, tx, key, value, txNum, prev))
}

func flushGoldenDel(t *testing.T, domains *execctx.SharedDomains, tx kv.TemporalRwTx, domain kv.Domain, key []byte, txNum uint64) {
	t.Helper()
	prev, _, err := domains.GetLatest(domain, tx, key)
	require.NoError(t, err)
	require.NoError(t, domains.DomainDel(domain, tx, key, txNum, prev))
}

func flushGoldenHash(label string, block uint64) common.Hash {
	var hash common.Hash
	copy(hash[:], label)
	binary.BigEndian.PutUint64(hash[24:], block)
	return hash
}

func flushGoldenAddress(id byte) []byte {
	key := make([]byte, 20)
	key[0] = id
	key[19] = id ^ 0xff
	return key
}

func flushGoldenStorageKey(address []byte, slot byte) []byte {
	key := make([]byte, 52)
	copy(key, address)
	key[20] = slot
	key[51] = slot ^ 0xff
	return key
}

func dumpFlushGoldenRows(t *testing.T, tx kv.TemporalRwTx) []flushGoldenRow {
	t.Helper()
	var rows []flushGoldenRow
	for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain, kv.CommitmentDomain, kv.ReceiptDomain} {
		for _, table := range statecfg.Schema.GetDomainCfg(domain).Tables() {
			cursor, err := tx.Cursor(table)
			require.NoError(t, err)
			t.Cleanup(cursor.Close)
			for key, value, err := cursor.First(); key != nil; key, value, err = cursor.Next() {
				require.NoError(t, err)
				rows = append(rows, flushGoldenRow{table: table, key: bytes.Clone(key), value: bytes.Clone(value)})
			}
		}
	}
	cursor, err := tx.Cursor(kv.Sequence)
	require.NoError(t, err)
	t.Cleanup(cursor.Close)
	key, value, err := cursor.First()
	for ; key != nil; key, value, err = cursor.Next() {
		require.NoError(t, err)
		rows = append(rows, flushGoldenRow{table: kv.Sequence, key: bytes.Clone(key), value: bytes.Clone(value)})
	}
	cursor, err = tx.Cursor(kv.ChangeSets3)
	require.NoError(t, err)
	t.Cleanup(cursor.Close)
	for key, value, err := cursor.First(); key != nil; key, value, err = cursor.Next() {
		require.NoError(t, err)
		rows = append(rows, flushGoldenRow{table: kv.ChangeSets3, key: bytes.Clone(key), value: bytes.Clone(value)})
	}
	return rows
}

func formatFlushGolden(rows []flushGoldenRow) string {
	var out strings.Builder
	for _, row := range rows {
		fmt.Fprintf(&out, "%s\t%x\t%x\n", row.table, row.key, row.value)
	}
	return out.String()
}

func firstFlushGoldenDifference(want, got string) (int, string, string, bool) {
	wantLines := strings.Split(want, "\n")
	gotLines := strings.Split(got, "\n")
	for i := 0; i < max(len(wantLines), len(gotLines)); i++ {
		var wantLine, gotLine string
		if i < len(wantLines) {
			wantLine = wantLines[i]
		}
		if i < len(gotLines) {
			gotLine = gotLines[i]
		}
		if wantLine != gotLine {
			return i + 1, wantLine, gotLine, true
		}
	}
	return 0, "", "", false
}

func assertUnwindRows(t *testing.T, rows []flushGoldenRow) {
	t.Helper()
	oldAccount5 := flushGoldenAddress(0x72)
	oldAccount6 := flushGoldenAddress(0x73)
	oldStorage5 := flushGoldenStorageKey(oldAccount5, 0x12)
	oldStorage6 := flushGoldenStorageKey(oldAccount6, 0x13)
	counts := map[string]int{
		kv.TblAccountHistoryVals: 0,
		kv.TblAccountHistoryKeys: 0,
		kv.TblStorageHistoryVals: 0,
		kv.TblStorageHistoryKeys: 0,
		kv.TblCodeHistoryVals:    0,
		kv.TblCodeHistoryKeys:    0,
	}
	for _, row := range rows {
		switch row.table {
		case kv.TblAccountVals:
			if (bytes.Equal(row.key, oldAccount5) || bytes.Equal(row.key, oldAccount6)) && len(row.value) >= 8 {
				step := ^binary.BigEndian.Uint64(row.value[:8])
				if step == 5 || step == 6 {
					counts[row.table]++
				}
			}
		case kv.TblStorageVals:
			if (bytes.Equal(row.key, oldStorage5) || bytes.Equal(row.key, oldStorage6)) && len(row.value) >= 8 {
				step := ^binary.BigEndian.Uint64(row.value[:8])
				if step == 5 || step == 6 {
					counts[row.table]++
				}
			}
		case kv.TblCodeVals:
			if len(row.key) >= 8 && bytes.Equal(row.key[:len(row.key)-8], oldAccount6) {
				step := ^binary.BigEndian.Uint64(row.key[len(row.key)-8:])
				if step == 5 || step == 6 {
					counts[row.table]++
				}
			}
		case kv.TblAccountHistoryVals, kv.TblStorageHistoryVals:
			if len(row.value) >= 8 && (bytes.Equal(row.key, oldAccount5) || bytes.Equal(row.key, oldAccount6) || bytes.Equal(row.key, oldStorage5) || bytes.Equal(row.key, oldStorage6)) {
				txNum := binary.BigEndian.Uint64(row.value[:8])
				if txNum == 5 || txNum == 6 {
					counts[row.table]++
				}
			}
		case kv.TblCodeHistoryVals:
			if len(row.key) >= 8 && bytes.Equal(row.key[:len(row.key)-8], oldAccount6) {
				txNum := binary.BigEndian.Uint64(row.key[len(row.key)-8:])
				if txNum == 5 || txNum == 6 {
					counts[row.table]++
				}
			}
		case kv.TblAccountHistoryKeys, kv.TblStorageHistoryKeys, kv.TblCodeHistoryKeys:
			if len(row.key) >= 8 {
				txNum := binary.BigEndian.Uint64(row.key[:8])
				if txNum == 5 || txNum == 6 {
					if bytes.Equal(row.value, oldAccount5) || bytes.Equal(row.value, oldAccount6) || bytes.Equal(row.value, oldStorage5) || bytes.Equal(row.value, oldStorage6) {
						counts[row.table]++
					}
				}
			}
		}
	}
	require.Equal(t, map[string]int{
		kv.TblAccountVals:        3,
		kv.TblAccountHistoryVals: 0,
		kv.TblAccountHistoryKeys: 0,
		kv.TblStorageVals:        3,
		kv.TblStorageHistoryVals: 0,
		kv.TblStorageHistoryKeys: 0,
		kv.TblCodeVals:           2,
		kv.TblCodeHistoryVals:    0,
		kv.TblCodeHistoryKeys:    0,
	}, counts)
	t.Logf("unwind rows for old-only keys at txNums 5..6: %v", counts)
}
