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

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
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

		{"HistoryReaderV3.ReadAccountStorage", 0, func() { _, _, _ = hr.ReadAccountStorage(addr, key) }},
		{"HistoryReaderV3.ReadAccountCode", 0, func() { _, _ = hr.ReadAccountCode(addr) }},
		{"HistoryReaderV3.ReadAccountCodeSize", 0, func() { _, _ = hr.ReadAccountCodeSize(addr) }},
		{"HistoryReaderV3.ReadAccountData", 1, func() { _, _ = hr.ReadAccountData(addr) }},                 // 1: returns *accounts.Account
		{"HistoryReaderV3.ReadAccountDataForDebug", 1, func() { _, _ = hr.ReadAccountDataForDebug(addr) }}, // 1: returns *accounts.Account
	} {
		t.Run(tc.name, func(t *testing.T) {
			allocs := testing.AllocsPerRun(100, tc.fn)
			require.Equal(t, tc.want, allocs, "%s: alloc count changed", tc.name)
		})
	}
}
