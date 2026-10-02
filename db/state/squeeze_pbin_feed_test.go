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
	"context"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func pbinRebuildFeedStream(keys *etl.Collector, reader commitmentdb.StateReader, emitter *pbt.FeedOpEmitter, emit func(pbt.Op) error) error {
	var address []byte
	var previousKey []byte
	var emitted bool
	var accountExists bool
	emitAccount := func() error {
		account, err := commitmentdb.BinFeedAccountFromState(address, nil, true, false, reader)
		if err != nil {
			return err
		}
		accountExists = account.Exists
		emitted = true
		return emitter.EmitAccount(account, emit)
	}
	return keys.Load(nil, "", func(key, _ []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		if len(key) != length.Addr && len(key) != length.Addr+length.Hash {
			return fmt.Errorf("commitment rebuild: plain key has length %d", len(key))
		}
		if bytes.Equal(previousKey, key) {
			return nil
		}
		previousKey = bytes.Clone(key)
		keyAddress := key[:length.Addr]
		if !bytes.Equal(address, keyAddress) {
			address = bytes.Clone(keyAddress)
			emitted = false
		}
		if !emitted {
			if err := emitAccount(); err != nil {
				return err
			}
		}
		if len(key) == length.Addr+length.Hash && accountExists {
			slot, err := commitmentdb.BinFeedStorageSlotFromState(address, key[length.Addr:], reader)
			if err != nil {
				return err
			}
			if err := emitter.EmitStorageSlot(address, slot, emit); err != nil {
				return err
			}
		}
		return nil
	}, etl.TransformArgs{})
}

type pbinAbsentAccountReader struct {
	address []byte
	value   []byte
}

func (r *pbinAbsentAccountReader) WithHistory() bool { return false }

func (r *pbinAbsentAccountReader) CheckDataAvailable(kv.Domain, kv.Step) error { return nil }

func (r *pbinAbsentAccountReader) Read(domain kv.Domain, key []byte, _ uint64) ([]byte, kv.Step, error) {
	if domain == kv.AccountsDomain && bytes.Equal(key, r.address) {
		return nil, 0, nil
	}
	if domain == kv.StorageDomain {
		return bytes.Clone(r.value), 0, nil
	}
	return nil, 0, nil
}

func (r *pbinAbsentAccountReader) Clone(kv.TemporalTx) commitmentdb.StateReader { return r }

func (r *pbinAbsentAccountReader) CloneForWorker(context.Context, kv.TemporalTx) commitmentdb.StateReader {
	return r
}

func TestPBinRebuildSkipsStorageForAbsentAccount(t *testing.T) {
	address := bytes.Repeat([]byte{0x41}, length.Addr)
	slot := bytes.Repeat([]byte{0x17}, length.Hash)
	plainKeys := etl.NewCollector("pbin-rebuild-feed-test", t.TempDir(), etl.NewSortableBuffer(1024), log.Root())
	defer plainKeys.Close()
	require.NoError(t, plainKeys.Collect(address, nil))
	require.NoError(t, plainKeys.Collect(append(bytes.Clone(address), slot...), nil))
	require.NoError(t, plainKeys.Flush())

	reader := &pbinAbsentAccountReader{address: address, value: bytes.Repeat([]byte{0x22}, eip8297.ValueLength)}
	var ops []pbt.Op
	emitter := pbt.NewRebuildFeedOpEmitter()
	require.NoError(t, pbinRebuildFeedStream(plainKeys, reader, emitter, func(op pbt.Op) error {
		ops = append(ops, op)
		return nil
	}))

	storageKey := eip8297.TreeKeyStorage(address, slot)
	for _, op := range ops {
		require.False(t, bytes.Equal(op.Key, storageKey) && len(op.Drop) == 0)
	}
}

type pbinExistingCodelessReader struct {
	address []byte
	account []byte
}

func (r *pbinExistingCodelessReader) WithHistory() bool { return false }

func (r *pbinExistingCodelessReader) CheckDataAvailable(kv.Domain, kv.Step) error { return nil }

func (r *pbinExistingCodelessReader) Read(domain kv.Domain, key []byte, _ uint64) ([]byte, kv.Step, error) {
	if domain == kv.AccountsDomain && bytes.Equal(key, r.address) {
		return bytes.Clone(r.account), 0, nil
	}
	if domain == kv.CodeDomain && bytes.Equal(key, r.address) {
		return append(append([]byte(nil), eip8297.DelegationMarker[:]...), r.address...), 0, nil
	}
	return nil, 0, nil
}

func (r *pbinExistingCodelessReader) Clone(kv.TemporalTx) commitmentdb.StateReader { return r }

func (r *pbinExistingCodelessReader) CloneForWorker(context.Context, kv.TemporalTx) commitmentdb.StateReader {
	return r
}

func TestPBinRebuildFeedRewritesExistingCodelessCodeFields(t *testing.T) {
	address := bytes.Repeat([]byte{0x42}, length.Addr)
	account := accounts.Account{Nonce: 7, Balance: *uint256.NewInt(9), CodeHash: accounts.EmptyCodeHash}
	plainKeys := etl.NewCollector("pbin-rebuild-feed-codeless-test", t.TempDir(), etl.NewSortableBuffer(1024), log.Root())
	defer plainKeys.Close()
	require.NoError(t, plainKeys.Collect(address, nil))
	require.NoError(t, plainKeys.Flush())

	reader := &pbinExistingCodelessReader{address: address, account: accounts.SerialiseV3(&account)}
	emitter := pbt.NewRebuildFeedOpEmitter()
	var ops []pbt.Op
	require.NoError(t, pbinRebuildFeedStream(plainKeys, reader, emitter, func(op pbt.Op) error {
		ops = append(ops, op)
		return nil
	}))

	basic, err := eip8297.EncodeBasicData(account.Nonce, &account.Balance, 0)
	require.NoError(t, err)
	codeHash := eip8297.CodeHashValue(common.Hash{})
	keys := map[string]pbt.Op{}
	for _, op := range ops {
		keys[string(op.Key)] = op
	}
	require.Equal(t, basic, keys[string(eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey))].Value)
	require.Equal(t, codeHash, keys[string(eip8297.TreeKeyAccount(address, eip8297.CodeHashLeafKey))].Value)
	_, ok := keys[string(eip8297.TreeKeyAccount(address, eip8297.DelegationLeafKey))]
	require.True(t, ok)
}
