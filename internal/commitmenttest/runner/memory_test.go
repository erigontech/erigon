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

package runner

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/stretchr/testify/require"
)

func TestMemoryReadModes(t *testing.T) {
	for _, spec := range []ContextSpec{{}, {Borrowed: true}, {Owned: true}} {
		memory := NewMemory(spec)
		require.NoError(t, memory.PutBranch([]byte("a"), []byte{1, 2}, nil))
		require.NoError(t, memory.PutBranch([]byte("b"), []byte{3, 4}, nil))
		reader, _ := memory.Open(context.Background())
		other, _ := memory.Open(context.Background())
		a, _, err := reader.Branch([]byte("a"))
		require.NoError(t, err)
		_, _, err = other.Branch([]byte("b"))
		require.NoError(t, err)
		require.Equal(t, []byte{1, 2}, a)
		_, _, err = reader.Branch([]byte("b"))
		require.NoError(t, err)
		if spec.Borrowed {
			require.Equal(t, []byte{3, 4}, a)
		} else {
			require.Equal(t, []byte{1, 2}, a)
		}
		owned, ok := reader.(interface {
			BranchOwned([]byte) ([]byte, kv.Step, error)
		})
		require.Equal(t, spec.Owned, ok)
		if ok {
			data, _, err := owned.BranchOwned([]byte("a"))
			require.NoError(t, err)
			require.NoError(t, memory.PutBranch([]byte("a"), []byte{9}, []byte{1, 2}))
			require.Equal(t, []byte{1, 2}, data)
		}
		require.NoError(t, memory.PutBranch([]byte("tombstone"), nil, nil))
		data, _, err := reader.Branch([]byte("tombstone"))
		require.NoError(t, err)
		require.NotNil(t, data)
		require.Empty(t, data)
		data, _, err = reader.Branch([]byte("missing"))
		require.NoError(t, err)
		require.Nil(t, data)
	}
}

func TestMemoryPreviousValuesAndCounters(t *testing.T) {
	m := NewMemory(ContextSpec{CaptureDeltas: true, CheckPrevious: true, ForbidStateReads: true})
	key, value := []byte("key"), []byte{1}
	require.NoError(t, m.PutBranch(key, value, nil))
	key[0], value[0] = 'x', 2
	require.ErrorContains(t, m.PutBranch([]byte("key"), nil, []byte{2}), "previous value mismatch")
	require.NoError(t, m.PutBranch([]byte("key"), nil, []byte{1}))
	deltas := m.Deltas()
	require.Equal(t, []commitment.BranchDelta{{Key: []byte("key"), Data: []byte{1}}, {Key: []byte("key"), Prev: []byte{1}}}, deltas)
	deltas[0].Data[0] = 9
	require.Equal(t, byte(1), m.Deltas()[0].Data[0])
	_, err := m.Account([]byte("a"))
	require.ErrorContains(t, err, "unexpected state read")
	_, err = m.Storage([]byte("s"))
	require.ErrorContains(t, err, "unexpected state read")
	require.Equal(t, Counts{Writes: 2, AccountReads: 1, StorageReads: 1}, m.Counts())
}

func TestMemoryConcurrentReaders(t *testing.T) {
	m := NewMemory(ContextSpec{Borrowed: true})
	require.NoError(t, m.PutBranch([]byte("key"), []byte{1, 2}, nil))
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		reader, _ := m.Open(context.Background())
		wg.Go(func() {
			for range 100 {
				data, _, err := reader.Branch([]byte("key"))
				if err != nil {
					errs <- err
					return
				}
				if len(data) != 2 {
					errs <- errors.New("short branch read")
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, Counts{Writes: 1, BranchReads: 800, Readers: 8}, m.Counts())
}

func TestMemoryErrorsAndGate(t *testing.T) {
	injected := errors.New("injected")
	m := NewMemory(ContextSpec{ReadError: injected, PutError: injected})
	_, _, err := m.Branch(nil)
	require.ErrorIs(t, err, injected)
	require.ErrorIs(t, m.PutBranch(nil, nil, nil), injected)
	m = NewMemory(ContextSpec{BeforeRead: func(ctx context.Context, _ []byte) error { <-ctx.Done(); return ctx.Err() }})
	ctx, cancel := context.WithCancel(context.Background())
	reader, _ := m.Open(ctx)
	cancel()
	_, _, err = reader.Branch(nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestMemoryLogicalState(t *testing.T) {
	m := NewMemory(ContextSpec{})
	address := commitmenttest.Key(commitmenttest.KeySpec{Kind: "integer", Size: 20}, 1)
	slot := append(append([]byte(nil), address...), make([]byte, 32)...)
	value := commitmenttest.Account(commitmenttest.AccountSpec{Kind: "plain", Number: 3})
	m.Apply([]commitmenttest.Op{{Key: address, Account: &value}, {Key: slot, Storage: []byte{1}}})
	m.Apply([]commitmenttest.Op{{Key: address, Account: &commitmenttest.AccountValue{Fields: commitmenttest.NonceField, Nonce: 9}}})
	account, err := m.Account(address)
	require.NoError(t, err)
	require.Equal(t, uint64(9), account.Nonce)
	require.Equal(t, value.Balance, account.Balance)
	m.Apply([]commitmenttest.Op{{Key: address, Delete: true}})
	storage, err := m.Storage(slot)
	require.NoError(t, err)
	require.True(t, storage.Deleted())
}
