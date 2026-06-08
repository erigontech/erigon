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

package balcache_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/balcache"
)

// fakeRegenerator records every regeneration request + serves canned responses.
type fakeRegenerator struct {
	calls atomic.Int32
	// resultFor maps the (hash) → bytes the regenerator will return. Unset
	// hashes get the default canned bytes if set, else (nil, nil).
	resultFor    map[common.Hash][]byte
	defaultBytes []byte
	errAlways    error
}

func (f *fakeRegenerator) RegenerateBlockAccessList(_ context.Context, hash common.Hash, _ uint64) ([]byte, error) {
	f.calls.Add(1)
	if f.errAlways != nil {
		return nil, f.errAlways
	}
	if v, ok := f.resultFor[hash]; ok {
		return v, nil
	}
	if f.defaultBytes != nil {
		return f.defaultBytes, nil
	}
	return nil, nil
}

var errFakeRegen = errors.New("fake regen failure")

func hashFromByte(b byte) common.Hash {
	var h common.Hash
	for i := range h {
		h[i] = b
	}
	return h
}

func TestBlockAccessListBytes_CacheHitShortCircuits(t *testing.T) {
	t.Cleanup(balcache.ResetBALCacheForTest)
	balcache.ResetBALCacheForTest()

	hash := hashFromByte(0x01)
	data := []byte{0xc1, 0x00}
	balcache.CacheBlockAccessList(hash, data)

	// Install a regenerator that, if called, fails the test.
	balcache.SetBALRegenerator(&fakeRegenerator{errAlways: errFakeRegen})

	got, err := balcache.BlockAccessListBytes(context.Background(), hash, 7)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestBlockAccessListBytes_RegeneratorFallback(t *testing.T) {
	t.Cleanup(balcache.ResetBALCacheForTest)
	balcache.ResetBALCacheForTest()

	hash := hashFromByte(0x03)
	regenerated := []byte{0xc3, 0x42}
	regen := &fakeRegenerator{defaultBytes: regenerated}
	balcache.SetBALRegenerator(regen)

	got, err := balcache.BlockAccessListBytes(context.Background(), hash, 22)
	require.NoError(t, err)
	require.Equal(t, regenerated, got)
	require.Equal(t, int32(1), regen.calls.Load(), "regenerator should be called once on miss")

	// Repeated lookup hits the cache, regenerator NOT called again.
	got2, err := balcache.BlockAccessListBytes(context.Background(), hash, 22)
	require.NoError(t, err)
	require.Equal(t, regenerated, got2)
	require.Equal(t, int32(1), regen.calls.Load(), "cached regenerated BAL must short-circuit subsequent lookups")
}

func TestBlockAccessListBytes_NoRegeneratorOnMiss(t *testing.T) {
	t.Cleanup(balcache.ResetBALCacheForTest)
	balcache.ResetBALCacheForTest()

	hash := hashFromByte(0x04)
	balcache.SetBALRegenerator(nil)
	got, err := balcache.BlockAccessListBytes(context.Background(), hash, 33)
	require.NoError(t, err)
	require.Nil(t, got, "no cache, no regenerator → nil bytes (peer sees 'not available')")
}

func TestBlockAccessListBytes_RegeneratorReturnsNil(t *testing.T) {
	t.Cleanup(balcache.ResetBALCacheForTest)
	balcache.ResetBALCacheForTest()

	hash := hashFromByte(0x05)
	regen := &fakeRegenerator{} // defaultBytes nil
	balcache.SetBALRegenerator(regen)

	got, err := balcache.BlockAccessListBytes(context.Background(), hash, 44)
	require.NoError(t, err)
	require.Nil(t, got)
	require.Equal(t, int32(1), regen.calls.Load())

	// A nil-from-regenerator must NOT be cached (so a later install of a
	// real regenerator can succeed).
	_, ok := balcache.CachedBlockAccessList(hash)
	require.False(t, ok, "nil regeneration result must not be cached")
}

func TestBlockAccessListBytes_RegeneratorError(t *testing.T) {
	t.Cleanup(balcache.ResetBALCacheForTest)
	balcache.ResetBALCacheForTest()

	hash := hashFromByte(0x06)
	regen := &fakeRegenerator{errAlways: errFakeRegen}
	balcache.SetBALRegenerator(regen)

	_, err := balcache.BlockAccessListBytes(context.Background(), hash, 55)
	require.ErrorIs(t, err, errFakeRegen)
	_, ok := balcache.CachedBlockAccessList(hash)
	require.False(t, ok, "regenerator error must not pollute the cache")
}

func TestCacheBlockAccessList_EmptyIsNoOp(t *testing.T) {
	t.Cleanup(balcache.ResetBALCacheForTest)
	balcache.ResetBALCacheForTest()
	hash := hashFromByte(0x07)
	balcache.CacheBlockAccessList(hash, nil)
	balcache.CacheBlockAccessList(hash, []byte{})
	_, ok := balcache.CachedBlockAccessList(hash)
	require.False(t, ok, "empty data must not be cached (would conflate with 'not available')")
}

func TestCacheBlockAccessList_CopiesBytes(t *testing.T) {
	t.Cleanup(balcache.ResetBALCacheForTest)
	balcache.ResetBALCacheForTest()
	hash := hashFromByte(0x08)
	src := []byte{0xde, 0xad, 0xbe, 0xef}
	balcache.CacheBlockAccessList(hash, src)
	src[0] = 0xff // mutate caller's slice — cache must hold its own copy

	got, ok := balcache.CachedBlockAccessList(hash)
	require.True(t, ok)
	require.Equal(t, byte(0xde), got[0], "cache must defensively copy the input bytes")
}

func TestSetBALRegenerator_ReplaceAndClear(t *testing.T) {
	t.Cleanup(balcache.ResetBALCacheForTest)
	balcache.ResetBALCacheForTest()

	hash := hashFromByte(0x09)
	r1 := &fakeRegenerator{defaultBytes: []byte{0xa1}}
	r2 := &fakeRegenerator{defaultBytes: []byte{0xa2}}
	balcache.SetBALRegenerator(r1)
	balcache.SetBALRegenerator(r2) // replace

	got, err := balcache.BlockAccessListBytes(context.Background(), hash, 99)
	require.NoError(t, err)
	require.Equal(t, []byte{0xa2}, got)
	require.Zero(t, r1.calls.Load(), "old regenerator must not be called after replacement")
	require.Equal(t, int32(1), r2.calls.Load())

	balcache.ResetBALCacheForTest() // clear cache so the next lookup hits the regenerator again
	balcache.SetBALRegenerator(nil) // explicit clear
	got, err = balcache.BlockAccessListBytes(context.Background(), hash, 99)
	require.NoError(t, err)
	require.Nil(t, got, "cleared regenerator → miss returns nil")
}
