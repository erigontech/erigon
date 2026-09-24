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

package transactions

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	tracersConfig "github.com/erigontech/erigon/execution/tracing/tracers/config"
	"github.com/erigontech/erigon/execution/tracing/tracers/logger"
	_ "github.com/erigontech/erigon/execution/tracing/tracers/native" // registers callTracer
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

func assembleWithLogConfig(t *testing.T, cfg *logger.LogConfig, tracerName *string) error {
	t.Helper()
	_, _, cancel, err := AssembleTracer(
		t.Context(),
		&tracersConfig.TraceConfig{LogConfig: cfg, Tracer: tracerName},
		common.Hash{}, nil, common.Hash{}, 0,
		jsonstream.New(io.Discard),
		time.Second,
	)
	cancel()
	return err
}

// execution-apis gives the opcode logger's limit a minimum of 0, and a negative
// one would suppress every step, so it must be refused rather than served as an
// empty trace.
func TestAssembleTracerRejectsNegativeLimit(t *testing.T) {
	err := assembleWithLogConfig(t, &logger.LogConfig{Limit: -1}, nil)

	var invalidParams *rpc.InvalidParamsError
	require.ErrorAs(t, err, &invalidParams)
}

func TestAssembleTracerAcceptsNonNegativeLimit(t *testing.T) {
	for _, limit := range []int{0, 1, 1000} {
		require.NoError(t, assembleWithLogConfig(t, &logger.LogConfig{Limit: limit}, nil))
	}
}

// The limit belongs to the opcode logger, and execution-apis says a named tracer
// ignores it, so it must not turn into an error there.
func TestAssembleTracerIgnoresLimitForNamedTracer(t *testing.T) {
	callTracer := "callTracer"
	require.NoError(t, assembleWithLogConfig(t, &logger.LogConfig{Limit: -1}, &callTracer))
}

// countingReader records how often the inner reader is consulted and hands back
// buffers it reuses, the way a real reader backed by a pooled buffer does.
type countingReader struct {
	accountReads, storageReads, codeReads, codeSizeReads int
	account                                              *accounts.Account
	codeBuf                                              []byte
}

func (c *countingReader) ReadAccountData(accounts.Address) (*accounts.Account, error) {
	c.accountReads++
	return c.account, nil
}

// The value follows the key, so a cache that loses the key replays the wrong slot.
func (c *countingReader) ReadAccountStorage(_ accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	c.storageReads++
	return *uint256.NewInt(7 + uint64(key.Value()[31])), true, nil
}

func (c *countingReader) ReadAccountCode(accounts.Address) ([]byte, error) {
	c.codeReads++
	c.codeBuf = append(c.codeBuf[:0], byte(c.codeReads), 0x60, 0x00)
	return c.codeBuf, nil
}

func (c *countingReader) ReadAccountCodeSize(accounts.Address) (int, error) {
	c.codeSizeReads++
	return 3, nil
}

func (c *countingReader) ReadAccountDataForDebug(accounts.Address) (*accounts.Account, error) {
	return c.account, nil
}
func (c *countingReader) ReadAccountIncarnation(accounts.Address) (uint64, error) { return 0, nil }
func (c *countingReader) SetTrace(bool, string)                                   {}
func (c *countingReader) Trace() bool                                             { return false }
func (c *countingReader) TracePrefix() string                                     { return "" }

func TestMemoReaderServesRepeatedReadsFromItsCache(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0x01"))
	key := accounts.InternKey(common.Hash{})
	other := accounts.InternKey(common.Hash{31: 1})
	inner := &countingReader{account: &accounts.Account{Nonce: 3}}
	m := newMemoReader(inner)

	first, err := m.ReadAccountData(addr)
	require.NoError(t, err)
	first.Nonce = 99

	second, err := m.ReadAccountData(addr)
	require.NoError(t, err)
	require.Equal(t, 1, inner.accountReads, "a repeated account read must not reach the inner reader")
	require.Equal(t, uint64(3), second.Nonce, "mutating a returned account must not change the cache")

	// Two slots of one account, so a cache keyed on the address alone would answer the second
	// with the first one's value.
	for range 2 {
		v, found, err := m.ReadAccountStorage(addr, key)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, *uint256.NewInt(7), v)

		w, found, err := m.ReadAccountStorage(addr, other)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, *uint256.NewInt(8), w)

		n, err := m.ReadAccountCodeSize(addr)
		require.NoError(t, err)
		require.Equal(t, 3, n)
	}
	require.Equal(t, 2, inner.storageReads, "a repeated storage read must not reach the inner reader")
	require.Equal(t, 1, inner.codeSizeReads, "a repeated code-size read must not reach the inner reader")
}

func TestMemoReaderKeepsCodeAfterTheInnerBufferIsReused(t *testing.T) {
	first := accounts.InternAddress(common.HexToAddress("0x01"))
	second := accounts.InternAddress(common.HexToAddress("0x02"))
	inner := &countingReader{account: &accounts.Account{}}
	m := newMemoReader(inner)

	want, err := m.ReadAccountCode(first)
	require.NoError(t, err)
	snapshot := bytes.Clone(want)

	_, err = m.ReadAccountCode(second)
	require.NoError(t, err)
	require.Equal(t, snapshot, want, "the inner reader reusing its buffer must not rewrite a slice already returned")

	got, err := m.ReadAccountCode(first)
	require.NoError(t, err)
	require.Equal(t, 2, inner.codeReads, "a repeated code read must not reach the inner reader")
	require.Equal(t, snapshot, got, "the inner reader reusing its buffer must not rewrite a cached entry")
}
