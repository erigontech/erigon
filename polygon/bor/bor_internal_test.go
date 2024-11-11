package bor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/polygon/bor/valset"
	"github.com/erigontech/erigon/polygon/heimdall"
)

func TestUseBridgeReader(t *testing.T) {
	// test for Go's interface nil-ness caveat - https://codefibershq.com/blog/golang-why-nil-is-not-always-nil
	var br *mockBridgeReader
	bor := New(params.AmoyChainConfig, nil, nil, nil, nil, nil, nil, br, nil)
	require.False(t, bor.useBridgeReader)
	br = &mockBridgeReader{}
	bor = New(params.AmoyChainConfig, nil, nil, nil, nil, nil, nil, br, nil)
	require.True(t, bor.useBridgeReader)
}

func TestUseSpanReader(t *testing.T) {
	// test for Go's interface nil-ness caveat - https://codefibershq.com/blog/golang-why-nil-is-not-always-nil
	var sr *mockSpanReader
	b := New(params.AmoyChainConfig, nil, nil, nil, nil, nil, nil, nil, sr)
	require.False(t, b.useSpanReader)
	sr = &mockSpanReader{}
	b = New(params.AmoyChainConfig, nil, nil, nil, nil, nil, nil, nil, sr)
	require.True(t, b.useSpanReader)
}

var _ bridgeReader = mockBridgeReader{}

type mockBridgeReader struct{}

func (m mockBridgeReader) Events(context.Context, uint64) ([]*types.Message, error) {
	panic("mock")
}

func (m mockBridgeReader) EventTxnLookup(context.Context, libcommon.Hash) (uint64, bool, error) {
	panic("mock")
}

var _ spanReader = mockSpanReader{}

type mockSpanReader struct{}

func (m mockSpanReader) Span(context.Context, uint64) (*heimdall.Span, bool, error) {
	panic("mock")
}

func (m mockSpanReader) Producers(context.Context, uint64) (*valset.ValidatorSet, error) {
	panic("mock")
}
