package jsonrpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/polygon/bor/valset"
)

func TestUseSpanProducersReader(t *testing.T) {
	// test for Go's interface nil-ness caveat - https://codefibershq.com/blog/golang-why-nil-is-not-always-nil
	var spr *mockSpanProducersReader
	api := NewBorAPI(nil, nil, spr)
	require.False(t, api.useSpanProducersReader)
	spr = &mockSpanProducersReader{}
	api = NewBorAPI(nil, nil, spr)
	require.True(t, api.useSpanProducersReader)
}

var _ spanProducersReader = mockSpanProducersReader{}

type mockSpanProducersReader struct{}

func (m mockSpanProducersReader) Producers(context.Context, uint64) (*valset.ValidatorSet, error) {
	panic("mock")
}
