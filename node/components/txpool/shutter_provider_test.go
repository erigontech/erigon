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

package txpool

import (
	"context"
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
	"github.com/erigontech/erigon/txnprovider/shutter/shuttercfg"
	"github.com/stretchr/testify/require"
)

// stubTxnProvider satisfies txnprovider.TxnProvider for tests.
type stubTxnProvider struct{}

func (s *stubTxnProvider) ProvideTxns(_ context.Context, _ ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	return nil, nil
}

func TestShutterProviderDisabled(t *testing.T) {
	p := &ShutterProvider{}
	p.Configure(shuttercfg.Config{Enabled: false}, log.Root())

	base := &stubTxnProvider{}
	err := p.Initialize(context.Background(), ShutterDeps{
		BaseTxnProvider: base,
	})
	require.NoError(t, err)
	require.False(t, p.IsEnabled())
	require.Nil(t, p.Pool)
	require.Equal(t, base, p.TxnProvider) // pass-through

	eg := &noopErrGroup{}
	p.Start(context.Background(), eg)
	require.Equal(t, 0, eg.count) // no goroutines started
}

func TestShutterProviderNilBasePanics(t *testing.T) {
	p := &ShutterProvider{}
	p.Configure(shuttercfg.Config{Enabled: false}, log.Root())

	err := p.Initialize(context.Background(), ShutterDeps{
		BaseTxnProvider: nil,
	})
	require.Error(t, err)
}
