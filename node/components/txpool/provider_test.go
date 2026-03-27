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
	"math/big"
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
	"github.com/stretchr/testify/require"
)

func TestProviderDisabled(t *testing.T) {
	cfg := txpoolcfg.DefaultConfig
	cfg.Disable = true

	p := &Provider{}
	p.Configure(cfg, nil, log.Root())

	err := p.Initialize(context.Background(), Deps{})
	require.NoError(t, err)
	require.False(t, p.IsEnabled())
	require.Nil(t, p.Pool)
	require.NotNil(t, p.GrpcServer) // GrpcDisabled, not nil

	// Start on a disabled provider is a no-op
	eg := &noopErrGroup{}
	p.Start(context.Background(), eg)
	require.Equal(t, 0, eg.count)
}

func TestNonBorNoPriceLimitChange(t *testing.T) {
	cfg := txpoolcfg.DefaultConfig
	original := cfg.MinFeeCap

	// Non-Bor chain: MinFeeCap should not change
	chainConfig := &chain.Config{ChainID: big.NewInt(1)}

	p := &Provider{}
	p.Configure(cfg, chainConfig, log.Root())

	require.Equal(t, original, p.cfg.MinFeeCap)
}

type noopErrGroup struct{ count int }

func (g *noopErrGroup) Go(f func() error) { g.count++ }
