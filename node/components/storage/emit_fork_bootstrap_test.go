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

package storage

import (
	"github.com/holiman/uint256"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/components/storage/flow"
)

// captureForkBootstrap returns a subscriber that records every
// ForkBootstrapRequired the test publishes, plus a slice the test
// inspects after the call returns.
func captureForkBootstrap(t *testing.T, bus event.EventBus) *[]flow.ForkBootstrapRequired {
	t.Helper()
	var mu sync.Mutex
	captured := []flow.ForkBootstrapRequired{}
	require.NoError(t, bus.Subscribe(func(e flow.ForkBootstrapRequired) {
		mu.Lock()
		captured = append(captured, e)
		mu.Unlock()
	}))
	return &captured
}

func TestEmitForkBootstrap_NilProvider(t *testing.T) {
	var p *Provider
	require.NotPanics(t, p.EmitForkBootstrap)
}

func TestEmitForkBootstrap_NilEventBus(t *testing.T) {
	p := &Provider{logger: log.New()} // no eventBus
	require.NotPanics(t, p.EmitForkBootstrap)
}

func TestEmitForkBootstrap_NoOpForRootChain(t *testing.T) {
	bus := event.NewEventBus(nil)
	captured := captureForkBootstrap(t, bus)
	p := &Provider{
		eventBus: bus,
		logger:   log.New(),
		ChainConfig: &chain.Config{
			ChainName: "mainnet",
			ChainID:   uint256.NewInt(1),
			// Parent == "" → not a fork; EmitForkBootstrap must not publish.
		},
	}
	p.EmitForkBootstrap()
	bus.WaitAsync()
	require.Empty(t, *captured, "root chain must not publish ForkBootstrapRequired")
}

func TestEmitForkBootstrap_NoOpForNilChainConfig(t *testing.T) {
	bus := event.NewEventBus(nil)
	captured := captureForkBootstrap(t, bus)
	p := &Provider{eventBus: bus, logger: log.New(), ChainConfig: nil}
	p.EmitForkBootstrap()
	bus.WaitAsync()
	require.Empty(t, *captured)
}

func TestEmitForkBootstrap_PublishesForForkConfig(t *testing.T) {
	bus := event.NewEventBus(nil)
	captured := captureForkBootstrap(t, bus)

	hash := [20]byte{0xab, 0xcd, 0xef, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11}
	p := &Provider{
		eventBus: bus,
		logger:   log.New(),
		ChainConfig: &chain.Config{
			ChainName:          "mainnet-fork-20000000",
			ChainID:            uint256.NewInt(1),
			Parent:             "mainnet",
			CutBlock:           20_000_000,
			ParentManifestHash: hash,
		},
	}
	p.EmitForkBootstrap()
	bus.WaitAsync()

	require.Len(t, *captured, 1, "exactly one ForkBootstrapRequired emitted")
	got := (*captured)[0]
	require.Equal(t, "mainnet", got.Parent)
	require.Equal(t, uint64(20_000_000), got.CutBlock)
	require.Equal(t, hash, got.ParentManifestHash)
}

func TestEmitForkBootstrap_SkipsMalformedConfig(t *testing.T) {
	// Parent set but CutBlock=0 → BuildForkBootstrapPlan returns an
	// error; EmitForkBootstrap logs Warn and skips publication so
	// startup continues. ValidateForkDatadir in Initialize would have
	// surfaced the same upstream config error earlier; this is the
	// defensive second check.
	bus := event.NewEventBus(nil)
	captured := captureForkBootstrap(t, bus)
	p := &Provider{
		eventBus: bus,
		logger:   log.New(),
		ChainConfig: &chain.Config{
			ChainName: "fork",
			Parent:    "mainnet",
			// CutBlock missing — malformed
		},
	}
	p.EmitForkBootstrap()
	bus.WaitAsync()
	require.Empty(t, *captured, "malformed fork config must not publish")
}

func TestEmitForkBootstrap_PublishesWithZeroParentManifestHash(t *testing.T) {
	// A fork from a pre-Phase-1 root parent has no V2 manifest to pin
	// against. ParentManifestHash stays zero; the publication still
	// fires — the subscriber treats zero-hash as a "no parent fetch"
	// signal and skips the fetch (verified in manifest_exchange tests).
	bus := event.NewEventBus(nil)
	captured := captureForkBootstrap(t, bus)
	p := &Provider{
		eventBus: bus,
		logger:   log.New(),
		ChainConfig: &chain.Config{
			ChainName: "fork-of-old-chain",
			Parent:    "legacy-root",
			CutBlock:  100,
			// ParentManifestHash is zero
		},
	}
	p.EmitForkBootstrap()
	bus.WaitAsync()
	require.Len(t, *captured, 1)
	require.Equal(t, [20]byte{}, (*captured)[0].ParentManifestHash)
}
