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

// Package sentry provides the Sentry component — extracted from backend.go
// as part of the Erigon componentization effort.
//
// The Sentry component owns the node's P2P / DevP2P stack: sentry servers,
// the multi-sentry client, the status-data provider, and the execution-P2P
// layer (message listener, peer tracker, publisher). It supports two modes:
//   - Local: in-process sentry servers per protocol version
//   - Remote: gRPC connection(s) to external sentry processes via --sentry.api.addr
//
// Consumers access its public fields (set after Initialize) directly. The
// Provider lifecycle is:
//   - Configure: store config (cheap, no side effects)
//   - Initialize: build sentry clients, multi-client, status-data provider,
//     execution-P2P layer
//   - Start: kick off background work (stream loops, status updates, peer
//     logging)
//   - Close: shut down servers and background goroutines
package sentry

import (
	"context"
)

// Provider is the Sentry component's runtime state. Subsequent commits will
// populate its public fields as sentry setup moves out of backend.go.
//
// After Initialize, the public fields are ready for consumers. Consumers that
// need background work (peer list updates, stream loops) must wait until after
// Start returns.
type Provider struct {
	// Public fields populated by Initialize will be added in subsequent commits
	// as the corresponding code moves out of backend.go.
}

// Configure stores the Provider's configuration. Call before Initialize.
// Cheap: no network, no file I/O, no goroutines.
func (p *Provider) Configure() {
	// Config fields will be added as sentry setup migrates in.
}

// Initialize builds the sentry stack: sentry servers (local mode) or gRPC
// clients (remote mode), status-data provider, multi-sentry client, and
// execution-P2P layer. After this returns, the Provider's public fields are
// ready for consumers.
//
// Initialize does NOT start background goroutines — call Start for that.
func (p *Provider) Initialize(ctx context.Context) error {
	return nil
}

// Start kicks off background work: stream loops on the multi-client, the
// status-data provider's header-subscription goroutine, peer-count logging,
// and the execution-P2P layer's run loop.
//
// Requires Initialize to have completed successfully.
func (p *Provider) Start(ctx context.Context) error {
	return nil
}

// Close shuts down sentry servers and cancels background goroutines.
// Safe to call multiple times.
func (p *Provider) Close() error {
	return nil
}
