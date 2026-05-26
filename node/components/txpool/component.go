// Copyright 2025 The Erigon Authors
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
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

// Component is an alias for Provider, providing a clean constructor-based API.
type Component = Provider

// NewComponent creates and configures a new txpool component.
// This is a convenience constructor that calls NewProvider() and Configure().
// The caller must still call Initialize() and Activate() before use.
func NewComponent(
	cfg txpoolcfg.Config,
	chainDB kv.TemporalRoDB,
	chainCfg *chain.Config,
	ethBackend remoteproto.ETHBACKENDClient,
	stateChanges StateChangesClient,
	logger log.Logger,
) (*Component, error) {
	p := NewProvider()
	if err := p.Configure(cfg, chainDB, chainCfg, ethBackend, stateChanges, logger); err != nil {
		return nil, err
	}
	return p, nil
}

// MustNewComponent creates a new component, panicking on configuration error.
// Useful for testing and one-off scripts where error handling is not critical.
func MustNewComponent(
	cfg txpoolcfg.Config,
	chainDB kv.TemporalRoDB,
	chainCfg *chain.Config,
	ethBackend remoteproto.ETHBACKENDClient,
	stateChanges StateChangesClient,
	logger log.Logger,
) *Component {
	p, err := NewComponent(cfg, chainDB, chainCfg, ethBackend, stateChanges, logger)
	if err != nil {
		panic(err)
	}
	return p
}
