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

package manifest_exchange

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

var _ rpchelper.ChainConfigReconfigurable = (*Provider)(nil)

// TestReconfigure_AcceptsValidConfig covers the stub surface: the
// orchestrator can call Reconfigure uniformly on the captor list;
// manifest_exchange's actual chain-config coupling lives in the
// SetForkIDFilter / SetCanonicalValidator setters the orchestrator
// invokes separately.
func TestReconfigure_AcceptsValidConfig(t *testing.T) {
	p := &Provider{}
	require.NoError(t, p.Reconfigure(context.Background(), &chain.Config{ChainID: uint256.NewInt(560048)}))
	require.Error(t, p.Reconfigure(context.Background(), nil))
}
