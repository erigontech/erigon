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

package txpool_test

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

var _ rpchelper.ChainConfigReconfigurable = (*txpool.TxPool)(nil)

// TestReconfigure_RejectsNil covers the input-validation surface;
// full chain-config swap coverage lives internal to the package
// where the private chainConfig field is reachable.
func TestReconfigure_RejectsNil(t *testing.T) {
	p := &txpool.TxPool{}
	require.Error(t, p.Reconfigure(context.Background(), nil))
	require.NoError(t, p.Reconfigure(context.Background(), &chain.Config{ChainID: uint256.NewInt(1)}))
}
