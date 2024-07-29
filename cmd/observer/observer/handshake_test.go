// Copyright 2024 The Erigon Authors
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

package observer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/direct"

	"github.com/erigontech/erigon/crypto"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/params"
)

func TestHandshake(t *testing.T) {
	t.Skip("only for dev")

	// grep 'self=enode' the log, and paste it here
	// url := "enode://..."
	url := params.MainnetBootnodes[0]
	node := enode.MustParseV4(url)
	myPrivateKey, _ := crypto.GenerateKey()

	ctx := context.Background()
	hello, status, err := Handshake(ctx, node.IP(), node.TCP(), node.Pubkey(), myPrivateKey)

	require.Nil(t, err)
	require.NotNil(t, hello)
	assert.Equal(t, uint64(5), hello.Version)
	assert.NotEmpty(t, hello.ClientID)
	assert.Contains(t, hello.ClientID, "erigon")

	require.NotNil(t, status)
	assert.Equal(t, uint32(direct.ETH66), status.ProtocolVersion)
	assert.Equal(t, uint64(1), status.NetworkID)
}
