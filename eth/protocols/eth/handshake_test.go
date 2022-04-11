// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package eth_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/forkid"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/stretchr/testify/require"
)

// Tests that handshake failures are detected and reported correctly.
func TestHandshake64(t *testing.T) { testHandshake(t, 64) }
func TestHandshake65(t *testing.T) { testHandshake(t, 65) }

func testHandshake(t *testing.T, protocol uint) {
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please")
	}

	t.Parallel()

	// Create a test backend only to have some valid genesis chain
	backend := newTestBackend(t, 3)

	tx, err := backend.db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	var (
		genesis = backend.genesis
		head    = rawdb.ReadCurrentBlock(tx)
		td, _   = rawdb.ReadTd(tx, head.Hash(), head.NumberU64())
		forkID  = forkid.NewID(backend.chainConfig, backend.genesis.Hash(), backend.headBlock.NumberU64())
	)
	tests := []struct {
		code uint64
		data interface{}
		want error
	}{
		{
			code: eth.TransactionsMsg, data: []interface{}{},
			want: eth.ErrNoStatusMsg,
		},
		{
			code: eth.StatusMsg, data: eth.StatusPacket{10, 1, td, head.Hash(), genesis.Hash(), forkID},
			want: eth.ErrProtocolVersionMismatch,
		},
		{
			code: eth.StatusMsg, data: eth.StatusPacket{uint32(protocol), 999, td, head.Hash(), genesis.Hash(), forkID},
			want: eth.ErrNetworkIDMismatch,
		},
		{
			code: eth.StatusMsg, data: eth.StatusPacket{uint32(protocol), 1, td, head.Hash(), common.Hash{3}, forkID},
			want: eth.ErrGenesisMismatch,
		},
		{
			code: eth.StatusMsg, data: eth.StatusPacket{uint32(protocol), 1, td, head.Hash(), genesis.Hash(), forkid.ID{Hash: [4]byte{0x00, 0x01, 0x02, 0x03}}},
			want: eth.ErrForkIDRejected,
		},
	}
	for i, test := range tests {
		// Create the two peers to shake with each other
		app, net := p2p.MsgPipe()
		defer app.Close()
		defer net.Close()

		peer := eth.NewPeer(protocol, p2p.NewPeer(enode.ID{}, [64]byte{1}, "peer", nil), net, nil)
		defer peer.Close()

		// Send the junk test with one peer, check the handshake failure
		go func() {
			if err := p2p.Send(app, test.code, test.data); err != nil {
				fmt.Printf("Could not send: %v\n", err)
			}
		}()

		err := peer.Handshake(1, td, head.Hash(), genesis.Hash(), forkID, forkid.NewFilter(backend.chainConfig, backend.genesis.Hash(), func() uint64 { return backend.headBlock.NumberU64() }))
		if err == nil {
			t.Errorf("test %d: protocol returned nil error, want %q", i, test.want)
		} else if !errors.Is(err, test.want) {
			t.Errorf("test %d: wrong error: got %q, want %q", i, err, test.want)
		}
	}
}
