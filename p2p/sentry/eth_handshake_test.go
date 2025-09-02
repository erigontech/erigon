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

package sentry

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	proto_sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/p2p/forkid"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

func TestCheckPeerStatusCompatibility(t *testing.T) {
	var version uint = direct.ETH67
	networkID := chainspec.Mainnet.Config.ChainID.Uint64()
	heightForks, timeForks := forkid.GatherForks(chainspec.Mainnet.Config, 0 /* genesisTime */)
	goodReply := eth.StatusPacket{
		ProtocolVersion: uint32(version),
		NetworkID:       networkID,
		TD:              big.NewInt(0),
		Head:            common.Hash{},
		Genesis:         chainspec.Mainnet.GenesisHash,
		ForkID:          forkid.NewIDFromForks(heightForks, timeForks, chainspec.Mainnet.GenesisHash, 0, 0),
	}
	status := proto_sentry.StatusData{
		NetworkId:       networkID,
		TotalDifficulty: gointerfaces.ConvertUint256IntToH256(new(uint256.Int)),
		BestHash:        nil,
		ForkData: &proto_sentry.Forks{
			Genesis:     gointerfaces.ConvertHashToH256(chainspec.Mainnet.GenesisHash),
			HeightForks: heightForks,
			TimeForks:   timeForks,
		},
		MaxBlockHeight: 0,
	}

	t.Run("ok", func(t *testing.T) {
		err := checkPeerStatusCompatibility(&goodReply, &status, version, version)
		assert.NoError(t, err)
	})
	t.Run("network mismatch", func(t *testing.T) {
		reply := goodReply
		reply.NetworkID = 0
		err := checkPeerStatusCompatibility(&reply, &status, version, version)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "network")
	})
	t.Run("version mismatch min", func(t *testing.T) {
		reply := goodReply
		reply.ProtocolVersion = direct.ETH67 - 1
		err := checkPeerStatusCompatibility(&reply, &status, version, version)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "version is less")
	})
	t.Run("version mismatch max", func(t *testing.T) {
		reply := goodReply
		reply.ProtocolVersion = direct.ETH67 + 1
		err := checkPeerStatusCompatibility(&reply, &status, version, version)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "version is more")
	})
	t.Run("genesis mismatch", func(t *testing.T) {
		reply := goodReply
		reply.Genesis = common.Hash{}
		err := checkPeerStatusCompatibility(&reply, &status, version, version)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "genesis")
	})
	t.Run("fork mismatch", func(t *testing.T) {
		reply := goodReply
		reply.ForkID = forkid.ID{}
		err := checkPeerStatusCompatibility(&reply, &status, version, version)
		assert.Error(t, err)
		assert.ErrorIs(t, err, forkid.ErrLocalIncompatibleOrStale)
	})
}
