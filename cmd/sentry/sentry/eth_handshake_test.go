package sentry

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	proto_sentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/forkid"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/params"
)

func TestCheckPeerStatusCompatibility(t *testing.T) {
	var version uint = eth.ETH66
	networkID := params.MainnetChainConfig.ChainID.Uint64()
	goodReply := eth.StatusPacket{
		ProtocolVersion: uint32(version),
		NetworkID:       networkID,
		TD:              big.NewInt(0),
		Head:            common.Hash{},
		Genesis:         params.MainnetGenesisHash,
		ForkID:          forkid.NewID(params.MainnetChainConfig, params.MainnetGenesisHash, 0, 0),
	}
	heightForks, timeForks := forkid.GatherForks(params.MainnetChainConfig)
	status := proto_sentry.StatusData{
		NetworkId:       networkID,
		TotalDifficulty: gointerfaces.ConvertUint256IntToH256(new(uint256.Int)),
		BestHash:        nil,
		ForkData: &proto_sentry.Forks{
			Genesis:     gointerfaces.ConvertHashToH256(params.MainnetGenesisHash),
			HeightForks: heightForks,
			TimeForks:   timeForks,
		},
		MaxBlockHeight: 0,
	}

	t.Run("ok", func(t *testing.T) {
		err := checkPeerStatusCompatibility(&goodReply, &status, version, version)
		assert.Nil(t, err)
	})
	t.Run("network mismatch", func(t *testing.T) {
		reply := goodReply
		reply.NetworkID = 0
		err := checkPeerStatusCompatibility(&reply, &status, version, version)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "network")
	})
	t.Run("version mismatch min", func(t *testing.T) {
		reply := goodReply
		reply.ProtocolVersion = eth.ETH66 - 1
		err := checkPeerStatusCompatibility(&reply, &status, version, version)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "version is less")
	})
	t.Run("version mismatch max", func(t *testing.T) {
		reply := goodReply
		reply.ProtocolVersion = eth.ETH66 + 1
		err := checkPeerStatusCompatibility(&reply, &status, version, version)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "version is more")
	})
	t.Run("genesis mismatch", func(t *testing.T) {
		reply := goodReply
		reply.Genesis = common.Hash{}
		err := checkPeerStatusCompatibility(&reply, &status, version, version)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "genesis")
	})
	t.Run("fork mismatch", func(t *testing.T) {
		reply := goodReply
		reply.ForkID = forkid.ID{}
		err := checkPeerStatusCompatibility(&reply, &status, version, version)
		assert.NotNil(t, err)
		assert.ErrorIs(t, err, forkid.ErrLocalIncompatibleOrStale)
	})
}
