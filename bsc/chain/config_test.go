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

package chain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bscchain "github.com/erigontech/erigon/bsc/chain"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/p2p/enode"
)

// TestChapelForkPrecompiles pins that Chapel's fork schedule actually reaches the
// EVM. A fork block parsed into the config but never gated resolves to the previous
// fork's precompile set, which under-charges gas and diverges the state root.
func TestChapelForkPrecompiles(t *testing.T) {
	t.Parallel()

	spec, err := chainspec.ChainSpecByName(networkname.Chapel)
	require.NoError(t, err)

	addr := func(b ...byte) accounts.Address { return accounts.InternAddress(common.BytesToAddress(b)) }
	blsVerify, cometBFT, doubleSign, secp256k1Recover := addr(102), addr(103), addr(104), addr(105)
	pointEvaluation, p256Verify := addr(0x0a), addr(0x01, 0x00)

	for _, tc := range []struct {
		name       string
		block      uint64
		time       uint64
		bls        bool
		cometBFT   string
		doubleSign bool
		cancun     bool
		haber      bool
	}{
		{"planck", 28196022, 1679276104, false, "", false, false, false},
		{"luban", 29613785, 1683534184, true, "CometBFTLightBlockValidate", false, false, false},
		{"plato", 29861024, 1684276126, true, "CometBFTLightBlockValidate", false, false, false},
		{"hertz", 31103030, 1688004519, true, "CometBFTLightBlockValidateHertz", false, false, false},
		{"feynman", 39000000, 1711712272, true, "CometBFTLightBlockValidateHertz", true, false, false},
		{"cancun", 40000000, 1714713485, true, "CometBFTLightBlockValidateHertz", true, true, false},
		{"haber", 42000000, 1720719209, true, "CometBFTLightBlockValidateHertz", true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blockContext := evmtypes.BlockContext{BlockNumber: tc.block, Time: tc.time}
			set := vm.Precompiles(blockContext.Rules(spec.Config))
			assert.Equal(t, tc.doubleSign, set[doubleSign] != nil)
			assert.Equal(t, tc.doubleSign, set[secp256k1Recover] != nil)
			assert.Equal(t, tc.cancun, set[pointEvaluation] != nil)
			if tc.haber {
				require.Contains(t, set, p256Verify)
				assert.Equal(t, params.P256VerifyGas, set[p256Verify].RequiredGas(nil))
			} else {
				assert.NotContains(t, set, p256Verify)
			}
			if !tc.bls {
				require.NotContains(t, set, blsVerify)
				return
			}
			require.Contains(t, set, blsVerify)
			require.Contains(t, set, cometBFT)
			assert.Equal(t, tc.cometBFT, set[cometBFT].Name())
		})
	}
}

func TestChapelSpec(t *testing.T) {
	t.Parallel()

	spec, err := chainspec.ChainSpecByName(networkname.Chapel)
	require.NoError(t, err)
	require.False(t, spec.IsEmpty())

	assert.Equal(t, uint64(97), spec.Config.ChainID.Uint64())
	assert.Equal(t, chain.ParliaRules, spec.Config.Rules)
	assert.Equal(t, bscchain.Chapel.GenesisHash, spec.GenesisHash)
}

// TestChapelStaticPeers covers the path setStaticPeers takes when --staticpeers
// is not given: Chapel publishes no bootnodes of its own, so losing this lookup
// would leave the node with nothing to dial.
func TestChapelStaticPeers(t *testing.T) {
	t.Parallel()

	peers := chainspec.StaticPeerURLsOfChain(networkname.Chapel)
	require.Len(t, peers, 4)
	assert.Equal(t, bscchain.Chapel.StaticPeers, peers)

	nodes, err := enode.ParseNodesFromURLs(peers)
	require.NoError(t, err)
	require.Len(t, nodes, 4)
	for _, n := range nodes {
		assert.Equal(t, 30311, n.TCP())
	}
}
