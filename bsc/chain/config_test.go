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
	"maps"
	"math/big"
	"slices"
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
	pointEvaluation, p256Verify, blsG1Add := addr(0x0a), addr(0x01, 0x00), addr(0x0b)

	for _, tc := range []struct {
		name       string
		block      uint64
		time       uint64
		bls        bool
		cometBFT   string
		doubleSign bool
		cancun     bool
		haber      bool
		prague     bool
	}{
		{"planck", 28196022, 1679276104, false, "", false, false, false, false},
		{"luban", 29613785, 1683534184, true, "CometBFTLightBlockValidate", false, false, false, false},
		{"plato", 29861024, 1684276126, true, "CometBFTLightBlockValidate", false, false, false, false},
		{"hertz", 31103030, 1688004519, true, "CometBFTLightBlockValidateHertz", false, false, false, false},
		{"feynman", 39000000, 1711712272, true, "CometBFTLightBlockValidateHertz", true, false, false, false},
		{"cancun", 40000000, 1714713485, true, "CometBFTLightBlockValidateHertz", true, true, false, false},
		{"haber", 42000000, 1720719209, true, "CometBFTLightBlockValidateHertz", true, true, true, false},
		{"prague", 49000000, 1741722894, true, "CometBFTLightBlockValidateHertz", true, true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blockContext := evmtypes.BlockContext{BlockNumber: tc.block, Time: tc.time}
			set := vm.Precompiles(blockContext.Rules(spec.Config))
			assert.Equal(t, tc.doubleSign, set[doubleSign] != nil)
			assert.Equal(t, tc.doubleSign, set[secp256k1Recover] != nil)
			assert.Equal(t, tc.cancun, set[pointEvaluation] != nil)
			assert.Equal(t, tc.prague, set[blsG1Add] != nil)
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

// TestChapelMendelActivatesOsaka pins that Mendel is BSC's Osaka: without
// osakaTime the Mendel blocks run under Prague rules.
func TestChapelMendelActivatesOsaka(t *testing.T) {
	t.Parallel()

	spec, err := chainspec.ChainSpecByName(networkname.Chapel)
	require.NoError(t, err)
	cfg := spec.Config

	require.NotNil(t, cfg.MendelTime)
	require.NotNil(t, cfg.OsakaTime)
	assert.Equal(t, *cfg.MendelTime, *cfg.OsakaTime)
	assert.False(t, cfg.IsOsaka(*cfg.MendelTime-1))
	assert.True(t, cfg.IsOsaka(*cfg.MendelTime))

	assert.Equal(t, &params.BlobConfig{Target: 3, Max: 6, BaseFeeUpdateFraction: 3338477}, cfg.GetBlobConfig(*cfg.MendelTime))
}

// TestChapelMendelPrecompiles pins BSC's Osaka precompile set: the Prague BSC set
// with EIP-7823/7883 MODEXP and EIP-7951 P256VERIFY pricing.
func TestChapelMendelPrecompiles(t *testing.T) {
	t.Parallel()

	spec, err := chainspec.ChainSpecByName(networkname.Chapel)
	require.NoError(t, err)

	addr := func(b ...byte) accounts.Address { return accounts.InternAddress(common.BytesToAddress(b)) }
	modExp, p256Verify := addr(0x05), addr(0x01, 0x00)

	// secp256k1 square root, as computed by VRF proof verification:
	// base^((P+1)/4) mod P with 32-byte base, exponent and modulus.
	secp256k1P, _ := new(big.Int).SetString("fffffffffffffffffffffffffffffffffffffffffffffffffffffffefffffc2f", 16)
	sqrtPower := new(big.Int).Rsh(new(big.Int).Add(secp256k1P, big.NewInt(1)), 2)
	word := func(x *big.Int) []byte { return x.FillBytes(make([]byte, 32)) }
	sqrtInput := slices.Concat(word(big.NewInt(32)), word(big.NewInt(32)), word(big.NewInt(32)),
		word(big.NewInt(2)), word(sqrtPower), word(secp256k1P))

	for _, tc := range []struct {
		name      string
		block     uint64
		time      uint64
		modExpGas uint64
		p256Gas   uint64
	}{
		{"fermi", 97510088, 1774319399, 1349, params.P256VerifyGas},
		{"mendel", 97510144, 1774319424, 4048, params.P256VerifyGasEIP7951},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blockContext := evmtypes.BlockContext{BlockNumber: tc.block, Time: tc.time}
			rules := blockContext.Rules(spec.Config)
			set := vm.Precompiles(rules)
			for b := byte(100); b <= 105; b++ {
				require.Contains(t, set, addr(b))
			}
			assert.Equal(t, "CometBFTLightBlockValidateHertz", set[addr(103)].Name())
			assert.Equal(t, tc.modExpGas, set[modExp].RequiredGas(sqrtInput))
			assert.Equal(t, tc.p256Gas, set[p256Verify].RequiredGas(nil))
			assert.ElementsMatch(t, slices.Collect(maps.Keys(set)), vm.ActivePrecompiles(rules))
		})
	}
}

func TestChapelIsPasteur(t *testing.T) {
	t.Parallel()

	spec, err := chainspec.ChainSpecByName(networkname.Chapel)
	require.NoError(t, err)
	cfg := spec.Config

	require.NotNil(t, cfg.PasteurTime)
	assert.Equal(t, uint64(1784601000), *cfg.PasteurTime)
	assert.False(t, cfg.IsPasteur(120346348, 1784600999))
	assert.True(t, cfg.IsPasteur(120346349, 1784601000))
	assert.False(t, cfg.IsPasteur(*cfg.LondonBlock-1, *cfg.PasteurTime))
}

// TestChapelPasteurPrecompiles pins the Pasteur precompile set on the first Pasteur
// block: the legacy Tendermint precompiles are deprecated and the CometBFT light-block
// precompile is priced per input byte.
func TestChapelPasteurPrecompiles(t *testing.T) {
	t.Parallel()

	spec, err := chainspec.ChainSpecByName(networkname.Chapel)
	require.NoError(t, err)

	addr := func(b ...byte) accounts.Address { return accounts.InternAddress(common.BytesToAddress(b)) }
	tmHeader, iavlProof, cometBFT := addr(0x64), addr(0x65), addr(0x67)
	input := make([]byte, 1000)

	for _, tc := range []struct {
		name        string
		block       uint64
		time        uint64
		tmHeader    string
		iavlProof   string
		cometBFT    string
		cometBFTGas uint64
	}{
		{"mendel", 120346348, 1784600999, "TMHeaderValidate", "IAVLMerkleProofValidatePlato", "CometBFTLightBlockValidateHertz", 3000},
		{"pasteur", 120346349, 1784601000, "TMHeaderValidateDeprecated", "IAVLMerkleProofValidateDeprecated", "CometBFTLightBlockValidatePasteur", 3000 + 16*1000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blockContext := evmtypes.BlockContext{BlockNumber: tc.block, Time: tc.time}
			rules := blockContext.Rules(spec.Config)
			set := vm.Precompiles(rules)
			assert.ElementsMatch(t, slices.Collect(maps.Keys(set)), vm.ActivePrecompiles(rules))
			assert.ElementsMatch(t, slices.Collect(maps.Keys(vm.PrecompiledContractsOsakaForBSC)), vm.ActivePrecompiles(rules))

			assert.Equal(t, tc.tmHeader, set[tmHeader].Name())
			assert.Equal(t, tc.iavlProof, set[iavlProof].Name())
			assert.Equal(t, tc.cometBFT, set[cometBFT].Name())
			assert.Equal(t, params.TendermintHeaderValidateGas, set[tmHeader].RequiredGas(input))
			assert.Equal(t, params.IAVLMerkleProofValidateGas, set[iavlProof].RequiredGas(input))
			assert.Equal(t, tc.cometBFTGas, set[cometBFT].RequiredGas(input))
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
