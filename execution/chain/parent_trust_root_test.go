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

package chain

import (
	"encoding/json"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func TestParentTrustRoot_JSONRoundTrip(t *testing.T) {
	original := ParentTrustRoot{
		Kind:   "did",
		Pubkey: []byte{0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09},
		DID:    "did:key:z6MkfooBar",
	}
	data, err := json.Marshal(original)
	require.NoError(t, err)

	var decoded ParentTrustRoot
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, original, decoded)
}

func TestParentTrustRoot_DIDOmittedWhenEmpty(t *testing.T) {
	enrRoot := ParentTrustRoot{
		Kind:   "enr",
		Pubkey: []byte{0x03, 0xff},
	}
	data, err := json.Marshal(enrRoot)
	require.NoError(t, err)
	require.NotContains(t, string(data), "did",
		"non-DID kinds omit the DID field")
}

func TestConfig_ValidParentTrustRoots_JSONRoundTrip(t *testing.T) {
	// A fork chain.Config carrying a pinned accept-set of two trust
	// roots round-trips through JSON exactly.
	original := &Config{
		ChainName:          "mainnet-fork-20000000",
		ChainID:            uint256.NewInt(1),
		Parent:             "mainnet",
		CutBlock:           20_000_000,
		ParentManifestHash: [20]byte{0xab, 0xcd, 0xef},
		ValidParentTrustRoots: []ParentTrustRoot{
			{Kind: "did", Pubkey: []byte{0x02, 0x03}, DID: "did:key:zfoo"},
			{Kind: "enr", Pubkey: []byte{0x03, 0xff}},
		},
	}
	data, err := json.Marshal(original)
	require.NoError(t, err)
	require.Contains(t, string(data), "validParentTrustRoots")

	var decoded Config
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Len(t, decoded.ValidParentTrustRoots, 2)
	require.Equal(t, "did", decoded.ValidParentTrustRoots[0].Kind)
	require.Equal(t, []byte{0x02, 0x03}, decoded.ValidParentTrustRoots[0].Pubkey)
	require.Equal(t, "did:key:zfoo", decoded.ValidParentTrustRoots[0].DID)
	require.Equal(t, "enr", decoded.ValidParentTrustRoots[1].Kind)
	require.Empty(t, decoded.ValidParentTrustRoots[1].DID)
}

func TestConfig_ValidParentTrustRoots_OmittedOnRootChain(t *testing.T) {
	// A root chain (Parent == "") emits no validParentTrustRoots field —
	// the omitempty JSON tag drops it. Back-compat for every chain
	// that exists today: existing genesis.json files don't need to be
	// regenerated.
	root := &Config{
		ChainName: "mainnet",
		ChainID:   uint256.NewInt(1),
		// ValidParentTrustRoots: nil — root chain
	}
	data, err := json.Marshal(root)
	require.NoError(t, err)
	require.NotContains(t, string(data), "validParentTrustRoots",
		"root chains omit the optional field")
}

func TestConfig_MinForkUnwindBlock_JSONRoundTrip(t *testing.T) {
	original := &Config{
		ChainName:          "mainnet-fork-20000000",
		ChainID:            uint256.NewInt(1),
		Parent:             "mainnet",
		CutBlock:           20_000_000,
		MinForkUnwindBlock: 15_000_000,
	}
	data, err := json.Marshal(original)
	require.NoError(t, err)
	require.Contains(t, string(data), `"minForkUnwindBlock":15000000`)

	var decoded Config
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, uint64(15_000_000), decoded.MinForkUnwindBlock)
}

func TestConfig_ParentGenesisHash_JSONRoundTrip(t *testing.T) {
	original := &Config{
		ChainName: "mainnet-fork-20000000",
		ChainID:   uint256.NewInt(1),
		Parent:    "mainnet",
		CutBlock:  20_000_000,
		ParentGenesisHash: [32]byte{
			0xd4, 0xe5, 0x67, 0x40, 0xf8, 0x76, 0xae, 0xf8,
			0xc0, 0x10, 0xb8, 0x6a, 0x40, 0xd5, 0xf5, 0x67,
			0x45, 0xa1, 0x18, 0xd0, 0x90, 0x6a, 0x34, 0xe6,
			0x9a, 0xec, 0x8c, 0x0d, 0xb1, 0xcb, 0x8f, 0xa3,
		},
	}
	data, err := json.Marshal(original)
	require.NoError(t, err)
	require.Contains(t, string(data), "parentGenesisHash")

	var decoded Config
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, original.ParentGenesisHash, decoded.ParentGenesisHash)
}

func TestConfig_MinForkUnwindBlock_OmittedWhenZero(t *testing.T) {
	// Zero-value MinForkUnwindBlock is dropped by omitempty and
	// interpreted at read-time as CutBlock (fork-local unwind only).
	// Verifies unconfigured forks stay minimal on the wire.
	fork := &Config{
		ChainName: "mainnet-fork-20000000",
		ChainID:   uint256.NewInt(1),
		Parent:    "mainnet",
		CutBlock:  20_000_000,
	}
	data, err := json.Marshal(fork)
	require.NoError(t, err)
	require.NotContains(t, string(data), "minForkUnwindBlock",
		"zero-value omitted; runtime interprets absence as CutBlock")

	var decoded Config
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Zero(t, decoded.MinForkUnwindBlock)
}
