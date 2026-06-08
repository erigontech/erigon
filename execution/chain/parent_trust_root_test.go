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
