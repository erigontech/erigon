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

package announce_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/eez/announce"
)

// vector is an announcement of vectors/unsafe_block_announcement_vectors.json.
type vector struct {
	Name       string         `json:"name"`
	PrivateKey hexutil.Bytes  `json:"privateKey"`
	Address    common.Address `json:"address"`
	ChainID    hexutil.Big    `json:"chainId"`
	BlockHash  common.Hash    `json:"blockHash"`
	Message    common.Hash    `json:"message"`
	Envelope   hexutil.Bytes  `json:"envelope"`
}

type vectors struct {
	DomainTag common.Hash `json:"domainTag"`
	Valid     []vector    `json:"valid"`
	// Invalid envelopes each break one rule of App. D.6.
	Invalid []vector `json:"invalid"`
}

func load(t *testing.T) vectors {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "testdata", "vectors", "unsafe_block_announcement_vectors.json"))
	require.NoError(t, err)
	var v vectors
	require.NoError(t, json.Unmarshal(raw, &v))
	require.NotEmpty(t, v.Valid)
	return v
}

// TestTag is T-SEQ-8 on the domain tag, whose value App. D.6 gives.
func TestTag(t *testing.T) {
	require.Equal(t, common.HexToHash("0xdb821d3d941bda3532841751ab0568201783ed2ad81cdc4f5474e73c0552a234"), announce.Tag)
	require.Equal(t, load(t).DomainTag, announce.Tag)
}

// TestSign is T-SEQ-8 on the producer's side: the signed message and the
// envelope of each vector signed with its key.
func TestSign(t *testing.T) {
	signed := 0
	for _, v := range load(t).Valid {
		chainID := uint256.MustFromBig(v.ChainID.ToInt())
		require.Equal(t, v.Message, announce.Message(chainID, v.BlockHash), v.Name)
		if v.PrivateKey == nil {
			continue
		}
		key, err := crypto.ToECDSA(v.PrivateKey)
		require.NoError(t, err)
		require.Equal(t, v.Address, crypto.PubkeyToAddress(key.PublicKey), v.Name)
		env, err := announce.Sign(key, chainID, v.BlockHash)
		require.NoError(t, err, v.Name)
		require.Equal(t, v.Envelope, hexutil.Bytes(env), v.Name)
		signed++
	}
	require.Equal(t, 3, signed)
}

// TestVerify is T-SEQ-8 on the follower's side: each valid envelope recovers
// its producer, whichever key signed it, and each malformed one is rejected,
// the high-s twin of a valid signature among them.
func TestVerify(t *testing.T) {
	v := load(t)
	for _, x := range v.Valid {
		producer, err := announce.Verify(x.Envelope, uint256.MustFromBig(x.ChainID.ToInt()), x.BlockHash)
		require.NoError(t, err, x.Name)
		require.Equal(t, x.Address, producer, x.Name)
	}
	require.Len(t, v.Invalid, 26)
	for _, x := range v.Invalid {
		_, err := announce.Verify(x.Envelope, uint256.MustFromBig(x.ChainID.ToInt()), x.BlockHash)
		require.ErrorIs(t, err, announce.ErrInvalid, x.Name)
	}

	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	chainID, hash := uint256.NewInt(6290), common.Hash{7}
	env, err := announce.Sign(key, chainID, hash)
	require.NoError(t, err)
	producer, err := announce.Verify(env, chainID, hash)
	require.NoError(t, err)
	require.Equal(t, crypto.PubkeyToAddress(key.PublicKey), producer)
}
