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

package seal

import (
	"encoding/json"
	"math/big"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types"
)

type chapelHeader struct {
	types.Header
	BlockHash common.Hash `json:"hash"`
}

func (h *chapelHeader) UnmarshalJSON(data []byte) error {
	if err := json.Unmarshal(data, &h.Header); err != nil {
		return err
	}
	var hash struct {
		Hash common.Hash `json:"hash"`
	}
	if err := json.Unmarshal(data, &hash); err != nil {
		return err
	}
	h.BlockHash = hash.Hash
	return nil
}

// TestHashRecoversChapelSealer checks the seal hash against canonical Chapel headers
// from eth_getBlockByNumber: pre-Cancun, Cancun (blob fields present but unsealed),
// Bohr (sealed through ParentBeaconBlockRoot) and Pascal (sealed RequestsHash).
func TestHashRecoversChapelSealer(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile("testdata/chapel_headers.json")
	require.NoError(t, err)
	var headers []*chapelHeader
	require.NoError(t, json.Unmarshal(raw, &headers))
	require.Len(t, headers, 4)

	chainID := big.NewInt(97)
	for _, h := range headers {
		t.Run(h.Number.String(), func(t *testing.T) {
			require.Equal(t, h.BlockHash, h.Hash())

			sealHash, err := Hash(&h.Header, chainID)
			require.NoError(t, err)
			pubkey, err := crypto.Ecrecover(sealHash[:], h.Extra[len(h.Extra)-crypto.SignatureLength:])
			require.NoError(t, err)
			var signer common.Address
			copy(signer[:], crypto.Keccak256(pubkey[1:])[12:])
			require.Equal(t, h.Coinbase, signer)
		})
	}
}

func TestHashRejectsShortExtra(t *testing.T) {
	t.Parallel()

	_, err := Hash(&types.Header{Extra: make([]byte, crypto.SignatureLength-1)}, big.NewInt(97))
	require.ErrorIs(t, err, ErrMissingSignature)
}
