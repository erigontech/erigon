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

package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

func TestManagerCalls(t *testing.T) {
	require.Equal(t, hexutil.MustDecode("0x42cde4e8"), EncodeThreshold(), "threshold()")
	require.Equal(t, hexutil.MustDecode("0x238ac933"), EncodeSigner(), "signer()")
	ps := common.HexToAddress("0xe7f1725E7734CE288F8367e1Bb143E90bb3F0512")
	require.Equal(t, hexutil.MustDecode("0xeea5eec7000000000000000000000000e7f1725e7734ce288f8367e1bb143e90bb3f0512"), EncodeVerificationKey(ps), "verificationKey(address)")
}

func TestDecodeManagerReturns(t *testing.T) {
	n, err := DecodeThreshold(hexutil.MustDecode("0x0000000000000000000000000000000000000000000000000000000000000002"))
	require.NoError(t, err)
	require.Equal(t, uint64(2), n)
	_, err = DecodeThreshold(hexutil.MustDecode("0x0000000000000000000000000000000000000000000000010000000000000000"))
	require.Error(t, err, "above 64 bits")
	_, err = DecodeThreshold(nil)
	require.Error(t, err)

	key := common.HexToHash("0x00000000000000000000000070997970c51812dc3a010c7d01b50e0d17dc79c8")
	got, err := DecodeVerificationKey(key[:])
	require.NoError(t, err)
	require.Equal(t, key, got)
	_, err = DecodeVerificationKey(key[:31])
	require.Error(t, err)

	signer, err := DecodeSigner(key[:])
	require.NoError(t, err)
	require.Equal(t, common.HexToAddress("0x70997970C51812dc3A010C7d01b50e0d17dc79C8"), signer)
	_, err = DecodeSigner(hexutil.MustDecode("0x0100000000000000000000000000000000000000000000000000000000000000"))
	require.Error(t, err, "dirty high bytes")
}

// TestProofSystemHashOfASingleProofSystemCopy: a proof system's hash in a
// batch is the hash it has in a copy listing it alone, so each attester of a
// quorum can sign such a copy.
func TestProofSystemHashOfASingleProofSystemCopy(t *testing.T) {
	b := &ProofSystemBatch{
		CallData:                  []byte{1, 2, 3},
		ProofSystems:              []common.Address{{0xa}, {0xb}},
		RollupIDsWithProofSystems: []RollupIDWithProofSystems{{RollupID: 2, ProofSystemIndexes: []uint64{0, 1}}},
	}
	keys := []common.Hash{{0x1a}, {0x1b}}
	p, err := NewPublicInputs(b, nil, [][]byte{{9}}, [][]common.Hash{keys}, common.Address{})
	require.NoError(t, err)
	both, err := p.ProofSystemHashes(2)
	require.NoError(t, err)
	require.NotEqual(t, both[0], both[1])
	for i, ps := range b.ProofSystems {
		alone := *b
		alone.ProofSystems = []common.Address{ps}
		alone.RollupIDsWithProofSystems = []RollupIDWithProofSystems{{RollupID: 2, ProofSystemIndexes: []uint64{0}}}
		p, err := NewPublicInputs(&alone, nil, [][]byte{{9}}, [][]common.Hash{{keys[i]}}, common.Address{})
		require.NoError(t, err)
		one, err := p.ProofSystemHashes(1)
		require.NoError(t, err)
		require.Equal(t, both[i], one[0], "proof system %d", i)
	}
}
