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
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

// TestPublicInputsVectors is T-VEC-2: the shared and per-proof-system hashes
// PublicInputsHashVectors.t.sol asserts and submits to the 855fe06 registry.
func TestPublicInputsVectors(t *testing.T) {
	vectors, err := testfixtures.LoadPublicInputsVectors()
	require.NoError(t, err)
	for i := range vectors.Vectors {
		v := &vectors.Vectors[i]
		t.Run(v.Name, func(t *testing.T) {
			p := PublicInputs{
				EntryHashes:       v.EntryHashes,
				StaticEntryHashes: v.StaticEntryHashes,
				BlobHashes:        v.BlobHashes,
				CallData:          v.CallData,
				BoundSender:       v.BoundSender,
			}
			for _, a := range v.RollupAssignments {
				p.Rollups = append(p.Rollups, RollupPublicInput{
					RollupID:           a.RollupID,
					ProofSystemIndexes: a.ProofSystemIndexes,
					VKeys:              a.VKeys,
					CustomData:         a.CustomData,
				})
			}
			require.Equal(t, v.ExpectedSharedPublicInput, p.SharedHash())
			hashes, err := p.ProofSystemHashes(v.ProofSystemCount)
			require.NoError(t, err)
			require.Equal(t, v.ExpectedPublicInputsHashes, hashes)
		})
	}
}

// recoverSigner recovers r ‖ s ‖ v over a raw digest with the checks of
// OpenZeppelin's ECDSA.recover, which ECDSAProofSystem.verify uses.
func recoverSigner(t *testing.T, digest common.Hash, sig []byte) common.Address {
	t.Helper()
	require.Len(t, sig, 65)
	require.Contains(t, []byte{27, 28}, sig[64])
	v := sig[64] - 27
	r, s := new(uint256.Int).SetBytes(sig[:32]), new(uint256.Int).SetBytes(sig[32:64])
	require.True(t, crypto.TransactionSignatureIsValid(v, r, s, false), "high s or zero r, s")
	pub, err := crypto.Ecrecover(digest[:], append(bytes.Clone(sig[:64]), v))
	require.NoError(t, err)
	return common.BytesToAddress(crypto.Keccak256(pub[1:])[12:])
}

// TestWindow84PublicInputs recomputes the devnet batch's public-inputs hash
// (T-VEC-5) and recovers its proof to the attester: blockNumber 0 makes the
// manager's custom data empty, and the batch binds no sender and no blobs.
func TestWindow84PublicInputs(t *testing.T) {
	w, err := testfixtures.LoadWindow("captured-devnet-window-84")
	require.NoError(t, err)
	var oracle struct {
		VKey     common.Hash
		Hash     common.Hash
		Attester common.Address
	}
	for key, dst := range map[string]any{"proof_system_vkey": &oracle.VKey, "public_inputs_hash": &oracle.Hash, "attester": &oracle.Attester} {
		require.NoError(t, json.Unmarshal(w.Oracle[key], dst), key)
	}

	b, err := DecodePostAndVerifyBatch(w.PostBatch)
	require.NoError(t, err)
	require.Zero(t, b.BlockNumber)
	p, err := NewPublicInputs(b, nil, [][]byte{nil}, [][]common.Hash{{oracle.VKey}}, common.Address{})
	require.NoError(t, err)
	hashes, err := p.ProofSystemHashes(len(b.ProofSystems))
	require.NoError(t, err)
	require.Equal(t, []common.Hash{oracle.Hash}, hashes)
	require.Equal(t, oracle.Attester, recoverSigner(t, hashes[0], b.Proofs[0]))
}

// TestChiadoPublicInputs recomputes the live settlement's shared public
// input, which its BatchPosted log reports, and recovers its proof to the
// proof system's signer. The batch binds the latest L1 context, for which the
// manager returns abi.encode(block.timestamp, blockhash(block.number - 1)),
// and takes its blob hash from the transaction.
func TestChiadoPublicInputs(t *testing.T) {
	s, err := testfixtures.LoadChiadoSettlement()
	require.NoError(t, err)
	headerRLP, err := testfixtures.LoadChiadoHeader()
	require.NoError(t, err)
	var header types.Header
	require.NoError(t, rlp.DecodeBytes(headerRLP, &header))
	require.Equal(t, s.L1BlockHash, header.Hash())

	b, err := DecodePostAndVerifyBatch(s.Transaction.Input)
	require.NoError(t, err)
	require.Equal(t, uint64(1<<64-1), b.BlockNumber)
	customData := make([]byte, 64)
	binary.BigEndian.PutUint64(customData[24:32], header.Time)
	copy(customData[32:], header.ParentHash[:])

	p, err := NewPublicInputs(b, s.Transaction.BlobVersionedHashes, [][]byte{customData},
		[][]common.Hash{{s.StateAtBlock.ManagerVerificationKey}}, s.Transaction.From)
	require.NoError(t, err)
	var posted *BatchPosted
	for i := range s.Logs {
		if s.Logs[i].Topics[0] == BatchPostedTopic {
			posted, err = ParseBatchPosted(chiadoLog(&s.Logs[i]))
			require.NoError(t, err)
		}
	}
	require.NotNil(t, posted)
	require.Equal(t, posted.SharedPublicInput, p.SharedHash())

	hashes, err := p.ProofSystemHashes(len(b.ProofSystems))
	require.NoError(t, err)
	require.Len(t, hashes, 1)
	require.Equal(t, s.StateAtBlock.ProofSystemSigner, recoverSigner(t, hashes[0], b.Proofs[0]))
}

func TestPublicInputsRejects(t *testing.T) {
	w, err := testfixtures.LoadWindow("captured-devnet-window-84")
	require.NoError(t, err)
	b, err := DecodePostAndVerifyBatch(w.PostBatch)
	require.NoError(t, err)
	vkeys := [][]common.Hash{{{}}}

	_, err = NewPublicInputs(b, nil, nil, vkeys, common.Address{})
	require.ErrorContains(t, err, "custom data")

	withBlob := *b
	withBlob.BlobIndices = []*big.Int{big.NewInt(1)}
	_, err = NewPublicInputs(&withBlob, []common.Hash{{1}}, [][]byte{nil}, vkeys, common.Address{})
	require.ErrorContains(t, err, "blobIndices[0]")

	bound := *b
	bound.BindMsgSenderInPublicInput = true
	sender := common.HexToAddress("0xbeef")
	p, err := NewPublicInputs(&bound, nil, [][]byte{nil}, vkeys, sender)
	require.NoError(t, err)
	require.Equal(t, sender, p.BoundSender)

	for name, r := range map[string]RollupPublicInput{
		"vkey count":     {RollupID: 1, ProofSystemIndexes: []uint64{0}},
		"index too high": {RollupID: 1, ProofSystemIndexes: []uint64{2}, VKeys: []common.Hash{{}}},
		"not increasing": {RollupID: 1, ProofSystemIndexes: []uint64{1, 0}, VKeys: []common.Hash{{}, {}}},
		"duplicate":      {RollupID: 1, ProofSystemIndexes: []uint64{1, 1}, VKeys: []common.Hash{{}, {}}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := (&PublicInputs{Rollups: []RollupPublicInput{r}}).ProofSystemHashes(2)
			require.Error(t, err)
		})
	}
}
