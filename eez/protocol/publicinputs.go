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
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/erigontech/erigon/common"
)

// RollupPublicInput is one rollup's contribution to a batch's public inputs.
type RollupPublicInput struct {
	RollupID uint64
	// ProofSystemIndexes select proof systems of the batch, strictly increasing.
	ProofSystemIndexes []uint64
	// VKeys holds the rollup's verification key for each selected proof system.
	VKeys []common.Hash
	// CustomData is what the rollup's manager returned from
	// getCustomData(blockNumber).
	CustomData []byte
}

// PublicInputs are the batch values postAndVerifyBatch commits to before
// each proof system verifies its proof.
type PublicInputs struct {
	EntryHashes       []common.Hash
	StaticEntryHashes []common.Hash
	BlobHashes        []common.Hash
	CallData          []byte
	// Rollups follow ProofSystemBatch.RollupIDsWithProofSystems.
	Rollups []RollupPublicInput
	// BoundSender is msg.sender when the batch binds it, zero otherwise.
	BoundSender common.Address
}

// NewPublicInputs gathers the public inputs of b. txBlobHashes are the
// versioned hashes of the transaction that carried b and sender its
// msg.sender; customData and vkeys are per rollup of b, as the rollup
// managers return them from getCustomData and checkProofSystemsAndGetVkeys.
func NewPublicInputs(b *ProofSystemBatch, txBlobHashes []common.Hash, customData [][]byte, vkeys [][]common.Hash, sender common.Address) (*PublicInputs, error) {
	n := len(b.RollupIDsWithProofSystems)
	if len(customData) != n || len(vkeys) != n {
		return nil, fmt.Errorf("public inputs: %d rollups, %d custom data, %d vkey lists", n, len(customData), len(vkeys))
	}
	p := &PublicInputs{
		EntryHashes:       make([]common.Hash, len(b.Entries)),
		StaticEntryHashes: make([]common.Hash, len(b.StaticEntries)),
		BlobHashes:        make([]common.Hash, len(b.BlobIndices)),
		CallData:          b.CallData,
		Rollups:           make([]RollupPublicInput, n),
	}
	for i := range b.Entries {
		h, err := ExecutionEntryHash(&b.Entries[i])
		if err != nil {
			return nil, fmt.Errorf("entries[%d]: %w", i, err)
		}
		p.EntryHashes[i] = h
	}
	for i := range b.StaticEntries {
		h, err := StaticExecutionEntryHash(&b.StaticEntries[i])
		if err != nil {
			return nil, fmt.Errorf("staticEntries[%d]: %w", i, err)
		}
		p.StaticEntryHashes[i] = h
	}
	for i, idx := range b.BlobIndices {
		if !idx.IsUint64() || idx.Uint64() >= uint64(len(txBlobHashes)) {
			return nil, fmt.Errorf("blobIndices[%d]: blob %v of %d not found", i, idx, len(txBlobHashes))
		}
		p.BlobHashes[i] = txBlobHashes[idx.Uint64()]
	}
	for i := range b.RollupIDsWithProofSystems {
		r := &b.RollupIDsWithProofSystems[i]
		p.Rollups[i] = RollupPublicInput{
			RollupID:           r.RollupID,
			ProofSystemIndexes: r.ProofSystemIndexes,
			VKeys:              vkeys[i],
			CustomData:         customData[i],
		}
	}
	if b.BindMsgSenderInPublicInput {
		p.BoundSender = sender
	}
	return p, nil
}

// SharedHash returns the shared public input that BatchPosted reports:
// keccak256(abi.encode(entryHashes) ‖ abi.encode(staticEntryHashes) ‖
// abi.encode(blobHashes) ‖ keccak256(callData) ‖ abi.encode(customDataHashes)
// ‖ boundSender), where customDataHashes[r] is
// keccak256(abi.encode(rollupId, customData)).
func (p *PublicInputs) SharedHash() common.Hash {
	customDataHashes := make([]common.Hash, len(p.Rollups))
	for i := range p.Rollups {
		customDataHashes[i] = customDataHash(p.Rollups[i].RollupID, p.Rollups[i].CustomData)
	}
	callDataHash := keccak(p.CallData)
	return keccak(
		encodeHashes(p.EntryHashes),
		encodeHashes(p.StaticEntryHashes),
		encodeHashes(p.BlobHashes),
		callDataHash[:],
		encodeHashes(customDataHashes),
		p.BoundSender[:],
	)
}

// ProofSystemHashes returns the public-inputs hash each of the batch's
// proofSystemCount proof systems verifies: keccak256(shared ‖ acc), where acc
// folds keccak256(abi.encode(acc, rollupId, vkey)) from zero over the rollups
// that selected the proof system, in batch order.
func (p *PublicInputs) ProofSystemHashes(proofSystemCount int) ([]common.Hash, error) {
	for i := range p.Rollups {
		r := &p.Rollups[i]
		if len(r.VKeys) != len(r.ProofSystemIndexes) {
			return nil, fmt.Errorf("rollup %d: %d vkeys for %d proof systems", r.RollupID, len(r.VKeys), len(r.ProofSystemIndexes))
		}
		for j, k := range r.ProofSystemIndexes {
			if k >= uint64(proofSystemCount) || (j > 0 && k <= r.ProofSystemIndexes[j-1]) {
				return nil, fmt.Errorf("rollup %d: proof system indexes %v not increasing below %d", r.RollupID, r.ProofSystemIndexes, proofSystemCount)
			}
		}
	}
	shared := p.SharedHash()
	hashes := make([]common.Hash, proofSystemCount)
	var rollupID common.Hash
	for k := range hashes {
		var acc common.Hash
		for i := range p.Rollups {
			r := &p.Rollups[i]
			j, found := slices.BinarySearch(r.ProofSystemIndexes, uint64(k))
			if !found {
				continue
			}
			binary.BigEndian.PutUint64(rollupID[24:], r.RollupID)
			acc = keccak(acc[:], rollupID[:], r.VKeys[j][:])
		}
		hashes[k] = keccak(shared[:], acc[:])
	}
	return hashes, nil
}

// encodeHashes returns abi.encode(hs) for hs of type bytes32[].
func encodeHashes(hs []common.Hash) []byte {
	enc := make([]byte, 64, 64+32*len(hs))
	enc[31] = 0x20
	binary.BigEndian.PutUint64(enc[56:], uint64(len(hs)))
	for i := range hs {
		enc = append(enc, hs[i][:]...)
	}
	return enc
}

// customDataHash returns keccak256(abi.encode(uint64 rollupID, bytes data)).
func customDataHash(rollupID uint64, data []byte) common.Hash {
	enc := make([]byte, 96, 96+len(data)+31)
	binary.BigEndian.PutUint64(enc[24:], rollupID)
	enc[63] = 0x40
	binary.BigEndian.PutUint64(enc[88:], uint64(len(data)))
	enc = append(enc, data...)
	enc = append(enc, make([]byte, (32-len(data)%32)%32)...)
	return keccak(enc)
}
