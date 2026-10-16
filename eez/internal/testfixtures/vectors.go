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

package testfixtures

import (
	"fmt"
	"os"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

const (
	vectorSchemaVersion       = 1
	rollingHashOracle         = "contracts/test/RollingHashVectors.t.sol"
	publicInputsHashOracle    = "contracts/test/PublicInputsHashVectors.t.sol"
	rollingHashVectorsFile    = "rolling_hash_vectors.json"
	publicInputsVectorsFile   = "public_inputs_hash_vectors.json"
	vectorsDir                = "vectors"
	errVectorHeaderMismatchFm = "%s: %s is %v, want %v"
)

// RollingHashVectors are the rolling-hash values the Solidity oracle asserts
// for fixed inputs: rollup updates [(1, 0x…11), (2^64-1, 0x…22)], proxy entry
// hash 0x…33, call hash 0x…44, return data 0xaabbcc, and the static results
// (true, 0xaabbcc) then (false, 0xdeadbeef).
type RollingHashVectors struct {
	SchemaVersion      int         `json:"schemaVersion"`
	ProtocolCommit     string      `json:"protocolCommit"`
	SolidityOracle     string      `json:"solidityOracle"`
	L1Seed             common.Hash `json:"l1Seed"`
	L2Seed             common.Hash `json:"l2Seed"`
	AfterCallBegin     common.Hash `json:"afterCallBegin"`
	AfterCallEnd       common.Hash `json:"afterCallEnd"`
	AfterNestedBegin   common.Hash `json:"afterNestedBegin"`
	AfterNestedEnd     common.Hash `json:"afterNestedEnd"`
	AfterCallNotFound  common.Hash `json:"afterCallNotFound"`
	AfterStaticSuccess common.Hash `json:"afterStaticSuccess"`
	AfterStaticFailure common.Hash `json:"afterStaticFailure"`
}

// PublicInputsVectors are public-input hashes the Solidity oracle asserts.
type PublicInputsVectors struct {
	SchemaVersion  int                  `json:"schemaVersion"`
	ProtocolCommit string               `json:"protocolCommit"`
	SolidityOracle string               `json:"solidityOracle"`
	Vectors        []PublicInputsVector `json:"vectors"`
}

// PublicInputsVector is one case: the hashed batch inputs, the per-rollup
// proof-system assignment, and the expected shared and per-proof-system hashes.
type PublicInputsVector struct {
	Name                       string             `json:"name"`
	EntryHashes                []common.Hash      `json:"entryHashes"`
	StaticEntryHashes          []common.Hash      `json:"staticEntryHashes"`
	BlobHashes                 []common.Hash      `json:"blobHashes"`
	CallData                   hexutil.Bytes      `json:"callData"`
	RollupAssignments          []RollupAssignment `json:"rollupAssignments"`
	ProofSystemCount           int                `json:"proofSystemCount"`
	BoundSender                common.Address     `json:"boundSender"`
	ExpectedSharedPublicInput  common.Hash        `json:"expectedSharedPublicInput"`
	ExpectedPublicInputsHashes []common.Hash      `json:"expectedPublicInputsHashes"`
}

// RollupAssignment is one rollup's proof-system selection, its verification
// key per selected proof system, and the custom data its manager returned.
type RollupAssignment struct {
	RollupID           uint64        `json:"rollupId"`
	ProofSystemIndexes []uint64      `json:"proofSystemIndexes"`
	VKeys              []common.Hash `json:"vkeys"`
	CustomData         hexutil.Bytes `json:"customData"`
}

// LoadRollingHashVectors loads vectors/rolling_hash_vectors.json.
func LoadRollingHashVectors() (*RollingHashVectors, error) {
	data, err := os.ReadFile(Path(vectorsDir, rollingHashVectorsFile))
	if err != nil {
		return nil, err
	}
	return ParseRollingHashVectors(data)
}

// ParseRollingHashVectors decodes rolling-hash vectors and checks their header.
func ParseRollingHashVectors(data []byte) (*RollingHashVectors, error) {
	var v RollingHashVectors
	if err := decodeStrict(data, &v); err != nil {
		return nil, fmt.Errorf("%s: %w", rollingHashVectorsFile, err)
	}
	if err := checkHeader(rollingHashVectorsFile, v.SchemaVersion, v.ProtocolCommit, v.SolidityOracle, rollingHashOracle); err != nil {
		return nil, err
	}
	return &v, nil
}

// LoadPublicInputsVectors loads vectors/public_inputs_hash_vectors.json.
func LoadPublicInputsVectors() (*PublicInputsVectors, error) {
	data, err := os.ReadFile(Path(vectorsDir, publicInputsVectorsFile))
	if err != nil {
		return nil, err
	}
	return ParsePublicInputsVectors(data)
}

// ParsePublicInputsVectors decodes public-input vectors, checks their header
// and the internal consistency of every case.
func ParsePublicInputsVectors(data []byte) (*PublicInputsVectors, error) {
	var v PublicInputsVectors
	if err := decodeStrict(data, &v); err != nil {
		return nil, fmt.Errorf("%s: %w", publicInputsVectorsFile, err)
	}
	if err := checkHeader(publicInputsVectorsFile, v.SchemaVersion, v.ProtocolCommit, v.SolidityOracle, publicInputsHashOracle); err != nil {
		return nil, err
	}
	if len(v.Vectors) == 0 {
		return nil, fmt.Errorf("%s: no vectors", publicInputsVectorsFile)
	}
	for i := range v.Vectors {
		c := &v.Vectors[i]
		if len(c.ExpectedPublicInputsHashes) != c.ProofSystemCount {
			return nil, fmt.Errorf("%s: vector %q: %d expected hashes for %d proof systems",
				publicInputsVectorsFile, c.Name, len(c.ExpectedPublicInputsHashes), c.ProofSystemCount)
		}
		for _, a := range c.RollupAssignments {
			if len(a.VKeys) != len(a.ProofSystemIndexes) {
				return nil, fmt.Errorf("%s: vector %q: rollup %d has %d vkeys for %d proof systems",
					publicInputsVectorsFile, c.Name, a.RollupID, len(a.VKeys), len(a.ProofSystemIndexes))
			}
		}
	}
	return &v, nil
}

func checkHeader(file string, schemaVersion int, protocolCommit, solidityOracle, wantOracle string) error {
	if schemaVersion != vectorSchemaVersion {
		return fmt.Errorf(errVectorHeaderMismatchFm, file, "schemaVersion", schemaVersion, vectorSchemaVersion)
	}
	if protocolCommit != ProtocolCommit {
		return fmt.Errorf(errVectorHeaderMismatchFm, file, "protocolCommit", protocolCommit, ProtocolCommit)
	}
	if solidityOracle != wantOracle {
		return fmt.Errorf(errVectorHeaderMismatchFm, file, "solidityOracle", solidityOracle, wantOracle)
	}
	return nil
}
