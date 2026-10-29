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
	"encoding/json"
	"fmt"
	"os"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

const nativeProfileVectorsFile = "native-profile-v1.json"

// NativeProfileVectors are eez-rollupX's encoding vectors for its v0.1 native
// profile: source hashes, history calldata and the 0x45 and 0x46 envelopes,
// each built independently of the reference client.
type NativeProfileVectors struct {
	FormatVersion          int                   `json:"formatVersion"`
	Status                 string                `json:"status"`
	Notes                  []string              `json:"notes"`
	Vectors                []NativeProfileVector `json:"vectors"`
	Relationships          []json.RawMessage     `json:"relationships"`
	ValidationVersion      int                   `json:"validationVersion"`
	HistoryEnvelopeVersion int                   `json:"historyEnvelopeVersion"`
	HistoryTransactionType string                `json:"historyTransactionType"`
	CrossChainFields       []string              `json:"crossChainFields"`
}

// NativeProfileVector is one encoding: its arguments (ABI) or fields (RLP),
// the bytes they encode to and the keccak256 of those bytes.
type NativeProfileVector struct {
	ID                string          `json:"id"`
	Encoding          string          `json:"encoding"`
	Signature         string          `json:"signature"`
	Arguments         []string        `json:"arguments"`
	Fields            json.RawMessage `json:"fields"`
	Prefix            string          `json:"prefix"`
	ExpectedBytes     hexutil.Bytes   `json:"expectedBytes"`
	ExpectedKeccak256 common.Hash     `json:"expectedKeccak256"`
	Meaning           string          `json:"meaning"`
}

// LoadNativeProfileVectors loads vectors/native-profile-v1.json and checks
// it is format 1 of validation version 7.
func LoadNativeProfileVectors() (*NativeProfileVectors, error) {
	data, err := os.ReadFile(Path(vectorsDir, nativeProfileVectorsFile))
	if err != nil {
		return nil, err
	}
	var v NativeProfileVectors
	if err := decodeStrict(data, &v); err != nil {
		return nil, fmt.Errorf("%s: %w", nativeProfileVectorsFile, err)
	}
	if v.FormatVersion != 1 || v.ValidationVersion != 7 || v.HistoryEnvelopeVersion != 1 || v.HistoryTransactionType != "0x46" {
		return nil, fmt.Errorf("%s: format %d, validation version %d, history envelope %d of type %s",
			nativeProfileVectorsFile, v.FormatVersion, v.ValidationVersion, v.HistoryEnvelopeVersion, v.HistoryTransactionType)
	}
	return &v, nil
}

// Vector returns the vector with id.
func (v *NativeProfileVectors) Vector(id string) (NativeProfileVector, bool) {
	for i := range v.Vectors {
		if v.Vectors[i].ID == id {
			return v.Vectors[i], true
		}
	}
	return NativeProfileVector{}, false
}
