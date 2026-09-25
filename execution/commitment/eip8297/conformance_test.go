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

package eip8297

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"lukechampine.com/blake3"

	"github.com/erigontech/erigon/common"
)

type conformanceVectors struct {
	Source       string `json:"source"`
	SourceCommit string `json:"source_commit"`

	Embedding struct {
		Address20       string            `json:"address20"`
		Address32       string            `json:"address32"`
		BasicDataKey    string            `json:"basic_data_key"`
		CodeHashKey     string            `json:"code_hash_key"`
		DelegationKey   string            `json:"delegation_key"`
		StorageSlotKeys map[string]string `json:"storage_slot_keys"`
		CodeChunkKeys   map[string]string `json:"code_chunk_keys"`
		CodeHash        string            `json:"code_hash"`
	} `json:"embedding"`

	ChunkifyCode []struct {
		Name   string   `json:"name"`
		Code   string   `json:"code"`
		Chunks []string `json:"chunks"`
	} `json:"chunkify_code"`

	EncodeBasicData []struct {
		CodeSize uint64 `json:"code_size"`
		Nonce    uint64 `json:"nonce"`
		Balance  string `json:"balance"`
		Encoded  string `json:"encoded"`
	} `json:"encode_basic_data"`
}

func loadConformance(t *testing.T) *conformanceVectors {
	t.Helper()
	raw, err := os.ReadFile("../testdata/binary_trie_vectors.json")
	require.NoError(t, err)
	v, err := parseConformance(raw)
	require.NoError(t, err)
	return v
}

func parseConformance(raw []byte) (*conformanceVectors, error) {
	v := new(conformanceVectors)
	if err := json.Unmarshal(raw, v); err != nil {
		return nil, err
	}
	if err := validateConformance(v); err != nil {
		return nil, err
	}
	return v, nil
}

func validateConformance(v *conformanceVectors) error {
	if v == nil || v.SourceCommit == "" {
		return fmt.Errorf("conformance vectors have no source commit")
	}
	if v.Embedding.Address20 == "" || v.Embedding.Address32 == "" || v.Embedding.BasicDataKey == "" ||
		v.Embedding.CodeHashKey == "" || v.Embedding.DelegationKey == "" || v.Embedding.CodeHash == "" {
		return fmt.Errorf("conformance embedding is empty")
	}
	if len(v.Embedding.StorageSlotKeys) == 0 || len(v.Embedding.CodeChunkKeys) == 0 {
		return fmt.Errorf("conformance embedding collections are empty")
	}
	if len(v.ChunkifyCode) == 0 {
		return fmt.Errorf("conformance chunkify collection is empty")
	}
	if len(v.EncodeBasicData) == 0 {
		return fmt.Errorf("conformance basic-data collection is empty")
	}
	return nil
}

func TestConformanceRejectsMissingMetadata(t *testing.T) {
	_, err := parseConformance([]byte(`{}`))
	require.Error(t, err)
}

func unhex(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "0x"))
	require.NoError(t, err)
	return decoded
}

func slotBytes(t *testing.T, decimal string) []byte {
	t.Helper()
	number, ok := new(big.Int).SetString(decimal, 10)
	require.True(t, ok, decimal)
	var slot [32]byte
	number.FillBytes(slot[:])
	return slot[:]
}

func blake3Hash(data []byte) common.Hash {
	return common.Hash(blake3.Sum256(data))
}

func TestPBinConformanceEmbedding(t *testing.T) {
	vectors := loadConformance(t).Embedding
	address := unhex(t, vectors.Address20)
	codeHash := common.BytesToHash(unhex(t, vectors.CodeHash))
	keys := DigestCache{Sum: blake3Hash}
	address32 := RightAlign32(address)

	require.Equal(t, vectors.Address32, "0x"+hex.EncodeToString(address32[:]))
	hexKey := func(key []byte) string { return "0x" + hex.EncodeToString(key) }
	require.Equal(t, vectors.BasicDataKey, hexKey(keys.AccountKey(address, BasicDataLeafKey)))
	require.Equal(t, vectors.CodeHashKey, hexKey(keys.AccountKey(address, CodeHashLeafKey)))
	require.Equal(t, vectors.DelegationKey, hexKey(keys.AccountKey(address, DelegationLeafKey)))
	for slot, want := range vectors.StorageSlotKeys {
		require.Equal(t, want, hexKey(keys.StorageKey(address, slotBytes(t, slot))), slot)
	}
	for chunk, want := range vectors.CodeChunkKeys {
		id, err := strconv.Atoi(chunk)
		require.NoError(t, err)
		require.Equal(t, want, hexKey(keys.CodeChunkKey(codeHash, id)), chunk)
	}
}

func TestPBinConformanceChunkifyCode(t *testing.T) {
	for _, vector := range loadConformance(t).ChunkifyCode {
		t.Run(vector.Name, func(t *testing.T) {
			chunks := ChunkifyCode(unhex(t, vector.Code))
			require.Len(t, chunks, len(vector.Chunks))
			for i, want := range vector.Chunks {
				require.Equal(t, want, "0x"+hex.EncodeToString(chunks[i][:]), i)
			}
		})
	}
}

func TestPBinConformanceEncodeBasicData(t *testing.T) {
	for _, vector := range loadConformance(t).EncodeBasicData {
		t.Run(vector.Balance, func(t *testing.T) {
			balance, err := uint256.FromHex(vector.Balance)
			require.NoError(t, err)
			got, err := EncodeBasicData(vector.Nonce, balance, vector.CodeSize)
			require.NoError(t, err)
			require.Equal(t, vector.Encoded, "0x"+hex.EncodeToString(got[:]))
		})
	}
}
