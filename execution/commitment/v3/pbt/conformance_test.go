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

package pbt

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/internal/commitmenttest/commitmentflags"
)

type pbtConformance struct {
	SourceCommit string `json:"source_commit"`
	PBTState     []struct {
		Name     string `json:"name"`
		Accounts map[string]struct {
			Nonce    uint64            `json:"nonce"`
			Balance  string            `json:"balance"`
			Code     string            `json:"code"`
			CodeHash string            `json:"code_hash"`
			Storage  map[string]string `json:"storage"`
		} `json:"accounts"`
		Root string `json:"root"`
	} `json:"pbt_state"`
}

type pbtSpecVectors struct {
	TrieVectors []struct {
		Name    string `json:"name"`
		Entries []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"entries"`
		Root string `json:"root"`
	} `json:"trie_vectors"`
	SequenceVectors []struct {
		Seed int `json:"seed"`
		Ops  []struct {
			Op    string `json:"op"`
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"ops"`
		RootsAfter []string `json:"roots_after"`
	} `json:"sequence_vectors"`
}

func loadPBTConformance(t *testing.T) *pbtConformance {
	t.Helper()
	raw, err := os.ReadFile("../../testdata/binary_trie_vectors.json")
	require.NoError(t, err)
	v := new(pbtConformance)
	require.NoError(t, json.Unmarshal(raw, v))
	require.NotEmpty(t, v.SourceCommit)
	require.NotEmpty(t, v.PBTState)
	return v
}

func pbtConformanceHex(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "0x"))
	require.NoError(t, err)
	return decoded
}

func pbtConformanceSlot(t *testing.T, value string) []byte {
	t.Helper()
	slot := uint256.MustFromDecimal(value).Bytes32()
	return slot[:]
}

func loadPBTSpecVectors(t *testing.T) *pbtSpecVectors {
	t.Helper()
	raw, err := os.ReadFile("../../testdata/eip8297_vectors.json")
	require.NoError(t, err)
	v := new(pbtSpecVectors)
	require.NoError(t, json.Unmarshal(raw, v))
	require.NotEmpty(t, v.TrieVectors)
	require.NotEmpty(t, v.SequenceVectors)
	return v
}

func TestPBinConformancePBTState(t *testing.T) {
	commitmentflags.Restore(t)
	require.NoError(t, eip8297.SetHashSuite(eip8297.HashBlake3))
	for _, vector := range loadPBTConformance(t).PBTState {
		t.Run(vector.Name, func(t *testing.T) {
			states := make([]eip8297.State, 0, len(vector.Accounts))
			feed := commitment.PBinFeed{Accounts: make([]commitment.PBinFeedAccount, 0, len(vector.Accounts))}
			for addressHex, account := range vector.Accounts {
				address := pbtConformanceHex(t, addressHex)
				code := pbtConformanceHex(t, account.Code)
				codeHash := common.BytesToHash(pbtConformanceHex(t, account.CodeHash))
				balance, err := uint256.FromHex(account.Balance)
				require.NoError(t, err)
				state := eip8297.State{Address: address, Nonce: account.Nonce, Balance: *balance, Code: code, Slots: make(map[string][]byte)}
				feedAccount := commitment.PBinFeedAccount{Address: address, Exists: true, Nonce: account.Nonce, Balance: *balance, CodeHash: codeHash, CodeWritten: true, Code: code}
				for slot, valueHex := range account.Storage {
					slotBytes := pbtConformanceSlot(t, slot)
					value := pbtConformanceHex(t, valueHex)
					state.Slots[string(slotBytes)] = value
					feedAccount.Slots = append(feedAccount.Slots, commitment.PBinFeedSlot{Key: slotBytes, Value: value})
				}
				states = append(states, state)
				feed.Accounts = append(feed.Accounts, feedAccount)
			}
			want := eip8297.StateRootWithHash(eip8297.EmbedState([][]eip8297.State{states}), eip8297.SelectedHash())
			recorded := common.HexToHash(vector.Root)
			ctx := newTrieTestContext()
			got, err := NewTrie(ctx).ProcessFeed(&feed)
			require.NoError(t, err)
			require.Equal(t, recorded, got)
			require.Equal(t, recorded, want)
			require.Equal(t, want, got)
			require.NoError(t, NewTrie(ctx).Verify())
		})
	}
}

func TestPBinEngineMatchesSpecTrieRoots(t *testing.T) {
	commitmentflags.Restore(t)
	require.NoError(t, eip8297.SetHashSuite(eip8297.HashBlake3))
	for _, vector := range loadPBTSpecVectors(t).TrieVectors {
		t.Run(vector.Name, func(t *testing.T) {
			ops := make([]Op, 0, len(vector.Entries))
			entries := make([]eip8297.Entry, 0, len(vector.Entries))
			for _, entry := range vector.Entries {
				key := pbtConformanceHex(t, entry.Key)
				value := pbtConformanceHex(t, entry.Value)
				ops = append(ops, Op{Key: key, Value: valueArray(value)})
				entries = append(entries, eip8297.Entry{Key: key, Value: value})
			}
			sort.Slice(ops, func(i, j int) bool { return bytes.Compare(ops[i].Key, ops[j].Key) < 0 })
			want := common.HexToHash(vector.Root)
			if vector.Name == "full_header_stem" {
				require.Equal(t, want, eip8297.StateRootWithHash(entries, eip8297.SelectedHash()))
				t.Log("reference-only: full_header_stem carries an invalid DELEGATION marker")
				return
			}
			got, err := NewTrie(newTrieTestContext()).Process(ops)
			require.NoError(t, err)
			require.Equal(t, want, got)
			require.Equal(t, want, eip8297.StateRootWithHash(entries, eip8297.SelectedHash()))
		})
	}
}

func TestPBinEngineMatchesSpecSequenceRoots(t *testing.T) {
	commitmentflags.Restore(t)
	require.NoError(t, eip8297.SetHashSuite(eip8297.HashBlake3))
	for _, vector := range loadPBTSpecVectors(t).SequenceVectors {
		t.Run(fmt.Sprintf("seed-%d", vector.Seed), func(t *testing.T) {
			require.Len(t, vector.Ops, len(vector.RootsAfter))
			trie := NewTrie(newTrieTestContext())
			for i, operation := range vector.Ops {
				key := pbtConformanceHex(t, operation.Key)
				op := Op{Key: key}
				if operation.Op != "delete" {
					op.Value = valueArray(pbtConformanceHex(t, operation.Value))
				}
				got, err := trie.Process([]Op{op})
				require.NoError(t, err)
				require.Equal(t, common.HexToHash(vector.RootsAfter[i]), got, "operation %d", i)
			}
		})
	}
}

func valueArray(value []byte) [eip8297.ValueLength]byte {
	var out [eip8297.ValueLength]byte
	copy(out[:], value)
	return out
}
