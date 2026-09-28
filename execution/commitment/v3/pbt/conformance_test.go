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
	"encoding/hex"
	"encoding/json"
	"math/big"
	"os"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
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
	number, ok := new(big.Int).SetString(value, 10)
	require.True(t, ok)
	var slot [32]byte
	number.FillBytes(slot[:])
	return slot[:]
}

func TestPBinConformancePBTState(t *testing.T) {
	previous := eip8297.HashSuiteName()
	t.Cleanup(func() { require.NoError(t, eip8297.SetHashSuite(previous)) })
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
			require.Equal(t, common.HexToHash(vector.Root), want)
			ctx := newTrieTestContext()
			got, err := NewTrie(ctx).ProcessFeed(&feed)
			require.NoError(t, err)
			require.Equal(t, want, got)
			require.NoError(t, NewTrie(ctx).Verify())
		})
	}
}
