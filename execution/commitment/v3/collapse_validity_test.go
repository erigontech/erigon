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

package v3

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/trie"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/erigontech/erigon/internal/commitmenttest/runner"
)

func validityWitness(t *testing.T, b *witnessBed, batch []commitmenttest.Op, candidates []collapseTestEvent, drop int) [][]byte {
	t.Helper()
	tr := b.openV3(context.Background())
	_, _, err := tr.RestoreState(b.v3State)
	require.NoError(t, err)
	updates := testUpdates(t, commitment.ModeCollect, batch)
	for i, candidate := range candidates {
		if i != drop {
			updates.TouchHashedKey(candidate.sibling)
		}
	}
	nodes, _, err := tr.WitnessNodes(context.Background(), updates, true)
	require.NoError(t, err)
	return nodes
}

func oracleValidityError(nodes [][]byte, batch []commitmenttest.Op, expected common.Hash) (err error) {
	for _, deletesFirst := range []bool{false, true} {
		root, applyErr := applyValidityBatch(nodes, batch, deletesFirst)
		if applyErr != nil {
			return fmt.Errorf("deletesFirst=%t: %w", deletesFirst, applyErr)
		}
		if root != expected {
			return fmt.Errorf("deletesFirst=%t: root=%x, want=%x", deletesFirst, root, expected)
		}
	}
	return nil
}

func applyValidityBatch(nodes [][]byte, batch []commitmenttest.Op, deletesFirst bool) (root common.Hash, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("hash-node resolve: %v", recovered)
		}
	}()
	tr, err := trie.RLPDecode(nodes)
	if err != nil {
		return common.Hash{}, err
	}
	for phase := range 2 {
		deletePhase := deletesFirst == (phase == 0)
		for _, op := range batch {
			if op.Delete == deletePhase {
				if err := applyValidityOp(tr, op); err != nil {
					return common.Hash{}, err
				}
			}
		}
	}
	return tr.Hash(), nil
}

func applyValidityOp(tr *trie.Trie, op commitmenttest.Op) error {
	key := validityTrieKey(op.Key)
	if len(op.Key) == length.Addr {
		if op.Delete {
			if account, resolved := tr.GetAccount(key); !resolved || account == nil {
				return fmt.Errorf("account delete did not resolve %x: resolved=%t account=%v", key, resolved, account)
			}
			tr.DeleteSubtree(key)
			tr.Delete(key)
			if account, resolved := tr.GetAccount(key); resolved && account != nil {
				return fmt.Errorf("account delete left account %x", key)
			}
			return nil
		}
		update := runner.Update(op)
		if update == nil {
			return nil
		}
		current, resolved := tr.GetAccount(key)
		if !resolved {
			return fmt.Errorf("hash-node resolve for account %x", key)
		}
		account := accounts.NewAccount()
		if current != nil {
			account = *current
		}
		if update.DeleteStorageSubtree {
			tr.DeleteSubtree(key)
			account.Root = empty.RootHash
		}
		if update.Flags&commitment.NonceUpdate != 0 {
			account.Nonce = update.Nonce
		}
		if update.Flags&commitment.BalanceUpdate != 0 {
			account.Balance = update.Balance
		}
		if update.Flags&commitment.CodeUpdate != 0 {
			account.CodeHash = accounts.InternCodeHash(update.CodeHash)
		}
		tr.UpdateAccount(key, &account)
		return nil
	}
	if op.Delete {
		tr.Delete(key)
		return nil
	}
	update := runner.Update(op)
	if update == nil {
		return nil
	}
	tr.Update(key, bytes.Clone(update.Storage[:update.StorageLen]))
	return nil
}

func validityTrieKey(key []byte) []byte {
	if len(key) == length.Addr {
		return crypto.Keccak256(key)
	}
	return append(crypto.Keccak256(key[:length.Addr]), crypto.Keccak256(key[length.Addr:])...)
}

var validityCascadeScenarios = []struct {
	name  string
	shape witnessShape
	batch func(*witnessBed) []commitmenttest.Op
}{
	{
		name:  "account-cascade",
		shape: witnessShape{accounts: 3, accountPrefixes: [][]byte{{1, 2, 3}, {1, 2, 4}, {8}}},
		batch: func(b *witnessBed) []commitmenttest.Op {
			return []commitmenttest.Op{commitmenttest.DeleteAt(b.accounts[0]), commitmenttest.DeleteAt(b.accounts[1])}
		},
	},
	{
		name:  "storage-cascade",
		shape: witnessShape{accounts: 1, slots: 3, slotPrefixes: [][]byte{{1, 2, 3}, {1, 2, 4}, {8}}},
		batch: func(b *witnessBed) []commitmenttest.Op {
			return []commitmenttest.Op{commitmenttest.DeleteAt(b.slots[0]), commitmenttest.DeleteAt(b.slots[1])}
		},
	},
	{
		name:  "self-destruct",
		shape: witnessShape{accounts: 3, slots: 2, accountPrefixes: [][]byte{{1, 2}, {1, 3}, {8}}},
		batch: func(b *witnessBed) []commitmenttest.Op {
			return []commitmenttest.Op{commitmenttest.DeleteAt(b.accounts[0])}
		},
	},
}

func TestCollapseOracleHasTeeth(t *testing.T) {
	tc := validityCascadeScenarios[0]
	rng := rand.New(rand.NewSource(17))
	b := newWitnessBed(t, witnessState(rng, tc.shape))
	batch := tc.batch(b)
	candidates, postRoot := b.v3Collapses(t, batch, true)
	require.NotEmpty(t, candidates)
	nodes := validityWitness(t, b, batch, candidates, -1)
	require.NoError(t, oracleValidityError(nodes, batch, common.BytesToHash(postRoot)))

	for dropped := range candidates {
		nodes = validityWitness(t, b, batch, candidates, dropped)
		err := oracleValidityError(nodes, batch, common.BytesToHash(postRoot))
		if err != nil {
			t.Logf("dropped candidate %d sibling=%x prefix=%x: %v", dropped, candidates[dropped].sibling, candidates[dropped].prefix, err)
			return
		}
	}
	t.Fatalf("dropping every collapse candidate preserved the oracle root")
}

func TestCollapseValiditySearch(t *testing.T) {
	for _, tc := range validityCascadeScenarios {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(17))
			b := newWitnessBed(t, witnessState(rng, tc.shape))
			batch := tc.batch(b)
			candidates, postRoot := b.v3Collapses(t, batch, true)
			require.NotEmpty(t, candidates)
			nodes := validityWitness(t, b, batch, candidates, -1)
			require.NoError(t, oracleValidityError(nodes, batch, common.BytesToHash(postRoot)))
		})
	}

	seeds := 32
	if testing.Short() {
		seeds = 4
	}
	for _, plane := range []string{"account", "storage"} {
		t.Run("random-"+plane, func(t *testing.T) {
			for seed := range seeds {
				rng := rand.New(rand.NewSource(int64(seed)))
				shape := witnessShape{accounts: 8}
				if plane == "storage" {
					shape.slots = 5
				}
				b := newWitnessBed(t, witnessState(rng, shape))
				batch := validityRandomBatch(rng, b, plane == "storage")
				candidates, postRoot := b.v3Collapses(t, batch, true)
				nodes := validityWitness(t, b, batch, candidates, -1)
				require.NoError(t, oracleValidityError(nodes, batch, common.BytesToHash(postRoot)), "seed %d batch=%+v", seed, batch)
			}
		})
	}
}

func validityRandomBatch(rng *rand.Rand, b *witnessBed, storage bool) []commitmenttest.Op {
	if storage {
		batch := make([]commitmenttest.Op, 0, len(b.slots))
		for i, key := range b.slots {
			if i%3 != 2 || rng.Intn(2) == 0 {
				batch = append(batch, commitmenttest.DeleteAt(key))
			} else {
				batch = append(batch, commitmenttest.Op{Key: key, Storage: commitmenttest.RandomStorage(rng)})
			}
		}
		return batch
	}

	batch := make([]commitmenttest.Op, 0, len(b.accounts)+2)
	for i, key := range b.accounts {
		if i%3 != 2 || rng.Intn(2) == 0 {
			batch = append(batch, commitmenttest.DeleteAt(key))
		} else {
			batch = append(batch, commitmenttest.Op{Key: key, Account: randomWitnessAccount(rng)})
		}
	}
	for range 2 {
		batch = append(batch, commitmenttest.Op{Key: craftKey(rng, length.Addr, nil, accountNibbles), Account: randomWitnessAccount(rng)})
	}
	return batch
}
