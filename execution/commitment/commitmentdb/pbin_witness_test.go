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

package commitmentdb

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/trie"
)

// pbinWitnessState is the in-memory PatriciaContext both engines are driven
// over: branch records they write, plus the accounts and slots they read back.
type pbinWitnessState struct {
	branches map[string][]byte
	accounts map[string]*commitment.Update
	storage  map[string]*commitment.Update
}

func newPBinWitnessState() *pbinWitnessState {
	return &pbinWitnessState{
		branches: make(map[string][]byte),
		accounts: make(map[string]*commitment.Update),
		storage:  make(map[string]*commitment.Update),
	}
}

func (s *pbinWitnessState) Branch(prefix []byte) ([]byte, kv.Step, error) {
	return s.branches[string(prefix)], 0, nil
}

func (s *pbinWitnessState) PutBranch(prefix, data, prevData []byte) error {
	s.branches[string(prefix)] = bytes.Clone(data)
	return nil
}

func (s *pbinWitnessState) Account(plainKey []byte) (*commitment.Update, error) {
	if u, ok := s.accounts[string(plainKey)]; ok {
		return u, nil
	}
	return new(commitment.Update), nil
}

func (s *pbinWitnessState) Storage(plainKey []byte) (*commitment.Update, error) {
	if u, ok := s.storage[string(plainKey)]; ok {
		return u, nil
	}
	return new(commitment.Update), nil
}

// Code satisfies the seam the binary update stream type-asserts for; the corpus
// carries no code, so it is never asked for any.
func (s *pbinWitnessState) Code(plainKey []byte) ([]byte, error) { return nil, nil }

func (s *pbinWitnessState) addAccount(addr []byte, nonce, balance uint64) []byte {
	u := &commitment.Update{Flags: commitment.BalanceUpdate | commitment.NonceUpdate, Nonce: nonce}
	u.Balance.SetUint64(balance)
	u.CodeHash = empty.CodeHash
	s.accounts[string(addr)] = u
	return addr
}

func (s *pbinWitnessState) addStorage(addr, slot []byte, val byte) []byte {
	key := append(bytes.Clone(addr), slot...)
	u := &commitment.Update{Flags: commitment.StorageUpdate, StorageLen: 1}
	u.Storage[length.Hash-1] = val
	s.storage[string(key)] = u
	return key
}

type pbinWitnessTouch struct {
	domain kv.Domain
	key    []byte
}

// pbinWitnessDBCorpus spans two accounts and their slots, so a witness over it
// captures branch nodes rather than a bare root.
func pbinWitnessDBCorpus(state *pbinWitnessState) []pbinWitnessTouch {
	var touches []pbinWitnessTouch
	for i := byte(1); i <= 4; i++ {
		addr := bytes.Repeat([]byte{i}, length.Addr)
		touches = append(touches, pbinWitnessTouch{kv.AccountsDomain, state.addAccount(addr, uint64(i), uint64(i)*1000)})
		for _, slot := range []byte{0, 7, 64} {
			key := bytes.Repeat([]byte{slot}, length.Hash)
			touches = append(touches, pbinWitnessTouch{kv.StorageDomain, state.addStorage(addr, key, i)})
		}
	}
	return touches
}

// pbinWitnessTrieCtx wires a fresh engine of the given variant over state. The
// witness pass runs on its own engine so it starts from the stored records
// rather than from a folded one left behind by the build.
func pbinWitnessTrieCtx(t *testing.T, variant commitment.TrieVariant, state *pbinWitnessState) *SharedDomainsCommitmentContext {
	t.Helper()
	sdc := pbinStateTestCtx(t, variant)
	sdc.patriciaTrie.ResetContext(state)
	return sdc
}

func pbinWitnessTouchAll(sdc *SharedDomainsCommitmentContext, touches []pbinWitnessTouch) {
	for _, touch := range touches {
		sdc.TouchKey(touch.domain, string(touch.key), nil)
	}
}

func pbinWitnessCommit(t *testing.T, variant commitment.TrieVariant, state *pbinWitnessState, touches []pbinWitnessTouch) []byte {
	t.Helper()
	sdc := pbinWitnessTrieCtx(t, variant, state)
	pbinWitnessTouchAll(sdc, touches)
	root, err := sdc.patriciaTrie.Process(t.Context(), sdc.updates, "test", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	return bytes.Clone(root)
}

// pbinWitnessCapture builds the corpus under variant and then captures a witness
// over the same touches, returning the committed root alongside the capture.
func pbinWitnessCapture(t *testing.T, variant commitment.TrieVariant) (nodes, provedKeys [][]byte, root, committedRoot []byte, err error) {
	t.Helper()
	state := newPBinWitnessState()
	touches := pbinWitnessDBCorpus(state)
	committedRoot = pbinWitnessCommit(t, variant, state, touches)

	sdc := pbinWitnessTrieCtx(t, variant, state)
	pbinWitnessTouchAll(sdc, touches)
	nodes, provedKeys, root, err = sdc.witnessCapture(t.Context(), false, "test")
	return nodes, provedKeys, root, committedRoot, err
}

// TestPBinWitnessCaptureServesBothVariants: the capture used to type-assert the
// hex engine, so the bin variant failed before it ever walked a tree.
func TestPBinWitnessCaptureServesBothVariants(t *testing.T) {
	t.Parallel()

	for _, variant := range []commitment.TrieVariant{commitment.VariantHexPatriciaTrie, commitment.VariantBinPatriciaTrie} {
		t.Run(string(variant), func(t *testing.T) {
			t.Parallel()

			nodes, provedKeys, root, committedRoot, err := pbinWitnessCapture(t, variant)
			require.NoError(t, err)
			require.Equal(t, committedRoot, root, "the capture must return the pre-state root")
			require.Len(t, root, length.Hash)
			require.Greater(t, len(nodes), 1, "a corpus this wide must capture more than the root node")
			require.NotEmpty(t, provedKeys)
		})
	}
}

// TestPBinWitnessCaptureHexUnchanged pins the hex capture against the engine
// called directly, so the interface dispatch cannot alter what hex returns.
func TestPBinWitnessCaptureHexUnchanged(t *testing.T) {
	t.Parallel()

	state := newPBinWitnessState()
	touches := pbinWitnessDBCorpus(state)
	committedRoot := pbinWitnessCommit(t, commitment.VariantHexPatriciaTrie, state, touches)

	viaCapture := pbinWitnessTrieCtx(t, commitment.VariantHexPatriciaTrie, state)
	pbinWitnessTouchAll(viaCapture, touches)
	nodes, provedKeys, root, err := viaCapture.witnessCapture(t.Context(), true, "test")
	require.NoError(t, err)

	direct := pbinWitnessTrieCtx(t, commitment.VariantHexPatriciaTrie, state)
	pbinWitnessTouchAll(direct, touches)
	hph, ok := direct.Trie().(*commitment.HexPatriciaHashed)
	require.True(t, ok)
	wantNodes, wantKeys, wantRoot, err := hph.Witnesses(t.Context(), direct.updates, true, "test")
	require.NoError(t, err)

	require.Equal(t, committedRoot, wantRoot)
	require.Equal(t, wantRoot, root)
	require.Equal(t, wantKeys, provedKeys)
	require.Equal(t, wantNodes[0], nodes[0], "root node must stay first")
	require.ElementsMatch(t, wantNodes, nodes)
}

// TestPBinWitnessNodesPrunesPerVariant: the lean set is cut by the walker that
// can read the capture — the MPT one cannot follow a binary preimage.
func TestPBinWitnessNodesPrunesPerVariant(t *testing.T) {
	t.Parallel()

	for _, variant := range []commitment.TrieVariant{commitment.VariantHexPatriciaTrie, commitment.VariantBinPatriciaTrie} {
		t.Run(string(variant), func(t *testing.T) {
			t.Parallel()

			state := newPBinWitnessState()
			touches := pbinWitnessDBCorpus(state)
			committedRoot := pbinWitnessCommit(t, variant, state, touches)

			capture := pbinWitnessTrieCtx(t, variant, state)
			pbinWitnessTouchAll(capture, touches)
			full, provedKeys, root, err := capture.witnessCapture(t.Context(), false, "test")
			require.NoError(t, err)

			sdc := pbinWitnessTrieCtx(t, variant, state)
			pbinWitnessTouchAll(sdc, touches)
			lean, rootHash, err := sdc.WitnessNodes(t.Context(), false, "test")
			require.NoError(t, err)
			require.Equal(t, committedRoot, rootHash)
			require.NotEmpty(t, lean)

			want := trie.WitnessNodesForKeysFromNodes
			if variant == commitment.VariantBinPatriciaTrie {
				want = func(nodes, keys [][]byte) ([][]byte, error) {
					return commitment.PBinWitnessNodesForKeys(nodes, root, keys)
				}
			}
			wantNodes, err := want(full, provedKeys)
			require.NoError(t, err)
			require.Equal(t, wantNodes[0], lean[0], "root node must stay first")
			require.ElementsMatch(t, wantNodes, lean)
		})
	}
}

// pbinWitnessCaptureLessTrie is a Trie that captures no witness, standing in for
// the parallel variants the capture cannot serve.
type pbinWitnessCaptureLessTrie struct{ commitment.Trie }

func (pbinWitnessCaptureLessTrie) Release() {}

// TestPBinWitnessCaptureRejectsUnknownTrie: falling through to a nil capturer
// would panic instead of naming the trie that cannot serve the request.
func TestPBinWitnessCaptureRejectsUnknownTrie(t *testing.T) {
	t.Parallel()

	sdc := pbinStateTestCtx(t, commitment.VariantHexPatriciaTrie)
	sdc.patriciaTrie = pbinWitnessCaptureLessTrie{}

	_, _, _, err := sdc.witnessCapture(context.Background(), false, "test")
	require.Error(t, err)
	require.Contains(t, err.Error(), "pbinWitnessCaptureLessTrie")
	require.Contains(t, err.Error(), "captures no witness")
}
