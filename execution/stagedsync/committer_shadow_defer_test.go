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

package stagedsync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type shadowDeferredParts struct {
	parts [][]commitment.BranchDelta
}

func (s *shadowDeferredParts) SetDeferCommitmentUpdates(bool) {}

func (s *shadowDeferredParts) TakeDeferredDeltas() [][]commitment.BranchDelta {
	parts := s.parts
	s.parts = nil
	return parts
}

type shadowRecordingContext struct {
	commitment.PatriciaContext
	keys []string
}

func (c *shadowRecordingContext) PutBranch(prefix, _, _ []byte) error {
	c.keys = append(c.keys, string(prefix))
	return nil
}

func TestDualCompletionAppliesShadowPartsOnlyWhenAccepted(t *testing.T) {
	canonicalRoot := dualFoldResult{domain: kv.CommitmentDomain, root: []byte{1}}
	shadowRoot := dualFoldResult{domain: kv.CommitmentBinDomain, root: []byte{2}}
	for _, tc := range []struct {
		name       string
		folds      []dualFoldResult
		wantErr    bool
		wantKeys   []string
		wantStop   bool
		wantStored bool
	}{
		{name: "accepted", folds: []dualFoldResult{canonicalRoot, shadowRoot}, wantKeys: []string{"a", "b", "c", "state"}, wantStored: true},
		{name: "canonical error", folds: []dualFoldResult{{domain: kv.CommitmentDomain, err: errors.New("fold failed")}, shadowRoot}, wantErr: true},
		{name: "cancel", folds: []dualFoldResult{canonicalRoot, {domain: kv.CommitmentBinDomain, err: fmt.Errorf("fold: %w", context.Canceled)}}, wantErr: true},
		{name: "no canonical root", folds: []dualFoldResult{{domain: kv.CommitmentDomain}, shadowRoot}, wantErr: true},
		{name: "shadow fold error", folds: []dualFoldResult{canonicalRoot, {domain: kv.CommitmentBinDomain, err: errors.New("fold failed")}}, wantStop: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inner := &shadowRecordingContext{}
			buffered := commitmentdb.NewBufferedPatriciaContext(inner)
			require.NoError(t, buffered.PutBranch([]byte("state"), []byte{9}, nil))
			deferred := &shadowDeferredParts{parts: [][]commitment.BranchDelta{
				{{Key: []byte("a"), Data: []byte{1}}, {Key: []byte("b"), Data: []byte{2}}},
				{{Key: []byte("c"), Data: []byte{3}}},
			}}
			cc := &commitmentCalculator{}
			result, err := cc.finishDualFolds(kv.CommitmentDomain, kv.CommitmentBinDomain, tc.folds, &commitmentFoldArm{buffered: buffered, deferred: deferred})
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Nil(t, deferred.parts, "the shadow trie kept its parts")
			require.Equal(t, tc.wantKeys, inner.keys)
			if !tc.wantErr {
				require.Equal(t, tc.wantStored, result.shadowRoot != nil)
			}
			require.Equal(t, tc.wantStop, cc.ShadowDomainStopped(kv.CommitmentBinDomain))
		})
	}
}

type shadowDeferRun struct {
	roots   [][]byte
	records map[string][]byte
}

func runShadowDeferBlocks(t *testing.T, buffered bool, perBlockFrom uint64) shadowDeferRun {
	t.Helper()
	db, tx, doms := dualCalculatorTest(t)
	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	t.Cleanup(roTx.Rollback)
	cc := &commitmentCalculator{doms: doms, db: db, roTx: roTx, logger: log.New(), perBlockFrom: perBlockFrom, bufferShadowParts: buffered}
	var run shadowDeferRun
	for block, span := range [][2]byte{{1, 40}, {20, 60}} {
		blockNum := uint64(block + 1)
		updates := commitment.NewUpdates(commitment.ModeUpdate, t.TempDir(), commitment.KeyToHexNibbleHash)
		feed := &commitment.Feed{}
		for b := span[0]; b <= span[1]; b++ {
			key := bytes.Repeat([]byte{b}, 20)
			account := accounts.Account{Nonce: blockNum*100 + uint64(b)}
			accountBytes := accounts.SerialiseV3(&account)
			require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, key, accountBytes, blockNum, nil))
			updates.TouchPlainKey(string(key), accountBytes, updates.TouchAccount)
			var hash [32]byte
			copy(hash[:], crypto.Keccak256(key))
			feed.Accounts = append(feed.Accounts, commitment.FeedAccount{Hash: hash, Update: &commitment.Update{Flags: commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate, Nonce: account.Nonce, CodeHash: empty.CodeHash}})
		}
		sort.Slice(feed.Accounts, func(i, j int) bool { return bytes.Compare(feed.Accounts[i].Hash[:], feed.Accounts[j].Hash[:]) < 0 })
		feed.Keys = len(feed.Accounts)
		target := commitTarget{blockNum: blockNum, blockHash: common.Hash{byte(blockNum)}, lastTxNum: blockNum}
		dual, err := cc.computeDualFromUpdatesWithRole(t.Context(), target, updates, &asOfStateReader{sd: doms, roTx: roTx, commitmentDomain: kv.CommitmentDomain}, doms.GetCommitmentCtxForDomain(kv.CommitmentDomain), doms.GetCommitmentCtxForDomain(kv.CommitmentBinDomain), feed, nil)
		updates.Close()
		require.NoError(t, err)
		require.NotEmpty(t, dual.shadowRoot, "block %d has no shadow root", blockNum)
		run.roots = append(run.roots, dual.shadowRoot)
	}
	require.NoError(t, doms.Flush(t.Context(), tx))
	run.records = make(map[string][]byte)
	it, err := tx.Debug().RangeLatest(kv.CommitmentBinDomain, nil, nil, kv.Unlim)
	require.NoError(t, err)
	defer it.Close()
	for it.HasNext() {
		key, value, err := it.Next()
		require.NoError(t, err)
		run.records[string(key)] = bytes.Clone(value)
	}
	require.Greater(t, len(run.records), 1)
	return run
}

func TestDualShadowDeferredPartsMatchBufferedWrites(t *testing.T) {
	for _, tc := range []struct {
		name         string
		perBlockFrom uint64
	}{
		{name: "owned changeset", perBlockFrom: 0},
		{name: "isolated", perBlockFrom: math.MaxUint64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := runShadowDeferBlocks(t, true, tc.perBlockFrom)
			got := runShadowDeferBlocks(t, false, tc.perBlockFrom)
			require.Equal(t, want.roots, got.roots, "shadow roots")
			stateKey := string(commitment.KeyCommitmentState)
			for key, value := range want.records {
				if key != stateKey {
					require.Equal(t, value, got.records[key], "bin record %x", key)
				}
			}
			require.Len(t, got.records, len(want.records))
			rootRecord := got.records[string(pbt.GlobalRootKey())]
			require.NotEmpty(t, rootRecord)
			require.True(t, bytes.HasSuffix(got.records[stateKey], rootRecord), "the bin state does not embed the stored root record")
		})
	}
}
