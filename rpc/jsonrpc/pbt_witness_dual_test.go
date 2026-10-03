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

package jsonrpc

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/rpc"
)

type pbinWitnessWithoutCommitmentHistory struct {
	kv.TemporalTx
}

func (tx pbinWitnessWithoutCommitmentHistory) GetAsOf(domain kv.Domain, key []byte, ts uint64) ([]byte, bool, error) {
	if domain == kv.CommitmentDomain || domain == kv.CommitmentBinDomain {
		return nil, false, errors.New("commitment history read")
	}
	return tx.TemporalTx.GetAsOf(domain, key, ts)
}

func (tx pbinWitnessWithoutCommitmentHistory) BlockFilesRoTx() *blocksnapshots.View {
	if p, ok := tx.TemporalTx.(interface{ BlockFilesRoTx() *blocksnapshots.View }); ok {
		return p.BlockFilesRoTx()
	}
	return nil
}

func pbtDualAnchors(t *testing.T, m *execmoduletester.ExecModuleTester, number uint64, trie witnessTrie) (common.Hash, common.Hash) {
	t.Helper()
	var parentRoot, postRoot common.Hash
	require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		parent := rawdb.ReadHeaderByNumber(tx, number-1)
		post := rawdb.ReadHeaderByNumber(tx, number)
		require.NotNil(t, parent)
		require.NotNil(t, post)
		var err error
		parentRoot, err = witnessAnchorForBlock(tx, parent, number-1, trie, m.ChainConfig)
		if err != nil {
			return err
		}
		postRoot, err = witnessAnchorForBlock(tx, post, number, trie, m.ChainConfig)
		return err
	}))
	return parentRoot, postRoot
}

func pbtDualTrieName(trie witnessTrie) string {
	if trie == witnessTriePBT {
		return "pbt"
	}
	return "mpt"
}

func TestPBinDualExecutionWitness(t *testing.T) {
	for _, tc := range []struct {
		name       string
		activation uint64
		blocks     []uint64
		tries      []witnessTrie
	}{
		{name: "bin-only", activation: 0, blocks: []uint64{2, 4}, tries: []witnessTrie{witnessTriePBT}},
		{name: "dual-before", activation: 30, blocks: []uint64{2}, tries: []witnessTrie{witnessTrieMPT, witnessTriePBT}},
		{name: "dual-at-and-after", activation: 2, blocks: []uint64{2, 3, 4}, tries: []witnessTrie{witnessTrieMPT, witnessTriePBT}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api, m := pbinWitnessFixture(t, tc.activation)
			if tc.activation > 0 {
				repairPBinPreForkShadows(t, m, tc.activation)
			}
			for _, number := range tc.blocks {
				for _, trie := range tc.tries {
					t.Run(pbtDualTrieName(trie)+"/"+rpc.BlockNumber(number).String(), func(t *testing.T) {
						requested := pbtDualTrieName(trie)
						result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(number)), nil, &requested)
						require.NoError(t, err)
						parentRoot, postRoot := pbtDualAnchors(t, m, number, trie)
						block := pbtWitnessTestBlock(t, m, number)
						if trie == witnessTriePBT {
							require.NoError(t, verifyPBinWitnessAgainstBlock(t.Context(), result, block, parentRoot, postRoot, m.ChainConfig, m.Engine))
						} else {
							require.NoError(t, verifyWitnessAgainstBlock(t.Context(), result, block, m.ChainConfig, m.Engine, postRoot))
							require.NotEmpty(t, result.Codes, "MPT witness must carry accessed code")
							require.NotEqual(t, common.Hash{}, parentRoot)
							require.NotEqual(t, common.Hash{}, postRoot)
						}
					})
				}
			}
		})
	}
}

func TestPBinDualExecutionWitnessBinOnlyRejectsMPT(t *testing.T) {
	api, _ := pbinWitnessFixture(t, 0)
	mpt := "mpt"
	_, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(2), nil, &mpt)
	require.ErrorContains(t, err, "mpt commitment domain is missing from datadir")
}

func TestPBinDualExecutionWitnessPrunedHistory(t *testing.T) {
	api, m := pbinWitnessFixture(t, 30)
	tx, err := m.DB.BeginRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	pruneTo, err := m.BlockReader.TxnumReader().Min(t.Context(), tx, 3)
	require.NoError(t, err)
	cursor, err := tx.RwCursorDupSort(kv.TblCommitmentHistoryKeys)
	require.NoError(t, err)
	t.Cleanup(cursor.Close)
	for {
		key, _, cursorErr := cursor.First()
		require.NoError(t, cursorErr)
		if key == nil || binary.BigEndian.Uint64(key) >= pruneTo {
			break
		}
		require.NoError(t, cursor.DeleteCurrentDuplicates())
	}
	require.NoError(t, tx.Commit())
	trie := "mpt"
	result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(2), nil, &trie)
	require.ErrorContains(t, err, "mpt commitment history pruned")
	require.Nil(t, result)
}

func TestPBinHeadCaptureWithoutCommitmentHistory(t *testing.T) {
	var pin *rollingPin
	api, m := pbinWitnessFixtureWithGeneratorN(t, 1000, 5, func(m *execmoduletester.ExecModuleTester, pack *blockgen.ChainPack) error {
		if err := m.InsertChain(pack.Slice(0, 4)); err != nil {
			return err
		}
		var err error
		pin, err = openRollingPin(t.Context(), m.DB)
		if err != nil {
			return err
		}
		return m.InsertChain(pack.Slice(4, 5))
	}, nil)
	require.NotNil(t, pin)
	t.Cleanup(pin.close)
	repairPBinPreForkShadows(t, m, 1000)
	tx, err := m.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	blockNumber := rpc.BlockNumber(5)
	info, err := api.resolveWitnessBlock(t.Context(), tx, rpc.BlockNumberOrHashWithNumber(blockNumber))
	require.NoError(t, err)
	result, err := api.buildWitnessResultHeadCapture(t.Context(), pbinWitnessWithoutCommitmentHistory{TemporalTx: tx}, pin.tx, info, witnessModeLegacy, witnessTriePBT)
	require.NoError(t, err)
	require.NotEmpty(t, result.State)
}
