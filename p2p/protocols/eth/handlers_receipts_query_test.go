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

package eth

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
)

// Per the devp2p spec, a Receipts response carries one receipt list per requested
// block in request order, ending at the first block the server cannot serve.
// These tests pin that down for the ways a block can be unavailable mid-query;
// internal errors are propagated to the caller instead.

// A block whose receipts cannot be produced (and is not empty) ends the response;
// skipping it instead would misalign every later entry against the request.
func TestAnswerGetReceiptsQuery_EndsResponseWhenReceiptsUnavailable(t *testing.T) {
	f := newReceiptsQueryFixture(t, 3)
	delete(f.getter.receipts, f.blocks[1].Hash())
	resp, lastBlockIncomplete, err := answerQuery(t, f)
	require.NoError(t, err)
	require.False(t, lastBlockIncomplete)
	require.Equal(t, []rlp.RawValue{f.encoded(t, 0)}, resp)
}

// An error producing one block's receipts is returned to the caller.
func TestAnswerGetReceiptsQuery_ReturnsReceiptsError(t *testing.T) {
	f := newReceiptsQueryFixture(t, 3)
	f.getter.errs[f.blocks[1].Hash()] = errors.New("receipts unavailable")
	resp, lastBlockIncomplete, err := answerQuery(t, f)
	require.ErrorContains(t, err, "receipts unavailable")
	require.False(t, lastBlockIncomplete)
	require.Nil(t, resp)
}

// An error reading one block is returned to the caller.
func TestAnswerGetReceiptsQuery_ReturnsBlockReadError(t *testing.T) {
	f := newReceiptsQueryFixture(t, 3)
	f.br.blockErrs[f.blocks[1].Hash()] = errors.New("block read failed")
	resp, lastBlockIncomplete, err := answerQuery(t, f)
	require.ErrorContains(t, err, "block read failed")
	require.False(t, lastBlockIncomplete)
	require.Nil(t, resp)
}

// An error resolving a hash to a block number is returned to the caller.
func TestAnswerGetReceiptsQuery_ReturnsHeaderNumberError(t *testing.T) {
	f := newReceiptsQueryFixture(t, 3)
	f.br.numberErrs[f.blocks[1].Hash()] = errors.New("header number read failed")
	resp, lastBlockIncomplete, err := answerQuery(t, f)
	require.ErrorContains(t, err, "header number read failed")
	require.False(t, lastBlockIncomplete)
	require.Nil(t, resp)
}

// A hash whose number resolves but whose block cannot be read ends the response
// there; entries collected before it must not be discarded.
func TestAnswerGetReceiptsQuery_EndsResponseWhenBlockUnreadable(t *testing.T) {
	f := newReceiptsQueryFixture(t, 3)
	delete(f.br.blocks, f.blocks[1].Hash())
	resp, lastBlockIncomplete, err := answerQuery(t, f)
	require.NoError(t, err)
	require.False(t, lastBlockIncomplete)
	require.Equal(t, []rlp.RawValue{f.encoded(t, 0)}, resp)
}

func answerQuery(t *testing.T, f *receiptsQueryFixture) ([]rlp.RawValue, bool, error) {
	t.Helper()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginTemporalRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	opts := ReceiptQueryOpts{EthVersion: direct.ETH68, SizeLimit: NoSizeLimit}
	return AnswerGetReceiptsQuery(context.Background(), chain.TestChainBerlinConfig, f.getter, f.br, tx, f.query, nil, opts)
}

type receiptsQueryFixture struct {
	blocks   []*types.Block
	receipts []types.Receipts
	br       *queryTestBlockReader
	getter   *queryTestReceiptsGetter
	query    GetReceiptsPacket
}

// newReceiptsQueryFixture builds n chained non-empty blocks, each with one receipt,
// all known to the block reader and the receipts getter.
func newReceiptsQueryFixture(t *testing.T, n int) *receiptsQueryFixture {
	t.Helper()
	f := &receiptsQueryFixture{
		br:     &queryTestBlockReader{numbers: map[common.Hash]uint64{}, numberErrs: map[common.Hash]error{}, blocks: map[common.Hash]*types.Block{}, blockErrs: map[common.Hash]error{}},
		getter: &queryTestReceiptsGetter{receipts: map[common.Hash]types.Receipts{}, errs: map[common.Hash]error{}},
	}
	parent := common.Hash{}
	for i := range n {
		header := &types.Header{
			ParentHash:  parent,
			ReceiptHash: common.Hash{0xde, 0xad},
		}
		header.Number.SetUint64(uint64(i + 1))
		block := types.NewBlockWithHeader(header)
		hash := block.Hash()
		parent = hash
		receipts := types.Receipts{&types.Receipt{CumulativeGasUsed: uint64(21_000 * (i + 1))}}
		f.blocks = append(f.blocks, block)
		f.receipts = append(f.receipts, receipts)
		f.br.numbers[hash] = uint64(i + 1)
		f.br.blocks[hash] = block
		f.getter.receipts[hash] = receipts
		f.query = append(f.query, hash)
	}
	return f
}

func (f *receiptsQueryFixture) encoded(t *testing.T, blockIdx int) rlp.RawValue {
	t.Helper()
	encoded, err := rlp.EncodeToBytes(f.receipts[blockIdx])
	require.NoError(t, err)
	return encoded
}

type queryTestBlockReader struct {
	dbservices.HeaderAndBodyReader
	numbers    map[common.Hash]uint64
	numberErrs map[common.Hash]error
	blocks     map[common.Hash]*types.Block
	blockErrs  map[common.Hash]error
}

func (m *queryTestBlockReader) HeaderNumber(_ context.Context, _ kv.Getter, hash common.Hash) (*uint64, error) {
	if err := m.numberErrs[hash]; err != nil {
		return nil, err
	}
	n, ok := m.numbers[hash]
	if !ok {
		return nil, nil
	}
	return &n, nil
}

func (m *queryTestBlockReader) BlockWithSenders(_ context.Context, _ kv.Getter, hash common.Hash, _ uint64) (*types.Block, []common.Address, error) {
	if err := m.blockErrs[hash]; err != nil {
		return nil, nil, err
	}
	return m.blocks[hash], nil, nil
}

type queryTestReceiptsGetter struct {
	receipts map[common.Hash]types.Receipts
	errs     map[common.Hash]error
}

func (m *queryTestReceiptsGetter) GetReceipts(_ context.Context, _ *chain.Config, _ kv.TemporalTx, block *types.Block, _ ReceiptsOpts) (types.Receipts, error) {
	if err := m.errs[block.Hash()]; err != nil {
		return nil, err
	}
	return m.receipts[block.Hash()], nil
}

func (m *queryTestReceiptsGetter) GetCachedReceipts(_ context.Context, _ common.Hash) (types.Receipts, bool) {
	return nil, false
}
