// Copyright 2018 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package mock_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/sha3"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/stages/mock"
	"github.com/erigontech/erigon/execution/types"
)

// Tests block header storage and retrieval operations.
func TestHeaderStorage(t *testing.T) {
	t.Parallel()
	m := mock.Mock(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	ctx := m.Ctx
	br := m.BlockReader

	// Create a test header to move around the database and make sure it's really new
	header := &types.Header{Number: big.NewInt(42), Extra: []byte("test header")}
	entry, err := br.Header(ctx, tx, header.Hash(), header.Number.Uint64())
	require.NoError(t, err)
	if entry != nil {
		t.Fatalf("Non existent header returned: %v", entry)
	}
	// Write and verify the header in the database
	rawdb.WriteHeader(tx, header)
	if entry, _ := br.Header(ctx, tx, header.Hash(), header.Number.Uint64()); entry == nil {
		t.Fatalf("Stored header not found")
	} else if entry.Hash() != header.Hash() {
		t.Fatalf("Retrieved header mismatch: have %v, want %v", entry, header)
	}
	if entry := rawdb.ReadHeaderRLP(tx, header.Hash(), header.Number.Uint64()); entry == nil {
		t.Fatalf("Stored header RLP not found")
	} else {
		hasher := sha3.NewLegacyKeccak256()
		hasher.Write(entry)

		if hash := common.BytesToHash(hasher.Sum(nil)); hash != header.Hash() {
			t.Fatalf("Retrieved RLP header mismatch: have %v, want %v", entry, header)
		}
	}
	// Delete the header and verify the execution
	rawdb.DeleteHeader(tx, header.Hash(), header.Number.Uint64())
	if entry, _ := br.Header(ctx, tx, header.Hash(), header.Number.Uint64()); entry != nil {
		t.Fatalf("Deleted header returned: %v", entry)
	}
}

// Tests block body storage and retrieval operations.
func TestBodyStorage(t *testing.T) {
	t.Parallel()
	m := mock.Mock(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	ctx := m.Ctx
	br := m.BlockReader
	require := require.New(t)

	var testKey, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	testAddr := crypto.PubkeyToAddress(testKey.PublicKey)

	mustSign := func(tx types.Transaction, s types.Signer) types.Transaction {
		r, err := types.SignTx(tx, s, testKey)
		require.NoError(err)
		return r
	}

	// prepare db so it works with our test
	signer1 := types.MakeSigner(chainspec.Mainnet.Config, 1, 0)
	body := &types.Body{
		Transactions: []types.Transaction{
			mustSign(types.NewTransaction(1, testAddr, u256.Num1, 1, u256.Num1, nil), *signer1),
			mustSign(types.NewTransaction(2, testAddr, u256.Num1, 2, u256.Num1, nil), *signer1),
		},
		Uncles: []*types.Header{{Extra: []byte("test header")}},
	}

	// Create a test body to move around the database and make sure it's really new
	hasher := sha3.NewLegacyKeccak256()
	_ = rlp.Encode(hasher, body)
	hash := common.BytesToHash(hasher.Sum(nil))
	header := &types.Header{Number: common.Big1}

	if entry, _ := br.BodyWithTransactions(ctx, tx, header.Hash(), 1); entry != nil {
		t.Fatalf("Non existent body returned: %v", entry)
	}
	require.NoError(rawdb.WriteCanonicalHash(tx, header.Hash(), 1))
	require.NoError(rawdb.WriteHeader(tx, header))
	require.NoError(rawdb.WriteBody(tx, header.Hash(), 1, body))
	if entry, _ := br.BodyWithTransactions(ctx, tx, header.Hash(), 1); entry == nil {
		t.Fatalf("Stored body not found")
	} else if types.DeriveSha(types.Transactions(entry.Transactions)) != types.DeriveSha(types.Transactions(body.Transactions)) || types.CalcUncleHash(entry.Uncles) != types.CalcUncleHash(body.Uncles) {
		t.Fatalf("Retrieved body mismatch: have %v, want %v", entry, body)
	}
	if entry := rawdb.ReadBodyRLP(tx, header.Hash(), 1); entry == nil {
		//if entry, _ := br.BodyWithTransactions(ctx, tx, hash, 0); entry == nil {
		t.Fatalf("Stored body RLP not found")
	} else {
		bodyRlp, err := rlp.EncodeToBytes(entry)
		if err != nil {
			log.Error("ReadBodyRLP failed", "err", err)
		}
		hasher := sha3.NewLegacyKeccak256()
		hasher.Write(bodyRlp)

		if calc := common.BytesToHash(hasher.Sum(nil)); calc != hash {
			t.Fatalf("Retrieved RLP body mismatch: have %v, want %v", entry, body)
		}
	}
	// Delete the body and verify the execution
	rawdb.DeleteBody(tx, hash, 1)
	if entry, _ := br.BodyWithTransactions(ctx, tx, hash, 1); entry != nil {
		t.Fatalf("Deleted body returned: %v", entry)
	}
}

// Tests block storage and retrieval operations.
func TestBlockStorage(t *testing.T) {
	t.Parallel()
	m := mock.Mock(t)
	require := require.New(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(err)
	defer tx.Rollback()
	ctx := m.Ctx
	br, bw := m.BlocksIO()

	// Create a test block to move around the database and make sure it's really new
	block := types.NewBlockWithHeader(&types.Header{
		Number:      big.NewInt(1),
		Extra:       []byte("test block"),
		UncleHash:   empty.UncleHash,
		TxHash:      empty.RootHash,
		ReceiptHash: empty.RootHash,
	})
	if entry, _, _ := br.BlockWithSenders(ctx, tx, block.Hash(), block.NumberU64()); entry != nil {
		t.Fatalf("Non existent block returned: %v", entry)
	}
	if entry, _ := br.Header(ctx, tx, block.Hash(), block.NumberU64()); entry != nil {
		t.Fatalf("Non existent header returned: %v", entry)
	}
	if entry, _ := br.BodyWithTransactions(ctx, tx, block.Hash(), block.NumberU64()); entry != nil {
		t.Fatalf("Non existent body returned: %v", entry)
	}

	// Write and verify the block in the database
	err = rawdb.WriteBlock(tx, block)
	if err != nil {
		t.Fatalf("Could not write block: %v", err)
	}
	if entry, _, _ := br.BlockWithSenders(ctx, tx, block.Hash(), block.NumberU64()); entry == nil {
		t.Fatalf("Stored block not found")
	} else if entry.Hash() != block.Hash() {
		t.Fatalf("Retrieved block mismatch: have %v, want %v", entry, block)
	}
	if entry, _ := br.Header(ctx, tx, block.Hash(), block.NumberU64()); entry == nil {
		t.Fatalf("Stored header not found")
	} else if entry.Hash() != block.Hash() {
		t.Fatalf("Retrieved header mismatch: have %v, want %v", entry, block.Header())
	}
	if err := rawdb.TruncateBlocks(context.Background(), tx, 2); err != nil {
		t.Fatal(err)
	}
	if entry, _ := br.BodyWithTransactions(ctx, tx, block.Hash(), block.NumberU64()); entry == nil {
		t.Fatalf("Stored body not found")
	} else if types.DeriveSha(types.Transactions(entry.Transactions)) != types.DeriveSha(block.Transactions()) || types.CalcUncleHash(entry.Uncles) != types.CalcUncleHash(block.Uncles()) {
		t.Fatalf("Retrieved body mismatch: have %v, want %v", entry, block.Body())
	}
	// Delete the block and verify the execution
	if err := rawdb.TruncateBlocks(context.Background(), tx, block.NumberU64()); err != nil {
		t.Fatal(err)
	}
	//if err := DeleteBlock(tx, block.Hash(), block.NumberU64()); err != nil {
	//	t.Fatalf("Could not delete block: %v", err)
	//}
	if entry, _, _ := br.BlockWithSenders(ctx, tx, block.Hash(), block.NumberU64()); entry != nil {
		t.Fatalf("Deleted block returned: %v", entry)
	}
	if entry, _ := br.Header(ctx, tx, block.Hash(), block.NumberU64()); entry != nil {
		t.Fatalf("Deleted header returned: %v", entry)
	}
	if entry, _ := br.BodyWithTransactions(ctx, tx, block.Hash(), block.NumberU64()); entry != nil {
		t.Fatalf("Deleted body returned: %v", entry)
	}

	// write again and delete it as old one
	require.NoError(rawdb.WriteBlock(tx, block))

	{
		// mark chain as bad
		//  - it must be not available by hash
		//  - but available by hash+num - if read num from kv.BadHeaderNumber table
		//  - prune blocks: must delete Canonical/NonCanonical/BadBlocks also
		foundBn, _ := br.BadHeaderNumber(ctx, tx, block.Hash())
		require.Nil(foundBn)
		found, _ := br.BlockByHash(ctx, tx, block.Hash())
		require.NotNil(found)

		err = rawdb.WriteCanonicalHash(tx, block.Hash(), block.NumberU64())
		require.NoError(err)
		err = rawdb.TruncateCanonicalHash(tx, block.NumberU64(), true)
		require.NoError(err)
		foundBlock, _ := br.BlockByHash(ctx, tx, block.Hash())
		require.Nil(foundBlock)

		foundBn = rawdb.ReadHeaderNumber(tx, block.Hash())
		require.Nil(foundBn)
		foundBn, _ = br.BadHeaderNumber(ctx, tx, block.Hash())
		require.NotNil(foundBn)
		foundBlock, _ = br.BlockByNumber(ctx, tx, *foundBn)
		require.Nil(foundBlock)
		foundBlock, _, _ = br.BlockWithSenders(ctx, tx, block.Hash(), *foundBn)
		require.NotNil(foundBlock)
	}

	// prune: [1: N)
	var deleted int
	deleted, err = bw.PruneBlocks(ctx, tx, 0, 1)
	require.NoError(err)
	require.Equal(0, deleted)
	entry, _ := br.BodyWithTransactions(ctx, tx, block.Hash(), block.NumberU64())
	require.NotNil(entry)
	deleted, err = bw.PruneBlocks(ctx, tx, 1, 1)
	require.NoError(err)
	require.Equal(0, deleted)
	entry, _ = br.BodyWithTransactions(ctx, tx, block.Hash(), block.NumberU64())
	require.NotNil(entry)
	deleted, err = bw.PruneBlocks(ctx, tx, 2, 1)
	require.NoError(err)
	require.Equal(1, deleted)
	entry, _ = br.BodyWithTransactions(ctx, tx, block.Hash(), block.NumberU64())
	require.Nil(entry)
}

// Tests that partial block contents don't get reassembled into full blocks.
func TestPartialBlockStorage(t *testing.T) {
	t.Parallel()
	m := mock.Mock(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	ctx := m.Ctx
	br := m.BlockReader

	block := types.NewBlockWithHeader(&types.Header{
		Extra:       []byte("test block"),
		UncleHash:   empty.UncleHash,
		TxHash:      empty.RootHash,
		ReceiptHash: empty.RootHash,
	})
	header := block.Header() // Not identical to struct literal above, due to other fields

	// Store a header and check that it's not recognized as a block
	rawdb.WriteHeader(tx, header)
	if entry, _, _ := br.BlockWithSenders(ctx, tx, block.Hash(), block.NumberU64()); entry != nil {
		t.Fatalf("Non existent block returned: %v", entry)
	}
	rawdb.DeleteHeader(tx, block.Hash(), block.NumberU64())

	// Store a body and check that it's not recognized as a block
	if err := rawdb.WriteBody(tx, block.Hash(), block.NumberU64(), block.Body()); err != nil {
		t.Fatal(err)
	}
	if entry, _, _ := br.BlockWithSenders(ctx, tx, block.Hash(), block.NumberU64()); entry != nil {
		t.Fatalf("Non existent block returned: %v", entry)
	}
	rawdb.DeleteBody(tx, block.Hash(), block.NumberU64())

	// Store a header and a body separately and check reassembly
	rawdb.WriteHeader(tx, header)
	if err := rawdb.WriteBody(tx, block.Hash(), block.NumberU64(), block.Body()); err != nil {
		t.Fatal(err)
	}

	if entry, _, _ := br.BlockWithSenders(ctx, tx, block.Hash(), block.NumberU64()); entry == nil {
		t.Fatalf("Stored block not found")
	} else if entry.Hash() != block.Hash() {
		t.Fatalf("Retrieved block mismatch: have %v, want %v", entry, block)
	}
}

// Tests block total difficulty storage and retrieval operations.
func TestTdStorage(t *testing.T) {
	t.Parallel()
	m := mock.Mock(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	// Create a test TD to move around the database and make sure it's really new
	hash, td := common.Hash{}, big.NewInt(314)
	entry, err := rawdb.ReadTd(tx, hash, 0)
	if err != nil {
		t.Fatalf("ReadTd failed: %v", err)
	}
	if entry != nil {
		t.Fatalf("Non existent TD returned: %v", entry)
	}
	// Write and verify the TD in the database
	err = rawdb.WriteTd(tx, hash, 0, td)
	if err != nil {
		t.Fatalf("WriteTd failed: %v", err)
	}
	entry, err = rawdb.ReadTd(tx, hash, 0)
	if err != nil {
		t.Fatalf("ReadTd failed: %v", err)
	}
	if entry == nil {
		t.Fatalf("Stored TD not found")
	} else if entry.Cmp(td) != 0 {
		t.Fatalf("Retrieved TD mismatch: have %v, want %v", entry, td)
	}
	// Delete the TD and verify the execution
	err = rawdb.TruncateTd(tx, 0)
	if err != nil {
		t.Fatalf("DeleteTd failed: %v", err)
	}
	entry, err = rawdb.ReadTd(tx, hash, 0)
	if err != nil {
		t.Fatalf("ReadTd failed: %v", err)
	}
	if entry != nil {
		t.Fatalf("Deleted TD returned: %v", entry)
	}
}

// Tests that canonical numbers can be mapped to hashes and retrieved.
func TestCanonicalMappingStorage(t *testing.T) {
	t.Parallel()
	m := mock.Mock(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	br := m.BlockReader

	// Create a test canonical number and assinged hash to move around
	hash, number := common.Hash{0: 0xff}, uint64(314)
	entry, _, err := br.CanonicalHash(m.Ctx, tx, number)
	if err != nil {
		t.Fatalf("ReadCanonicalHash failed: %v", err)
	}
	if entry != (common.Hash{}) {
		t.Fatalf("Non existent canonical mapping returned: %v", entry)
	}
	// Write and verify the TD in the database
	err = rawdb.WriteCanonicalHash(tx, hash, number)
	if err != nil {
		t.Fatalf("WriteCanoncalHash failed: %v", err)
	}
	entry, _, err = br.CanonicalHash(m.Ctx, tx, number)
	if err != nil {
		t.Fatalf("ReadCanonicalHash failed: %v", err)
	}
	if entry == (common.Hash{}) {
		t.Fatalf("Stored canonical mapping not found")
	} else if entry != hash {
		t.Fatalf("Retrieved canonical mapping mismatch: have %v, want %v", entry, hash)
	}
	// Delete the TD and verify the execution
	err = rawdb.TruncateCanonicalHash(tx, number, false)
	if err != nil {
		t.Fatalf("DeleteCanonicalHash failed: %v", err)
	}
	entry, _, err = br.CanonicalHash(m.Ctx, tx, number)
	if err != nil {
		t.Error(err)
	}
	if entry != (common.Hash{}) {
		t.Fatalf("Deleted canonical mapping returned: %v", entry)
	}
}

// Tests that head headers and head blocks can be assigned, individually.
func TestHeadStorage2(t *testing.T) {
	t.Parallel()
	_, db := memdb.NewTestTx(t)

	blockHead := types.NewBlockWithHeader(&types.Header{Extra: []byte("test block header")})
	blockFull := types.NewBlockWithHeader(&types.Header{Extra: []byte("test block full")})

	// Check that no head entries are in a pristine database
	if entry := rawdb.ReadHeadHeaderHash(db); entry != (common.Hash{}) {
		t.Fatalf("Non head header entry returned: %v", entry)
	}
	if entry := rawdb.ReadHeadBlockHash(db); entry != (common.Hash{}) {
		t.Fatalf("Non head block entry returned: %v", entry)
	}
	// Assign separate entries for the head header and block
	rawdb.WriteHeadHeaderHash(db, blockHead.Hash())
	rawdb.WriteHeadBlockHash(db, blockFull.Hash())

	// Check that both heads are present, and different (i.e. two heads maintained)
	if entry := rawdb.ReadHeadHeaderHash(db); entry != blockHead.Hash() {
		t.Fatalf("Head header hash mismatch: have %v, want %v", entry, blockHead.Hash())
	}
	if entry := rawdb.ReadHeadBlockHash(db); entry != blockFull.Hash() {
		t.Fatalf("Head block hash mismatch: have %v, want %v", entry, blockFull.Hash())
	}
}

// Tests that head headers and head blocks can be assigned, individually.
func TestHeadStorage(t *testing.T) {
	t.Parallel()
	m := mock.Mock(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	blockHead := types.NewBlockWithHeader(&types.Header{Extra: []byte("test block header"), Number: common.Big1})
	blockFull := types.NewBlockWithHeader(&types.Header{Extra: []byte("test block full"), Number: common.Big1})

	// Assign separate entries for the head header and block
	rawdb.WriteHeadHeaderHash(tx, blockHead.Hash())
	rawdb.WriteHeadBlockHash(tx, blockFull.Hash())

	// Check that both heads are present, and different (i.e. two heads maintained)
	if entry := rawdb.ReadHeadHeaderHash(tx); entry != blockHead.Hash() {
		t.Fatalf("Head header hash mismatch: have %v, want %v", entry, blockHead.Hash())
	}
	if entry := rawdb.ReadHeadBlockHash(tx); entry != blockFull.Hash() {
		t.Fatalf("Head block hash mismatch: have %v, want %v", entry, blockFull.Hash())
	}
}

// Tests that receipts associated with a single block can be stored and retrieved.
func TestBlockReceiptStorage(t *testing.T) {
	t.Parallel()
	m := mock.Mock(t)
	m.DB.(state.HasAgg).Agg().(*state.Aggregator).EnableDomain(kv.RCacheDomain)
	tx, err := m.DB.BeginTemporalRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	br := m.BlockReader
	txNumReader := br.TxnumReader(context.Background())
	require := require.New(t)
	ctx := m.Ctx

	// Create a live block since we need metadata to reconstruct the receipt
	tx1 := types.NewTransaction(1, common.HexToAddress("0x1"), u256.Num1, 1, u256.Num1, nil)
	tx2 := types.NewTransaction(2, common.HexToAddress("0x2"), u256.Num2, 2, u256.Num2, nil)

	header := &types.Header{Number: big.NewInt(1)}
	body := &types.Body{Transactions: types.Transactions{tx1, tx2}}

	// Create the two receipts to manage afterwards
	receipt1 := &types.Receipt{
		Status:            types.ReceiptStatusFailed,
		CumulativeGasUsed: 1,
		Logs: []*types.Log{
			{Address: common.BytesToAddress([]byte{0x11})},
			{Address: common.BytesToAddress([]byte{0x01, 0x11})},
		},
		TxHash:          tx1.Hash(),
		ContractAddress: common.BytesToAddress([]byte{0x01, 0x11, 0x11}),
		GasUsed:         111111,
		BlockNumber:     header.Number,
		BlockHash:       header.Hash(),

		TransactionIndex: 0,
	}
	receipt1.Bloom = types.CreateBloom(types.Receipts{receipt1})

	receipt2 := &types.Receipt{
		PostState:         common.Hash{2}.Bytes(),
		CumulativeGasUsed: 2,
		Logs: []*types.Log{
			{Address: common.BytesToAddress([]byte{0x22})},
			{Address: common.BytesToAddress([]byte{0x02, 0x22})},
		},
		TxHash:           tx2.Hash(),
		ContractAddress:  common.BytesToAddress([]byte{0x02, 0x22, 0x22}),
		GasUsed:          222222,
		BlockNumber:      header.Number,
		BlockHash:        header.Hash(),
		TransactionIndex: 1,
	}
	receipt2.Bloom = types.CreateBloom(types.Receipts{receipt2})
	receipts := types.Receipts{receipt1, receipt2}

	// Check that no receipt entries are in a pristine database
	hash := header.Hash() //common.BytesToHash([]byte{0x03, 0x14})

	require.NoError(rawdb.WriteCanonicalHash(tx, header.Hash(), header.Number.Uint64()))
	require.NoError(rawdb.WriteHeader(tx, header))
	// Insert the body that corresponds to the receipts
	require.NoError(rawdb.WriteBody(tx, hash, 1, body))
	require.NoError(rawdb.WriteSenders(tx, hash, 1, body.SendersFromTxs()))

	{
		sd, err := state.NewSharedDomains(tx, log.New())
		require.NoError(err)
		defer sd.Close()
		base, err := txNumReader.Min(tx, 1)
		require.NoError(err)
		// Insert the receipt slice into the database and check presence
		sd.SetTxNum(base)
		require.NoError(rawdb.WriteReceiptCacheV2(sd.AsPutDel(tx), nil, base))
		for i, r := range receipts {
			sd.SetTxNum(base + 1 + uint64(i))
			require.NoError(rawdb.WriteReceiptCacheV2(sd.AsPutDel(tx), r, base+1+uint64(i)))
		}
		sd.SetTxNum(base + uint64(len(receipts)) + 1)
		require.NoError(rawdb.WriteReceiptCacheV2(sd.AsPutDel(tx), nil, base+uint64(len(receipts))+1))

		_, err = sd.ComputeCommitment(ctx, true, sd.BlockNum(), sd.TxNum(), "flush-commitment")
		require.NoError(err)

		require.NoError(sd.Flush(ctx, tx))
	}

	b, _, err := br.BlockWithSenders(ctx, tx, hash, 1)
	require.NoError(err)
	require.NotNil(b)
	rs, err := rawdb.ReadReceiptsCacheV2(tx, b, txNumReader)
	require.NoError(err)
	require.NotEmpty(rs)
	require.NoError(checkReceiptsRLP(rs, receipts))

	// Ensure that receipts without metadata can be returned without the block body too
	rFromDB, err := rawdb.ReadReceiptsCacheV2(tx, b, txNumReader)
	require.NoError(err)
	require.NoError(checkReceiptsRLP(rFromDB, receipts))

	require.NoError(rawdb.WriteHeader(tx, header))
	// Sanity check that body alone without the receipt is a full purge
	require.NoError(rawdb.WriteBody(tx, hash, 1, body))
	b, _, err = br.BlockWithSenders(ctx, tx, hash, 1)
	require.NoError(err)
	require.NotNil(b)
}

// Tests block storage and retrieval operations with withdrawals.
func TestBlockWithdrawalsStorage(t *testing.T) {
	t.Parallel()
	m := mock.Mock(t)
	require := require.New(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(err)
	defer tx.Rollback()
	br, bw := m.BlocksIO()
	ctx := context.Background()

	// create fake withdrawals
	w := types.Withdrawal{
		Index:     uint64(15),
		Validator: uint64(5500),
		Address:   common.Address{0: 0xff},
		Amount:    1000,
	}

	w2 := types.Withdrawal{
		Index:     uint64(16),
		Validator: uint64(5501),
		Address:   common.Address{0: 0xff},
		Amount:    1001,
	}

	withdrawals := make([]*types.Withdrawal, 0)
	withdrawals = append(withdrawals, &w)
	withdrawals = append(withdrawals, &w2)

	// Create a test block to move around the database and make sure it's really new
	block := types.NewBlockWithHeader(&types.Header{
		Number:      big.NewInt(1),
		Extra:       []byte("test block"),
		UncleHash:   empty.UncleHash,
		TxHash:      empty.RootHash,
		ReceiptHash: empty.RootHash,
	})
	if entry, _, _ := br.BlockWithSenders(ctx, tx, block.Hash(), block.NumberU64()); entry != nil {
		t.Fatalf("Non existent block returned: %v", entry)
	}
	if entry, _ := br.Header(ctx, tx, block.Hash(), block.NumberU64()); entry != nil {
		t.Fatalf("Non existent header returned: %v", entry)
	}
	if entry, _ := br.BodyWithTransactions(ctx, tx, block.Hash(), block.NumberU64()); entry != nil {
		t.Fatalf("Non existent body returned: %v", entry)
	}

	// Write withdrawals to block
	wBlock := types.NewBlockFromStorage(block.Hash(), block.Header(), block.Transactions(), block.Uncles(), withdrawals)
	if err := rawdb.WriteHeader(tx, wBlock.HeaderNoCopy()); err != nil {
		t.Fatalf("Could not write body: %v", err)
	}
	if err := rawdb.WriteBody(tx, wBlock.Hash(), wBlock.NumberU64(), wBlock.Body()); err != nil {
		t.Fatalf("Could not write body: %v", err)
	}

	// Write and verify the block in the database
	err = rawdb.WriteBlock(tx, wBlock)
	if err != nil {
		t.Fatalf("Could not write block: %v", err)
	}
	if entry, _, _ := br.BlockWithSenders(ctx, tx, block.Hash(), block.NumberU64()); entry == nil {
		t.Fatalf("Stored block not found")
	} else if entry.Hash() != block.Hash() {
		t.Fatalf("Retrieved block mismatch: have %v, want %v", entry, block)
	}
	if entry, _ := br.Header(ctx, tx, block.Hash(), block.NumberU64()); entry == nil {
		t.Fatalf("Stored header not found")
	} else if entry.Hash() != block.Hash() {
		t.Fatalf("Retrieved header mismatch: have %v, want %v", entry, block.Header())
	}
	if err := rawdb.TruncateBlocks(context.Background(), tx, 2); err != nil {
		t.Fatal(err)
	}
	entry, _ := br.BodyWithTransactions(ctx, tx, block.Hash(), block.NumberU64())
	if entry == nil {
		t.Fatalf("Stored body not found")
	} else if types.DeriveSha(types.Transactions(entry.Transactions)) != types.DeriveSha(block.Transactions()) || types.CalcUncleHash(entry.Uncles) != types.CalcUncleHash(block.Uncles()) {
		t.Fatalf("Retrieved body mismatch: have %v, want %v", entry, block.Body())
	}

	// Verify withdrawals on block
	readWithdrawals := entry.Withdrawals
	if len(readWithdrawals) != len(withdrawals) {
		t.Fatalf("Retrieved withdrawals mismatch: have %v, want %v", readWithdrawals, withdrawals)
	}

	rw := readWithdrawals[0]
	rw2 := readWithdrawals[1]

	require.NotNil(rw)
	require.Equal(uint64(15), rw.Index)
	require.Equal(uint64(5500), rw.Validator)
	require.Equal(common.Address{0: 0xff}, rw.Address)
	require.Equal(uint64(1000), rw.Amount)

	require.NotNil(rw2)
	require.Equal(uint64(16), rw2.Index)
	require.Equal(uint64(5501), rw2.Validator)
	require.Equal(common.Address{0: 0xff}, rw2.Address)
	require.Equal(uint64(1001), rw2.Amount)

	// Delete the block and verify the execution
	if err := rawdb.TruncateBlocks(context.Background(), tx, block.NumberU64()); err != nil {
		t.Fatal(err)
	}
	//if err := DeleteBlock(tx, block.Hash(), block.NumberU64()); err != nil {
	//	t.Fatalf("Could not delete block: %v", err)
	//}
	if entry, _, _ := br.BlockWithSenders(ctx, tx, block.Hash(), block.NumberU64()); entry != nil {
		t.Fatalf("Deleted block returned: %v", entry)
	}
	if entry, _ := br.Header(ctx, tx, block.Hash(), block.NumberU64()); entry != nil {
		t.Fatalf("Deleted header returned: %v", entry)
	}
	if entry, _ := br.BodyWithTransactions(ctx, tx, block.Hash(), block.NumberU64()); entry != nil {
		t.Fatalf("Deleted body returned: %v", entry)
	}

	// write again and delete it as old one
	if err := rawdb.WriteBlock(tx, block); err != nil {
		t.Fatalf("Could not write block: %v", err)
	}
	// prune: [1: N)
	var deleted int
	deleted, err = bw.PruneBlocks(ctx, tx, 0, 1)
	require.NoError(err)
	require.Equal(0, deleted)
	entry, _ = br.BodyWithTransactions(ctx, tx, block.Hash(), block.NumberU64())
	require.NotNil(entry)
	deleted, err = bw.PruneBlocks(ctx, tx, 1, 1)
	require.NoError(err)
	require.Equal(0, deleted)
	entry, _ = br.BodyWithTransactions(ctx, tx, block.Hash(), block.NumberU64())
	require.NotNil(entry)
	deleted, err = bw.PruneBlocks(ctx, tx, 2, 1)
	require.NoError(err)
	require.Equal(1, deleted)
	entry, _ = br.BodyWithTransactions(ctx, tx, block.Hash(), block.NumberU64())
	require.Nil(entry)
}

// Tests pre-shanghai body to make sure withdrawals doesn't panic
func TestPreShanghaiBodyNoPanicOnWithdrawals(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	const bodyRlp = "f902bef8bef85d0101019471562b71999873db5b286df957af199ec94617f701801ca023f4aad9a71341d2990012a732366c3bc8a4ce9ff54c05546a9487445ac67692a0290d3a1411c2a675a4c12c98af60e34ea4d689f0ddfe0250a9e09c0819dfe3bff85d0201029471562b71999873db5b286df957af199ec94617f701801ca0f824d7edc241758aca948ff34d3797e4e31003f76cc9e05fb9c19e967fc48113a070e1389f0fa23fe765a04b23e98f98db6d630e3a035c1c7c968142ababb85a1df901fbf901f8a00000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000940000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000b901000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000080808080808b7465737420686561646572a00000000000000000000000000000000000000000000000000000000000000000880000000000000000"
	bstring, _ := hex.DecodeString(bodyRlp)

	body := new(types.Body)
	rlp.DecodeBytes(bstring, body)

	require.Nil(body.Withdrawals)
	require.Len(body.Transactions, 2)
}

// Tests pre-shanghai bodyForStorage to make sure withdrawals doesn't panic
func TestPreShanghaiBodyForStorageNoPanicOnWithdrawals(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	const bodyForStorageRlp = "c38002c0"
	bstring, _ := hex.DecodeString(bodyForStorageRlp)

	body := new(types.BodyForStorage)
	rlp.DecodeBytes(bstring, body)

	require.Nil(body.Withdrawals)
	require.Equal(uint32(2), body.TxCount)
}

// Tests shanghai bodyForStorage to make sure withdrawals are present
func TestShanghaiBodyForStorageHasWithdrawals(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	const bodyForStorageRlp = "f83f8002c0f83adc0f82157c94ff000000000000000000000000000000000000008203e8dc1082157d94ff000000000000000000000000000000000000008203e9"
	bstring, _ := hex.DecodeString(bodyForStorageRlp)

	body := new(types.BodyForStorage)
	rlp.DecodeBytes(bstring, body)

	require.NotNil(body.Withdrawals)
	require.Len(body.Withdrawals, 2)
	require.Equal(uint32(2), body.TxCount)
}

// Tests shanghai bodyForStorage to make sure when no withdrawals the slice is empty (not nil)
func TestShanghaiBodyForStorageNoWithdrawals(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	const bodyForStorageRlp = "c48002c0c0c0"
	bstring, _ := hex.DecodeString(bodyForStorageRlp)

	body := new(types.BodyForStorage)
	rlp.DecodeBytes(bstring, body)

	require.NotNil(body.Withdrawals)
	require.Empty(body.Withdrawals)
	require.Equal(uint32(2), body.TxCount)
}

func TestBadBlocks(t *testing.T) {
	t.Parallel()
	m := mock.Mock(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	require := require.New(t)
	var testKey, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	testAddr := crypto.PubkeyToAddress(testKey.PublicKey)

	mustSign := func(tx types.Transaction, s types.Signer) types.Transaction {
		r, err := types.SignTx(tx, s, testKey)
		require.NoError(err)
		return r
	}

	putBlock := func(number uint64) common.Hash {
		// prepare db so it works with our test
		signer1 := types.MakeSigner(chainspec.Mainnet.Config, number, number-1)
		body := &types.Body{
			Transactions: []types.Transaction{
				mustSign(types.NewTransaction(number, testAddr, u256.Num1, 1, u256.Num1, nil), *signer1),
				mustSign(types.NewTransaction(number+1, testAddr, u256.Num1, 2, u256.Num1, nil), *signer1),
			},
			Uncles: []*types.Header{{Extra: []byte("test header")}},
		}

		header := &types.Header{Number: big.NewInt(int64(number))}
		require.NoError(rawdb.WriteCanonicalHash(tx, header.Hash(), number))
		require.NoError(rawdb.WriteHeader(tx, header))
		require.NoError(rawdb.WriteBody(tx, header.Hash(), number, body))

		return header.Hash()
	}
	rawdb.ResetBadBlockCache(tx, 4)

	// put some blocks
	for i := 1; i <= 6; i++ {
		putBlock(uint64(i))
	}
	hash1 := putBlock(7)
	hash2 := putBlock(8)
	hash3 := putBlock(9)
	hash4 := putBlock(10)

	// mark some blocks as bad
	require.NoError(rawdb.TruncateCanonicalHash(tx, 7, true))
	badBlks, err := rawdb.GetLatestBadBlocks(tx)
	require.NoError(err)
	require.Len(badBlks, 4)

	require.Equal(badBlks[0].Hash(), hash4)
	require.Equal(badBlks[1].Hash(), hash3)
	require.Equal(badBlks[2].Hash(), hash2)
	require.Equal(badBlks[3].Hash(), hash1)

	// testing the "limit"
	rawdb.ResetBadBlockCache(tx, 2)
	badBlks, err = rawdb.GetLatestBadBlocks(tx)
	require.NoError(err)
	require.Len(badBlks, 2)
	require.Equal(badBlks[0].Hash(), hash4)
	require.Equal(badBlks[1].Hash(), hash3)
}

func checkReceiptsRLP(have, want types.Receipts) error {
	if len(have) != len(want) {
		return fmt.Errorf("receipts sizes mismatch: have %d, want %d", len(have), len(want))
	}
	for i := 0; i < len(want); i++ {
		rlpHave, err := rlp.EncodeToBytes(have[i])
		if err != nil {
			return err
		}
		rlpWant, err := rlp.EncodeToBytes(want[i])
		if err != nil {
			return err
		}
		if !bytes.Equal(rlpHave, rlpWant) {
			return fmt.Errorf("receipt #%d: receipt mismatch: have %s, want %s", i, hex.EncodeToString(rlpHave), hex.EncodeToString(rlpWant))
		}
	}
	return nil
}
