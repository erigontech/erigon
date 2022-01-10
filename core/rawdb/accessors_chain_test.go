// Copyright 2018 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package rawdb

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/big"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/sha3"
)

// Tests block header storage and retrieval operations.
func TestHeaderStorage(t *testing.T) {
	_, tx := memdb.NewTestTx(t)

	// Create a test header to move around the database and make sure it's really new
	header := &types.Header{Number: big.NewInt(42), Extra: []byte("test header")}
	if entry := ReadHeader(tx, header.Hash(), header.Number.Uint64()); entry != nil {
		t.Fatalf("Non existent header returned: %v", entry)
	}
	// Write and verify the header in the database
	WriteHeader(tx, header)
	if entry := ReadHeader(tx, header.Hash(), header.Number.Uint64()); entry == nil {
		t.Fatalf("Stored header not found")
	} else if entry.Hash() != header.Hash() {
		t.Fatalf("Retrieved header mismatch: have %v, want %v", entry, header)
	}
	if entry := ReadHeaderRLP(tx, header.Hash(), header.Number.Uint64()); entry == nil {
		t.Fatalf("Stored header RLP not found")
	} else {
		hasher := sha3.NewLegacyKeccak256()
		hasher.Write(entry)

		if hash := common.BytesToHash(hasher.Sum(nil)); hash != header.Hash() {
			t.Fatalf("Retrieved RLP header mismatch: have %v, want %v", entry, header)
		}
	}
	// Delete the header and verify the execution
	DeleteHeader(tx, header.Hash(), header.Number.Uint64())
	if entry := ReadHeader(tx, header.Hash(), header.Number.Uint64()); entry != nil {
		t.Fatalf("Deleted header returned: %v", entry)
	}
}

// Tests block body storage and retrieval operations.
func TestBodyStorage(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	require := require.New(t)

	var testKey, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	testAddr := crypto.PubkeyToAddress(testKey.PublicKey)

	mustSign := func(tx types.Transaction, s types.Signer) types.Transaction {
		r, err := types.SignTx(tx, s, testKey)
		require.NoError(err)
		return r
	}

	// prepare db so it works with our test
	signer1 := types.MakeSigner(params.MainnetChainConfig, 1)
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

	if entry := ReadCanonicalBodyWithTransactions(tx, hash, 0); entry != nil {
		t.Fatalf("Non existent body returned: %v", entry)
	}
	require.NoError(WriteBody(tx, hash, 0, body))
	if entry := ReadCanonicalBodyWithTransactions(tx, hash, 0); entry == nil {
		t.Fatalf("Stored body not found")
	} else if types.DeriveSha(types.Transactions(entry.Transactions)) != types.DeriveSha(types.Transactions(body.Transactions)) || types.CalcUncleHash(entry.Uncles) != types.CalcUncleHash(body.Uncles) {
		t.Fatalf("Retrieved body mismatch: have %v, want %v", entry, body)
	}
	if entry := ReadBodyRLP(tx, hash, 0); entry == nil {
		t.Fatalf("Stored body RLP not found")
	} else {
		hasher := sha3.NewLegacyKeccak256()
		hasher.Write(entry)

		if calc := common.BytesToHash(hasher.Sum(nil)); calc != hash {
			t.Fatalf("Retrieved RLP body mismatch: have %v, want %v", entry, body)
		}
	}
	// Delete the body and verify the execution
	DeleteBody(tx, hash, 0)
	if entry := ReadCanonicalBodyWithTransactions(tx, hash, 0); entry != nil {
		t.Fatalf("Deleted body returned: %v", entry)
	}
}

// Tests block storage and retrieval operations.
func TestBlockStorage(t *testing.T) {
	_, tx := memdb.NewTestTx(t)

	// Create a test block to move around the database and make sure it's really new
	block := types.NewBlockWithHeader(&types.Header{
		Extra:       []byte("test block"),
		UncleHash:   types.EmptyUncleHash,
		TxHash:      types.EmptyRootHash,
		ReceiptHash: types.EmptyRootHash,
	})
	if entry := ReadBlock(tx, block.Hash(), block.NumberU64()); entry != nil {
		t.Fatalf("Non existent block returned: %v", entry)
	}
	if entry := ReadHeader(tx, block.Hash(), block.NumberU64()); entry != nil {
		t.Fatalf("Non existent header returned: %v", entry)
	}
	if entry := ReadCanonicalBodyWithTransactions(tx, block.Hash(), block.NumberU64()); entry != nil {
		t.Fatalf("Non existent body returned: %v", entry)
	}
	// Write and verify the block in the database
	err := WriteBlock(tx, block)
	if err != nil {
		t.Fatalf("Could not write block: %v", err)
	}
	if entry := ReadBlock(tx, block.Hash(), block.NumberU64()); entry == nil {
		t.Fatalf("Stored block not found")
	} else if entry.Hash() != block.Hash() {
		t.Fatalf("Retrieved block mismatch: have %v, want %v", entry, block)
	}
	if entry := ReadHeader(tx, block.Hash(), block.NumberU64()); entry == nil {
		t.Fatalf("Stored header not found")
	} else if entry.Hash() != block.Hash() {
		t.Fatalf("Retrieved header mismatch: have %v, want %v", entry, block.Header())
	}
	if entry := ReadCanonicalBodyWithTransactions(tx, block.Hash(), block.NumberU64()); entry == nil {
		t.Fatalf("Stored body not found")
	} else if types.DeriveSha(types.Transactions(entry.Transactions)) != types.DeriveSha(block.Transactions()) || types.CalcUncleHash(entry.Uncles) != types.CalcUncleHash(block.Uncles()) {
		t.Fatalf("Retrieved body mismatch: have %v, want %v", entry, block.Body())
	}
	// Delete the block and verify the execution
	if err := DeleteBlock(tx, block.Hash(), block.NumberU64()); err != nil {
		t.Fatalf("Could not delete block: %v", err)
	}
	if entry := ReadBlock(tx, block.Hash(), block.NumberU64()); entry != nil {
		t.Fatalf("Deleted block returned: %v", entry)
	}
	if entry := ReadHeader(tx, block.Hash(), block.NumberU64()); entry != nil {
		t.Fatalf("Deleted header returned: %v", entry)
	}
	if entry := ReadCanonicalBodyWithTransactions(tx, block.Hash(), block.NumberU64()); entry != nil {
		t.Fatalf("Deleted body returned: %v", entry)
	}
}

// Tests that partial block contents don't get reassembled into full blocks.
func TestPartialBlockStorage(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	block := types.NewBlockWithHeader(&types.Header{
		Extra:       []byte("test block"),
		UncleHash:   types.EmptyUncleHash,
		TxHash:      types.EmptyRootHash,
		ReceiptHash: types.EmptyRootHash,
	})
	header := block.Header() // Not identical to struct literal above, due to other fields

	// Store a header and check that it's not recognized as a block
	WriteHeader(tx, header)
	if entry := ReadBlock(tx, block.Hash(), block.NumberU64()); entry != nil {
		t.Fatalf("Non existent block returned: %v", entry)
	}
	DeleteHeader(tx, block.Hash(), block.NumberU64())

	// Store a body and check that it's not recognized as a block
	if err := WriteBody(tx, block.Hash(), block.NumberU64(), block.Body()); err != nil {
		t.Fatal(err)
	}
	if entry := ReadBlock(tx, block.Hash(), block.NumberU64()); entry != nil {
		t.Fatalf("Non existent block returned: %v", entry)
	}
	DeleteBody(tx, block.Hash(), block.NumberU64())

	// Store a header and a body separately and check reassembly
	WriteHeader(tx, header)
	if err := WriteBody(tx, block.Hash(), block.NumberU64(), block.Body()); err != nil {
		t.Fatal(err)
	}

	if entry := ReadBlock(tx, block.Hash(), block.NumberU64()); entry == nil {
		t.Fatalf("Stored block not found")
	} else if entry.Hash() != block.Hash() {
		t.Fatalf("Retrieved block mismatch: have %v, want %v", entry, block)
	}
}

// Tests block total difficulty storage and retrieval operations.
func TestTdStorage(t *testing.T) {
	_, tx := memdb.NewTestTx(t)

	// Create a test TD to move around the database and make sure it's really new
	hash, td := common.Hash{}, big.NewInt(314)
	entry, err := ReadTd(tx, hash, 0)
	if err != nil {
		t.Fatalf("ReadTd failed: %v", err)
	}
	if entry != nil {
		t.Fatalf("Non existent TD returned: %v", entry)
	}
	// Write and verify the TD in the database
	err = WriteTd(tx, hash, 0, td)
	if err != nil {
		t.Fatalf("WriteTd failed: %v", err)
	}
	entry, err = ReadTd(tx, hash, 0)
	if err != nil {
		t.Fatalf("ReadTd failed: %v", err)
	}
	if entry == nil {
		t.Fatalf("Stored TD not found")
	} else if entry.Cmp(td) != 0 {
		t.Fatalf("Retrieved TD mismatch: have %v, want %v", entry, td)
	}
	// Delete the TD and verify the execution
	err = DeleteTd(tx, hash, 0)
	if err != nil {
		t.Fatalf("DeleteTd failed: %v", err)
	}
	entry, err = ReadTd(tx, hash, 0)
	if err != nil {
		t.Fatalf("ReadTd failed: %v", err)
	}
	if entry != nil {
		t.Fatalf("Deleted TD returned: %v", entry)
	}
}

// Tests that canonical numbers can be mapped to hashes and retrieved.
func TestCanonicalMappingStorage(t *testing.T) {
	_, tx := memdb.NewTestTx(t)

	// Create a test canonical number and assinged hash to move around
	hash, number := common.Hash{0: 0xff}, uint64(314)
	entry, err := ReadCanonicalHash(tx, number)
	if err != nil {
		t.Fatalf("ReadCanonicalHash failed: %v", err)
	}
	if entry != (common.Hash{}) {
		t.Fatalf("Non existent canonical mapping returned: %v", entry)
	}
	// Write and verify the TD in the database
	err = WriteCanonicalHash(tx, hash, number)
	if err != nil {
		t.Fatalf("WriteCanoncalHash failed: %v", err)
	}
	entry, err = ReadCanonicalHash(tx, number)
	if err != nil {
		t.Fatalf("ReadCanonicalHash failed: %v", err)
	}
	if entry == (common.Hash{}) {
		t.Fatalf("Stored canonical mapping not found")
	} else if entry != hash {
		t.Fatalf("Retrieved canonical mapping mismatch: have %v, want %v", entry, hash)
	}
	// Delete the TD and verify the execution
	err = DeleteCanonicalHash(tx, number)
	if err != nil {
		t.Fatalf("DeleteCanonicalHash failed: %v", err)
	}
	entry, err = ReadCanonicalHash(tx, number)
	if err != nil {
		t.Error(err)
	}
	if entry != (common.Hash{}) {
		t.Fatalf("Deleted canonical mapping returned: %v", entry)
	}
}

// Tests that head headers and head blocks can be assigned, individually.
func TestHeadStorage(t *testing.T) {
	_, db := memdb.NewTestTx(t)

	blockHead := types.NewBlockWithHeader(&types.Header{Extra: []byte("test block header")})
	blockFull := types.NewBlockWithHeader(&types.Header{Extra: []byte("test block full")})

	// Check that no head entries are in a pristine database
	if entry := ReadHeadHeaderHash(db); entry != (common.Hash{}) {
		t.Fatalf("Non head header entry returned: %v", entry)
	}
	if entry := ReadHeadBlockHash(db); entry != (common.Hash{}) {
		t.Fatalf("Non head block entry returned: %v", entry)
	}
	// Assign separate entries for the head header and block
	WriteHeadHeaderHash(db, blockHead.Hash())
	WriteHeadBlockHash(db, blockFull.Hash())

	// Check that both heads are present, and different (i.e. two heads maintained)
	if entry := ReadHeadHeaderHash(db); entry != blockHead.Hash() {
		t.Fatalf("Head header hash mismatch: have %v, want %v", entry, blockHead.Hash())
	}
	if entry := ReadHeadBlockHash(db); entry != blockFull.Hash() {
		t.Fatalf("Head block hash mismatch: have %v, want %v", entry, blockFull.Hash())
	}
}

// Tests that receipts associated with a single block can be stored and retrieved.
func TestBlockReceiptStorage(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	require := require.New(t)

	// Create a live block since we need metadata to reconstruct the receipt
	tx1 := types.NewTransaction(1, common.HexToAddress("0x1"), u256.Num1, 1, u256.Num1, nil)
	tx2 := types.NewTransaction(2, common.HexToAddress("0x2"), u256.Num2, 2, u256.Num2, nil)

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
	}
	//receipt1.Bloom = types.CreateBloom(types.Receipts{receipt1})

	receipt2 := &types.Receipt{
		PostState:         common.Hash{2}.Bytes(),
		CumulativeGasUsed: 2,
		Logs: []*types.Log{
			{Address: common.BytesToAddress([]byte{0x22})},
			{Address: common.BytesToAddress([]byte{0x02, 0x22})},
		},
		TxHash:          tx2.Hash(),
		ContractAddress: common.BytesToAddress([]byte{0x02, 0x22, 0x22}),
		GasUsed:         222222,
	}
	//receipt2.Bloom = types.CreateBloom(types.Receipts{receipt2})
	receipts := []*types.Receipt{receipt1, receipt2}
	header := &types.Header{Number: big.NewInt(0)}

	// Check that no receipt entries are in a pristine database
	hash := header.Hash() //common.BytesToHash([]byte{0x03, 0x14})
	b, senders, err := ReadBlockWithSenders(tx, hash, 0)
	require.NoError(err)
	//require.NotNil(t, b)
	if rs := ReadReceipts(tx, b, senders); len(rs) != 0 {
		t.Fatalf("non existent receipts returned: %v", rs)
	}

	WriteHeader(tx, header)
	// Insert the body that corresponds to the receipts
	require.NoError(WriteBody(tx, hash, 0, body))
	require.NoError(WriteSenders(tx, hash, 0, body.SendersFromTxs()))

	// Insert the receipt slice into the database and check presence
	require.NoError(WriteReceipts(tx, 0, receipts))

	b, senders, err = ReadBlockWithSenders(tx, hash, 0)
	require.NoError(err)
	require.NotNil(b)
	if rs := ReadReceipts(tx, b, senders); len(rs) == 0 {
		t.Fatalf("no receipts returned")
	} else {
		if err := checkReceiptsRLP(rs, receipts); err != nil {
			t.Fatalf(err.Error())
		}
	}
	// Delete the body and ensure that the receipts are no longer returned (metadata can't be recomputed)
	DeleteHeader(tx, hash, 0)
	DeleteBody(tx, hash, 0)
	b, senders, err = ReadBlockWithSenders(tx, hash, 0)
	require.NoError(err)
	require.Nil(b)
	if rs := ReadReceipts(tx, b, senders); rs != nil {
		t.Fatalf("receipts returned when body was deleted: %v", rs)
	}
	// Ensure that receipts without metadata can be returned without the block body too
	if err := checkReceiptsRLP(ReadRawReceipts(tx, 0), receipts); err != nil {
		t.Fatal(err)
	}
	WriteHeader(tx, header)
	// Sanity check that body alone without the receipt is a full purge
	require.NoError(WriteBody(tx, hash, 0, body))
	require.NoError(DeleteReceipts(tx, 0))
	b, senders, err = ReadBlockWithSenders(tx, hash, 0)
	require.NoError(err)
	require.NotNil(b)
	if rs := ReadReceipts(tx, b, senders); len(rs) != 0 {
		t.Fatalf("deleted receipts returned: %v", rs)
	}
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
