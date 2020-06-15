package remotechain

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/golang/snappy"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

// ReadTd reimplemented rawdb.ReadTd
func ReadTd(tx ethdb.Tx, hash common.Hash, number uint64) (*hexutil.Big, error) {
	bucket := tx.Bucket(dbutils.HeaderPrefix)
	if bucket == nil {
		return nil, nil
	}

	data, err := bucket.Get(dbutils.HeaderTDKey(number, hash))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	td := new(big.Int)
	if err := rlp.Decode(bytes.NewReader(data), td); err != nil {
		return nil, fmt.Errorf("invalid block total difficulty RLP. Hash: %s. Err: %w", hash, err)
	}
	return (*hexutil.Big)(td), nil
}

// ReadCanonicalHash reimplementation of rawdb.ReadCanonicalHash
func ReadCanonicalHash(tx ethdb.Tx, number uint64) (common.Hash, error) {
	bucket := tx.Bucket(dbutils.HeaderPrefix)
	if bucket == nil {
		return common.Hash{}, fmt.Errorf("bucket %s not found", dbutils.HeaderPrefix)
	}

	data, err := bucket.Get(dbutils.HeaderHashKey(number))
	if err != nil {
		return common.Hash{}, err
	}
	if len(data) == 0 {
		return common.Hash{}, nil
	}
	return common.BytesToHash(data), nil
}

// GetBlockByNumber reimplementation of chain.GetBlockByNumber
func GetBlockByNumber(tx ethdb.Tx, number uint64) (*types.Block, error) {
	hash, err := ReadCanonicalHash(tx, number)
	if err != nil {
		return nil, err
	}
	if hash == (common.Hash{}) {
		return nil, nil
	}
	return ReadBlock(tx, hash, number)
}

// ReadBlock reimplementation of rawdb.ReadBlock
func ReadBlock(tx ethdb.Tx, hash common.Hash, number uint64) (*types.Block, error) {
	header, err := ReadHeader(tx, hash, number)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, nil
	}
	body, err := ReadBody(tx, hash, number)
	if err != nil {
		return nil, err
	}
	if body == nil {
		return nil, nil
	}
	return types.NewBlockWithHeader(header).WithBody(body.Transactions, body.Uncles), nil
}

// ReadHeaderRLP reimplementation of rawdb.ReadHeaderRLP
func ReadHeaderRLP(tx ethdb.Tx, hash common.Hash, number uint64) (rlp.RawValue, error) {
	bucket := tx.Bucket(dbutils.HeaderPrefix)
	if bucket == nil {
		return rlp.RawValue{}, fmt.Errorf("bucket %s not found", dbutils.HeaderPrefix)
	}
	return bucket.Get(dbutils.HeaderKey(number, hash))
}

// ReadHeader reimplementation of rawdb.ReadHeader
func ReadHeader(tx ethdb.Tx, hash common.Hash, number uint64) (*types.Header, error) {
	data, err := ReadHeaderRLP(tx, hash, number)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, err
	}
	header := new(types.Header)
	if err := rlp.Decode(bytes.NewReader(data), header); err != nil {
		return nil, fmt.Errorf("invalid block header RLP. Hash: %s. Err: %w", hash, err)
	}
	return header, nil
}

// ReadBodyRLP retrieves the block body (transactions and uncles) in RLP encoding.
func ReadBodyRLP(tx ethdb.Tx, hash common.Hash, number uint64) (rlp.RawValue, error) {
	bucket := tx.Bucket(dbutils.BlockBodyPrefix)

	if bucket == nil {
		return rlp.RawValue{}, fmt.Errorf("bucket %s not found", dbutils.HeaderPrefix)
	}

	if debug.IsBlockCompressionEnabled() {
		data, err := bucket.Get(dbutils.BlockBodyKey(number, hash))
		if err != nil {
			return nil, err
		}
		return snappy.Decode(nil, data)
	}

	return bucket.Get(dbutils.BlockBodyKey(number, hash))
}

// ReadBody reimplementation of rawdb.ReadBody
func ReadBody(tx ethdb.Tx, hash common.Hash, number uint64) (*types.Body, error) {
	data, err := ReadBodyRLP(tx, hash, number)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, nil
	}
	body := new(types.Body)
	if err := rlp.Decode(bytes.NewReader(data), body); err != nil {
		return nil, fmt.Errorf("invalid block body RLP: %s, %w", hash, err)
	}
	// Post-processing
	body.SendersToTxs()
	return body, nil
}

func ReadLastBlockNumber(tx ethdb.Tx) (uint64, error) {
	b := tx.Bucket(dbutils.HeadHeaderKey)

	if b == nil {
		return 0, fmt.Errorf("bucket %s not found", dbutils.HeadHeaderKey)
	}
	blockHashData, err := b.Get(dbutils.HeadHeaderKey)
	if err != nil {
		return 0, err
	}
	if len(blockHashData) != common.HashLength {
		return 0, fmt.Errorf("head header hash not found or wrong size: %x", blockHashData)
	}
	b1 := tx.Bucket(dbutils.HeaderNumberPrefix)

	if b1 == nil {
		return 0, fmt.Errorf("bucket %s not found", dbutils.HeaderNumberPrefix)
	}
	blockNumberData, err := b1.Get(blockHashData)
	if err != nil {
		return 0, err
	}

	if len(blockNumberData) != 8 {
		return 0, fmt.Errorf("head block number not found or wrong size: %x", blockNumberData)
	}

	return binary.BigEndian.Uint64(blockNumberData), nil
}
