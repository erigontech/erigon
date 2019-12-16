package remote

import (
	"bytes"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

// ReadTd reimplemented rawdb.ReadTd
func ReadTd(tx *Tx, hash common.Hash, number uint64) *hexutil.Big {
	bucket := tx.Bucket(dbutils.HeaderPrefix)
	if bucket == nil {
		return nil
	}

	data := bucket.Get(dbutils.HeaderTDKey(number, hash))
	if len(data) == 0 {
		return nil
	}
	td := new(big.Int)
	if err := rlp.Decode(bytes.NewReader(data), td); err != nil {
		log.Error("Invalid block total difficulty RLP", "hash", hash, "err", err)
		return nil
	}
	return (*hexutil.Big)(td)
}

// ReadCanonicalHash reimplementation of rawdb.ReadCanonicalHash
func ReadCanonicalHash(tx *Tx, number uint64) common.Hash {
	bucket := tx.Bucket(dbutils.HeaderPrefix)
	if bucket == nil {
		return common.Hash{}
		//return fmt.Errorf("bucket %s not found", dbutils.HeaderPrefix)
	}

	data := bucket.Get(dbutils.HeaderHashKey(number))
	if len(data) == 0 {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

// GetBlockByNumber reimplementation of chain.GetBlockByNumber
func GetBlockByNumber(tx *Tx, number uint64) *types.Block {
	hash := ReadCanonicalHash(tx, number)
	if hash == (common.Hash{}) {
		return nil
	}
	return ReadBlock(tx, hash, number)
}

// ReadBlock reimplementation of rawdb.ReadBlock
func ReadBlock(tx *Tx, hash common.Hash, number uint64) *types.Block {
	header := ReadHeader(tx, hash, number)
	if header == nil {
		return nil
	}
	body := ReadBody(tx, hash, number)
	if body == nil {
		return nil
	}
	return types.NewBlockWithHeader(header).WithBody(body.Transactions, body.Uncles)
}

// ReadHeaderRLP reimplementation of rawdb.ReadHeaderRLP
func ReadHeaderRLP(tx *Tx, hash common.Hash, number uint64) rlp.RawValue {
	bucket := tx.Bucket(dbutils.HeaderPrefix)
	if bucket == nil {
		//return fmt.Errorf("bucket %s not found", dbutils.HeaderPrefix)
		log.Error("Bucket not founc", "error", dbutils.HeaderPrefix)
		return rlp.RawValue{}
	}
	return bucket.Get(dbutils.HeaderKey(number, hash))
}

// ReadHeader reimplementation of rawdb.ReadHeader
func ReadHeader(tx *Tx, hash common.Hash, number uint64) *types.Header {
	data := ReadHeaderRLP(tx, hash, number)
	if len(data) == 0 {
		return nil
	}
	header := new(types.Header)
	if err := rlp.Decode(bytes.NewReader(data), header); err != nil {
		log.Error("Invalid block header RLP", "hash", hash, "err", err)
		return nil
	}
	return header
}

// ReadBodyRLP retrieves the block body (transactions and uncles) in RLP encoding.
func ReadBodyRLP(tx *Tx, hash common.Hash, number uint64) rlp.RawValue {
	bucket := tx.Bucket(dbutils.BlockBodyPrefix)
	if bucket == nil {
		//return fmt.Errorf("bucket %s not found", dbutils.HeaderPrefix)
		log.Error("Bucket not founc", "error", dbutils.BlockBodyPrefix)
		return rlp.RawValue{}
	}
	return bucket.Get(dbutils.BlockBodyKey(number, hash))
}

// ReadBody reimplementation of rawdb.ReadBody
func ReadBody(tx *Tx, hash common.Hash, number uint64) *types.Body {
	data := ReadBodyRLP(tx, hash, number)
	if len(data) == 0 {
		return nil
	}
	body := new(types.Body)
	if err := rlp.Decode(bytes.NewReader(data), body); err != nil {
		log.Error("Invalid block body RLP", "hash", hash, "err", err)
		return nil
	}
	// Post-processing
	body.SendersToTxs()
	return body
}
