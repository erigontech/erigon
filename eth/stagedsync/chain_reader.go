package stagedsync

import (
	"math/big"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
)

// Implements consensus.ChainReader
type ChainReader struct {
	Cfg chain.Config

	Db kv.Getter
}

// Config retrieves the blockchain's chain configuration.
func (cr ChainReader) Config() *chain.Config {
	return &cr.Cfg
}

// CurrentHeader retrieves the current header from the local chain.
func (cr ChainReader) CurrentHeader() *types.Header {
	hash := rawdb.ReadHeadHeaderHash(cr.Db)
	number := rawdb.ReadHeaderNumber(cr.Db, hash)
	return rawdb.ReadHeader(cr.Db, hash, *number)
}

// GetHeader retrieves a block header from the database by hash and number.
func (cr ChainReader) GetHeader(hash libcommon.Hash, number uint64) *types.Header {
	return rawdb.ReadHeader(cr.Db, hash, number)
}

// GetHeaderByNumber retrieves a block header from the database by number.
func (cr ChainReader) GetHeaderByNumber(number uint64) *types.Header {
	hash, err := rawdb.ReadCanonicalHash(cr.Db, number)
	if err != nil {
		log.Error("ReadCanonicalHash failed", "err", err)
		return nil
	}
	return rawdb.ReadHeader(cr.Db, hash, number)
}

// GetHeaderByHash retrieves a block header from the database by its hash.
func (cr ChainReader) GetHeaderByHash(hash libcommon.Hash) *types.Header {
	number := rawdb.ReadHeaderNumber(cr.Db, hash)
	return rawdb.ReadHeader(cr.Db, hash, *number)
}

// GetBlock retrieves a block from the database by hash and number.
func (cr ChainReader) GetBlock(hash libcommon.Hash, number uint64) *types.Block {
	return rawdb.ReadBlock(cr.Db, hash, number)
}

// HasBlock retrieves a block from the database by hash and number.
func (cr ChainReader) HasBlock(hash libcommon.Hash, number uint64) bool {
	return rawdb.HasBlock(cr.Db, hash, number)
}

// GetTd retrieves the total difficulty from the database by hash and number.
func (cr ChainReader) GetTd(hash libcommon.Hash, number uint64) *big.Int {
	td, err := rawdb.ReadTd(cr.Db, hash, number)
	if err != nil {
		log.Error("ReadTd failed", "err", err)
		return nil
	}
	return td
}
