package testutil

import (
	"math/big"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	rulesif "github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/types"
)

// Compile-time interface check.
var _ rulesif.ChainReader = (*lightChainReader)(nil)

// lightChainReader implements rules.ChainReader with in-memory maps
// for headers and total difficulties, falling back to rawdb reads on
// the provided tx for genesis data.
type lightChainReader struct {
	config  *chain.Config
	headers map[common.Hash]*types.Header // hash -> header
	tds     map[common.Hash]*big.Int      // hash -> td
	tx      kv.Tx                         // fallback for genesis reads
}

func (cr *lightChainReader) Config() *chain.Config                  { return cr.config }
func (cr *lightChainReader) CurrentHeader() *types.Header           { return nil }
func (cr *lightChainReader) CurrentFinalizedHeader() *types.Header  { return nil }
func (cr *lightChainReader) CurrentSafeHeader() *types.Header       { return nil }
func (cr *lightChainReader) FrozenBlocks() uint64                    { return 0 }
func (cr *lightChainReader) FrozenBorBlocks(bool) uint64             { return 0 }

func (cr *lightChainReader) GetHeader(hash common.Hash, number uint64) *types.Header {
	if h, ok := cr.headers[hash]; ok {
		return h
	}
	return rawdb.ReadHeader(cr.tx, hash, number)
}

func (cr *lightChainReader) GetHeaderByNumber(number uint64) *types.Header {
	hash, err := rawdb.ReadCanonicalHash(cr.tx, number)
	if err != nil || hash == (common.Hash{}) {
		return nil
	}
	return cr.GetHeader(hash, number)
}

func (cr *lightChainReader) GetHeaderByHash(hash common.Hash) *types.Header {
	if h, ok := cr.headers[hash]; ok {
		return h
	}
	num := rawdb.ReadHeaderNumber(cr.tx, hash)
	if num == nil {
		return nil
	}
	return rawdb.ReadHeader(cr.tx, hash, *num)
}

func (cr *lightChainReader) GetTd(hash common.Hash, number uint64) *big.Int {
	if td, ok := cr.tds[hash]; ok {
		return td
	}
	td, _ := rawdb.ReadTd(cr.tx, hash, number)
	return td
}

func (cr *lightChainReader) GetBlock(hash common.Hash, number uint64) *types.Block {
	return rawdb.ReadBlock(cr.tx, hash, number)
}

func (cr *lightChainReader) HasBlock(hash common.Hash, number uint64) bool {
	return rawdb.ReadHeader(cr.tx, hash, number) != nil
}
