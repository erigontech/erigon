package exec

import (
	"context"
	"math/big"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

type ChainReaderImpl struct {
	config      *chain.Config
	tx          kv.Tx
	blockReader services.FullBlockReader
	logger      log.Logger
}

func NewChainReader(config *chain.Config, tx kv.Tx, blockReader services.FullBlockReader, logger log.Logger) *ChainReaderImpl {
	return &ChainReaderImpl{config, tx, blockReader, logger}
}

func (cr ChainReaderImpl) Config() *chain.Config        { return cr.config }
func (cr ChainReaderImpl) CurrentHeader() *types.Header { panic("") }
func (cr ChainReaderImpl) CurrentFinalizedHeader() *types.Header {
	hash := rawdb.ReadForkchoiceFinalized(cr.tx)
	if hash == (common.Hash{}) {
		return nil
	}
	return cr.GetHeaderByHash(hash)
}
func (cr ChainReaderImpl) CurrentSafeHeader() *types.Header {
	hash := rawdb.ReadForkchoiceSafe(cr.tx)
	if hash == (common.Hash{}) {
		return nil
	}

	return cr.GetHeaderByHash(hash)
}
func (cr ChainReaderImpl) GetHeader(hash common.Hash, number uint64) *types.Header {
	if cr.blockReader != nil {
		h, _ := cr.blockReader.Header(context.Background(), cr.tx, hash, number)
		return h
	}
	return rawdb.ReadHeader(cr.tx, hash, number)
}
func (cr ChainReaderImpl) GetHeaderByNumber(number uint64) *types.Header {
	if cr.blockReader != nil {
		h, _ := cr.blockReader.HeaderByNumber(context.Background(), cr.tx, number)
		return h
	}
	return rawdb.ReadHeaderByNumber(cr.tx, number)
}
func (cr ChainReaderImpl) GetHeaderByHash(hash common.Hash) *types.Header {
	if cr.blockReader != nil {
		h, _ := cr.blockReader.HeaderByHash(context.Background(), cr.tx, hash)
		return h
	}
	h, _ := rawdb.ReadHeaderByHash(cr.tx, hash)
	return h
}
func (cr ChainReaderImpl) GetTd(hash common.Hash, number uint64) *big.Int {
	td, err := rawdb.ReadTd(cr.tx, hash, number)
	if err != nil {
		cr.logger.Error("ReadTd failed", "err", err)
		return nil
	}
	return td
}
func (cr ChainReaderImpl) FrozenBlocks() uint64 { return cr.blockReader.FrozenBlocks() }
func (cr ChainReaderImpl) FrozenBorBlocks(align bool) uint64 {
	return cr.blockReader.FrozenBorBlocks(align)
}
func (cr ChainReaderImpl) GetBlock(hash common.Hash, number uint64) *types.Block {
	b, _, _ := cr.blockReader.BlockWithSenders(context.Background(), cr.tx, hash, number)
	return b
}
func (cr ChainReaderImpl) HasBlock(hash common.Hash, number uint64) bool {
	b, _ := cr.blockReader.BodyRlp(context.Background(), cr.tx, hash, number)
	return b != nil
}
