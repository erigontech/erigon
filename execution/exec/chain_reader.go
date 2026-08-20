package exec

import (
	"context"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

type ChainReaderImpl struct {
	config      *chain.Config
	tx          kv.Tx
	blockReader dbservices.FullBlockReader
	logger      log.Logger
}

func NewChainReader(config *chain.Config, tx kv.Tx, blockReader dbservices.FullBlockReader, logger log.Logger) *ChainReaderImpl {
	return &ChainReaderImpl{config, tx, blockReader, logger}
}

func (cr ChainReaderImpl) Config() *chain.Config { return cr.config }

func (cr ChainReaderImpl) CurrentHeader() (*types.Header, bool, error) {
	return rawdb.ReadCurrentHeader(cr.tx)
}

func (cr ChainReaderImpl) CurrentFinalizedHeader() (*types.Header, bool, error) {
	hash, ok, err := rawdb.ReadForkchoiceFinalized(cr.tx)
	if err != nil || !ok {
		return nil, false, err
	}
	return cr.GetHeaderByHash(hash)
}

func (cr ChainReaderImpl) CurrentSafeHeader() (*types.Header, bool, error) {
	hash, ok, err := rawdb.ReadForkchoiceSafe(cr.tx)
	if err != nil || !ok {
		return nil, false, err
	}

	return cr.GetHeaderByHash(hash)
}

func (cr ChainReaderImpl) GetHeader(hash common.Hash, number uint64) (*types.Header, bool, error) {
	if cr.blockReader != nil {
		return cr.blockReader.Header(context.Background(), cr.tx, hash, number)
	}
	return rawdb.ReadHeader(cr.tx, hash, number)
}

func (cr ChainReaderImpl) GetHeaderByNumber(number uint64) (*types.Header, bool, error) {
	if cr.blockReader != nil {
		return cr.blockReader.HeaderByNumber(context.Background(), cr.tx, number)
	}
	return rawdb.ReadHeaderByNumber(cr.tx, number)
}

func (cr ChainReaderImpl) GetHeaderByHash(hash common.Hash) (*types.Header, bool, error) {
	if cr.blockReader != nil {
		return cr.blockReader.HeaderByHash(context.Background(), cr.tx, hash)
	}
	return rawdb.ReadHeaderByHash(cr.tx, hash)
}

func (cr ChainReaderImpl) GetTd(hash common.Hash, number uint64) (*uint256.Int, bool, error) {
	return rawdb.ReadTd(cr.tx, hash, number)
}

func (cr ChainReaderImpl) FrozenBlocks() uint64 { return cr.blockReader.FrozenBlocks() }

func (cr ChainReaderImpl) FrozenBorBlocks(align bool) uint64 {
	return cr.blockReader.FrozenBorBlocks(align)
}

func (cr ChainReaderImpl) GetBlock(hash common.Hash, number uint64) (*types.Block, bool, error) {
	b, _, ok, err := cr.blockReader.BlockWithSenders(context.Background(), cr.tx, hash, number)
	return b, ok, err
}

func (cr ChainReaderImpl) HasBlock(hash common.Hash, number uint64) (bool, error) {
	_, ok, err := cr.blockReader.BodyRlp(context.Background(), cr.tx, hash, number)
	return ok, err
}
