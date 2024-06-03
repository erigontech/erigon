package consensuschain

import (
	"context"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

type Reader struct {
	config      *chain.Config
	tx          kv.Tx
	blockReader services.FullBlockReader
	logger      log.Logger
}

func NewReader(config *chain.Config, tx kv.Tx, blockReader services.FullBlockReader, logger log.Logger) *Reader {
	return &Reader{config, tx, blockReader, logger}
}

func (cr Reader) Config() *chain.Config        { return cr.config }
func (cr Reader) CurrentHeader() *types.Header { panic("") }
func (cr Reader) GetHeader(hash common.Hash, number uint64) *types.Header {
	if cr.blockReader != nil {
		h, _ := cr.blockReader.Header(context.Background(), cr.tx, hash, number)
		return h
	}
	return rawdb.ReadHeader(cr.tx, hash, number)
}
func (cr Reader) GetHeaderByNumber(number uint64) *types.Header {
	if cr.blockReader != nil {
		h, _ := cr.blockReader.HeaderByNumber(context.Background(), cr.tx, number)
		return h
	}
	return rawdb.ReadHeaderByNumber(cr.tx, number)

}
func (cr Reader) GetHeaderByHash(hash common.Hash) *types.Header {
	if cr.blockReader != nil {
		number := rawdb.ReadHeaderNumber(cr.tx, hash)
		if number == nil {
			return nil
		}
		return cr.GetHeader(hash, *number)
	}
	h, _ := rawdb.ReadHeaderByHash(cr.tx, hash)
	return h
}
func (cr Reader) GetTd(hash common.Hash, number uint64) *big.Int {
	td, err := rawdb.ReadTd(cr.tx, hash, number)
	if err != nil {
		cr.logger.Warn("ReadTd failed", "err", err)
		return nil
	}
	return td
}
func (cr Reader) FrozenBlocks() uint64    { return cr.blockReader.FrozenBlocks() }
func (cr Reader) FrozenBorBlocks() uint64 { return cr.blockReader.FrozenBorBlocks() }
func (cr Reader) GetBlock(hash common.Hash, number uint64) *types.Block {
	panic("")
}
func (cr Reader) HasBlock(hash common.Hash, number uint64) bool {
	panic("")
}
func (cr Reader) BorStartEventID(hash common.Hash, number uint64) uint64 {
	id, err := cr.blockReader.BorStartEventID(context.Background(), cr.tx, hash, number)
	if err != nil {
		cr.logger.Warn("BorEventsByBlock failed", "err", err)
		return 0
	}
	return id

}
func (cr Reader) BorEventsByBlock(hash common.Hash, number uint64) []rlp.RawValue {
	events, err := cr.blockReader.EventsByBlock(context.Background(), cr.tx, hash, number)
	if err != nil {
		cr.logger.Warn("BorEventsByBlock failed", "err", err)
		return nil
	}
	return events
}

func (cr Reader) BorSpan(spanId uint64) []byte {
	span, err := cr.blockReader.Span(context.Background(), cr.tx, spanId)
	if err != nil {
		log.Warn("BorSpan failed", "err", err)
		return nil
	}
	return span
}
