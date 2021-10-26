package adapter

import (
	"math/big"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/rpc"
)

type chainContext struct {
	tx kv.Tx
}

func NewChainContext(tx kv.Tx) *chainContext {
	return &chainContext{
		tx: tx,
	}
}

type powEngine struct {
}

func (c *powEngine) VerifyHeader(chain consensus.ChainHeaderReader, header *types.Header, seal bool) error {

	panic("must not be called")
}
func (c *powEngine) VerifyHeaders(chain consensus.ChainHeaderReader, headers []*types.Header, seals []bool) error {
	panic("must not be called")
}
func (c *powEngine) VerifyUncles(chain consensus.ChainReader, block *types.Header, uncles []*types.Header) error {
	panic("must not be called")
}
func (c *powEngine) VerifySeal(chain consensus.ChainHeaderReader, header *types.Header) error {
	panic("must not be called")
}
func (c *powEngine) Prepare(chain consensus.ChainHeaderReader, header *types.Header, syscall consensus.SystemCall) error {
	panic("must not be called")
}
func (c *powEngine) Initialize(config *params.ChainConfig, chain consensus.ChainHeaderReader, e consensus.EpochReader, header *types.Header, txs []types.Transaction, uncles []*types.Header, syscall consensus.SystemCall) {
	panic("must not be called")
}
func (c *powEngine) Finalize(config *params.ChainConfig, header *types.Header, state *state.IntraBlockState, txs []types.Transaction, uncles []*types.Header, r types.Receipts, e consensus.EpochReader, chain consensus.ChainHeaderReader, syscall consensus.SystemCall) error {
	panic("must not be called")
}
func (c *powEngine) FinalizeAndAssemble(chainConfig *params.ChainConfig, header *types.Header, state *state.IntraBlockState,
	txs []types.Transaction, uncles []*types.Header, receipts types.Receipts,
	e consensus.EpochReader, h consensus.ChainHeaderReader, syscall consensus.SystemCall, call consensus.Call) (*types.Block, error) {
	panic("must not be called")
}

func (c *powEngine) Seal(chain consensus.ChainHeaderReader, block *types.Block, results chan<- *types.Block, stop <-chan struct{}) error {
	panic("must not be called")
}
func (c *powEngine) SealHash(header *types.Header) common.Hash {
	panic("must not be called")
}

func (c *powEngine) GenerateSeal(chain consensus.ChainHeaderReader, currnt, parent *types.Header, call consensus.Call) []rlp.RawValue {
	return nil
}

func (c *powEngine) CalcDifficulty(chain consensus.ChainHeaderReader, time, parentTime uint64, parentDifficulty *big.Int, parentNumber uint64, parentHash, parentUncleHash common.Hash, _ []rlp.RawValue) *big.Int {
	panic("must not be called")
}
func (c *powEngine) APIs(chain consensus.ChainHeaderReader) []rpc.API {
	panic("must not be called")
}

func (c *powEngine) Close() error {
	panic("must not be called")
}

func (c *powEngine) Author(header *types.Header) (common.Address, error) {
	return header.Coinbase, nil
}

func (c *chainContext) GetHeader(hash common.Hash, number uint64) *types.Header {
	return rawdb.ReadHeader(c.tx, hash, number)
}

func (c *chainContext) Engine() consensus.Engine {
	return &powEngine{}
}
