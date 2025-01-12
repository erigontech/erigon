package beacon

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/rpc"
)

type OpLegacy struct{}

func (o *OpLegacy) Author(header *types.Header) (common.Address, error) {
	return header.Coinbase, nil
}

func (o *OpLegacy) VerifyHeader(chain consensus.ChainHeaderReader, header *types.Header, _ bool) error {
	// Short circuit if the header is known, or its parent not
	number := header.Number.Uint64()
	if chain.GetHeader(header.Hash(), number) != nil {
		return nil
	}
	parent := chain.GetHeader(header.ParentHash, number-1)
	if parent == nil {
		return consensus.ErrUnknownAncestor
	}
	// legacy chain is verified by block-hash reverse sync otherwise
	return nil
}

func (o *OpLegacy) VerifyHeaders(chain consensus.ChainHeaderReader, headers []*types.Header) (chan<- struct{}, <-chan error) {
	abort := make(chan struct{})
	results := make(chan error, len(headers))

	for i := range headers {
		// legacy chain is verified by block-hash reverse sync
		var parent *types.Header
		if i == 0 {
			parent = chain.GetHeader(headers[0].ParentHash, headers[0].Number.Uint64()-1)
		} else if headers[i-1].Hash() == headers[i].ParentHash {
			parent = headers[i-1]
		}
		var err error
		if parent == nil {
			err = consensus.ErrUnknownAncestor
		}
		results <- err
	}
	return abort, results
}

func (o *OpLegacy) VerifyUncles(consensus.ChainReader, *types.Header, []*types.Header) error {
	return nil
}

func (o *OpLegacy) Finalize(config *chain.Config, header *types.Header, state *state.IntraBlockState,
	txs types.Transactions, uncles []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal, chain consensus.ChainReader, syscall consensus.SystemCall, logger log.Logger,
) (types.Transactions, types.Receipts, types.FlatRequests, error) {
	return txs, receipts, nil, nil
}

func (o *OpLegacy) FinalizeAndAssemble(config *chain.Config, header *types.Header, state *state.IntraBlockState,
	txs types.Transactions, uncles []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal, chain consensus.ChainReader, syscall consensus.SystemCall, call consensus.Call, logger log.Logger,
) (*types.Block, types.Transactions, types.Receipts, types.FlatRequests, error) {
	return types.NewBlock(header, txs, uncles, receipts, withdrawals), txs, receipts, nil, nil
}

func (o *OpLegacy) SealHash(header *types.Header) common.Hash {
	panic(fmt.Errorf("cannot compute pow/poa seal-hash for legacy block header: %s (num %d)", header.Hash(), header.Number))
}

func (o *OpLegacy) CalcDifficulty(chain consensus.ChainHeaderReader, time uint64, parentTime uint64, parentDifficulty *big.Int, parentNumber uint64,
	parentHash common.Hash, parentUncleHash common.Hash, parentAuRaStep uint64) *big.Int {
	return big.NewInt(0)
}

func (o *OpLegacy) CalculateRewards(config *chain.Config, header *types.Header, uncles []*types.Header, call consensus.SystemCall) ([]consensus.Reward, error) {
	return nil, nil
}

func (o *OpLegacy) APIs(chain consensus.ChainHeaderReader) []rpc.API {
	return nil
}

func (o *OpLegacy) Close() error {
	return nil
}

func (o *OpLegacy) GetPostApplyMessageFunc() evmtypes.PostApplyMessageFunc {
	return nil
}

func (o *OpLegacy) GetTransferFunc() evmtypes.TransferFunc {
	return consensus.Transfer
}

func (o *OpLegacy) Initialize(config *chain.Config, chain consensus.ChainHeaderReader, header *types.Header,
	state *state.IntraBlockState, syscall consensus.SysCallCustom, logger log.Logger, tracer *tracing.Hooks) {
}

func (c *OpLegacy) IsServiceTransaction(sender common.Address, syscall consensus.SystemCall) bool {
	return false
}

func (c *OpLegacy) Prepare(chain consensus.ChainHeaderReader, header *types.Header, state *state.IntraBlockState) error {
	return fmt.Errorf("cannot prepare for legacy block header: %s (num %d)", header.Hash(), header.Number)
}

func (c *OpLegacy) Seal(chain consensus.ChainHeaderReader, blockWithReceipts *types.BlockWithReceipts, results chan<- *types.BlockWithReceipts, stop <-chan struct{}) error {
	return errors.New("Seal is not supported for legacy block header")
}

// Type returns underlying consensus engine
func (c *OpLegacy) Type() chain.ConsensusName {
	return chain.OpLegacyConsensus
}

var _ consensus.Engine = (*OpLegacy)(nil)
