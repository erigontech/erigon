package taiko

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/erigontech/erigon-lib/chain"
	params2 "github.com/erigontech/erigon-lib/chain/params"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/rpc"
	"github.com/holiman/uint256"
)

var (
	ErrOlderBlockTime       = errors.New("timestamp older than parent")
	ErrUnclesNotEmpty       = errors.New("uncles not empty")
	ErrEmptyBasefee         = errors.New("empty base fee")
	ErrEmptyWithdrawalsHash = errors.New("withdrawals hash missing")
	ErrAnchorTxNotFound     = errors.New("anchor transaction not found")

	GoldenTouchAccount   = common.HexToAddress("0x0000777735367b36bC9B61C50022d9D0700dB4Ec")
	TaikoL2AddressSuffix = "10001"
	AnchorSelector       = crypto.Keccak256([]byte("anchor(bytes32,bytes32,uint64,uint32)"))[:4]
	AnchorV2Selector     = crypto.Keccak256(
		[]byte("anchorV2(uint64,bytes32,uint32,(uint8,uint8,uint32,uint64,uint32))"),
	)[:4]
	AnchorV3Selector = crypto.Keccak256(
		[]byte("anchorV3(uint64,bytes32,uint32,(uint8,uint8,uint32,uint64,uint32),bytes32[])"),
	)[:4]
	AnchorGasLimit   = uint64(250_000)
	AnchorV3GasLimit = uint64(1_000_000)
)

// Taiko is a consensus engine used by L2 rollup.
type Taiko struct {
	chainConfig    *chain.Config
	taikoL2Address common.Address
	chainDB        kv.RwDB
}

var _ = new(Taiko)

func New(chainConfig *chain.Config, chainDB kv.RwDB) *Taiko {
	taikoL2AddressPrefix := strings.TrimPrefix(chainConfig.ChainID.String(), "0")

	return &Taiko{
		chainConfig: chainConfig,
		taikoL2Address: common.HexToAddress(
			"0x" +
				taikoL2AddressPrefix +
				strings.Repeat("0", length.Addr*2-len(taikoL2AddressPrefix)-len(TaikoL2AddressSuffix)) +
				TaikoL2AddressSuffix,
		),
		chainDB: chainDB,
	}
}

// check all method stubs for interface `Engine` without affect performance.
var _ consensus.Engine = (*Taiko)(nil)

func (t *Taiko) Initialize(config *chain.Config, chain consensus.ChainHeaderReader, header *types.Header,
	state *state.IntraBlockState, syscall consensus.SysCallCustom, logger log.Logger, tracer *tracing.Hooks) {
}

// Author retrieves the Ethereum address of the account that minted the given
// block, who proposes the block (not the prover).
func (t *Taiko) Author(header *types.Header) (common.Address, error) {
	return header.Coinbase, nil
}

// VerifyHeader checks whether a header conforms to the consensus rules of a
// given engine. Verifying the seal may be done optionally here, or explicitly
// via the VerifySeal method.
func (t *Taiko) VerifyHeader(chain consensus.ChainHeaderReader, header *types.Header, _ bool) error {
	// Short circuit if the header is known, or its parent not
	number := header.Number.Uint64()
	if chain.GetHeader(header.Hash(), number) != nil {
		return nil
	}
	parent := chain.GetHeaderByHash(header.ParentHash)

	if parent == nil {
		return consensus.ErrUnknownAncestor
	}
	// Sanity checks passed, do a proper verification
	return t.verifyHeader(header, parent, time.Now().Unix())
}

// VerifyHeaders is similar to VerifyHeader, but verifies a batch of headers
// concurrently. The method returns a quit channel to abort the operations and
// a results channel to retrieve the async verifications (the order is that of
// the input slice).
func (t *Taiko) VerifyHeaders(chain consensus.ChainHeaderReader, headers []*types.Header) (chan<- struct{}, <-chan error) {
	if len(headers) == 0 {
		return make(chan struct{}), make(chan error, len(headers))
	}
	abort := make(chan struct{})
	results := make(chan error, len(headers))
	unixNow := time.Now().Unix()

	go func() {
		for i, header := range headers {
			var parent *types.Header
			if i == 0 {
				parent = chain.GetHeader(headers[0].ParentHash, headers[0].Number.Uint64()-1)
			} else if headers[i-1].Hash() == headers[i].ParentHash {
				parent = headers[i-1]
			}
			var err error
			if parent == nil {
				err = consensus.ErrUnknownAncestor
			} else {
				err = t.verifyHeader(header, parent, unixNow)
			}
			select {
			case <-abort:
				return
			case results <- err:
			}
		}
	}()
	return abort, results
}

func (t *Taiko) verifyHeader(header, parent *types.Header, unixNow int64) error {
	// Ensure that the header's extra-data section is of a reasonable size (<= 32 bytes)
	if uint64(len(header.Extra)) > params2.MaximumExtraDataSize {
		return fmt.Errorf("extra-data too long: %d > %d", len(header.Extra), params2.MaximumExtraDataSize)
	}

	// Timestamp should later than or equal to parent (when many L2 blocks included in one L1 block)
	if header.Time < parent.Time {
		return ErrOlderBlockTime
	}

	// Verify that the block number is parent's +1
	if diff := new(big.Int).Sub(header.Number, parent.Number); diff.Cmp(big.NewInt(1)) != 0 {
		return consensus.ErrInvalidNumber
	}

	// Difficulty should always be zero
	if header.Difficulty != nil && header.Difficulty.Cmp(common.Big0) != 0 {
		return fmt.Errorf("invalid difficulty: have %v, want %v", header.Difficulty, common.Big0)
	}

	// Verify that the gas limit is <= 2^63-1
	if header.GasLimit > params2.MaxGasLimit {
		return fmt.Errorf("invalid gasLimit: have %v, max %v", header.GasLimit, params2.MaxGasLimit)
	}

	// Verify that the gasUsed is <= gasLimit
	if header.GasUsed > header.GasLimit {
		return fmt.Errorf("invalid gasUsed: have %d, gasLimit %d", header.GasUsed, header.GasLimit)
	}

	// Uncles should be empty
	if header.UncleHash != types.CalcUncleHash(nil) {
		return ErrUnclesNotEmpty
	}

	// BaseFee should not be empty
	if header.BaseFee == nil {
		return ErrEmptyBasefee
	}

	// WithdrawalsHash should not be empty
	if header.WithdrawalsHash == nil {
		return ErrEmptyWithdrawalsHash
	}

	roTx, err := t.chainDB.BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer roTx.Rollback()
	l1Origin, err := rawdb.ReadL1Origin(roTx, header.Number)
	if err != nil {
		return err
	}

	// If the current block is not a preconfirmation block, then check the timestamp.
	if l1Origin != nil && !l1Origin.IsPreconfBlock() && header.Time > uint64(unixNow) {
		return consensus.ErrFutureBlock
	}

	return nil
}

// VerifyUncles verifies that the given block's uncles conform to the consensus
// rules of a given engine.
//
// always returning an error for any uncles as this consensus mechanism doesn't permit uncles.
func (t *Taiko) VerifyUncles(chain consensus.ChainReader, header *types.Header, uncles []*types.Header) error {
	if len(uncles) > 0 {
		return ErrUnclesNotEmpty
	}

	return nil
}

// Prepare initializes the consensus fields of a block header according to the
// rules of a particular engine. The changes are executed inline.
func (t *Taiko) Prepare(chain consensus.ChainHeaderReader, header *types.Header, state *state.IntraBlockState) error {
	parent := chain.GetHeader(header.ParentHash, header.Number.Uint64()-1)
	if parent == nil {
		return consensus.ErrUnknownAncestor
	}
	header.Difficulty = common.Big0
	return nil
}

// Finalize runs any post-transaction state modifications (e.g. block rewards)
// but does not assemble the block.
//
// Note: The block header and state database might be updated to reflect any
// consensus rules that happen at finalization (e.g. block rewards).
func (t *Taiko) Finalize(config *chain.Config, header *types.Header, state *state.IntraBlockState,
	txs types.Transactions, uncles []*types.Header, r types.Receipts, withdrawals []*types.Withdrawal,
	chain consensus.ChainReader, syscall consensus.SystemCall, skipReceiptsEval bool, logger log.Logger,
) (types.Transactions, types.Receipts, types.FlatRequests, error) {
	// no block rewards in l2
	header.UncleHash = types.CalcUncleHash(nil)
	header.Difficulty = common.Big0
	// Withdrawals processing.
	for _, w := range withdrawals {
		state.AddBalance(
			w.Address,
			uint256.MustFromBig(new(big.Int).SetUint64(w.Amount)),
			tracing.BalanceIncreaseWithdrawal,
		)
	}
	return txs, r, nil, nil
}

// FinalizeAndAssemble runs any post-transaction state modifications (e.g. block
// rewards) and assembles the final block.
//
// Note: The block header and state database might be updated to reflect any
// consensus rules that happen at finalization (e.g. block rewards).
func (t *Taiko) FinalizeAndAssemble(chainConfig *chain.Config, header *types.Header, state *state.IntraBlockState,
	txs types.Transactions, uncles []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal, chain consensus.ChainReader, syscall consensus.SystemCall, call consensus.Call, logger log.Logger,
) (*types.Block, types.Transactions, types.Receipts, types.FlatRequests, error) {
	if withdrawals == nil {
		withdrawals = make([]*types.Withdrawal, 0)
	}

	// Verify anchor transaction
	if len(txs) != 0 { // Transactions list might be empty when building empty payload.
		isAnchor, err := t.ValidateAnchorTx(txs[0], header)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		if !isAnchor {
			return nil, nil, nil, nil, ErrAnchorTxNotFound
		}
	}

	// Finalize block
	t.Finalize(chainConfig, header, state, txs, uncles, receipts, withdrawals, chain, syscall /* sikReceiptsEval */, false, logger)
	// header.Root = state.IntermediateRoot(true)
	return types.NewBlockForAsembling(header, txs, uncles, receipts, withdrawals), txs, receipts, nil, nil
}

// Seal generates a new sealing request for the given input block and pushes
// the result into the given channel.
//
// Note, the method returns immediately and will send the result async. More
// than one result may also be returned depending on the consensus algorithm.
func (t *Taiko) Seal(chain consensus.ChainHeaderReader, blockWithReceipts *types.BlockWithReceipts, results chan<- *types.BlockWithReceipts, stop <-chan struct{}) error {
	header := blockWithReceipts.Block.Header()

	// Sealing the genesis block is not supported
	number := header.Number.Uint64()
	if number == 0 {
		return consensus.ErrInvalidNumber
	}

	sealedBlock := blockWithReceipts.Block.WithSeal(header)
	blockWithReceipts.Block = sealedBlock

	select {
	case results <- blockWithReceipts:
	case <-stop:
		return nil
	default:
		log.Warn("Sealing result is not read by miner", "sealHash", t.SealHash(header))
	}

	return nil
}

// SealHash returns the hash of a block prior to it being sealed.
func (t *Taiko) SealHash(header *types.Header) common.Hash {
	// Keccak(rlp(header))
	return header.Hash()
}

// CalcDifficulty is the difficulty adjustment algorithm. It returns the difficulty
// that a new block should have.
func (t *Taiko) CalcDifficulty(chain consensus.ChainHeaderReader, _, _ uint64, _ *big.Int, parentNumber uint64, parentHash, _ common.Hash, _ uint64) *big.Int {
	return common.Big0
}

func (t *Taiko) CalculateRewards(config *chain.Config, header *types.Header, uncles []*types.Header, syscall consensus.SystemCall,
) ([]consensus.Reward, error) {
	return []consensus.Reward{}, nil
}

func (t *Taiko) GetPostApplyMessageFunc() evmtypes.PostApplyMessageFunc {
	return nil
}

func (t *Taiko) GetTransferFunc() evmtypes.TransferFunc {
	return consensus.Transfer
}

func (t *Taiko) IsServiceTransaction(sender common.Address, syscall consensus.SystemCall) bool {
	return false
}

// Type returns underlying consensus engine
func (t *Taiko) Type() chain.ConsensusName {
	return chain.TaikoConsensus
}

// ValidateAnchorTx checks if the given transaction is a valid TaikoL2.anchor or TaikoL2.anchorV2 transaction.
func (t *Taiko) ValidateAnchorTx(tx types.Transaction, header *types.Header) (bool, error) {
	if tx.Type() != types.DynamicFeeTxType {
		return false, nil
	}

	if tx.GetTo() == nil || *tx.GetTo() != t.taikoL2Address {
		return false, nil
	}

	if !bytes.HasPrefix(tx.GetData(), AnchorSelector) &&
		!bytes.HasPrefix(tx.GetData(), AnchorV2Selector) &&
		!bytes.HasPrefix(tx.GetData(), AnchorV3Selector) {
		return false, nil
	}

	if tx.GetValue().Cmp(uint256.NewInt(0)) != 0 {
		return false, nil
	}

	if t.chainConfig.IsPacaya(header.Number.Uint64()) {
		if tx.GetGasLimit() != AnchorV3GasLimit {
			return false, nil
		}
	} else {
		if tx.GetGasLimit() != AnchorGasLimit {
			return false, nil
		}
	}

	if tx.GetFeeCap().ToBig().Cmp(header.BaseFee) != 0 {
		return false, nil
	}

	s := types.MakeSigner(t.chainConfig, header.Number.Uint64(), header.Time)

	addr, err := s.Sender(tx)
	if err != nil {
		return false, err
	}

	return strings.EqualFold(addr.String(), GoldenTouchAccount.String()), nil
}

// APIs returns the RPC APIs this consensus engine provides.
func (t *Taiko) APIs(chain consensus.ChainHeaderReader) []rpc.API {
	return nil
}

// Close terminates any background threads maintained by the consensus engine.
func (t *Taiko) Close() error {
	return nil
}
