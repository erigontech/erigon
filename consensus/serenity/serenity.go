package serenity

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/aura"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
)

// Constants for Serenity as specified into https://eips.ethereum.org/EIPS/eip-2982
var (
	SerenityDifficulty = common.Big0        // Serenity block's difficulty is always 0.
	SerenityNonce      = types.BlockNonce{} // Serenity chain's nonces are 0.
	RewardSerenity     = big.NewInt(300000000000000000)
)

var (
	// errInvalidDifficulty is returned if the difficulty is non-zero.
	errInvalidDifficulty = errors.New("invalid difficulty")

	// errInvalidNonce is returned if the nonce is non-zero.
	errInvalidNonce = errors.New("invalid nonce")

	// errInvalidUncleHash is returned if a block contains an non-empty uncle list.
	errInvalidUncleHash = errors.New("non empty uncle hash")

	errOlderBlockTime = errors.New("timestamp older than parent")
)

// Serenity Consensus Engine for the Execution Layer.
// Serenity is a consensus engine that combines the eth1 consensus and proof-of-stake
// algorithm. The transition rule is described in the eth1/2 merge spec:
// https://eips.ethereum.org/EIPS/eip-3675
//
// Note: After the Merge the work is mostly done on the Consensus Layer, so nothing much is to be added on this side.
type Serenity struct {
	eth1Engine consensus.Engine // Original consensus engine used in eth1, e.g. ethash or clique
}

// New creates a new instance of the Serenity Engine with the given embedded eth1 engine.
func New(eth1Engine consensus.Engine) *Serenity {
	if _, ok := eth1Engine.(*Serenity); ok {
		panic("nested consensus engine")
	}
	return &Serenity{eth1Engine: eth1Engine}
}

// InnerEngine returns the embedded eth1 consensus engine.
func (s *Serenity) InnerEngine() consensus.Engine {
	return s.eth1Engine
}

// Type returns the type of the underlying consensus engine.
func (s *Serenity) Type() params.ConsensusType {
	return s.eth1Engine.Type()
}

// Author implements consensus.Engine, returning the header's coinbase as the
// proof-of-stake verified author of the block.
// This is thread-safe (only access the header.Coinbase or the underlying engine's thread-safe method)
func (s *Serenity) Author(header *types.Header) (common.Address, error) {
	if !IsPoSHeader(header) {
		return s.eth1Engine.Author(header)
	}
	return header.Coinbase, nil
}

// VerifyHeader checks whether a header conforms to the consensus rules of the
// stock Ethereum serenity engine.
func (s *Serenity) VerifyHeader(chain consensus.ChainHeaderReader, header *types.Header, seal bool) error {
	reached, err := IsTTDReached(chain, header.ParentHash, header.Number.Uint64()-1)
	if err != nil {
		return err
	}
	if !reached {
		// Not verifying seals if the TTD is passed
		return s.eth1Engine.VerifyHeader(chain, header, !chain.Config().TerminalTotalDifficultyPassed)
	}
	// Short circuit if the parent is not known
	parent := chain.GetHeader(header.ParentHash, header.Number.Uint64()-1)
	if parent == nil {
		return consensus.ErrUnknownAncestor
	}

	return s.verifyHeader(chain, header, parent)
}

// VerifyUncles implements consensus.Engine, always returning an error for any
// uncles as this consensus mechanism doesn't permit uncles.
func (s *Serenity) VerifyUncles(chain consensus.ChainReader, header *types.Header, uncles []*types.Header) error {
	if !IsPoSHeader(header) {
		return s.eth1Engine.VerifyUncles(chain, header, uncles)
	}
	if len(uncles) > 0 {
		return errors.New("uncles not allowed")
	}
	return nil
}

// Prepare makes sure difficulty and nonce are correct
func (s *Serenity) Prepare(chain consensus.ChainHeaderReader, header *types.Header, state *state.IntraBlockState) error {
	reached, err := IsTTDReached(chain, header.ParentHash, header.Number.Uint64()-1)
	if err != nil {
		return err
	}
	if !reached {
		return s.eth1Engine.Prepare(chain, header, state)
	}
	header.Difficulty = SerenityDifficulty
	header.Nonce = SerenityNonce
	return nil
}

func (s *Serenity) Finalize(config *params.ChainConfig, header *types.Header, state *state.IntraBlockState,
	txs types.Transactions, uncles []*types.Header, r types.Receipts, withdrawals []*types.Withdrawal,
	e consensus.EpochReader, chain consensus.ChainHeaderReader, syscall consensus.SystemCall,
) (types.Transactions, types.Receipts, error) {
	if !IsPoSHeader(header) {
		return s.eth1Engine.Finalize(config, header, state, txs, uncles, r, withdrawals, e, chain, syscall)
	}
	if auraEngine, ok := s.eth1Engine.(*aura.AuRa); ok {
		if err := auraEngine.ApplyRewards(header, state, syscall); err != nil {
			return nil, nil, err
		}
	}
	for _, w := range withdrawals {
		state.AddBalance(w.Address, &w.Amount)
	}
	return txs, r, nil
}

func (s *Serenity) FinalizeAndAssemble(config *params.ChainConfig, header *types.Header, state *state.IntraBlockState,
	txs types.Transactions, uncles []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal,
	e consensus.EpochReader, chain consensus.ChainHeaderReader, syscall consensus.SystemCall, call consensus.Call,
) (*types.Block, types.Transactions, types.Receipts, error) {
	if !IsPoSHeader(header) {
		return s.eth1Engine.FinalizeAndAssemble(config, header, state, txs, uncles, receipts, withdrawals, e, chain, syscall, call)
	}
	outTxs, outReceipts, err := s.Finalize(config, header, state, txs, uncles, receipts, withdrawals, e, chain, syscall)
	if err != nil {
		return nil, nil, nil, err
	}
	return types.NewBlock(header, outTxs, uncles, outReceipts, withdrawals), outTxs, outReceipts, nil
}

func (s *Serenity) SealHash(header *types.Header) (hash common.Hash) {
	return s.eth1Engine.SealHash(header)
}

func (s *Serenity) CalcDifficulty(chain consensus.ChainHeaderReader, time, parentTime uint64, parentDifficulty *big.Int, parentNumber uint64, parentHash, parentUncleHash common.Hash, parentAuRaStep uint64) *big.Int {
	reached, err := IsTTDReached(chain, parentHash, parentNumber)
	if err != nil {
		return nil
	}
	if !reached {
		return s.eth1Engine.CalcDifficulty(chain, time, parentTime, parentDifficulty, parentNumber, parentHash, parentUncleHash, parentAuRaStep)
	}
	return SerenityDifficulty
}

// verifyHeader checks whether a Proof-of-Stake header conforms to the consensus rules of the
// stock Ethereum consensus engine with EIP-3675 modifications.
func (s *Serenity) verifyHeader(chain consensus.ChainHeaderReader, header, parent *types.Header) error {

	if uint64(len(header.Extra)) > params.MaximumExtraDataSize {
		return fmt.Errorf("extra-data longer than %d bytes (%d)", params.MaximumExtraDataSize, len(header.Extra))
	}

	if header.Time <= parent.Time {
		return errOlderBlockTime
	}

	if header.Difficulty.Cmp(SerenityDifficulty) != 0 {
		return errInvalidDifficulty
	}

	if !bytes.Equal(header.Nonce[:], SerenityNonce[:]) {
		return errInvalidNonce
	}

	// Verify that the gas limit is within cap
	if header.GasLimit > params.MaxGasLimit {
		return fmt.Errorf("invalid gasLimit: have %v, max %v", header.GasLimit, params.MaxGasLimit)
	}
	// Verify that the gasUsed is <= gasLimit
	if header.GasUsed > header.GasLimit {
		return fmt.Errorf("invalid gasUsed: have %d, gasLimit %d", header.GasUsed, header.GasLimit)
	}

	// Verify that the block number is parent's +1
	if diff := new(big.Int).Sub(header.Number, parent.Number); diff.Cmp(common.Big1) != 0 {
		return consensus.ErrInvalidNumber
	}

	if header.UncleHash != types.EmptyUncleHash {
		return errInvalidUncleHash
	}

	if err := misc.VerifyEip1559Header(chain.Config(), parent, header); err != nil {
		return err
	}

	// Verify existence / non-existence of withdrawalsHash
	shanghai := chain.Config().IsShanghai(header.Time)
	if shanghai && header.WithdrawalsHash == nil {
		return fmt.Errorf("missing withdrawalsHash")
	}
	if !shanghai && header.WithdrawalsHash != nil {
		return consensus.ErrUnexpectedWithdrawals
	}
	return nil
}

func (s *Serenity) Seal(chain consensus.ChainHeaderReader, block *types.Block, results chan<- *types.Block, stop <-chan struct{}) error {
	if !IsPoSHeader(block.Header()) {
		return s.eth1Engine.Seal(chain, block, results, stop)
	}
	return nil
}

func (s *Serenity) GenerateSeal(chain consensus.ChainHeaderReader, currnt, parent *types.Header, call consensus.Call) []byte {
	return nil
}

func (s *Serenity) IsServiceTransaction(sender common.Address, syscall consensus.SystemCall) bool {
	return s.eth1Engine.IsServiceTransaction(sender, syscall)
}

func (s *Serenity) Initialize(config *params.ChainConfig, chain consensus.ChainHeaderReader, e consensus.EpochReader, header *types.Header, state *state.IntraBlockState, txs []types.Transaction, uncles []*types.Header, syscall consensus.SystemCall) {
	s.eth1Engine.Initialize(config, chain, e, header, state, txs, uncles, syscall)
}

func (s *Serenity) APIs(chain consensus.ChainHeaderReader) []rpc.API {
	return s.eth1Engine.APIs(chain)
}

func (s *Serenity) Close() error {
	return s.eth1Engine.Close()
}

// IsPoSHeader reports the header belongs to the PoS-stage with some special fields.
// This function is not suitable for a part of APIs like Prepare or CalcDifficulty
// because the header difficulty is not set yet.
func IsPoSHeader(header *types.Header) bool {
	if header.Difficulty == nil {
		panic("IsPoSHeader called with invalid difficulty")
	}
	return header.Difficulty.Cmp(SerenityDifficulty) == 0
}

// IsTTDReached checks if the TotalTerminalDifficulty has been surpassed on the `parentHash` block.
// It depends on the parentHash already being stored in the database.
// If the total difficulty is not stored in the database a ErrUnknownAncestorTD error is returned.
func IsTTDReached(chain consensus.ChainHeaderReader, parentHash common.Hash, number uint64) (bool, error) {
	if chain.Config().TerminalTotalDifficulty == nil {
		return false, nil
	}
	td := chain.GetTd(parentHash, number)
	if td == nil {
		return false, consensus.ErrUnknownAncestorTD
	}
	return td.Cmp(chain.Config().TerminalTotalDifficulty) >= 0, nil
}
