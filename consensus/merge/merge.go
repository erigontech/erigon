package merge

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/aura"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
)

// Constants for The Merge as specified by EIP-3675: Upgrade consensus to Proof-of-Stake
var (
	ProofOfStakeDifficulty = libcommon.Big0     // PoS block's difficulty is always 0
	ProofOfStakeNonce      = types.BlockNonce{} // PoS block's have all-zero nonces
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

// Merge Consensus Engine for the Execution Layer.
// Merge is a consensus engine that combines the eth1 consensus and proof-of-stake
// algorithm. The transition rule is described in the eth1/2 merge spec:
// https://eips.ethereum.org/EIPS/eip-3675
//
// Note: After the Merge the work is mostly done on the Consensus Layer, so nothing much is to be added on this side.
type Merge struct {
	eth1Engine consensus.Engine // Original consensus engine used in eth1, e.g. ethash or clique
}

// New creates a new instance of the Merge Engine with the given embedded eth1 engine.
func New(eth1Engine consensus.Engine) *Merge {
	if _, ok := eth1Engine.(*Merge); ok {
		panic("nested consensus engine")
	}
	return &Merge{eth1Engine: eth1Engine}
}

// InnerEngine returns the embedded eth1 consensus engine.
func (s *Merge) InnerEngine() consensus.Engine {
	return s.eth1Engine
}

// Type returns the type of the underlying consensus engine.
func (s *Merge) Type() chain.ConsensusName {
	return s.eth1Engine.Type()
}

// Author implements consensus.Engine, returning the header's coinbase as the
// proof-of-stake verified author of the block.
// This is thread-safe (only access the header.Coinbase or the underlying engine's thread-safe method)
func (s *Merge) Author(header *types.Header) (libcommon.Address, error) {
	if !IsPoSHeader(header) {
		return s.eth1Engine.Author(header)
	}
	return header.Coinbase, nil
}

func (s *Merge) VerifyHeader(chain consensus.ChainHeaderReader, header *types.Header, seal bool) error {
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
func (s *Merge) VerifyUncles(chain consensus.ChainReader, header *types.Header, uncles []*types.Header) error {
	if !IsPoSHeader(header) {
		return s.eth1Engine.VerifyUncles(chain, header, uncles)
	}
	if len(uncles) > 0 {
		return errors.New("uncles not allowed")
	}
	return nil
}

// Prepare makes sure difficulty and nonce are correct
func (s *Merge) Prepare(chain consensus.ChainHeaderReader, header *types.Header, state *state.IntraBlockState) error {
	reached, err := IsTTDReached(chain, header.ParentHash, header.Number.Uint64()-1)
	if err != nil {
		return err
	}
	if !reached {
		return s.eth1Engine.Prepare(chain, header, state)
	}
	header.Difficulty = ProofOfStakeDifficulty
	header.Nonce = ProofOfStakeNonce
	return nil
}

func (s *Merge) CalculateRewards(config *chain.Config, header *types.Header, uncles []*types.Header, syscall consensus.SystemCall,
) ([]consensus.Reward, error) {
	_, isAura := s.eth1Engine.(*aura.AuRa)
	if !IsPoSHeader(header) || isAura {
		return s.eth1Engine.CalculateRewards(config, header, uncles, syscall)
	}
	return []consensus.Reward{}, nil
}

func (s *Merge) Finalize(config *chain.Config, header *types.Header, state *state.IntraBlockState,
	txs types.Transactions, uncles []*types.Header, r types.Receipts, withdrawals []*types.Withdrawal,
	chain consensus.ChainHeaderReader, syscall consensus.SystemCall,
) (types.Transactions, types.Receipts, error) {
	if !IsPoSHeader(header) {
		return s.eth1Engine.Finalize(config, header, state, txs, uncles, r, withdrawals, chain, syscall)
	}

	rewards, err := s.CalculateRewards(config, header, uncles, syscall)
	if err != nil {
		return nil, nil, err
	}
	for _, r := range rewards {
		state.AddBalance(r.Beneficiary, &r.Amount)
	}

	if withdrawals != nil {
		if auraEngine, ok := s.eth1Engine.(*aura.AuRa); ok {
			if err := auraEngine.ExecuteSystemWithdrawals(withdrawals, syscall); err != nil {
				return nil, nil, err
			}
		} else {
			for _, w := range withdrawals {
				amountInWei := new(uint256.Int).Mul(uint256.NewInt(w.Amount), uint256.NewInt(params.GWei))
				state.AddBalance(w.Address, amountInWei)
			}
		}
	}

	return txs, r, nil
}

func (s *Merge) FinalizeAndAssemble(config *chain.Config, header *types.Header, state *state.IntraBlockState,
	txs types.Transactions, uncles []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal,
	chain consensus.ChainHeaderReader, syscall consensus.SystemCall, call consensus.Call,
) (*types.Block, types.Transactions, types.Receipts, error) {
	if !IsPoSHeader(header) {
		return s.eth1Engine.FinalizeAndAssemble(config, header, state, txs, uncles, receipts, withdrawals, chain, syscall, call)
	}
	outTxs, outReceipts, err := s.Finalize(config, header, state, txs, uncles, receipts, withdrawals, chain, syscall)
	if err != nil {
		return nil, nil, nil, err
	}
	if config.IsCancun(header.Time) {
		dataGasUsed := uint64(misc.CountBlobs(txs) * params.DataGasPerBlob)
		header.DataGasUsed = &dataGasUsed
	}
	return types.NewBlock(header, outTxs, uncles, outReceipts, withdrawals), outTxs, outReceipts, nil
}

func (s *Merge) SealHash(header *types.Header) (hash libcommon.Hash) {
	return s.eth1Engine.SealHash(header)
}

func (s *Merge) CalcDifficulty(chain consensus.ChainHeaderReader, time, parentTime uint64, parentDifficulty *big.Int, parentNumber uint64, parentHash, parentUncleHash libcommon.Hash, parentAuRaStep uint64) *big.Int {
	reached, err := IsTTDReached(chain, parentHash, parentNumber)
	if err != nil {
		return nil
	}
	if !reached {
		return s.eth1Engine.CalcDifficulty(chain, time, parentTime, parentDifficulty, parentNumber, parentHash, parentUncleHash, parentAuRaStep)
	}
	return ProofOfStakeDifficulty
}

// verifyHeader checks whether a Proof-of-Stake header conforms to the consensus rules of the
// stock Ethereum consensus engine with EIP-3675 modifications.
func (s *Merge) verifyHeader(chain consensus.ChainHeaderReader, header, parent *types.Header) error {

	if uint64(len(header.Extra)) > params.MaximumExtraDataSize {
		return fmt.Errorf("extra-data longer than %d bytes (%d)", params.MaximumExtraDataSize, len(header.Extra))
	}

	if header.Time <= parent.Time {
		return errOlderBlockTime
	}

	if header.Difficulty.Cmp(ProofOfStakeDifficulty) != 0 {
		return errInvalidDifficulty
	}

	if !bytes.Equal(header.Nonce[:], ProofOfStakeNonce[:]) {
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
	if diff := new(big.Int).Sub(header.Number, parent.Number); diff.Cmp(libcommon.Big1) != 0 {
		return consensus.ErrInvalidNumber
	}

	if header.UncleHash != types.EmptyUncleHash {
		return errInvalidUncleHash
	}

	skipGasLimit := false
	if auraEngine, ok := s.eth1Engine.(*aura.AuRa); ok {
		skipGasLimit = auraEngine.HasGasLimitContract()
	}
	if err := misc.VerifyEip1559Header(chain.Config(), parent, header, skipGasLimit); err != nil {
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

	if !chain.Config().IsCancun(header.Time) {
		if header.DataGasUsed != nil {
			return fmt.Errorf("invalid dataGasUsed before fork: have %v, expected 'nil'", header.DataGasUsed)
		}
		if header.ExcessDataGas != nil {
			return fmt.Errorf("invalid excessDataGas before fork: have %v, expected 'nil'", header.ExcessDataGas)
		}
	} else if err := misc.VerifyEip4844Header(chain.Config(), parent, header); err != nil {
		// Verify the header's EIP-4844 attributes.
		return err
	}
	return nil
}

func (s *Merge) Seal(chain consensus.ChainHeaderReader, block *types.Block, results chan<- *types.Block, stop <-chan struct{}) error {
	if !IsPoSHeader(block.Header()) {
		return s.eth1Engine.Seal(chain, block, results, stop)
	}
	return nil
}

func (s *Merge) GenerateSeal(chain consensus.ChainHeaderReader, currnt, parent *types.Header, call consensus.Call) []byte {
	return nil
}

func (s *Merge) IsServiceTransaction(sender libcommon.Address, syscall consensus.SystemCall) bool {
	return s.eth1Engine.IsServiceTransaction(sender, syscall)
}

func (s *Merge) Initialize(config *chain.Config, chain consensus.ChainHeaderReader, header *types.Header, state *state.IntraBlockState, txs []types.Transaction, uncles []*types.Header, syscall consensus.SystemCall) {
	s.eth1Engine.Initialize(config, chain, header, state, txs, uncles, syscall)
}

func (s *Merge) APIs(chain consensus.ChainHeaderReader) []rpc.API {
	return s.eth1Engine.APIs(chain)
}

func (s *Merge) Close() error {
	return s.eth1Engine.Close()
}

// IsPoSHeader reports the header belongs to the PoS-stage with some special fields.
// This function is not suitable for a part of APIs like Prepare or CalcDifficulty
// because the header difficulty is not set yet.
func IsPoSHeader(header *types.Header) bool {
	if header.Difficulty == nil {
		panic("IsPoSHeader called with invalid difficulty")
	}
	return header.Difficulty.Cmp(ProofOfStakeDifficulty) == 0
}

// IsTTDReached checks if the TotalTerminalDifficulty has been surpassed on the `parentHash` block.
// It depends on the parentHash already being stored in the database.
// If the total difficulty is not stored in the database a ErrUnknownAncestorTD error is returned.
func IsTTDReached(chain consensus.ChainHeaderReader, parentHash libcommon.Hash, number uint64) (bool, error) {
	if chain.Config().TerminalTotalDifficulty == nil {
		return false, nil
	}
	td := chain.GetTd(parentHash, number)
	if td == nil {
		return false, consensus.ErrUnknownAncestorTD
	}
	return td.Cmp(chain.Config().TerminalTotalDifficulty) >= 0, nil
}
