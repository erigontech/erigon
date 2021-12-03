package serenity

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/rpc"

	"golang.org/x/crypto/sha3"
)

// Constants for Serenity as specified into https://eips.ethereum.org/EIPS/eip-2982
var (
	SerenityDifficulty = common.Big0                // Serenity block's difficulty is always 0.
	SerenityNonce      = types.BlockNonce{}         // Serenity chain's nonces are 0.
	serenityCap        = uint64(0x7fffffffffffffff) // Serenity's difficulty cap.
)

var (
	// errInvalidDifficulty is returned if the difficulty is non-zero.
	errInvalidDifficulty = errors.New("invalid difficulty")

	// errInvalidNonce is returned if the nonce is non-zero.
	errInvalidNonce = errors.New("invalid nonce")

	// errInvalidMixDigest is returned if a block's mix digest is non-zero.
	errInvalidMixDigest = errors.New("non-zero mix digest")
	// errInvalidUncleHash is returned if a block contains an non-empty uncle list.
	errInvalidUncleHash = errors.New("non empty uncle hash")
)

// Serenity Consensus Engine for the Execution Layer.
// Note: the work is mostly done on the Consensus Layer, so nothing much is to be added on this side.
type Serenity struct{}

// New creates a new instance of the Serenity Engine
func New() *Serenity {
	return &Serenity{}
}

// Author implements consensus.Engine, returning the header's coinbase as the
// proof-of-stake verified author of the block.
func (s *Serenity) Author(header *types.Header) (common.Address, error) {
	return header.Coinbase, nil
}

// VerifyHeader checks whether a header conforms to the consensus rules of the
// stock Ethereum serenity engine.
func (s *Serenity) VerifyHeader(chain consensus.ChainHeaderReader, header *types.Header, seal bool) error {
	// Retrieve parent from the chain
	parent := chain.GetHeader(header.ParentHash, header.Number.Uint64()-1)
	if parent == nil {
		return consensus.ErrUnknownAncestor
	}

	return s.verifyHeader(chain, header, parent)
}

// VerifyHeader checks whether a bunch of headers conforms to the consensus rules of the
// stock Ethereum serenity engine.
func (s *Serenity) VerifyHeaders(chain consensus.ChainHeaderReader, headers []*types.Header, seals []bool) error {
	for _, header := range headers {
		if err := s.VerifyHeader(chain, header, false); err != nil {
			return err
		}
	}
	return nil
}

// VerifyUncles implements consensus.Engine, always returning an error for any
// uncles as this consensus mechanism doesn't permit uncles.
func (s *Serenity) VerifyUncles(chain consensus.ChainReader, header *types.Header, uncles []*types.Header) error {
	if len(uncles) > 0 {
		return errors.New("uncles not allowed")
	}
	return nil
}

// Prepare makes sure difficulty and nonce are correct
func (s *Serenity) Prepare(chain consensus.ChainHeaderReader, header *types.Header) error {
	header.Difficulty = SerenityDifficulty
	header.Nonce = types.BlockNonce{}
	return nil
}

func (s *Serenity) FinalizeAndAssemble(config *params.ChainConfig, header *types.Header,
	state *state.IntraBlockState, txs []types.Transaction, uncles []*types.Header,
	receipts types.Receipts, e consensus.EpochReader, chain consensus.ChainHeaderReader,
	syscall consensus.SystemCall, call consensus.Call) (*types.Block, error) {
	return types.NewBlock(header, txs, uncles, receipts), nil
}

func (s *Serenity) SealHash(header *types.Header) (hash common.Hash) {
	hasher := sha3.NewLegacyKeccak256()

	enc := []interface{}{
		header.ParentHash,
		header.UncleHash,
		header.Coinbase,
		header.Root,
		header.TxHash,
		header.ReceiptHash,
		header.Bloom,
		header.Difficulty,
		header.Number,
		header.GasLimit,
		header.GasUsed,
		header.Time,
		header.Extra,
	}
	if header.Eip1559 {
		enc = append(enc, header.BaseFee)
	}
	rlp.Encode(hasher, enc)
	hasher.Sum(hash[:0])
	return hash
}

func (s *Serenity) CalcDifficulty(chain consensus.ChainHeaderReader, time, parentTime uint64, parentDifficulty *big.Int, parentNumber uint64, parentHash, parentUncleHash common.Hash, parentSeal []rlp.RawValue) *big.Int {
	return SerenityDifficulty
}

// verifyHeader checks whether a bunch of headers conforms to the consensus rules of the
// stock Ethereum serenity engine.
func (s *Serenity) verifyHeader(chain consensus.ChainHeaderReader, header, parent *types.Header) error {

	if len(header.Extra) > 32 {
		return fmt.Errorf("extra-data longer than 32 bytes (%d)", len(header.Extra))
	}

	if header.Difficulty.Cmp(SerenityDifficulty) != 0 {
		return errInvalidDifficulty
	}

	if !bytes.Equal(header.Nonce[:], SerenityNonce[:]) {
		return errInvalidNonce
	}

	// Verify that the gas limit is within cap
	if header.GasLimit > serenityCap {
		return fmt.Errorf("invalid gasLimit: have %v, max %v", header.GasLimit, serenityCap)
	}
	// Verify that the gasUsed is <= gasLimit
	if header.GasUsed > header.GasLimit {
		return fmt.Errorf("invalid gasUsed: have %d, gasLimit %d", header.GasUsed, header.GasLimit)
	}
	// Verify the block's gas usage and (if applicable) verify the base fee.
	if !chain.Config().IsLondon(header.Number.Uint64()) {
		// Verify BaseFee not present before EIP-1559 fork.
		if header.BaseFee != nil {
			return fmt.Errorf("invalid baseFee before fork: have %d, expected 'nil'", header.BaseFee)
		}
		// Verify that the gas limit remains within allowed bounds
		diff := int64(parent.GasLimit) - int64(header.GasLimit)
		if diff < 0 {
			diff *= -1
		}
		limit := parent.GasLimit / params.GasLimitBoundDivisor
		if uint64(diff) >= limit || header.GasLimit < params.MinGasLimit {
			return fmt.Errorf("invalid gas limit: have %d, want %d += %d", header.GasLimit, parent.GasLimit, limit)
		}
	} else if err := misc.VerifyEip1559Header(chain.Config(), parent, header); err != nil {
		// Verify the header's EIP-1559 attributes.
		return err
	}

	// Verify that the block number is parent's +1
	if diff := new(big.Int).Sub(header.Number, parent.Number); diff.Cmp(big.NewInt(1)) != 0 {
		return consensus.ErrInvalidNumber
	}

	if header.MixDigest != (common.Hash{}) {
		return errInvalidMixDigest
	}

	if header.UncleHash != types.EmptyUncleHash {
		return errInvalidUncleHash
	}
	return nil
}

// Methods now not needed for Serenity |
//                                     |
//                                     v

func (s *Serenity) Finalize(config *params.ChainConfig, header *types.Header, state *state.IntraBlockState,
	txs []types.Transaction, uncles []*types.Header, r types.Receipts, e consensus.EpochReader, chain consensus.ChainHeaderReader,
	syscall consensus.SystemCall) error {
	return nil
}

func (s *Serenity) Seal(chain consensus.ChainHeaderReader, block *types.Block, results chan<- *types.Block, stop <-chan struct{}) error {
	return nil
}

func (s *Serenity) GenerateSeal(chain consensus.ChainHeaderReader, currnt, parent *types.Header, call consensus.Call) []rlp.RawValue {
	return nil
}

func (s *Serenity) Initialize(config *params.ChainConfig, chain consensus.ChainHeaderReader, e consensus.EpochReader, header *types.Header, txs []types.Transaction, uncles []*types.Header, syscall consensus.SystemCall) {
}

func (s *Serenity) APIs(chain consensus.ChainHeaderReader) []rpc.API {
	return []rpc.API{}
}

func (s *Serenity) Close() error {
	return nil
}
