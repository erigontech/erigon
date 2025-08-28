// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package bor

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/arc/v2"
	"github.com/holiman/uint256"
	"github.com/xsleonard/go-merkle"
	"golang.org/x/crypto/sha3"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/params"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/consensus/misc"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/bor/statefull"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/services"
)

const inmemorySignatures = 4096 // Number of recent block signatures to keep in memory

// Bor protocol constants.
var (
	// Default number of blocks after which to checkpoint and reset the pending votes
	defaultSprintLength        = map[string]uint64{"0": 64}
	validatorHeaderBytesLength = length.Addr + 20 // address + power
	// maxCheckpointLength is the maximum number of blocks that can be requested for constructing a checkpoint root hash
	maxCheckpointLength = uint64(math.Pow(2, 15))
)

// Various error messages to mark blocks invalid. These should be private to
// prevent engine specific errors from being referenced in the remainder of the
// codebase, inherently breaking if the engine is swapped out. Please put common
// error types into the consensus package.
var (
	// errUnknownBlock is returned when the list of signers is requested for a block
	// that is not part of the local blockchain.
	errUnknownBlock = errors.New("unknown block")
	// errMissingVanity is returned if a block's extra-data section is shorter than
	// 32 bytes, which is required to store the signer vanity.
	errMissingVanity = errors.New("extra-data 32 byte vanity prefix missing")
	// errMissingSignature is returned if a block's extra-data section doesn't seem
	// to contain a 65 byte secp256k1 signature.
	errMissingSignature = errors.New("extra-data 65 byte signature suffix missing")
	// errExtraValidators is returned if non-sprint-end block contain validator data in
	// their extra-data fields.
	errExtraValidators = errors.New("non-sprint-end block contains extra validator list")
	// errInvalidSprintValidators is returned if a block contains an
	// invalid list of validators (i.e. non divisible by 40 bytes).
	errInvalidSprintValidators = errors.New("invalid validator list on sprint end block")
	// errInvalidMixDigest is returned if a block's mix digest is non-zero.
	errInvalidMixDigest = errors.New("non-zero mix digest")
	// errInvalidUncleHash is returned if a block contains an non-empty uncle list.
	errInvalidUncleHash = errors.New("non empty uncle hash")
	// errInvalidDifficulty is returned if the difficulty of a block neither 1 or 2.
	errInvalidDifficulty = errors.New("invalid difficulty")
	// errInvalidTimestamp is returned if the timestamp of a block is lower than
	// the previous block's timestamp + the minimum block period.
	errInvalidTimestamp = errors.New("invalid timestamp")
	errUncleDetected    = errors.New("uncles not allowed")
)

// SignerFn is a signer callback function to request a header to be signed by a
// backing account.
type SignerFn func(signer common.Address, mimeType string, message []byte) ([]byte, error)

// Ecrecover extracts the Ethereum account address from a signed header.
func Ecrecover(header *types.Header, sigcache *lru.ARCCache[common.Hash, common.Address], c *borcfg.BorConfig) (common.Address, error) {
	// If the signature's already cached, return that
	hash := header.Hash()
	if address, known := sigcache.Get(hash); known {
		return address, nil
	}
	// Retrieve the signature from the header extra-data
	if len(header.Extra) < types.ExtraSealLength {
		return common.Address{}, errMissingSignature
	}

	signature := header.Extra[len(header.Extra)-types.ExtraSealLength:]

	// Recover the public key and the Ethereum address
	sealHash := SealHash(header, c)
	pubkey, err := crypto.Ecrecover(sealHash[:], signature)
	if err != nil {
		return common.Address{}, err
	}
	var signer common.Address
	copy(signer[:], crypto.Keccak256(pubkey[1:])[12:])

	sigcache.Add(hash, signer)

	return signer, nil
}

// SealHash returns the hash of a block prior to it being sealed.
func SealHash(header *types.Header, c *borcfg.BorConfig) (hash common.Hash) {
	hasher := crypto.NewKeccakState()
	defer crypto.ReturnToPool(hasher)

	encodeSigHeader(hasher, header, c)
	hasher.Sum(hash[:0])

	return hash
}

func encodeSigHeader(w io.Writer, header *types.Header, c *borcfg.BorConfig) {
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
		header.Extra[:len(header.Extra)-65], // Yes, this will panic if extra is too short
		header.MixDigest,
		header.Nonce,
	}

	if c.IsJaipur(header.Number.Uint64()) {
		if header.BaseFee != nil {
			enc = append(enc, header.BaseFee)
		}
	}

	if err := rlp.Encode(w, enc); err != nil {
		panic("can't encode: " + err.Error())
	}
}

// CalcProducerDelay is the block delay algorithm based on block time, period, producerDelay and turn-ness of a signer
func CalcProducerDelay(number uint64, succession int, c *borcfg.BorConfig) uint64 {
	// When the block is the first block of the sprint, it is expected to be delayed by `producerDelay`.
	// That is to allow time for block propagation in the last sprint
	delay := c.CalculatePeriod(number)
	// Since there is only one producer in veblop, we don't need to add producer delay and backup multiplier
	if c.IsVeBlop(number) {
		return delay
	}
	if c.IsSprintStart(number) {
		delay = c.CalculateProducerDelay(number)
	}

	if succession > 0 {
		delay += uint64(succession) * c.CalculateBackupMultiplier(number)
	}

	return delay
}

func MinNextBlockTime(parent *types.Header, succession int, config *borcfg.BorConfig) uint64 {
	return parent.Time + CalcProducerDelay(parent.Number.Uint64()+1, succession, config)
}

// ValidateHeaderTimeSignerSuccessionNumber - heimdall.ValidatorSet abstraction for unit tests
type ValidateHeaderTimeSignerSuccessionNumber interface {
	GetSignerSuccessionNumber(signer common.Address, number uint64) (int, error)
}

type spanReader interface {
	Span(ctx context.Context, id uint64) (*heimdall.Span, bool, error)
	Producers(ctx context.Context, blockNum uint64) (*heimdall.ValidatorSet, error)
}

//go:generate mockgen -typed=true -destination=./bridge_reader_mock.go -package=bor . bridgeReader
type bridgeReader interface {
	Events(ctx context.Context, blockHash common.Hash, blockNum uint64) ([]*types.Message, error)
	EventsWithinTime(ctx context.Context, timeFrom, timeTo time.Time) ([]*types.Message, error)
}

func ValidateHeaderTime(
	header *types.Header,
	now time.Time,
	parent *types.Header,
	validatorSet ValidateHeaderTimeSignerSuccessionNumber,
	config *borcfg.BorConfig,
	signaturesCache *lru.ARCCache[common.Hash, common.Address],
) error {
	if config.IsBhilai(header.Number.Uint64()) {
		// Don't waste time checking blocks from the future but allow a buffer of block time for
		// early block announcements. Note that this is a loose check and would allow early blocks
		// from non-primary producer. Such blocks will be rejected later when we know the succession
		// number of the signer in the current sprint.
		if header.Time > uint64(now.Unix())+config.CalculatePeriod(header.Number.Uint64()) {
			return fmt.Errorf("%w: expected: %s(%s), got: %s", consensus.ErrFutureBlock, time.Unix(now.Unix(), 0), now, time.Unix(int64(header.Time), 0))
		}
	} else {
		// Don't waste time checking blocks from the future
		if header.Time > uint64(now.Unix()) {
			return fmt.Errorf("%w: expected: %s(%s), got: %s", consensus.ErrFutureBlock, time.Unix(now.Unix(), 0), now, time.Unix(int64(header.Time), 0))
		}
	}

	if parent == nil {
		return nil
	}

	signer, err := Ecrecover(header, signaturesCache, config)
	if err != nil {
		return err
	}

	succession, err := validatorSet.GetSignerSuccessionNumber(signer, header.Number.Uint64())
	if err != nil {
		return err
	}

	// Post Bhilai HF, reject blocks form non-primary producers if they're earlier than the expected time
	if config.IsBhilai(header.Number.Uint64()) && succession != 0 {
		if header.Time > uint64(now.Unix()) {
			return fmt.Errorf("%w: expected: %s(%s), got: %s", consensus.ErrFutureBlock, time.Unix(now.Unix(), 0), now, time.Unix(int64(header.Time), 0))
		}
	}

	if header.Time < MinNextBlockTime(parent, succession, config) {
		return &BlockTooSoonError{header.Number.Uint64(), succession}
	}

	return nil
}

// BorRLP returns the rlp bytes which needs to be signed for the bor
// sealing. The RLP to sign consists of the entire header apart from the 65 byte signature
// contained at the end of the extra data.
//
// Note, the method requires the extra data to be at least 65 bytes, otherwise it
// panics. This is done to avoid accidentally using both forms (signature present
// or not), which could be abused to produce different hashes for the same header.
func BorRLP(header *types.Header, c *borcfg.BorConfig) []byte {
	b := new(bytes.Buffer)
	encodeSigHeader(b, header, c)

	return b.Bytes()
}

// Bor is the matic-bor consensus engine
type Bor struct {
	mutex       sync.Mutex
	chainConfig *chain.Config     // Chain config
	config      *borcfg.BorConfig // Consensus engine configuration parameters for bor consensus
	blockReader services.FullBlockReader

	Signatures   *lru.ARCCache[common.Hash, common.Address] // Signatures of recent blocks to speed up mining
	Dependencies *lru.ARCCache[common.Hash, [][]int]

	authorizedSigner atomic.Pointer[signer] // Ethereum address and sign function of the signing key

	execCtx context.Context // context of caller execution stage

	spanner       Spanner
	stateReceiver StateReceiver
	spanReader    spanReader
	bridgeReader  bridgeReader

	// scope event.SubscriptionScope
	// The fields below are for testing only
	fakeDiff bool // Skip difficulty verifications

	logger         log.Logger
	rootHashCache  *lru.ARCCache[string, string]
	headerProgress HeaderProgress
}

type signer struct {
	signer common.Address // Ethereum address of the signing key
	signFn SignerFn       // Signer function to authorize hashes with
}

// New creates a Matic Bor consensus engine.
func New(
	chainConfig *chain.Config,
	blockReader services.FullBlockReader,
	spanner Spanner,
	genesisContracts StateReceiver,
	logger log.Logger,
	bridgeReader bridgeReader,
	spanReader spanReader,
) *Bor {
	// get bor config
	borConfig := chainConfig.Bor.(*borcfg.BorConfig)

	// Set any missing consensus parameters to their defaults
	if borConfig != nil && borConfig.CalculateSprintLength(0) == 0 {
		borConfig.Sprint = defaultSprintLength
	}

	// Allocate the snapshot caches and create the engine
	signatures, _ := lru.NewARC[common.Hash, common.Address](inmemorySignatures)
	dependencies, _ := lru.NewARC[common.Hash, [][]int](128)

	c := &Bor{
		chainConfig:   chainConfig,
		config:        borConfig,
		blockReader:   blockReader,
		Signatures:    signatures,
		Dependencies:  dependencies,
		spanner:       spanner,
		stateReceiver: genesisContracts,
		execCtx:       context.Background(),
		logger:        logger,
		bridgeReader:  bridgeReader,
		spanReader:    spanReader,
	}

	c.authorizedSigner.Store(&signer{
		common.Address{},
		func(_ common.Address, _ string, i []byte) ([]byte, error) {
			// return an error to prevent panics
			return nil, &heimdall.UnauthorizedSignerError{Number: 0, Signer: common.Address{}.Bytes()}
		},
	})

	// make sure we can decode all the GenesisAlloc in the BorConfig.
	for key, genesisAlloc := range c.config.BlockAlloc {
		if _, err := types.DecodeGenesisAlloc(genesisAlloc); err != nil {
			panic(fmt.Sprintf("BUG: Block alloc '%s' in genesis is not correct: %v", key, err))
		}
	}

	return c
}

// NewRo is used by the rpcdaemon and tests which need read only access to the provided data services
func NewRo(chainConfig *chain.Config, blockReader services.FullBlockReader, logger log.Logger) *Bor {
	// get bor config
	borConfig := chainConfig.Bor.(*borcfg.BorConfig)

	// Set any missing consensus parameters to their defaults
	if borConfig != nil && borConfig.CalculateSprintLength(0) == 0 {
		borConfig.Sprint = defaultSprintLength
	}

	signatures, _ := lru.NewARC[common.Hash, common.Address](inmemorySignatures)
	dependencies, _ := lru.NewARC[common.Hash, [][]int](128)

	return &Bor{
		chainConfig:  chainConfig,
		config:       borConfig,
		blockReader:  blockReader,
		logger:       logger,
		Dependencies: dependencies,
		Signatures:   signatures,
		execCtx:      context.Background(),
	}
}

// Type returns underlying consensus engine
func (c *Bor) Type() chain.ConsensusName {
	return chain.BorConsensus
}

func (c *Bor) Config() *borcfg.BorConfig {
	return c.config
}

type HeaderProgress interface {
	Progress() uint64
}

func (c *Bor) HeaderProgress(p HeaderProgress) {
	c.headerProgress = p
}

// Author implements consensus.Engine, returning the Ethereum address recovered
// from the signature in the header's extra-data section.
// This is thread-safe (only access the header and config (which is never updated),
// as well as signatures, which are lru.ARCCache, which is thread-safe)
func (c *Bor) Author(header *types.Header) (common.Address, error) {
	return Ecrecover(header, c.Signatures, c.config)
}

// VerifyHeader checks whether a header conforms to the consensus rules.
func (c *Bor) VerifyHeader(chain consensus.ChainHeaderReader, header *types.Header, seal bool) error {
	return c.verifyHeader(chain, header, nil)
}

// VerifyHeaders is similar to VerifyHeader, but verifies a batch of headers. The
// method returns a quit channel to abort the operations and a results channel to
// retrieve the async verifications (the order is that of the input slice).
func (c *Bor) VerifyHeaders(chain consensus.ChainHeaderReader, headers []*types.Header, _ []bool) (chan<- struct{}, <-chan error) {
	abort := make(chan struct{})
	results := make(chan error, len(headers))

	go func() {
		for i, header := range headers {
			err := c.verifyHeader(chain, header, headers[:i])

			select {
			case <-abort:
				return
			case results <- err:
			}
		}
	}()

	return abort, results
}

// verifyHeader checks whether a header conforms to the consensus rules.The
// caller may optionally pass in a batch of parents (ascending order) to avoid
// looking those up from the database. This is useful for concurrently verifying
// a batch of new headers.
func (c *Bor) verifyHeader(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header) error {
	if header.Number == nil {
		return errUnknownBlock
	}

	number := header.Number.Uint64()
	now := time.Now().Unix()

	// Don't waste time checking blocks from the future
	// Allow early blocks if Bhilai HF is enabled
	if c.config.IsBhilai(number) {
		// Don't waste time checking blocks from the future but allow a buffer of block time for
		// early block announcements. Note that this is a loose check and would allow early blocks
		// from non-primary producer. Such blocks will be rejected later when we know the succession
		// number of the signer in the current sprint.
		if header.Time > uint64(now)+c.config.CalculatePeriod(number) {
			return fmt.Errorf("%w: expected: %s, got: %s", consensus.ErrFutureBlock, time.Unix(now, 0), time.Unix(int64(header.Time), 0))
		}
	} else {
		// Don't waste time checking blocks from the future
		if header.Time > uint64(now) {
			return fmt.Errorf("%w: expected: %s, got: %s", consensus.ErrFutureBlock, time.Unix(now, 0), time.Unix(int64(header.Time), 0))
		}
	}

	if err := ValidateHeaderUnusedFields(header); err != nil {
		return err
	}

	if err := ValidateHeaderExtraLength(header.Extra); err != nil {
		return err
	}
	if err := ValidateHeaderSprintValidators(header, c.config); err != nil {
		return err
	}

	// Ensure that the block's difficulty is meaningful (may not be correct at this point)
	if (number > 0) && (header.Difficulty == nil) {
		return errInvalidDifficulty
	}

	// All basic checks passed, verify cascading fields
	return c.verifyCascadingFields(chain, header, parents)
}

// ValidateHeaderExtraLength validates that the extra-data contains both the vanity and signature.
// header.Extra = header.Vanity + header.ProducerBytes (optional) + header.Seal
func ValidateHeaderExtraLength(extraBytes []byte) error {
	if len(extraBytes) < types.ExtraVanityLength {
		return errMissingVanity
	}

	if len(extraBytes) < types.ExtraVanityLength+types.ExtraSealLength {
		return errMissingSignature
	}

	return nil
}

// ValidateHeaderSprintValidators validates that the extra-data contains a validators list only in the last header of a sprint.
func ValidateHeaderSprintValidators(header *types.Header, config *borcfg.BorConfig) error {
	number := header.Number.Uint64()
	isSprintEnd := config.IsSprintEnd(number)
	validatorBytes := GetValidatorBytes(header, config)
	validatorBytesLen := len(validatorBytes)

	if !isSprintEnd && (validatorBytesLen != 0) {
		return errExtraValidators
	}
	if isSprintEnd && (validatorBytesLen%validatorHeaderBytesLength != 0) {
		return errInvalidSprintValidators
	}
	return nil
}

// ValidateHeaderUnusedFields validates that unused fields are empty.
func ValidateHeaderUnusedFields(header *types.Header) error {
	// Ensure that the mix digest is zero as we don't have fork protection currently
	if header.MixDigest != (common.Hash{}) {
		return errInvalidMixDigest
	}

	// Ensure that the block doesn't contain any uncles which are meaningless in PoA
	if header.UncleHash != empty.UncleHash {
		return errInvalidUncleHash
	}

	if header.WithdrawalsHash != nil {
		return consensus.ErrUnexpectedWithdrawals
	}

	if header.RequestsHash != nil {
		return consensus.ErrUnexpectedRequests
	}

	return misc.VerifyAbsenceOfCancunHeaderFields(header)
}

// verifyCascadingFields verifies all the header fields that are not standalone,
// rather depend on a batch of previous headers. The caller may optionally pass
// in a batch of parents (ascending order) to avoid looking those up from the
// database. This is useful for concurrently verifying a batch of new headers.
func (c *Bor) verifyCascadingFields(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header) error {
	// The genesis block is the always valid dead-end
	number := header.Number.Uint64()

	if number == 0 {
		return nil
	}

	// Ensure that the block's timestamp isn't too close to it's parent
	var parent *types.Header

	if len(parents) > 0 {
		parent = parents[len(parents)-1]
	} else {
		parent = chain.GetHeader(header.ParentHash, number-1)
	}

	if parent == nil || parent.Number.Uint64() != number-1 || parent.Hash() != header.ParentHash {
		return consensus.ErrUnknownAncestor
	}

	if parent.Time+c.config.CalculatePeriod(number) > header.Time {
		return errInvalidTimestamp
	}

	return ValidateHeaderGas(header, parent, chain.Config())
}

// ValidateHeaderGas validates GasUsed, GasLimit and BaseFee.
func ValidateHeaderGas(header *types.Header, parent *types.Header, chainConfig *chain.Config) error {
	// Verify that the gas limit is <= 2^63-1
	if header.GasLimit > params.MaxBlockGasLimit {
		return fmt.Errorf("invalid gasLimit: have %v, max %v", header.GasLimit, params.MaxBlockGasLimit)
	}

	// Verify that the gasUsed is <= gasLimit
	if header.GasUsed > header.GasLimit {
		return fmt.Errorf("invalid gasUsed: have %d, gasLimit %d", header.GasUsed, header.GasLimit)
	}

	if parent == nil {
		return nil
	}

	if !chainConfig.IsLondon(header.Number.Uint64()) {
		// Verify BaseFee not present before EIP-1559 fork.
		if header.BaseFee != nil {
			return fmt.Errorf("invalid baseFee before fork: have %d, want <nil>", header.BaseFee)
		}
		if err := misc.VerifyGaslimit(parent.GasLimit, header.GasLimit); err != nil {
			return err
		}
	} else if err := misc.VerifyEip1559Header(chainConfig, parent, header, false /*skipGasLimit*/); err != nil {
		// Verify the header's EIP-1559 attributes.
		return err
	}

	return nil
}

// VerifyUncles implements consensus.Engine, always returning an error for any
// uncles as this consensus mechanism doesn't permit uncles.
func (c *Bor) VerifyUncles(_ consensus.ChainReader, _ *types.Header, uncles []*types.Header) error {
	return VerifyUncles(uncles)
}

// VerifySeal implements consensus.Engine, checking whether the signature contained
// in the header satisfies the consensus protocol requirements.
func (c *Bor) VerifySeal(chain ChainHeaderReader, header *types.Header) error {
	v, err := c.spanReader.Producers(context.Background(), header.Number.Uint64())
	if err != nil {
		return err
	}

	return c.verifySeal(chain, header, nil, v)
}

// verifySeal checks whether the signature contained in the header satisfies the
// consensus protocol requirements. The method accepts an optional list of parent
// headers that aren't yet part of the local blockchain to generate the snapshots
// from.
func (c *Bor) verifySeal(chain ChainHeaderReader, header *types.Header, parents []*types.Header, validatorSet *heimdall.ValidatorSet) error {
	// Verifying the genesis block is not supported
	number := header.Number.Uint64()
	if number == 0 {
		return errUnknownBlock
	}

	var parent *types.Header
	if len(parents) > 0 { // if parents is nil, len(parents) is zero
		parent = parents[len(parents)-1]
	} else if number > 0 {
		parent = chain.GetHeader(header.ParentHash, number-1)
	}

	if err := ValidateHeaderTime(header, time.Now(), parent, validatorSet, c.config, c.Signatures); err != nil {
		return err
	}

	// Ensure that the difficulty corresponds to the turn-ness of the signer
	if !c.fakeDiff {
		signer, err := Ecrecover(header, c.Signatures, c.config)
		if err != nil {
			return err
		}

		difficulty := validatorSet.SafeDifficulty(signer)
		if header.Difficulty.Uint64() != difficulty {
			return &WrongDifficultyError{number, difficulty, header.Difficulty.Uint64(), signer.Bytes()}
		}
	}

	return nil
}

// Prepare implements consensus.Engine, preparing all the consensus fields of the
// header for running the transactions on top.
func (c *Bor) Prepare(chain consensus.ChainHeaderReader, header *types.Header, state *state.IntraBlockState) error {
	// If the block isn't a checkpoint, cast a random vote (good enough for now)
	header.Coinbase = common.Address{}
	header.Nonce = types.BlockNonce{}

	number := header.Number.Uint64()
	// Assemble the validator snapshot to check which votes make sense
	validatorSet, err := c.spanReader.Producers(context.Background(), header.Number.Uint64())
	if err != nil {
		return err
	}

	// Set the correct difficulty
	header.Difficulty = new(big.Int).SetUint64(validatorSet.SafeDifficulty(c.authorizedSigner.Load().signer))

	// Ensure the extra data has all it's components
	if len(header.Extra) < types.ExtraVanityLength {
		header.Extra = append(header.Extra, bytes.Repeat([]byte{0x00}, types.ExtraVanityLength-len(header.Extra))...)
	}

	header.Extra = header.Extra[:types.ExtraVanityLength]

	// get validator set if number
	// Note: headers.Extra has producer set and not validator set. The bor
	// client calls `GetCurrentValidators` because it makes a contract call
	// where it fetches producers internally. As we fetch data from span
	// in Erigon, use directly the `GetCurrentProducers` function.
	if c.config.IsSprintEnd(number) {
		var newValidators []*heimdall.Validator
		validators, err := c.spanReader.Producers(context.Background(), number+1)
		if err != nil {
			return err
		}
		newValidators = validators.Validators

		// sort validator by address
		sort.Sort(heimdall.ValidatorsByAddress(newValidators))

		if c.config.IsNapoli(header.Number.Uint64()) { // PIP-16: Transaction Dependency Data
			var tempValidatorBytes []byte

			for _, validator := range newValidators {
				tempValidatorBytes = append(tempValidatorBytes, validator.HeaderBytes()...)
			}

			blockExtraData := &BlockExtraData{
				ValidatorBytes: tempValidatorBytes,
				TxDependencies: nil,
			}

			blockExtraDataBytes, err := rlp.EncodeToBytes(blockExtraData)
			if err != nil {
				log.Error("error while encoding block extra data: %v", err)
				return fmt.Errorf("error while encoding block extra data: %v", err)
			}

			header.Extra = append(header.Extra, blockExtraDataBytes...)
		} else {
			for _, validator := range newValidators {
				header.Extra = append(header.Extra, validator.HeaderBytes()...)
			}
		}
	} else if c.config.IsNapoli(header.Number.Uint64()) { // PIP-16: Transaction Dependency Data
		blockExtraData := &BlockExtraData{
			ValidatorBytes: nil,
			TxDependencies: nil,
		}

		blockExtraDataBytes, err := rlp.EncodeToBytes(blockExtraData)
		if err != nil {
			log.Error("error while encoding block extra data: %v", err)
			return fmt.Errorf("error while encoding block extra data: %v", err)
		}

		header.Extra = append(header.Extra, blockExtraDataBytes...)
	}

	// add extra seal space
	header.Extra = append(header.Extra, make([]byte, types.ExtraSealLength)...)

	// Mix digest is reserved for now, set to empty
	header.MixDigest = common.Hash{}

	// Ensure the timestamp has the correct delay
	parent := chain.GetHeader(header.ParentHash, number-1)
	if parent == nil {
		return consensus.ErrUnknownAncestor
	}

	var succession int
	signer := c.authorizedSigner.Load().signer
	// if signer is not empty
	if !bytes.Equal(signer.Bytes(), common.Address{}.Bytes()) {
		succession, err = validatorSet.GetSignerSuccessionNumber(signer, number)
		if err != nil {
			return err
		}
	}

	header.Time = MinNextBlockTime(parent, succession, c.config)
	now := time.Now()
	if header.Time < uint64(now.Unix()) {
		header.Time = uint64(now.Unix())
	} else {
		// For primary validators, wait until the current block production window
		// starts. This prevents bor from starting to build next block before time
		// as we'd like to wait for new transactions. Although this change doesn't
		// need a check for hard fork as it doesn't change any consensus rules, we
		// still keep it for safety and testing.
		if c.config.IsBhilai(number) && succession == 0 {
			startTime := time.Unix(int64(header.Time)-int64(c.config.CalculatePeriod(number)), 0)
			time.Sleep(time.Until(startTime))
		}
	}

	return nil
}

func (c *Bor) CalculateRewards(config *chain.Config, header *types.Header, uncles []*types.Header, syscall consensus.SystemCall,
) ([]consensus.Reward, error) {
	return []consensus.Reward{}, nil
}

// Finalize implements consensus.Engine, ensuring no uncles are set, nor block
// rewards given.
func (c *Bor) Finalize(config *chain.Config, header *types.Header, state *state.IntraBlockState,
	txs types.Transactions, uncles []*types.Header, r types.Receipts, withdrawals []*types.Withdrawal,
	chain consensus.ChainReader, syscall consensus.SystemCall, skipReceiptsEval bool, logger log.Logger,
) (types.FlatRequests, error) {
	headerNumber := header.Number.Uint64()

	if withdrawals != nil || header.WithdrawalsHash != nil {
		return nil, consensus.ErrUnexpectedWithdrawals
	}

	if header.RequestsHash != nil {
		return nil, consensus.ErrUnexpectedRequests
	}

	if c.config.IsSprintStart(headerNumber) {
		cx := statefull.ChainContext{Chain: chain, Bor: c}

		if c.blockReader != nil {
			// post VeBlop spans won't be committed to smart contract
			if !c.config.IsVeBlop(header.Number.Uint64()) {
				// check and commit span
				if err := c.checkAndCommitSpan(header, syscall); err != nil {
					err := fmt.Errorf("Finalize.checkAndCommitSpan: %w", err)
					c.logger.Error("[bor] committing span", "err", err)
					return nil, err
				}
			}

			// commit states
			if err := c.CommitStates(header, cx, syscall, false); err != nil {
				err := fmt.Errorf("Finalize.CommitStates: %w", err)
				c.logger.Error("[bor] Error while committing states", "err", err)
				return nil, err
			}
		}
	}

	if err := c.changeContractCodeIfNeeded(headerNumber, state); err != nil {
		c.logger.Error("[bor] Error changing contract code", "err", err)
		return nil, err
	}

	return nil, nil
}

func (c *Bor) changeContractCodeIfNeeded(headerNumber uint64, state *state.IntraBlockState) error {
	for blockNumber, genesisAlloc := range c.config.BlockAlloc {
		if blockNumber == strconv.FormatUint(headerNumber, 10) {
			allocs, err := types.DecodeGenesisAlloc(genesisAlloc)
			if err != nil {
				return fmt.Errorf("failed to decode genesis alloc: %v", err)
			}

			for addr, account := range allocs {
				c.logger.Trace("[bor] change contract code", "address", addr)
				state.SetCode(addr, account.Code)
			}
		}
	}

	return nil
}

// FinalizeAndAssemble implements consensus.Engine, ensuring no uncles are set,
// nor block rewards given, and returns the final block.
func (c *Bor) FinalizeAndAssemble(chainConfig *chain.Config, header *types.Header, state *state.IntraBlockState,
	txs types.Transactions, uncles []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal,
	chain consensus.ChainReader, syscall consensus.SystemCall, call consensus.Call, logger log.Logger,
) (*types.Block, types.FlatRequests, error) {
	headerNumber := header.Number.Uint64()

	if withdrawals != nil || header.WithdrawalsHash != nil {
		return nil, nil, consensus.ErrUnexpectedWithdrawals
	}

	if header.RequestsHash != nil {
		return nil, nil, consensus.ErrUnexpectedRequests
	}

	if c.config.IsSprintStart(headerNumber) {
		cx := statefull.ChainContext{Chain: chain, Bor: c}

		if c.blockReader != nil {
			// Post VeBlop spans won't be commited to smart contract
			if !c.config.IsVeBlop(header.Number.Uint64()) {
				// check and commit span
				if err := c.checkAndCommitSpan(header, syscall); err != nil {
					err := fmt.Errorf("FinalizeAndAssemble.checkAndCommitSpan: %w", err)
					c.logger.Error("[bor] committing span", "err", err)
					return nil, nil, err
				}
			}
			// commit states
			if err := c.CommitStates(header, cx, syscall, true); err != nil {
				err := fmt.Errorf("FinalizeAndAssemble.CommitStates: %w", err)
				c.logger.Error("[bor] committing states", "err", err)
				return nil, nil, err
			}
		}
	}

	if err := c.changeContractCodeIfNeeded(headerNumber, state); err != nil {
		c.logger.Error("[bor] Error changing contract code", "err", err)
		return nil, nil, err
	}

	return types.NewBlockForAsembling(header, txs, nil, receipts, withdrawals), nil, nil
}

func (c *Bor) Initialize(config *chain.Config, chain consensus.ChainHeaderReader, header *types.Header,
	state *state.IntraBlockState, syscall consensus.SysCallCustom, logger log.Logger, tracer *tracing.Hooks) {
	if chain != nil && chain.Config().IsBhilai(header.Number.Uint64()) {
		misc.StoreBlockHashesEip2935(header, state, config, chain)
	}
}

// Authorize injects a private key into the consensus engine to mint new blocks
// with.
func (c *Bor) Authorize(currentSigner common.Address, signFn SignerFn) {
	c.authorizedSigner.Store(&signer{
		signer: currentSigner,
		signFn: signFn,
	})
}

// Seal implements consensus.Engine, attempting to create a sealed block using
// the local signing credentials.
func (c *Bor) Seal(chain consensus.ChainHeaderReader, blockWithReceipts *types.BlockWithReceipts, results chan<- *types.BlockWithReceipts, stop <-chan struct{}) error {
	block := blockWithReceipts.Block
	receipts := blockWithReceipts.Receipts
	header := block.Header()
	// Sealing the genesis block is not supported
	number := header.Number.Uint64()

	if number == 0 {
		return errUnknownBlock
	}

	// For 0-period chains, refuse to seal empty blocks (no reward but would spin sealing)
	if c.config.CalculatePeriod(number) == 0 && len(block.Transactions()) == 0 {
		c.logger.Trace("[bor] Sealing paused, waiting for transactions")
		return nil
	}

	// Don't hold the signer fields for the entire sealing procedure
	currentSigner := c.authorizedSigner.Load()
	signer, signFn := currentSigner.signer, currentSigner.signFn

	var successionNumber int

	validatorSet, err := c.spanReader.Producers(context.Background(), number)
	if err != nil {
		return err
	}

	successionNumber, err = validatorSet.GetSignerSuccessionNumber(signer, number)
	if err != nil {
		return err
	}

	var delay time.Duration
	// Sweet, the protocol permits us to sign the block, wait for our time
	if c.config.IsBhilai(header.Number.Uint64()) && successionNumber == 0 {
		// For primary producers, set the delay to `header.Time - block time` instead of `header.Time`
		// for early block announcement instead of waiting for full block time.
		delay = time.Until(time.Unix(int64(header.Time)-int64(c.config.CalculatePeriod(number)), 0))
	} else {
		delay = time.Until(time.Unix(int64(header.Time), 0)) // Wait until we reach header time
	}
	// wiggle was already accounted for in header.Time, this is just for logging
	wiggle := time.Duration(successionNumber) * time.Duration(c.config.CalculateBackupMultiplier(number)) * time.Second

	// Sign all the things!
	sighash, err := signFn(signer, accounts.MimetypeBor, BorRLP(header, c.config))
	if err != nil {
		return err
	}
	copy(header.Extra[len(header.Extra)-types.ExtraSealLength:], sighash)

	go func() {
		// Wait until sealing is terminated or delay timeout.
		c.logger.Info("[bor] Waiting for slot to sign and propagate", "number", number, "hash", header.Hash, "delay", common.PrettyDuration(delay), "TxCount", block.Transactions().Len(), "Signer", signer)

		select {
		case <-stop:
			c.logger.Info("[bor] Stopped sealing operation for block", "number", number)
			results <- nil
			return
		case <-time.After(delay):

			if c.headerProgress != nil && c.headerProgress.Progress() >= number {
				c.logger.Info("Discarding sealing operation for block", "number", number)
				results <- nil
				return
			}

			if wiggle > 0 {
				c.logger.Info(
					"[bor] Sealed out-of-turn",
					"number", number,
					"wiggle", common.PrettyDuration(wiggle),
					"delay", delay,
					"headerDifficulty", header.Difficulty,
					"signer", signer.Hex(),
				)
			} else {
				c.logger.Info(
					"[bor] Sealed in-turn",
					"number", number,
					"delay", delay,
					"headerDifficulty", header.Difficulty,
					"signer", signer.Hex(),
				)
			}
		}
		select {
		case results <- &types.BlockWithReceipts{Block: block.WithSeal(header), Receipts: receipts}:
		default:
			c.logger.Warn("Sealing result was not read by miner", "number", number, "sealhash", SealHash(header, c.config))
		}
	}()
	return nil
}

// IsValidator returns true if this instance is the validator for this block
func (c *Bor) IsValidator(header *types.Header) (bool, error) {
	number := header.Number.Uint64()

	if number == 0 {
		return false, nil
	}

	currentSigner := c.authorizedSigner.Load()

	validatorSet, err := c.spanReader.Producers(context.Background(), number)
	if err != nil {
		return false, err
	}
	return validatorSet.HasAddress(currentSigner.signer), nil

}

// IsProposer returns true if this instance is the proposer for this block
func (c *Bor) IsProposer(header *types.Header) (bool, error) {
	number := header.Number.Uint64()
	if number == 0 {
		return false, nil
	}

	signer := c.authorizedSigner.Load().signer

	validatorSet, err := c.spanReader.Producers(context.Background(), number)
	if err != nil {
		return false, err
	}

	successionNumber, err := validatorSet.GetSignerSuccessionNumber(signer, number)
	return successionNumber == 0, err
}

// CalcDifficulty is the difficulty adjustment algorithm. It returns the difficulty
// that a new block should have based on the previous blocks in the chain and the
// current signer.
func (c *Bor) CalcDifficulty(chain consensus.ChainHeaderReader, _, _ uint64, _ *big.Int, parentNumber uint64, parentHash, _ common.Hash, _ uint64) *big.Int {
	signer := c.authorizedSigner.Load().signer

	validatorSet, err := c.spanReader.Producers(context.Background(), parentNumber+1)
	if err != nil {
		return nil
	}

	return big.NewInt(int64(validatorSet.SafeDifficulty(signer)))

}

// SealHash returns the hash of a block prior to it being sealed.
func (c *Bor) SealHash(header *types.Header) common.Hash {
	return SealHash(header, c.config)
}

func (c *Bor) IsServiceTransaction(sender common.Address, syscall consensus.SystemCall) bool {
	return false
}

// Depricated: To get the API use jsonrpc.APIList
func (c *Bor) APIs(chain consensus.ChainHeaderReader) []rpc.API {
	return []rpc.API{}
}

// Only needed to satisfy the consensus.Engine interface
func (c *Bor) Close() error {
	return nil
}

func (c *Bor) checkAndCommitSpan(header *types.Header, syscall consensus.SystemCall) error {
	headerNumber := header.Number.Uint64()
	currentSpan, err := c.spanner.GetCurrentSpan(syscall)
	if err != nil {
		return fmt.Errorf("GetCurrentSpan: %w", err)
	}

	// Whenever `checkAndCommitSpan` is called for the first time, during the start of 'technically'
	// second sprint, we need the 0th as well as the 1st span. The contract returns an empty
	// span (i.e. all fields set to 0). Span 0 doesn't need to be committed explicitly and
	// is committed eventually when we commit 1st span (as per the contract). The check below
	// takes care of that and commits the 1st span (hence the `currentSpan.Id+1` param).
	if currentSpan.EndBlock == 0 {
		if err := c.fetchAndCommitSpan(uint64(currentSpan.Id+1), syscall); err != nil {
			return fmt.Errorf("fetchAndCommitSpan: %w", err)
		}
	}

	// For subsequent calls, commit the next span on the first block of the last sprint of a span
	sprintLength := c.config.CalculateSprintLength(headerNumber)
	if currentSpan.EndBlock > sprintLength && currentSpan.EndBlock-sprintLength+1 == headerNumber {
		if err := c.fetchAndCommitSpan(uint64(currentSpan.Id+1), syscall); err != nil {
			return fmt.Errorf("fetchAndCommitSpan2: %w", err)
		}
	}

	return nil
}

func (c *Bor) fetchAndCommitSpan(newSpanID uint64, syscall consensus.SystemCall) error {
	heimdallSpan, ok, err := c.spanReader.Span(context.Background(), newSpanID)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New(fmt.Sprintf("error fetching span %v", newSpanID))
	}

	// check if chain id matches with heimdall span
	if heimdallSpan.ChainID != c.chainConfig.ChainID.String() {
		return fmt.Errorf(
			"chain id proposed span, %s, and bor chain id, %s, doesn't match",
			heimdallSpan.ChainID,
			c.chainConfig.ChainID.String(),
		)
	}

	return c.spanner.CommitSpan(*heimdallSpan, syscall)
}

func (c *Bor) GetRootHash(ctx context.Context, tx kv.Tx, start, end uint64) (string, error) {
	numHeaders := end - start + 1
	if numHeaders > maxCheckpointLength {
		return "", &MaxCheckpointLengthExceededError{Start: start, End: end}
	}

	cacheKey := strconv.FormatUint(start, 10) + "-" + strconv.FormatUint(end, 10)

	if c.rootHashCache == nil {
		c.rootHashCache, _ = lru.NewARC[string, string](100)
	}

	if root, known := c.rootHashCache.Get(cacheKey); known {
		return root, nil
	}

	header := rawdb.ReadCurrentHeader(tx)
	var currentHeaderNumber uint64 = 0
	if header == nil {
		return "", &heimdall.InvalidStartEndBlockError{Start: start, End: end, CurrentHeader: currentHeaderNumber}
	}
	currentHeaderNumber = header.Number.Uint64()
	if start > end || end > currentHeaderNumber {
		return "", &heimdall.InvalidStartEndBlockError{Start: start, End: end, CurrentHeader: currentHeaderNumber}
	}
	blockHeaders := make([]*types.Header, numHeaders)
	for number := start; number <= end; number++ {
		blockHeaders[number-start], _ = c.getHeaderByNumber(ctx, tx, number)
	}

	hash, err := ComputeHeadersRootHash(blockHeaders)
	if err != nil {
		return "", err
	}

	hashStr := hex.EncodeToString(hash)
	c.rootHashCache.Add(cacheKey, hashStr)
	return hashStr, nil
}

func ComputeHeadersRootHash(blockHeaders []*types.Header) ([]byte, error) {
	headers := make([][32]byte, NextPowerOfTwo(uint64(len(blockHeaders))))
	for i := 0; i < len(blockHeaders); i++ {
		blockHeader := blockHeaders[i]
		header := crypto.Keccak256(AppendBytes32(
			blockHeader.Number.Bytes(),
			new(big.Int).SetUint64(blockHeader.Time).Bytes(),
			blockHeader.TxHash[:],
			blockHeader.ReceiptHash[:],
		))

		var arr [32]byte
		copy(arr[:], header)
		headers[i] = arr
	}
	tree := merkle.NewTreeWithOpts(merkle.TreeOptions{EnableHashSorting: false, DisableHashLeaves: true})
	if err := tree.Generate(Convert(headers), sha3.NewLegacyKeccak256()); err != nil {
		return nil, err
	}

	return tree.Root().Hash, nil
}

func (c *Bor) getHeaderByNumber(ctx context.Context, tx kv.Tx, number uint64) (*types.Header, error) {
	header, err := c.blockReader.HeaderByNumber(ctx, tx, number)
	if err != nil {
		return nil, err
	}
	if header == nil {
		_, _ = c.blockReader.HeaderByNumber(dbg.ContextWithDebug(ctx, true), tx, number)
		return nil, fmt.Errorf("[bor] header not found: %d", number)
	}
	return header, nil
}

// CommitStates commit states
func (c *Bor) CommitStates(
	header *types.Header,
	chain statefull.ChainContext,
	syscall consensus.SystemCall,
	fetchEventsWithinTime bool,
) error {
	blockNum := header.Number.Uint64()
	var events []*types.Message
	var err error

	ctx := dbg.ContextWithDebug(c.execCtx, true)
	if fetchEventsWithinTime {
		sprintLength := c.config.CalculateSprintLength(blockNum)

		if blockNum < sprintLength {
			return nil
		}

		prevSprintStart := chain.Chain.GetHeaderByNumber(blockNum - sprintLength)
		stateSyncDelay := c.config.CalculateStateSyncDelay(blockNum)

		timeFrom := time.Unix(int64(prevSprintStart.Time-stateSyncDelay), 0)
		timeTo := time.Unix(int64(header.Time-stateSyncDelay), 0)

		// Previous sprint was not indore.
		if !c.config.IsIndore(prevSprintStart.Number.Uint64()) {
			if prevSprintStart.Number.Uint64() >= sprintLength {
				prevPrevSprintStart := chain.Chain.GetHeaderByNumber(prevSprintStart.Number.Uint64() - sprintLength)
				timeFrom = time.Unix(int64(prevPrevSprintStart.Time), 0)
			} else {
				timeFrom = time.Unix(0, 0)
			}
		}

		// Current sprint was not indore.
		if !c.config.IsIndore(blockNum) {
			timeTo = time.Unix(int64(prevSprintStart.Time), 0)
		}

		events, err = c.bridgeReader.EventsWithinTime(ctx, timeFrom, timeTo)
		if err != nil {
			return err
		}
	} else {
		events, err = c.bridgeReader.Events(ctx, header.Hash(), blockNum)
		if err != nil {
			return err
		}
	}

	for _, event := range events {
		_, err := syscall(*event.To(), event.Data())
		if err != nil {
			return err
		}
	}
	return nil
}

// BorTransfer transfer in Bor
func BorTransfer(db evmtypes.IntraBlockState, sender, recipient common.Address, amount *uint256.Int, bailout bool) error {
	// get inputs before
	input1, err := db.GetBalance(sender)
	if err != nil {
		return err
	}
	input2, err := db.GetBalance(recipient)
	if err != nil {
		return err
	}

	if !bailout {
		err := db.SubBalance(sender, *amount, tracing.BalanceChangeTransfer)
		if err != nil {
			return err
		}
	}
	err = db.AddBalance(recipient, *amount, tracing.BalanceChangeTransfer)
	if err != nil {
		return err
	}
	// get outputs after
	output1, err := db.GetBalance(sender)
	if err != nil {
		return err
	}
	output2, err := db.GetBalance(recipient)
	if err != nil {
		return err
	}
	// add transfer log into state
	addTransferLog(db, transferLogSig, sender, recipient, amount, &input1, &input2, &output1, &output2)
	return nil
}

func (c *Bor) GetTransferFunc() evmtypes.TransferFunc {
	return BorTransfer
}

// AddFeeTransferLog adds fee transfer log into state
// Deprecating transfer log and will be removed in future fork. PLEASE DO NOT USE this transfer log going forward. Parameters won't get updated as expected going forward with EIP1559
func AddFeeTransferLog(ibs evmtypes.IntraBlockState, sender common.Address, coinbase common.Address, result *evmtypes.ExecutionResult) {
	output1 := result.SenderInitBalance.Clone()
	output2 := result.CoinbaseInitBalance.Clone()
	addTransferLog(
		ibs,
		transferFeeLogSig,
		sender,
		coinbase,
		&result.FeeTipped,
		&result.SenderInitBalance,
		&result.CoinbaseInitBalance,
		output1.Sub(output1, &result.FeeTipped),
		output2.Add(output2, &result.FeeTipped),
	)

}

func (c *Bor) GetPostApplyMessageFunc() evmtypes.PostApplyMessageFunc {
	return AddFeeTransferLog
}

func (c *Bor) TxDependencies(h *types.Header) [][]int {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if dependencies, ok := c.Dependencies.Get(h.Hash()); ok {
		return dependencies
	}

	tempExtra := h.Extra

	if len(tempExtra) < types.ExtraVanityLength+types.ExtraSealLength {
		log.Error("length of extra less is than vanity and seal")
		return nil
	}

	var blockExtraData BlockExtraData

	if err := rlp.DecodeBytes(tempExtra[types.ExtraVanityLength:len(tempExtra)-types.ExtraSealLength], &blockExtraData); err != nil {
		log.Error("error while decoding block extra data", "err", err)
		return nil
	}

	c.Dependencies.Add(h.Hash(), blockExtraData.TxDependencies)

	return blockExtraData.TxDependencies
}

// In bor, RLP encoding of BlockExtraData will be stored in the Extra field in the header
type BlockExtraData struct {
	// Validator bytes of bor
	ValidatorBytes []byte

	// length of TxDependencies          ->   n (n = number of transactions in the block)
	// length of TxDependencies[i]       ->   k (k = a whole number)
	// k elements in TxDependencies[i]   ->   transaction indexes on which transaction i is dependent on
	TxDependencies [][]int
}

func GetValidatorBytes(h *types.Header, config *borcfg.BorConfig) []byte {
	tempExtra := h.Extra

	if !config.IsNapoli(h.Number.Uint64()) {
		return tempExtra[types.ExtraVanityLength : len(tempExtra)-types.ExtraSealLength]
	}

	if len(tempExtra) < types.ExtraVanityLength+types.ExtraSealLength {
		log.Error("length of extra less is than vanity and seal")
		return nil
	}

	var blockExtraData BlockExtraData
	if err := rlp.DecodeBytes(tempExtra[types.ExtraVanityLength:len(tempExtra)-types.ExtraSealLength], &blockExtraData); err != nil {
		log.Error("error while decoding block extra data", "err", err)
		return nil
	}

	return blockExtraData.ValidatorBytes
}

func VerifyUncles(uncles []*types.Header) error {
	if len(uncles) > 0 {
		return errUncleDetected
	}

	return nil
}
