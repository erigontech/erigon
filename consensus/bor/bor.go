package bor

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/btree"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/bor/clerk"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/span"
	"github.com/ledgerwatch/erigon/consensus/bor/statefull"
	"github.com/ledgerwatch/erigon/consensus/bor/valset"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/crypto/cryptopool"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/rpc"
)

const (
	checkpointInterval = 1024 // Number of blocks after which to save the vote snapshot to the database
	inmemorySnapshots  = 128  // Number of recent vote snapshots to keep in memory
	inmemorySignatures = 4096 // Number of recent block signatures to keep in memory
)

// Bor protocol constants.
var (
	defaultSprintLength = map[string]uint64{
		"0": 64,
	} // Default number of blocks after which to checkpoint and reset the pending votes

	extraVanity = 32 // Fixed number of extra-data prefix bytes reserved for signer vanity
	extraSeal   = 65 // Fixed number of extra-data suffix bytes reserved for signer seal

	uncleHash = types.CalcUncleHash(nil) // Always Keccak256(RLP([])) as uncles are meaningless outside of PoW.

	// diffInTurn = big.NewInt(2) // Block difficulty for in-turn signatures
	// diffNoTurn = big.NewInt(1) // Block difficulty for out-of-turn signatures

	validatorHeaderBytesLength = length.Addr + 20 // address + power
	// systemAddress              = libcommon.HexToAddress("0xffffFFFfFFffffffffffffffFfFFFfffFFFfFFfE")
)

// Various error messages to mark blocks invalid. These should be private to
// prevent engine specific errors from being referenced in the remainder of the
// codebase, inherently breaking if the engine is swapped out. Please put common
// error types into the consensus package.
var (
	// errUnknownBlock is returned when the list of signers is requested for a block
	// that is not part of the local blockchain.
	errUnknownBlock = errors.New("unknown block")

	// errInvalidCheckpointBeneficiary is returned if a checkpoint/epoch transition
	// block has a beneficiary set to non-zeroes.
	// errInvalidCheckpointBeneficiary = errors.New("beneficiary in checkpoint block non-zero")

	// errInvalidVote is returned if a nonce value is something else that the two
	// allowed constants of 0x00..0 or 0xff..f.
	// errInvalidVote = errors.New("vote nonce not 0x00..0 or 0xff..f")

	// errInvalidCheckpointVote is returned if a checkpoint/epoch transition block
	// has a vote nonce set to non-zeroes.
	// errInvalidCheckpointVote = errors.New("vote nonce in checkpoint block non-zero")

	// errMissingVanity is returned if a block's extra-data section is shorter than
	// 32 bytes, which is required to store the signer vanity.
	errMissingVanity = errors.New("extra-data 32 byte vanity prefix missing")

	// errMissingSignature is returned if a block's extra-data section doesn't seem
	// to contain a 65 byte secp256k1 signature.
	errMissingSignature = errors.New("extra-data 65 byte signature suffix missing")

	// errExtraValidators is returned if non-sprint-end block contain validator data in
	// their extra-data fields.
	errExtraValidators = errors.New("non-sprint-end block contains extra validator list")

	// errInvalidSpanValidators is returned if a block contains an
	// invalid list of validators (i.e. non divisible by 40 bytes).
	errInvalidSpanValidators = errors.New("invalid validator list on sprint end block")

	// errInvalidMixDigest is returned if a block's mix digest is non-zero.
	errInvalidMixDigest = errors.New("non-zero mix digest")

	// errInvalidUncleHash is returned if a block contains an non-empty uncle list.
	errInvalidUncleHash = errors.New("non empty uncle hash")

	// errInvalidDifficulty is returned if the difficulty of a block neither 1 or 2.
	errInvalidDifficulty = errors.New("invalid difficulty")

	// ErrInvalidTimestamp is returned if the timestamp of a block is lower than
	// the previous block's timestamp + the minimum block period.
	ErrInvalidTimestamp = errors.New("invalid timestamp")

	// errOutOfRangeChain is returned if an authorization list is attempted to
	// be modified via out-of-range or non-contiguous headers.
	errOutOfRangeChain = errors.New("out of range or non-contiguous chain")

	errUncleDetected     = errors.New("uncles not allowed")
	errUnknownValidators = errors.New("unknown validators")
)

// SignerFn is a signer callback function to request a header to be signed by a
// backing account.
type SignerFn func(signer libcommon.Address, mimeType string, message []byte) ([]byte, error)

// ecrecover extracts the Ethereum account address from a signed header.
func ecrecover(header *types.Header, sigcache *lru.ARCCache[libcommon.Hash, libcommon.Address], c *chain.BorConfig) (libcommon.Address, error) {
	// If the signature's already cached, return that
	hash := header.Hash()
	if address, known := sigcache.Get(hash); known {
		return address, nil
	}
	// Retrieve the signature from the header extra-data
	if len(header.Extra) < extraSeal {
		return libcommon.Address{}, errMissingSignature
	}

	signature := header.Extra[len(header.Extra)-extraSeal:]

	// Recover the public key and the Ethereum address
	pubkey, err := crypto.Ecrecover(SealHash(header, c).Bytes(), signature)
	if err != nil {
		return libcommon.Address{}, err
	}
	var signer libcommon.Address
	copy(signer[:], crypto.Keccak256(pubkey[1:])[12:])

	sigcache.Add(hash, signer)

	return signer, nil
}

// SealHash returns the hash of a block prior to it being sealed.
func SealHash(header *types.Header, c *chain.BorConfig) (hash libcommon.Hash) {
	hasher := cryptopool.NewLegacyKeccak256()
	defer cryptopool.ReturnToPoolKeccak256(hasher)

	encodeSigHeader(hasher, header, c)
	hasher.Sum(hash[:0])

	return hash
}

func encodeSigHeader(w io.Writer, header *types.Header, c *chain.BorConfig) {
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
func CalcProducerDelay(number uint64, succession int, c *chain.BorConfig) uint64 {
	// When the block is the first block of the sprint, it is expected to be delayed by `producerDelay`.
	// That is to allow time for block propagation in the last sprint
	delay := c.CalculatePeriod(number)
	if number%c.CalculateSprint(number) == 0 {
		delay = c.CalculateProducerDelay(number)
	}

	if succession > 0 {
		delay += uint64(succession) * c.CalculateBackupMultiplier(number)
	}

	return delay
}

// BorRLP returns the rlp bytes which needs to be signed for the bor
// sealing. The RLP to sign consists of the entire header apart from the 65 byte signature
// contained at the end of the extra data.
//
// Note, the method requires the extra data to be at least 65 bytes, otherwise it
// panics. This is done to avoid accidentally using both forms (signature present
// or not), which could be abused to produce different hashes for the same header.
func BorRLP(header *types.Header, c *chain.BorConfig) []byte {
	b := new(bytes.Buffer)
	encodeSigHeader(b, header, c)

	return b.Bytes()
}

// Bor is the matic-bor consensus engine
type Bor struct {
	chainConfig *chain.Config    // Chain config
	config      *chain.BorConfig // Consensus engine configuration parameters for bor consensus
	DB          kv.RwDB          // Database to store and retrieve snapshot checkpoints

	recents    *lru.ARCCache[libcommon.Hash, *Snapshot]         // Snapshots for recent block to speed up reorgs
	signatures *lru.ARCCache[libcommon.Hash, libcommon.Address] // Signatures of recent blocks to speed up mining

	authorizedSigner atomic.Pointer[signer] // Ethereum address and sign function of the signing key

	execCtx context.Context // context of caller execution stage

	spanner                Spanner
	GenesisContractsClient GenesisContract
	HeimdallClient         IHeimdallClient

	// scope event.SubscriptionScope
	// The fields below are for testing only
	fakeDiff  bool // Skip difficulty verifications
	spanCache *btree.BTree

	closeOnce sync.Once
	logger    log.Logger
}

type signer struct {
	signer libcommon.Address // Ethereum address of the signing key
	signFn SignerFn          // Signer function to authorize hashes with
}

// New creates a Matic Bor consensus engine.
func New(
	chainConfig *chain.Config,
	db kv.RwDB,
	spanner Spanner,
	heimdallClient IHeimdallClient,
	genesisContracts GenesisContract,
	logger log.Logger,
) *Bor {
	// get bor config
	borConfig := chainConfig.Bor

	// Set any missing consensus parameters to their defaults
	if borConfig != nil && borConfig.CalculateSprint(0) == 0 {
		borConfig.Sprint = defaultSprintLength
	}

	// Allocate the snapshot caches and create the engine
	recents, _ := lru.NewARC[libcommon.Hash, *Snapshot](inmemorySnapshots)
	signatures, _ := lru.NewARC[libcommon.Hash, libcommon.Address](inmemorySignatures)
	c := &Bor{
		chainConfig:            chainConfig,
		config:                 borConfig,
		DB:                     db,
		recents:                recents,
		signatures:             signatures,
		spanner:                spanner,
		GenesisContractsClient: genesisContracts,
		HeimdallClient:         heimdallClient,
		spanCache:              btree.New(32),
		execCtx:                context.Background(),
		logger:                 logger,
	}

	c.authorizedSigner.Store(&signer{
		libcommon.Address{},
		func(_ libcommon.Address, _ string, i []byte) ([]byte, error) {
			// return an error to prevent panics
			return nil, &UnauthorizedSignerError{0, libcommon.Address{}.Bytes()}
		},
	})

	// make sure we can decode all the GenesisAlloc in the BorConfig.
	for key, genesisAlloc := range c.config.BlockAlloc {
		if _, err := decodeGenesisAlloc(genesisAlloc); err != nil {
			panic(fmt.Sprintf("BUG: Block alloc '%s' in genesis is not correct: %v", key, err))
		}
	}

	return c
}

// Type returns underlying consensus engine
func (c *Bor) Type() chain.ConsensusName {
	return chain.BorConsensus
}

// Author implements consensus.Engine, returning the Ethereum address recovered
// from the signature in the header's extra-data section.
// This is thread-safe (only access the header and config (which is never updated),
// as well as signatures, which are lru.ARCCache, which is thread-safe)
func (c *Bor) Author(header *types.Header) (libcommon.Address, error) {
	return ecrecover(header, c.signatures, c.config)
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

	// Don't waste time checking blocks from the future
	if header.Time > uint64(time.Now().Unix()) {
		return consensus.ErrFutureBlock
	}

	if err := validateHeaderExtraField(header.Extra); err != nil {
		return err
	}

	// check extr adata
	isSprintEnd := isSprintStart(number+1, c.config.CalculateSprint(number))

	// Ensure that the extra-data contains a signer list on checkpoint, but none otherwise
	signersBytes := len(header.Extra) - extraVanity - extraSeal
	if !isSprintEnd && signersBytes != 0 {
		return errExtraValidators
	}

	if isSprintEnd && signersBytes%validatorHeaderBytesLength != 0 {
		return errInvalidSpanValidators
	}

	// Ensure that the mix digest is zero as we don't have fork protection currently
	if header.MixDigest != (libcommon.Hash{}) {
		return errInvalidMixDigest
	}

	// Ensure that the block doesn't contain any uncles which are meaningless in PoA
	if header.UncleHash != uncleHash {
		return errInvalidUncleHash
	}

	// Ensure that the block's difficulty is meaningful (may not be correct at this point)
	if number > 0 {
		if header.Difficulty == nil {
			return errInvalidDifficulty
		}
	}

	// Verify that the gas limit is <= 2^63-1
	gasCap := uint64(0x7fffffffffffffff)

	if header.GasLimit > gasCap {
		return fmt.Errorf("invalid gasLimit: have %v, max %v", header.GasLimit, gasCap)
	}

	// All basic checks passed, verify cascading fields
	return c.verifyCascadingFields(chain, header, parents)
}

// validateHeaderExtraField validates that the extra-data contains both the vanity and signature.
// header.Extra = header.Vanity + header.ProducerBytes (optional) + header.Seal
func validateHeaderExtraField(extraBytes []byte) error {
	if len(extraBytes) < extraVanity {
		return errMissingVanity
	}

	if len(extraBytes) < extraVanity+extraSeal {
		return errMissingSignature
	}

	return nil
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

	// Verify that the gasUsed is <= gasLimit
	if header.GasUsed > header.GasLimit {
		return fmt.Errorf("invalid gasUsed: have %d, gasLimit %d", header.GasUsed, header.GasLimit)
	}

	if !chain.Config().IsLondon(header.Number.Uint64()) {
		// Verify BaseFee not present before EIP-1559 fork.
		if header.BaseFee != nil {
			return fmt.Errorf("invalid baseFee before fork: have %d, want <nil>", header.BaseFee)
		}
		if err := misc.VerifyGaslimit(parent.GasLimit, header.GasLimit); err != nil {
			return err
		}
	} else if err := misc.VerifyEip1559Header(chain.Config(), parent, header); err != nil {
		// Verify the header's EIP-1559 attributes.
		return err
	}

	if header.WithdrawalsHash != nil {
		return consensus.ErrUnexpectedWithdrawals
	}

	if parent.Time+c.config.CalculatePeriod(number) > header.Time {
		return ErrInvalidTimestamp
	}

	// Retrieve the snapshot needed to verify this header and cache it
	snap, err := c.snapshot(chain, number-1, header.ParentHash, parents)
	if err != nil {
		return err
	}

	// Verify the validator list match the local contract
	if isSprintStart(number+1, c.config.CalculateSprint(number)) {
		newValidators, err := c.spanner.GetCurrentValidators(number+1, c.authorizedSigner.Load().signer, c.getSpanForBlock)

		if err != nil {
			return err
		}

		sort.Sort(valset.ValidatorsByAddress(newValidators))

		headerVals, err := valset.ParseValidators(header.Extra[extraVanity : len(header.Extra)-extraSeal])

		if err != nil {
			return err
		}

		if len(newValidators) != len(headerVals) {
			return errInvalidSpanValidators
		}

		for i, val := range newValidators {
			if !bytes.Equal(val.HeaderBytes(), headerVals[i].HeaderBytes()) {
				return errInvalidSpanValidators
			}
		}
	}

	// verify the validator list in the last sprint block
	if isSprintStart(number, c.config.CalculateSprint(number)) {
		parentValidatorBytes := parent.Extra[extraVanity : len(parent.Extra)-extraSeal]
		validatorsBytes := make([]byte, len(snap.ValidatorSet.Validators)*validatorHeaderBytesLength)

		currentValidators := snap.ValidatorSet.Copy().Validators
		// sort validator by address
		sort.Sort(valset.ValidatorsByAddress(currentValidators))
		for i, validator := range currentValidators {
			copy(validatorsBytes[i*validatorHeaderBytesLength:], validator.HeaderBytes())
		}
		// len(header.Extra) >= extraVanity+extraSeal has already been validated in validateHeaderExtraField, so this won't result in a panic
		if !bytes.Equal(parentValidatorBytes, validatorsBytes) {
			return &MismatchingValidatorsError{number - 1, validatorsBytes, parentValidatorBytes}
		}
	}

	// All basic checks passed, verify the seal and return
	return c.verifySeal(chain, header, parents)
}

// snapshot retrieves the authorization snapshot at a given point in time.
func (c *Bor) snapshot(chain consensus.ChainHeaderReader, number uint64, hash libcommon.Hash, parents []*types.Header) (*Snapshot, error) {
	// Search for a snapshot in memory or on disk for checkpoints
	var snap *Snapshot

	headers := make([]*types.Header, 0, 16)

	//nolint:govet
	for snap == nil {
		// If an in-memory snapshot was found, use that
		if s, ok := c.recents.Get(hash); ok {
			snap = s

			break
		}

		// If an on-disk checkpoint snapshot can be found, use that
		if number%checkpointInterval == 0 {
			if s, err := loadSnapshot(c.config, c.signatures, c.DB, hash); err == nil {
				c.logger.Trace("Loaded snapshot from disk", "number", number, "hash", hash)

				snap = s

				break
			}
		}

		// If we're at the genesis, snapshot the initial state. Alternatively if we're
		// at a checkpoint block without a parent (light client CHT), or we have piled
		// up more headers than allowed to be reorged (chain reinit from a freezer),
		// consider the checkpoint trusted and snapshot it.

		// TODO fix this
		// nolint:nestif
		if number == 0 {
			checkpoint := chain.GetHeaderByNumber(number)
			if checkpoint != nil {
				// get checkpoint data
				hash := checkpoint.Hash()

				// get validators and current span
				validators, err := c.spanner.GetCurrentValidators(number+1, c.authorizedSigner.Load().signer, c.getSpanForBlock)
				if err != nil {
					return nil, err
				}

				// new snap shot
				snap = newSnapshot(c.config, c.signatures, number, hash, validators)
				if err := snap.store(c.DB); err != nil {
					return nil, err
				}

				c.logger.Info("Stored checkpoint snapshot to disk", "number", number, "hash", hash)

				break
			}
		}

		// No snapshot for this header, gather the header and move backward
		var header *types.Header
		if len(parents) > 0 {
			// If we have explicit parents, pick from there (enforced)
			header = parents[len(parents)-1]
			if header.Hash() != hash || header.Number.Uint64() != number {
				return nil, consensus.ErrUnknownAncestor
			}

			parents = parents[:len(parents)-1]
		} else {
			// No explicit parents (or no more left), reach out to the database
			header = chain.GetHeader(hash, number)
			if header == nil {
				return nil, consensus.ErrUnknownAncestor
			}
		}

		headers = append(headers, header)
		number, hash = number-1, header.ParentHash
	}

	// check if snapshot is nil
	if snap == nil {
		return nil, fmt.Errorf("unknown error while retrieving snapshot at block number %v", number)
	}

	// Previous snapshot found, apply any pending headers on top of it
	for i := 0; i < len(headers)/2; i++ {
		headers[i], headers[len(headers)-1-i] = headers[len(headers)-1-i], headers[i]
	}

	snap, err := snap.apply(headers, c.logger)
	if err != nil {
		return nil, err
	}

	c.recents.Add(snap.Hash, snap)

	// If we've generated a new checkpoint snapshot, save to disk
	if snap.Number%checkpointInterval == 0 && len(headers) > 0 {
		if err = snap.store(c.DB); err != nil {
			return nil, err
		}

		c.logger.Trace("Stored snapshot to disk", "number", snap.Number, "hash", snap.Hash)
	}

	return snap, err
}

// VerifyUncles implements consensus.Engine, always returning an error for any
// uncles as this consensus mechanism doesn't permit uncles.
// VerifyUncles implements consensus.Engine, always returning an error for any
// uncles as this consensus mechanism doesn't permit uncles.
func (c *Bor) VerifyUncles(_ consensus.ChainReader, _ *types.Header, uncles []*types.Header) error {
	if len(uncles) > 0 {
		return errUncleDetected
	}

	return nil
}

// VerifySeal implements consensus.Engine, checking whether the signature contained
// in the header satisfies the consensus protocol requirements.
func (c *Bor) VerifySeal(chain consensus.ChainHeaderReader, header *types.Header) error {
	return c.verifySeal(chain, header, nil)
}

// verifySeal checks whether the signature contained in the header satisfies the
// consensus protocol requirements. The method accepts an optional list of parent
// headers that aren't yet part of the local blockchain to generate the snapshots
// from.
func (c *Bor) verifySeal(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header) error {
	// Verifying the genesis block is not supported
	number := header.Number.Uint64()
	if number == 0 {
		return errUnknownBlock
	}
	// Resolve the authorization key and check against signers
	signer, err := ecrecover(header, c.signatures, c.config)
	if err != nil {
		return err
	}

	// Retrieve the snapshot needed to verify this header and cache it
	snap, err := c.snapshot(chain, number-1, header.ParentHash, parents)
	if err != nil {
		return err
	}

	if !snap.ValidatorSet.HasAddress(signer) {
		// Check the UnauthorizedSignerError.Error() msg to see why we pass number-1
		return &UnauthorizedSignerError{number - 1, signer.Bytes()}
	}

	succession, err := snap.GetSignerSuccessionNumber(signer)
	if err != nil {
		return err
	}

	var parent *types.Header
	if len(parents) > 0 { // if parents is nil, len(parents) is zero
		parent = parents[len(parents)-1]
	} else if number > 0 {
		parent = chain.GetHeader(header.ParentHash, number-1)
	}

	if parent != nil && header.Time < parent.Time+CalcProducerDelay(number, succession, c.config) {
		return &BlockTooSoonError{number, succession}
	}

	// Ensure that the difficulty corresponds to the turn-ness of the signer
	if !c.fakeDiff {
		difficulty := snap.Difficulty(signer)
		if header.Difficulty.Uint64() != difficulty {
			return &WrongDifficultyError{number, difficulty, header.Difficulty.Uint64(), signer.Bytes()}
		}
	}

	return nil
}

func IsBlockOnTime(parent *types.Header, header *types.Header, number uint64, succession int, cfg *chain.BorConfig) bool {
	return parent != nil && header.Time < parent.Time+CalcProducerDelay(number, succession, cfg)
}

// Prepare implements consensus.Engine, preparing all the consensus fields of the
// header for running the transactions on top.
func (c *Bor) Prepare(chain consensus.ChainHeaderReader, header *types.Header, state *state.IntraBlockState) error {
	// If the block isn't a checkpoint, cast a random vote (good enough for now)
	header.Coinbase = libcommon.Address{}
	header.Nonce = types.BlockNonce{}

	number := header.Number.Uint64()
	// Assemble the validator snapshot to check which votes make sense
	snap, err := c.snapshot(chain, number-1, header.ParentHash, nil)
	if err != nil {
		return err
	}

	// Set the correct difficulty
	header.Difficulty = new(big.Int).SetUint64(snap.Difficulty(c.authorizedSigner.Load().signer))

	// Ensure the extra data has all it's components
	if len(header.Extra) < extraVanity {
		header.Extra = append(header.Extra, bytes.Repeat([]byte{0x00}, extraVanity-len(header.Extra))...)
	}

	header.Extra = header.Extra[:extraVanity]

	// get validator set if number
	if isSprintStart(number+1, c.config.CalculateSprint(number)) {
		newValidators, err := c.spanner.GetCurrentValidators(number+1, c.authorizedSigner.Load().signer, c.getSpanForBlock)
		if err != nil {
			return errUnknownValidators
		}

		// sort validator by address
		sort.Sort(valset.ValidatorsByAddress(newValidators))

		for _, validator := range newValidators {
			header.Extra = append(header.Extra, validator.HeaderBytes()...)
		}
	}

	// add extra seal space
	header.Extra = append(header.Extra, make([]byte, extraSeal)...)

	// Mix digest is reserved for now, set to empty
	header.MixDigest = libcommon.Hash{}

	// Ensure the timestamp has the correct delay
	parent := chain.GetHeader(header.ParentHash, number-1)
	if parent == nil {
		return consensus.ErrUnknownAncestor
	}

	var succession int
	// if signer is not empty
	if !bytes.Equal(c.authorizedSigner.Load().signer.Bytes(), libcommon.Address{}.Bytes()) {
		succession, err = snap.GetSignerSuccessionNumber(c.authorizedSigner.Load().signer)
		if err != nil {
			return err
		}
	}

	header.Time = parent.Time + CalcProducerDelay(number, succession, c.config)
	if header.Time < uint64(time.Now().Unix()) {
		header.Time = uint64(time.Now().Unix())
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
	chain consensus.ChainHeaderReader, syscall consensus.SystemCall,
) (types.Transactions, types.Receipts, error) {
	var err error

	headerNumber := header.Number.Uint64()

	if isSprintStart(headerNumber, c.config.CalculateSprint(headerNumber)) {
		cx := statefull.ChainContext{Chain: chain, Bor: c}
		// check and commit span
		if err := c.checkAndCommitSpan(state, header, cx, syscall); err != nil {
			c.logger.Error("Error while committing span", "err", err)
			return nil, types.Receipts{}, err
		}

		if c.HeimdallClient != nil {
			// commit states
			_, err = c.CommitStates(state, header, cx, syscall)
			if err != nil {
				c.logger.Error("Error while committing states", "err", err)
				return nil, types.Receipts{}, err
			}
		}
	}

	if err = c.changeContractCodeIfNeeded(headerNumber, state); err != nil {
		c.logger.Error("Error changing contract code", "err", err)
		return nil, types.Receipts{}, err
	}

	// No block rewards in PoA, so the state remains as is and uncles are dropped
	// header.Root = state.IntermediateRoot(chain.Config().IsSpuriousDragon(header.Number.Uint64()))
	header.UncleHash = types.CalcUncleHash(nil)

	// Set state sync data to blockchain
	// bc := chain.(*core.BlockChain)
	// bc.SetStateSync(stateSyncData)
	return nil, types.Receipts{}, nil
}

func decodeGenesisAlloc(i interface{}) (types.GenesisAlloc, error) {
	var alloc types.GenesisAlloc

	b, err := json.Marshal(i)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(b, &alloc); err != nil {
		return nil, err
	}

	return alloc, nil
}

func (c *Bor) changeContractCodeIfNeeded(headerNumber uint64, state *state.IntraBlockState) error {
	for blockNumber, genesisAlloc := range c.config.BlockAlloc {
		if blockNumber == strconv.FormatUint(headerNumber, 10) {
			allocs, err := decodeGenesisAlloc(genesisAlloc)
			if err != nil {
				return fmt.Errorf("failed to decode genesis alloc: %v", err)
			}

			for addr, account := range allocs {
				c.logger.Trace("change contract code", "address", addr)
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
	chain consensus.ChainHeaderReader, syscall consensus.SystemCall, call consensus.Call,
) (*types.Block, types.Transactions, types.Receipts, error) {
	// stateSyncData := []*types.StateSyncData{}

	headerNumber := header.Number.Uint64()
	if isSprintStart(headerNumber, c.config.CalculateSprint(headerNumber)) {
		cx := statefull.ChainContext{Chain: chain, Bor: c}

		// check and commit span
		err := c.checkAndCommitSpan(state, header, cx, syscall)
		if err != nil {
			c.logger.Error("Error while committing span", "err", err)
			return nil, nil, types.Receipts{}, err
		}

		if c.HeimdallClient != nil {
			// commit states
			_, err = c.CommitStates(state, header, cx, syscall)
			if err != nil {
				c.logger.Error("Error while committing states", "err", err)
				return nil, nil, types.Receipts{}, err
			}
		}
	}

	if err := c.changeContractCodeIfNeeded(headerNumber, state); err != nil {
		c.logger.Error("Error changing contract code", "err", err)
		return nil, nil, types.Receipts{}, err
	}

	// No block rewards in PoA, so the state remains as is and uncles are dropped
	// header.Root = state.IntermediateRoot(chain.Config().IsSpuriousDragon(header.Number))
	header.UncleHash = types.CalcUncleHash(nil)

	// Assemble block
	block := types.NewBlock(header, txs, nil, receipts, withdrawals)

	// set state sync
	// bc := chain.(*core.BlockChain)
	// bc.SetStateSync(stateSyncData)

	// return the final block for sealing
	return block, txs, receipts, nil
}

func (c *Bor) GenerateSeal(chain consensus.ChainHeaderReader, currnt, parent *types.Header, call consensus.Call) []byte {
	return nil
}

func (c *Bor) Initialize(config *chain.Config, chain consensus.ChainHeaderReader, header *types.Header,
	state *state.IntraBlockState, txs []types.Transaction, uncles []*types.Header, syscall consensus.SystemCall) {
}

// Authorize injects a private key into the consensus engine to mint new blocks
// with.
func (c *Bor) Authorize(currentSigner libcommon.Address, signFn SignerFn) {
	c.authorizedSigner.Store(&signer{
		signer: currentSigner,
		signFn: signFn,
	})
}

// Seal implements consensus.Engine, attempting to create a sealed block using
// the local signing credentials.
func (c *Bor) Seal(chain consensus.ChainHeaderReader, block *types.Block, results chan<- *types.Block, stop <-chan struct{}) error {
	header := block.Header()
	// Sealing the genesis block is not supported
	number := header.Number.Uint64()

	if number == 0 {
		return errUnknownBlock
	}

	// For 0-period chains, refuse to seal empty blocks (no reward but would spin sealing)
	if c.config.CalculatePeriod(number) == 0 && len(block.Transactions()) == 0 {
		c.logger.Trace("Sealing paused, waiting for transactions")
		return nil
	}

	// Don't hold the signer fields for the entire sealing procedure
	currentSigner := c.authorizedSigner.Load()
	signer, signFn := currentSigner.signer, currentSigner.signFn

	snap, err := c.snapshot(chain, number-1, header.ParentHash, nil)
	if err != nil {
		return err
	}

	// Bail out if we're unauthorized to sign a block
	if !snap.ValidatorSet.HasAddress(signer) {
		// Check the UnauthorizedSignerError.Error() msg to see why we pass number-1
		return &UnauthorizedSignerError{number - 1, signer.Bytes()}
	}

	successionNumber, err := snap.GetSignerSuccessionNumber(signer)
	if err != nil {
		return err
	}

	// Sweet, the protocol permits us to sign the block, wait for our time
	delay := time.Unix(int64(header.Time), 0).Sub(time.Now()) // nolint: gosimple
	// wiggle was already accounted for in header.Time, this is just for logging
	wiggle := time.Duration(successionNumber) * time.Duration(c.config.CalculateBackupMultiplier(number)) * time.Second

	// Sign all the things!
	sighash, err := signFn(signer, accounts.MimetypeBor, BorRLP(header, c.config))
	if err != nil {
		return err
	}
	copy(header.Extra[len(header.Extra)-extraSeal:], sighash)

	// Wait until sealing is terminated or delay timeout.
	c.logger.Info("Waiting for slot to sign and propagate", "number", number, "hash", header.Hash, "delay-in-sec", uint(delay), "delay", common.PrettyDuration(delay))

	go func() {
		select {
		case <-stop:
			c.logger.Info("Discarding sealing operation for block", "number", number)
			return
		case <-time.After(delay):
			if wiggle > 0 {
				c.logger.Info(
					"Sealing out-of-turn",
					"number", number,
					"wiggle", common.PrettyDuration(wiggle),
					"in-turn-signer", snap.ValidatorSet.GetProposer().Address.Hex(),
				)
			}

			c.logger.Info(
				"Sealing successful",
				"number", number,
				"delay", delay,
				"headerDifficulty", header.Difficulty,
			)
		}
		select {
		case results <- block.WithSeal(header):
		default:
			c.logger.Warn("Sealing result was not read by miner", "number", number, "sealhash", SealHash(header, c.config))
		}
	}()
	return nil
}

// CalcDifficulty is the difficulty adjustment algorithm. It returns the difficulty
// that a new block should have based on the previous blocks in the chain and the
// current signer.
// func (c *Bor) CalcDifficulty(chain consensus.ChainHeaderReader, time uint64, parent *types.Header) *big.Int {
// 	snap, err := c.snapshot(chain, parent.Number.Uint64(), parent.Hash(), nil)
// 	if err != nil {
// 		return nil
// 	}
// 	return new(big.Int).SetUint64(snap.Difficulty(c.signer))
// }

func (c *Bor) CalcDifficulty(chain consensus.ChainHeaderReader, _, _ uint64, _ *big.Int, parentNumber uint64, parentHash, _ libcommon.Hash, _ uint64) *big.Int {
	snap, err := c.snapshot(chain, parentNumber, parentHash, nil)
	if err != nil {
		return nil
	}

	return new(big.Int).SetUint64(snap.Difficulty(c.authorizedSigner.Load().signer))
}

// SealHash returns the hash of a block prior to it being sealed.
func (c *Bor) SealHash(header *types.Header) libcommon.Hash {
	return SealHash(header, c.config)
}

func (c *Bor) IsServiceTransaction(sender libcommon.Address, syscall consensus.SystemCall) bool {
	return false
}

// APIs implements consensus.Engine, returning the user facing RPC API to allow
// controlling the signer voting.
func (c *Bor) APIs(chain consensus.ChainHeaderReader) []rpc.API {
	return []rpc.API{{
		Namespace: "bor",
		Version:   "1.0",
		Service:   &API{chain: chain, bor: c},
		Public:    false,
	}}
}

// Close implements consensus.Engine. It's a noop for bor as there are no background threads.
func (c *Bor) Close() error {
	c.closeOnce.Do(func() {
		if c.HeimdallClient != nil {
			c.HeimdallClient.Close()
		}
	})

	return nil
}

func (c *Bor) checkAndCommitSpan(
	state *state.IntraBlockState,
	header *types.Header,
	chain statefull.ChainContext,
	syscall consensus.SystemCall,
) error {
	headerNumber := header.Number.Uint64()

	span, err := c.spanner.GetCurrentSpan(syscall)
	if err != nil {
		return err
	}

	if c.needToCommitSpan(span, headerNumber) {
		err := c.fetchAndCommitSpan(span.ID+1, state, header, chain, syscall)
		return err
	}

	return nil
}

func (c *Bor) needToCommitSpan(currentSpan *span.Span, headerNumber uint64) bool {
	// if span is nil
	if currentSpan == nil {
		return false
	}

	// check span is not set initially
	if currentSpan.EndBlock == 0 {
		return true
	}

	// if current block is first block of last sprint in current span
	if currentSpan.EndBlock > c.config.CalculateSprint(headerNumber) && currentSpan.EndBlock-c.config.CalculateSprint(headerNumber)+1 == headerNumber {
		return true
	}

	return false
}

func (c *Bor) getSpanForBlock(blockNum uint64) (*span.HeimdallSpan, error) {
	c.logger.Info("Getting span", "for block", blockNum)
	var borSpan *span.HeimdallSpan
	c.spanCache.AscendGreaterOrEqual(&span.HeimdallSpan{Span: span.Span{EndBlock: blockNum}}, func(item btree.Item) bool {
		borSpan = item.(*span.HeimdallSpan)
		return false
	})

	if borSpan == nil {
		// Span with high enough block number is not loaded
		var spanID uint64
		if c.spanCache.Len() > 0 {
			spanID = c.spanCache.Max().(*span.HeimdallSpan).ID + 1
		}
		for borSpan == nil || borSpan.EndBlock < blockNum {
			c.logger.Info("Span with high enough block number is not loaded", "fetching span", spanID)
			response, err := c.HeimdallClient.Span(c.execCtx, spanID)
			if err != nil {
				return nil, err
			}
			borSpan = response
			c.spanCache.ReplaceOrInsert(borSpan)
			spanID++
		}
	} else {
		for borSpan.StartBlock > blockNum {
			// Span wit low enough block number is not loaded
			var spanID = borSpan.ID - 1
			c.logger.Info("Span with low enough block number is not loaded", "fetching span", spanID)
			response, err := c.HeimdallClient.Span(c.execCtx, spanID)
			if err != nil {
				return nil, err
			}
			borSpan = response
			c.spanCache.ReplaceOrInsert(borSpan)
		}
	}

	for c.spanCache.Len() > 128 {
		c.spanCache.DeleteMin()
	}

	return borSpan, nil
}

func (c *Bor) fetchAndCommitSpan(
	newSpanID uint64,
	state *state.IntraBlockState,
	header *types.Header,
	chain statefull.ChainContext,
	syscall consensus.SystemCall,
) error {
	var heimdallSpan span.HeimdallSpan

	if c.HeimdallClient == nil {
		// fixme: move to a new mock or fake and remove c.HeimdallClient completely
		s, err := c.getNextHeimdallSpanForTest(newSpanID, state, header, chain, syscall)
		if err != nil {
			return err
		}

		heimdallSpan = *s
	} else {
		response, err := c.HeimdallClient.Span(c.execCtx, newSpanID)
		if err != nil {
			return err
		}

		heimdallSpan = *response
	}

	// check if chain id matches with heimdall span
	if heimdallSpan.ChainID != c.chainConfig.ChainID.String() {
		return fmt.Errorf(
			"chain id proposed span, %s, and bor chain id, %s, doesn't match",
			heimdallSpan.ChainID,
			c.chainConfig.ChainID.String(),
		)
	}

	return c.spanner.CommitSpan(heimdallSpan, syscall)
}

// CommitStates commit states
func (c *Bor) CommitStates(
	state *state.IntraBlockState,
	header *types.Header,
	chain statefull.ChainContext,
	syscall consensus.SystemCall,
) ([]*types.StateSyncData, error) {
	stateSyncs := make([]*types.StateSyncData, 0)
	number := header.Number.Uint64()

	_lastStateID, err := c.GenesisContractsClient.LastStateId(syscall)
	if err != nil {
		return nil, err
	}

	to := time.Unix(int64(chain.Chain.GetHeaderByNumber(number-c.config.CalculateSprint(number)).Time), 0)
	lastStateID := _lastStateID.Uint64()

	c.logger.Debug(
		"Fetching state updates from Heimdall",
		"fromID", lastStateID+1,
		"to", to.Format(time.RFC3339))

	eventRecords, err := c.HeimdallClient.StateSyncEvents(c.execCtx, lastStateID+1, to.Unix())
	if err != nil {
		return nil, err
	}

	if c.config.OverrideStateSyncRecords != nil {
		if val, ok := c.config.OverrideStateSyncRecords[strconv.FormatUint(number, 10)]; ok {
			eventRecords = eventRecords[0:val]
		}
	}

	chainID := c.chainConfig.ChainID.String()
	for _, eventRecord := range eventRecords {
		if eventRecord.ID <= lastStateID {
			continue
		}

		if err := validateEventRecord(eventRecord, number, to, lastStateID, chainID); err != nil {
			c.logger.Error(err.Error())
			break
		}

		stateData := types.StateSyncData{
			ID:       eventRecord.ID,
			Contract: eventRecord.Contract,
			Data:     hex.EncodeToString(eventRecord.Data),
			TxHash:   eventRecord.TxHash,
		}
		stateSyncs = append(stateSyncs, &stateData)

		if err := c.GenesisContractsClient.CommitState(eventRecord, syscall); err != nil {
			return nil, err
		}
		lastStateID++
	}

	return stateSyncs, nil
}

func validateEventRecord(eventRecord *clerk.EventRecordWithTime, number uint64, to time.Time, lastStateID uint64, chainID string) error {
	// event id should be sequential and event.Time should lie in the range [from, to)
	if lastStateID+1 != eventRecord.ID || eventRecord.ChainID != chainID || !eventRecord.Time.Before(to) {
		return &InvalidStateReceivedError{number, lastStateID, &to, eventRecord}
	}

	return nil
}

func (c *Bor) SetHeimdallClient(h IHeimdallClient) {
	c.HeimdallClient = h
}

func (c *Bor) GetCurrentValidators(blockNumber uint64, signer libcommon.Address, getSpanForBlock func(blockNum uint64) (*span.HeimdallSpan, error)) ([]*valset.Validator, error) {
	return c.spanner.GetCurrentValidators(blockNumber, signer, getSpanForBlock)
}

//
// Private methods
//

func (c *Bor) getNextHeimdallSpanForTest(
	newSpanID uint64,
	state *state.IntraBlockState,
	header *types.Header,
	chain statefull.ChainContext,
	syscall consensus.SystemCall,
) (*span.HeimdallSpan, error) {
	headerNumber := header.Number.Uint64()

	spanBor, err := c.spanner.GetCurrentSpan(syscall)
	if err != nil {
		return nil, err
	}

	// Retrieve the snapshot needed to verify this header and cache it
	snap, err := c.snapshot(chain.Chain, headerNumber-1, header.ParentHash, nil)
	if err != nil {
		return nil, err
	}

	// new span
	spanBor.ID = newSpanID
	if spanBor.EndBlock == 0 {
		spanBor.StartBlock = 256
	} else {
		spanBor.StartBlock = spanBor.EndBlock + 1
	}

	spanBor.EndBlock = spanBor.StartBlock + (100 * c.config.CalculateSprint(headerNumber)) - 1

	selectedProducers := make([]valset.Validator, len(snap.ValidatorSet.Validators))
	for i, v := range snap.ValidatorSet.Validators {
		selectedProducers[i] = *v
	}

	heimdallSpan := &span.HeimdallSpan{
		Span:              *spanBor,
		ValidatorSet:      *snap.ValidatorSet,
		SelectedProducers: selectedProducers,
		ChainID:           c.chainConfig.ChainID.String(),
	}

	return heimdallSpan, nil
}

func validatorContains(a []*valset.Validator, x *valset.Validator) (*valset.Validator, bool) {
	for _, n := range a {
		if bytes.Equal(n.Address.Bytes(), x.Address.Bytes()) {
			return n, true
		}
	}

	return nil, false
}

func getUpdatedValidatorSet(oldValidatorSet *valset.ValidatorSet, newVals []*valset.Validator, logger log.Logger) *valset.ValidatorSet {
	v := oldValidatorSet
	oldVals := v.Validators

	changes := make([]*valset.Validator, 0, len(oldVals))

	for _, ov := range oldVals {
		if f, ok := validatorContains(newVals, ov); ok {
			ov.VotingPower = f.VotingPower
		} else {
			ov.VotingPower = 0
		}

		changes = append(changes, ov)
	}

	for _, nv := range newVals {
		if _, ok := validatorContains(changes, nv); !ok {
			changes = append(changes, nv)
		}
	}

	if err := v.UpdateWithChangeSet(changes); err != nil {
		logger.Error("Error while updating change set", "error", err)
	}

	return v
}

func isSprintStart(number, sprint uint64) bool {
	return number%sprint == 0
}
