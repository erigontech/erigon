package parlia

import (
	"bytes"
	"encoding/hex"
	json2 "encoding/json"
	"errors"
	"fmt"
	lru "github.com/hashicorp/golang-lru"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/forkid"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/crypto/sha3"
	"io"
	"math/big"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	inMemorySnapshots  = 128  // Number of recent snapshots to keep in memory
	inMemorySignatures = 4096 // Number of recent block signatures to keep in memory

	checkpointInterval = 1024        // Number of blocks after which to save the snapshot to the database
	defaultEpochLength = uint64(100) // Default number of blocks of checkpoint to update validatorSet from contract

	extraVanity      = 32 // Fixed number of extra-data prefix bytes reserved for signer vanity
	extraSeal        = 65 // Fixed number of extra-data suffix bytes reserved for signer seal
	nextForkHashSize = 4  // Fixed number of extra-data suffix bytes reserved for nextForkHash.

	validatorBytesLength = common.AddressLength
	wiggleTime           = uint64(1) // second, Random delay (per signer) to allow concurrent signers
	initialBackOffTime   = uint64(1) // second
	processBackOffTime   = uint64(1) // second

	systemRewardPercent = 4 // it means 1/2^4 = 1/16 percentage of gas fee incoming will be distributed to system
)

var (
	uncleHash  = types.CalcUncleHash(nil) // Always Keccak256(RLP([])) as uncles are meaningless outside of PoW.
	diffInTurn = big.NewInt(2)            // Block difficulty for in-turn signatures
	diffNoTurn = big.NewInt(1)            // Block difficulty for out-of-turn signatures
	// 100 native token
	maxSystemBalance = new(uint256.Int).Mul(uint256.NewInt(100), uint256.NewInt(params.Ether))

	systemContracts = map[common.Address]bool{
		common.HexToAddress(systemcontracts.ValidatorContract):          true,
		common.HexToAddress(systemcontracts.SlashContract):              true,
		common.HexToAddress(systemcontracts.SystemRewardContract):       true,
		common.HexToAddress(systemcontracts.LightClientContract):        true,
		common.HexToAddress(systemcontracts.RelayerHubContract):         true,
		common.HexToAddress(systemcontracts.GovHubContract):             true,
		common.HexToAddress(systemcontracts.TokenHubContract):           true,
		common.HexToAddress(systemcontracts.RelayerIncentivizeContract): true,
		common.HexToAddress(systemcontracts.CrossChainContract):         true,
	}
)

// Various error messages to mark blocks invalid. These should be private to
// prevent engine specific errors from being referenced in the remainder of the
// codebase, inherently breaking if the engine is swapped out. Please put common
// error types into the consensus package.
var (
	// errUnknownBlock is returned when the list of validators is requested for a block
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

	// errInvalidSpanValidators is returned if a block contains an
	// invalid list of validators (i.e. non divisible by 20 bytes).
	errInvalidSpanValidators = errors.New("invalid validator list on sprint end block")

	// errInvalidMixDigest is returned if a block's mix digest is non-zero.
	errInvalidMixDigest = errors.New("non-zero mix digest")

	// errInvalidUncleHash is returned if a block contains an non-empty uncle list.
	errInvalidUncleHash = errors.New("non empty uncle hash")

	// errMismatchingEpochValidators is returned if a sprint block contains a
	// list of validators different than the one the local node calculated.
	errMismatchingEpochValidators = errors.New("mismatching validator list on epoch block")

	// errInvalidDifficulty is returned if the difficulty of a block is missing.
	errInvalidDifficulty = errors.New("invalid difficulty")

	// errWrongDifficulty is returned if the difficulty of a block doesn't match the
	// turn of the signer.
	errWrongDifficulty = errors.New("wrong difficulty")

	// errOutOfRangeChain is returned if an authorization list is attempted to
	// be modified via out-of-range or non-contiguous headers.
	errOutOfRangeChain = errors.New("out of range or non-contiguous chain")

	// errBlockHashInconsistent is returned if an authorization list is attempted to
	// insert an inconsistent block.
	errBlockHashInconsistent = errors.New("the block hash is inconsistent")

	// errUnauthorizedValidator is returned if a header is signed by a non-authorized entity.
	errUnauthorizedValidator = errors.New("unauthorized validator")

	// errCoinBaseMisMatch is returned if a header's coinbase do not match with signature
	errCoinBaseMisMatch = errors.New("coinbase do not match with signature")

	// errRecentlySigned is returned if a header is signed by an authorized entity
	// that already signed a header recently, thus is temporarily not allowed to.
	errRecentlySigned = errors.New("recently signed")

	errNotSupported = errors.New("validator mode is not supported")
)

// SignerFn is a signer callback function to request a header to be signed by a
// backing account.
type SignerFn func(accounts.Account, string, []byte) ([]byte, error)
type SignerTxFn func(accounts.Account, *types.Transaction, *big.Int) (*types.Transaction, error)

// ecrecover extracts the Ethereum account address from a signed header.
func ecrecover(header *types.Header, sigCache *lru.ARCCache, chainId *big.Int) (common.Address, error) {
	// If the signature's already cached, return that
	hash := header.Hash()
	if address, known := sigCache.Get(hash); known {
		return address.(common.Address), nil
	}
	// Retrieve the signature from the header extra-data
	if len(header.Extra) < extraSeal {
		return common.Address{}, errMissingSignature
	}
	signature := header.Extra[len(header.Extra)-extraSeal:]

	// Recover the public key and the Ethereum address
	pubkey, err := crypto.Ecrecover(SealHash(header, chainId).Bytes(), signature)
	if err != nil {
		return common.Address{}, err
	}
	var signer common.Address
	copy(signer[:], crypto.Keccak256(pubkey[1:])[12:])

	sigCache.Add(hash, signer)
	return signer, nil
}

// SealHash returns the hash of a block prior to it being sealed.
func SealHash(header *types.Header, chainId *big.Int) (hash common.Hash) {
	hasher := sha3.NewLegacyKeccak256()
	encodeSigHeader(hasher, header, chainId)
	hasher.Sum(hash[:0])
	return hash
}

func encodeSigHeader(w io.Writer, header *types.Header, chainId *big.Int) {
	err := rlp.Encode(w, []interface{}{
		chainId,
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
		header.Extra[:len(header.Extra)-65], // this will panic if extra is too short, should check before calling encodeSigHeader
		header.MixDigest,
		header.Nonce,
	})
	if err != nil {
		panic("can't encode: " + err.Error())
	}
}

// ParliaRLP returns the rlp bytes which needs to be signed for the parlia
// sealing. The RLP to sign consists of the entire header apart from the 65 byte signature
// contained at the end of the extra data.
//
// Note, the method requires the extra data to be at least 65 bytes, otherwise it
// panics. This is done to avoid accidentally using both forms (signature present
// or not), which could be abused to produce different hashes for the same header.
func ParliaRLP(header *types.Header, chainId *big.Int) []byte {
	b := new(bytes.Buffer)
	encodeSigHeader(b, header, chainId)
	return b.Bytes()
}

type Parlia struct {
	chainConfig *params.ChainConfig  // Chain config
	config      *params.ParliaConfig // Consensus engine configuration parameters for parlia consensus
	genesisHash common.Hash
	db          kv.RwDB // Database to store and retrieve snapshot checkpoints

	recentSnaps *lru.ARCCache // Snapshots for recent block to speed up
	signatures  *lru.ARCCache // Signatures of recent blocks to speed up mining

	signer *types.Signer

	val      common.Address // Ethereum address of the signing key
	signFn   SignerFn       // Signer function to authorize hashes with
	signTxFn SignerTxFn

	lock sync.RWMutex // Protects the signer fields

	validatorSetABI abi.ABI
	slashABI        abi.ABI

	// The fields below are for testing only
	fakeDiff bool // Skip difficulty verifications
}

// New creates a Parlia consensus engine.
func New(
	chainConfig *params.ChainConfig,
	db kv.RwDB,
) *Parlia {
	// get parlia config
	parliaConfig := chainConfig.Parlia

	// Set any missing consensus parameters to their defaults
	if parliaConfig != nil && parliaConfig.Epoch == 0 {
		parliaConfig.Epoch = defaultEpochLength
	}

	// Allocate the snapshot caches and create the engine
	recentSnaps, err := lru.NewARC(inMemorySnapshots)
	if err != nil {
		panic(err)
	}
	signatures, err := lru.NewARC(inMemorySignatures)
	if err != nil {
		panic(err)
	}
	vABI, err := abi.JSON(strings.NewReader(validatorSetABI))
	if err != nil {
		panic(err)
	}
	sABI, err := abi.JSON(strings.NewReader(slashABI))
	if err != nil {
		panic(err)
	}
	c := &Parlia{
		chainConfig:     chainConfig,
		config:          parliaConfig,
		db:              db,
		recentSnaps:     recentSnaps,
		signatures:      signatures,
		validatorSetABI: vABI,
		slashABI:        sABI,
		signer:          types.NewEIP155Signer(chainConfig.ChainID),
	}

	return c
}

// Author retrieves the Ethereum address of the account that minted the given
// block, which may be different from the header's coinbase if a consensus
// engine is based on signatures.
func (p *Parlia) Author(header *types.Header) (common.Address, error) {
	return header.Coinbase, nil
}

// VerifyHeader checks whether a header conforms to the consensus rules of a
// given engine. Verifying the seal may be done optionally here, or explicitly
// via the VerifySeal method.
func (p *Parlia) VerifyHeader(chain consensus.ChainHeaderReader, header *types.Header, seal bool) error {
	return p.verifyHeader(chain, header, nil)
}

// VerifyHeaders is similar to VerifyHeader, but verifies a batch of headers
// concurrently. The method returns a quit channel to abort the operations and
// a results channel to retrieve the async verifications (the order is that of
// the input slice).
func (p *Parlia) VerifyHeaders(chain consensus.ChainHeaderReader, headers []*types.Header, seals []bool) error {
	for i, header := range headers {
		err := p.verifyHeader(chain, header, headers[:i])
		if err != nil {
			return err
		}
	}
	return nil
}

// verifyHeader checks whether a header conforms to the consensus rules.The
// caller may optionally pass in a batch of parents (ascending order) to avoid
// looking those up from the database. This is useful for concurrently verifying
// a batch of new headers.
func (p *Parlia) verifyHeader(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header) error {
	if header.Number == nil {
		return errUnknownBlock
	}
	number := header.Number.Uint64()

	// Don't waste time checking blocks from the future
	if header.Time > uint64(time.Now().Unix()) {
		return consensus.ErrFutureBlock
	}
	// Check that the extra-data contains the vanity, validators and signature.
	if len(header.Extra) < extraVanity {
		return errMissingVanity
	}
	if len(header.Extra) < extraVanity+extraSeal {
		return errMissingSignature
	}
	// check extra data
	isEpoch := number%p.config.Epoch == 0

	// Ensure that the extra-data contains a signer list on checkpoint, but none otherwise
	signersBytes := len(header.Extra) - extraVanity - extraSeal
	if !isEpoch && signersBytes != 0 {
		return errExtraValidators
	}

	if isEpoch && signersBytes%validatorBytesLength != 0 {
		return errInvalidSpanValidators
	}

	// Ensure that the mix digest is zero as we don't have fork protection currently
	if header.MixDigest != (common.Hash{}) {
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
	// If all checks passed, validate any special fields for hard forks
	if err := misc.VerifyForkHashes(chain.Config(), header, false); err != nil {
		return err
	}
	// All basic checks passed, verify cascading fields
	return p.verifyCascadingFields(chain, header, parents)
}

// verifyCascadingFields verifies all the header fields that are not standalone,
// rather depend on a batch of previous headers. The caller may optionally pass
// in a batch of parents (ascending order) to avoid looking those up from the
// database. This is useful for concurrently verifying a batch of new headers.
func (p *Parlia) verifyCascadingFields(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header) error {
	// The genesis block is the always valid dead-end
	number := header.Number.Uint64()
	if number == 0 {
		return nil
	}

	var parent *types.Header
	if len(parents) > 0 {
		parent = parents[len(parents)-1]
	} else {
		parent = chain.GetHeader(header.ParentHash, number-1)
	}

	if parent == nil || parent.Number.Uint64() != number-1 || parent.Hash() != header.ParentHash {
		return consensus.ErrUnknownAncestor
	}

	snap, err := p.snapshot(chain, number-1, header.ParentHash, parents)
	if err != nil {
		return err
	}

	err = p.blockTimeVerifyForRamanujanFork(snap, header, parent)
	if err != nil {
		return err
	}

	// Verify that the gas limit is <= 2^63-1
	capacity := uint64(0x7fffffffffffffff)
	if header.GasLimit > capacity {
		return fmt.Errorf("invalid gasLimit: have %v, max %v", header.GasLimit, capacity)
	}
	// Verify that the gasUsed is <= gasLimit
	if header.GasUsed > header.GasLimit {
		return fmt.Errorf("invalid gasUsed: have %d, gasLimit %d", header.GasUsed, header.GasLimit)
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

	// All basic checks passed, verify the seal and return
	return p.verifySeal(chain, header, parents)
}

// verifySeal checks whether the signature contained in the header satisfies the
// consensus protocol requirements. The method accepts an optional list of parent
// headers that aren't yet part of the local blockchain to generate the snapshots
// from.
func (p *Parlia) verifySeal(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header) error {
	// Verifying the genesis block is not supported
	number := header.Number.Uint64()
	if number == 0 {
		return errUnknownBlock
	}
	// Retrieve the snapshot needed to verify this header and cache it
	snap, err := p.snapshot(chain, number-1, header.ParentHash, parents)
	if err != nil {
		return err
	}

	// Resolve the authorization key and check against validators
	signer, err := ecrecover(header, p.signatures, p.chainConfig.ChainID)
	if err != nil {
		return err
	}

	if signer != header.Coinbase {
		return errCoinBaseMisMatch
	}

	if _, ok := snap.Validators[signer]; !ok {
		return errUnauthorizedValidator
	}

	for seen, recent := range snap.Recents {
		if recent == signer {
			// Signer is among recents, only fail if the current block doesn't shift it out
			if limit := uint64(len(snap.Validators)/2 + 1); seen > number-limit {
				return errRecentlySigned
			}
		}
	}

	// Ensure that the difficulty corresponds to the turn-ness of the signer
	if !p.fakeDiff {
		inturn := snap.inturn(signer)
		if inturn && header.Difficulty.Cmp(diffInTurn) != 0 {
			return errWrongDifficulty
		}
		if !inturn && header.Difficulty.Cmp(diffNoTurn) != 0 {
			return errWrongDifficulty
		}
	}

	return nil
}

// snapshot retrieves the authorization snapshot at a given point in time.
func (p *Parlia) snapshot(chain consensus.ChainHeaderReader, number uint64, hash common.Hash, parents []*types.Header) (*Snapshot, error) {
	// Search for a snapshot in memory or on disk for checkpoints
	var (
		headers []*types.Header
		snap    *Snapshot
	)

	for snap == nil {
		// If an in-memory snapshot was found, use that
		if s, ok := p.recentSnaps.Get(hash); ok {
			snap = s.(*Snapshot)
			break
		}

		// If an on-disk checkpoint snapshot can be found, use that
		if number%checkpointInterval == 0 {
			if s, err := loadSnapshot(p.config, p.signatures, p.db, number, hash); err == nil {
				log.Trace("Loaded snapshot from disk", "number", number, "hash", hash)
				snap = s
				break
			}
		}

		// If we're at the genesis, snapshot the initial state.
		if number == 0 {
			checkpoint := chain.GetHeaderByNumber(number)
			if checkpoint != nil {
				// get checkpoint data
				hash := checkpoint.Hash()

				validatorBytes := checkpoint.Extra[extraVanity : len(checkpoint.Extra)-extraSeal]
				// get validators from headers
				validators, err := ParseValidators(validatorBytes)
				if err != nil {
					return nil, err
				}

				// new snapshot
				snap = newSnapshot(p.config, p.signatures, number, hash, validators)
				if err := snap.store(p.db); err != nil {
					return nil, err
				}
				log.Info("Stored checkpoint snapshot to disk", "number", number, "hash", hash)
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

	snap, err := snap.apply(headers, chain, parents, p.chainConfig.ChainID)
	if err != nil {
		return nil, err
	}
	p.recentSnaps.Add(snap.Hash, snap)

	// If we've generated a new checkpoint snapshot, save to disk
	if snap.Number%checkpointInterval == 0 && len(headers) > 0 {
		if err = snap.store(p.db); err != nil {
			return nil, err
		}
		log.Trace("Stored snapshot to disk", "number", snap.Number, "hash", snap.Hash)
	}
	return snap, err
}

// VerifyUncles verifies that the given block's uncles conform to the consensus
// rules of a given engine.
func (p *Parlia) VerifyUncles(chain consensus.ChainReader, header *types.Header, uncles []*types.Header) error {
	if len(uncles) > 0 {
		return errors.New("uncles not allowed")
	}
	return nil
}

// Prepare initializes the consensus fields of a block header according to the
// rules of a particular engine. The changes are executed inline.
func (p *Parlia) Prepare(chain consensus.ChainHeaderReader, header *types.Header) error {
	return errNotSupported
}

// Initialize runs any pre-transaction state modifications (e.g. epoch start)
func (p *Parlia) Initialize(config *params.ChainConfig, chain consensus.ChainHeaderReader, e consensus.EpochReader, header *types.Header, txs []types.Transaction, uncles []*types.Header, syscall consensus.SystemCall) {
}

func (p *Parlia) splitTxs(txs []types.Transaction, header *types.Header) (userTxs []types.Transaction, systemTxs []types.Transaction, err error) {
	for _, tx := range txs {
		isSystemTx, err2 := p.IsSystemTransaction(tx, header)
		if err2 != nil {
			err = err2
			return
		}
		if isSystemTx {
			systemTxs = append(systemTxs, tx)
		} else {
			userTxs = append(userTxs, tx)
		}
	}
	return
}

// Finalize runs any post-transaction state modifications (e.g. block rewards)
// but does not assemble the block.
//
// Note: The block header and state database might be updated to reflect any
// consensus rules that happen at finalization (e.g. block rewards).
func (p *Parlia) Finalize(_ *params.ChainConfig, header *types.Header, state *state.IntraBlockState, txs []types.Transaction, _ []*types.Header, receipts types.Receipts, e consensus.EpochReader, chain consensus.ChainHeaderReader, syscall consensus.SystemCall) error {
	return p.finalize(header, state, txs, &receipts, chain)
}

func (p *Parlia) finalize(header *types.Header, state *state.IntraBlockState, txs []types.Transaction, receipts *types.Receipts, chain consensus.ChainHeaderReader) error {
	txs, systemTxs, err := p.splitTxs(txs, header)
	if err != nil {
		return err
	}
	// warn if not in majority fork
	number := header.Number.Uint64()
	snap, err := p.snapshot(chain, number-1, header.ParentHash, nil)
	if err != nil {
		return err
	}
	nextForkHash := forkid.NextForkHash(p.chainConfig, p.genesisHash, number)
	if !snap.isMajorityFork(hex.EncodeToString(nextForkHash[:])) {
		log.Debug("there is a possible fork, and your client is not the majority. Please check...", "nextForkHash", hex.EncodeToString(nextForkHash[:]))
	}
	// If the block is an epoch end block, verify the validator list
	// The verification can only be done when the state is ready, it can't be done in VerifyHeader.
	if header.Number.Uint64()%p.config.Epoch == 0 {
		parentHeader := chain.GetHeader(header.ParentHash, header.Number.Uint64()-1)
		newValidators, err := p.getCurrentValidators(parentHeader, state)
		if err != nil {
			return err
		}
		// sort validator by address
		sort.Sort(validatorsAscending(newValidators))
		validatorsBytes := make([]byte, len(newValidators)*validatorBytesLength)
		for i, validator := range newValidators {
			copy(validatorsBytes[i*validatorBytesLength:], validator.Bytes())
		}

		extraSuffix := len(header.Extra) - extraSeal
		if !bytes.Equal(header.Extra[extraVanity:extraSuffix], validatorsBytes) {
			return errMismatchingEpochValidators
		}
	}
	// No block rewards in PoA, so the state remains as is and uncles are dropped
	if header.Number.Cmp(common.Big1) == 0 {
		err := p.initContract(state, header, &txs, receipts, &systemTxs, &header.GasUsed, false)
		if err != nil {
			log.Error("init contract failed")
		}
	}
	if header.Difficulty.Cmp(diffInTurn) != 0 {
		spoiledVal := snap.supposeValidator()
		signedRecently := false
		for _, recent := range snap.Recents {
			if recent == spoiledVal {
				signedRecently = true
				break
			}
		}
		if !signedRecently {
			log.Trace("slash validator", "block hash", header.Hash(), "address", spoiledVal)
			err = p.slash(spoiledVal, state, header, &txs, receipts, &systemTxs, &header.GasUsed, false)
			if err != nil {
				// it is possible that slash validator failed because of the slash channel is disabled.
				log.Error("slash validator failed", "block hash", header.Hash(), "address", spoiledVal)
			}
		}
	}
	val := header.Coinbase
	err = p.distributeIncoming(val, state, header, &txs, receipts, &systemTxs, &header.GasUsed, false)
	if err != nil {
		return err
	}
	if len(systemTxs) > 0 {
		return errors.New("the length of systemTxs do not match")
	}
	return nil
}

// FinalizeAndAssemble runs any post-transaction state modifications (e.g. block
// rewards) and assembles the final block.
//
// Note: The block header and state database might be updated to reflect any
// consensus rules that happen at finalization (e.g. block rewards).
func (p *Parlia) FinalizeAndAssemble(_ *params.ChainConfig, header *types.Header, state *state.IntraBlockState, txs []types.Transaction, _ []*types.Header, receipts types.Receipts, e consensus.EpochReader, chain consensus.ChainHeaderReader, syscall consensus.SystemCall, call consensus.Call) (*types.Block, error) {
	err := p.finalize(header, state, txs, &receipts, chain)
	if err != nil {
		return nil, err
	}
	for i, r := range receipts {
		var tx types.Transaction
		for _, tx2 := range txs {
			if r.TxHash == tx2.Hash() {
				tx = tx2
				break
			}
		}
		if tx == nil {
			panic("not possible")
		}
		txJson, _ := json2.Marshal(txs[i])
		json, _ := r.MarshalJSON()
		sender, _ := txs[i].GetSender()
		log.Info("receipt", "sender", sender.Hex(), "tx", string(txJson), "receipt", string(json))
	}
	// TODO: "calc block root"
	return types.NewBlock(header, txs, nil, receipts), nil

	//// No block rewards in PoA, so the state remains as is and uncles are dropped
	//if txs == nil {
	//	txs = make([]types.Transaction, 0)
	//}
	//if receipts == nil {
	//	receipts = make([]*types.Receipt, 0)
	//}
	//if header.Number.Cmp(common.Big1) == 0 {
	//	err := p.initContract(state, header, &txs, &receipts, nil, &header.GasUsed, true)
	//	if err != nil {
	//		log.Error("init contract failed")
	//	}
	//}
	//if header.Difficulty.Cmp(diffInTurn) != 0 {
	//	number := header.Number.Uint64()
	//	snap, err := p.snapshot(chain, number-1, header.ParentHash, nil)
	//	if err != nil {
	//		return nil, err
	//	}
	//	spoiledVal := snap.supposeValidator()
	//	signedRecently := false
	//	for _, recent := range snap.Recents {
	//		if recent == spoiledVal {
	//			signedRecently = true
	//			break
	//		}
	//	}
	//	if !signedRecently {
	//		err = p.slash(spoiledVal, state, header, &txs, &receipts, nil, &header.GasUsed, true)
	//		if err != nil {
	//			// it is possible that slash validator failed because of the slash channel is disabled.
	//			log.Error("slash validator failed", "block hash", header.Hash(), "address", spoiledVal)
	//		}
	//	}
	//}
	//err := p.distributeIncoming(p.val, state, header, &txs, &receipts, nil, &header.GasUsed, true)
	//if err != nil {
	//	return nil, err
	//}
	//// should not happen. Once happen, stop the node is better than broadcast the block
	//if header.GasLimit < header.GasUsed {
	//	return nil, errors.New("gas consumption of system txs exceed the gas limit")
	//}
	//header.UncleHash = types.CalcUncleHash(nil)
	//blk := types.NewBlock(header, txs, nil, receipts)
	//rootHash, err := trie.CalcRoot("GenerateChain", tx)
	//
	//state.CommitBlock()
	//
	//wg := sync.WaitGroup{}
	//wg.Add(2)
	//go func() {
	//	rootHash = state.IntermediateRoot(chain.Config().IsEIP158(header.Number))
	//	wg.Done()
	//}()
	//go func() {
	//	blk = types.NewBlock(header, txs, nil, receipts)
	//	wg.Done()
	//}()
	//wg.Wait()
	//blk.SetRoot(rootHash)
	//// Assemble and return the final block for sealing
	//return blk, receipts, nil
}

// Authorize injects a private key into the consensus engine to mint new blocks
// with.
func (p *Parlia) Authorize(val common.Address, signFn SignerFn, signTxFn SignerTxFn) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.val = val
	p.signFn = signFn
	p.signTxFn = signTxFn
}

// Seal generates a new sealing request for the given input block and pushes
// the result into the given channel.
//
// Note, the method returns immediately and will send the result async. More
// than one result may also be returned depending on the consensus algorithm.
func (p *Parlia) Seal(chain consensus.ChainHeaderReader, block *types.Block, results chan<- *types.Block, stop <-chan struct{}) error {
	return errNotSupported
}

// SealHash returns the hash of a block prior to it being sealed.
func (p *Parlia) SealHash(header *types.Header) common.Hash {
	return SealHash(header, p.chainConfig.ChainID)
}

// CalcDifficulty is the difficulty adjustment algorithm. It returns the difficulty
// that a new block should have.
func (p *Parlia) CalcDifficulty(chain consensus.ChainHeaderReader, time, parentTime uint64, parentDifficulty *big.Int, parentNumber uint64, parentHash, parentUncleHash common.Hash, parentSeal []rlp.RawValue) *big.Int {
	snap, err := p.snapshot(chain, parentNumber, parentHash, nil)
	if err != nil {
		return nil
	}
	return CalcDifficulty(snap, p.val)
}

// CalcDifficulty is the difficulty adjustment algorithm. It returns the difficulty
// that a new block should have based on the previous blocks in the chain and the
// current signer.
func CalcDifficulty(snap *Snapshot, signer common.Address) *big.Int {
	if snap.inturn(signer) {
		return new(big.Int).Set(diffInTurn)
	}
	return new(big.Int).Set(diffNoTurn)
}

func (p *Parlia) GenerateSeal(chain consensus.ChainHeaderReader, current, parent *types.Header, call consensus.Call) []rlp.RawValue {
	return nil
}

// APIs returns the RPC APIs this consensus engine provides.
func (p *Parlia) APIs(chain consensus.ChainHeaderReader) []rpc.API {
	return []rpc.API{{
		Namespace: "parlia",
		Version:   "1.0",
		Service:   &API{chain: chain, parlia: p},
		Public:    false,
	}}
}

func (p *Parlia) IsSystemTransaction(tx types.Transaction, header *types.Header) (bool, error) {
	// deploy a contract
	if tx.GetTo() == nil {
		return false, nil
	}
	sender, err := tx.Sender(*p.signer)
	if err != nil {
		return false, errors.New("UnAuthorized transaction")
	}
	if sender == header.Coinbase && isToSystemContract(*tx.GetTo()) && tx.GetPrice().IsZero() {
		return true, nil
	}
	return false, nil
}

func isToSystemContract(to common.Address) bool {
	return systemContracts[to]
}

func (p *Parlia) IsSystemContract(to *common.Address) bool {
	if to == nil {
		return false
	}
	return isToSystemContract(*to)
}

func (p *Parlia) EnoughDistance(chain consensus.ChainReader, header *types.Header) bool {
	snap, err := p.snapshot(chain, header.Number.Uint64()-1, header.ParentHash, nil)
	if err != nil {
		return true
	}
	return snap.enoughDistance(p.val, header)
}

func (p *Parlia) IsLocalBlock(header *types.Header) bool {
	return p.val == header.Coinbase
}

func (p *Parlia) AllowLightProcess(chain consensus.ChainReader, currentHeader *types.Header) bool {
	snap, err := p.snapshot(chain, currentHeader.Number.Uint64()-1, currentHeader.ParentHash, nil)
	if err != nil {
		return true
	}

	idx := snap.indexOfVal(p.val)
	// validator is not allowed to diff sync
	return idx < 0
}

// Close terminates any background threads maintained by the consensus engine.
func (p *Parlia) Close() error {
	return nil
}

// ==========================  interaction with contract/account =========

// getCurrentValidators get current validators
func (p *Parlia) getCurrentValidators(header *types.Header, state *state.IntraBlockState) ([]common.Address, error) {
	// method
	method := "getValidators"
	data, err := p.validatorSetABI.Pack(method)
	if err != nil {
		log.Error("Unable to pack tx for getValidators", "error", err)
		return nil, err
	}
	// call
	msgData := (hexutil.Bytes)(data)
	toAddress := common.HexToAddress(systemcontracts.ValidatorContract)
	result, err := core.SysCallContractReadonly(toAddress, msgData[:], *p.chainConfig, state, header, p)
	if err != nil {
		return nil, err
	}
	var ret0 = new([]common.Address)
	out := ret0
	if err := p.validatorSetABI.UnpackIntoInterface(out, method, result.ReturnData); err != nil {
		return nil, err
	}
	valz := make([]common.Address, len(*ret0))
	for i, a := range *ret0 {
		valz[i] = a
	}
	return valz, nil
}

// slash spoiled validators
func (p *Parlia) distributeIncoming(val common.Address, state *state.IntraBlockState, header *types.Header, txs *[]types.Transaction, receipts *types.Receipts, receivedTxs *[]types.Transaction, usedGas *uint64, mining bool) error {
	coinbase := header.Coinbase
	balance := state.GetBalance(consensus.SystemAddress)
	if balance.Cmp(u256.Num0) <= 0 {
		return nil
	}
	state.SetBalance(consensus.SystemAddress, u256.Num0)
	state.AddBalance(coinbase, balance)

	doDistributeSysReward := state.GetBalance(common.HexToAddress(systemcontracts.SystemRewardContract)).Cmp(maxSystemBalance) < 0
	if doDistributeSysReward {
		var rewards = new(uint256.Int)
		rewards = rewards.Rsh(balance, systemRewardPercent)
		if rewards.Cmp(u256.Num0) > 0 {
			err := p.distributeToSystem(rewards, state, header, txs, receipts, receivedTxs, usedGas, mining)
			if err != nil {
				return err
			}
			log.Trace("distribute to system reward pool", "block hash", header.Hash(), "amount", rewards)
			balance = balance.Sub(balance, rewards)
		}
	}
	log.Trace("distribute to validator contract", "block hash", header.Hash(), "amount", balance)
	return p.distributeToValidator(balance, val, state, header, txs, receipts, receivedTxs, usedGas, mining)
}

// slash spoiled validators
func (p *Parlia) slash(spoiledVal common.Address, state *state.IntraBlockState, header *types.Header, txs *[]types.Transaction, receipts *types.Receipts, receivedTxs *[]types.Transaction, usedGas *uint64, mining bool) error {
	// method
	method := "slash"

	// get packed data
	data, err := p.slashABI.Pack(method,
		spoiledVal,
	)
	if err != nil {
		log.Error("Unable to pack tx for slash", "error", err)
		return err
	}
	// apply message
	return p.applyTransaction(header.Coinbase, common.HexToAddress(systemcontracts.SlashContract), u256.Num0, data, state, header, txs, receipts, receivedTxs, usedGas, mining)
}

// init contract
func (p *Parlia) initContract(state *state.IntraBlockState, header *types.Header, txs *[]types.Transaction, receipts *types.Receipts, receivedTxs *[]types.Transaction, usedGas *uint64, mining bool) error {
	// method
	method := "init"
	// contracts
	contracts := []string{
		systemcontracts.ValidatorContract,
		systemcontracts.SlashContract,
		systemcontracts.LightClientContract,
		systemcontracts.RelayerHubContract,
		systemcontracts.TokenHubContract,
		systemcontracts.RelayerIncentivizeContract,
		systemcontracts.CrossChainContract,
	}
	// get packed data
	data, err := p.validatorSetABI.Pack(method)
	if err != nil {
		log.Error("Unable to pack tx for init validator set", "error", err)
		return err
	}
	for _, c := range contracts {
		log.Trace("init contract", "block hash", header.Hash(), "contract", c)
		err = p.applyTransaction(header.Coinbase, common.HexToAddress(c), u256.Num0, data,
			state, header, txs, receipts, receivedTxs, usedGas, mining)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Parlia) distributeToSystem(amount *uint256.Int, state *state.IntraBlockState, header *types.Header, txs *[]types.Transaction, receipts *types.Receipts, receivedTxs *[]types.Transaction, usedGas *uint64, mining bool) error {
	// apply message
	return p.applyTransaction(
		header.Coinbase, common.HexToAddress(systemcontracts.SystemRewardContract), amount, nil,
		state, header, txs, receipts, receivedTxs, usedGas, mining)
}

// slash spoiled validators
func (p *Parlia) distributeToValidator(amount *uint256.Int, validator common.Address, state *state.IntraBlockState, header *types.Header, txs *[]types.Transaction, receipts *types.Receipts, receivedTxs *[]types.Transaction, usedGas *uint64, mining bool) error {
	// method
	method := "deposit"

	// get packed data
	data, err := p.validatorSetABI.Pack(method,
		validator,
	)
	if err != nil {
		log.Error("Unable to pack tx for deposit", "error", err)
		return err
	}
	// apply message
	return p.applyTransaction(
		header.Coinbase,
		common.HexToAddress(systemcontracts.ValidatorContract),
		amount,
		data,
		state,
		header,
		txs,
		receipts,
		receivedTxs,
		usedGas,
		mining)
}

func (p *Parlia) applyTransaction(
	from common.Address,
	to common.Address,
	value *uint256.Int,
	data []byte,
	state *state.IntraBlockState,
	header *types.Header,
	txs *[]types.Transaction,
	receipts *types.Receipts,
	receivedTxs *[]types.Transaction,
	usedGas *uint64,
	mining bool,
) (err error) {
	nonce := state.GetNonce(from)
	expectedTx := types.Transaction(types.NewTransaction(nonce, to, value, math.MaxUint64/2, u256.Num0, data))
	expectedHash := expectedTx.SigningHash(p.chainConfig.ChainID)
	if from == p.val && mining {
		return errNotSupported
		//expectedTx, err = p.signTxFn(accounts.Account{Address: msg.From()}, expectedTx, p.chainConfig.ChainID)
		//if err != nil {
		//	return err
		//}
	} else {
		if receivedTxs == nil || len(*receivedTxs) == 0 || (*receivedTxs)[0] == nil {
			return errors.New("supposed to get a actual transaction, but get none")
		}
		actualTx := (*receivedTxs)[0]
		if !bytes.Equal(actualTx.SigningHash(p.chainConfig.ChainID).Bytes(), expectedHash.Bytes()) {
			log.Error(fmt.Sprintf("expected tx hash %v, get %v, nonce %d, to %s, value %s, gas %d, gasPrice %s, data %s", expectedHash.String(), actualTx.Hash().String(),
				expectedTx.GetNonce(),
				expectedTx.GetTo().String(),
				expectedTx.GetValue().String(),
				expectedTx.GetGas(),
				expectedTx.GetPrice().String(),
				hex.EncodeToString(expectedTx.GetData()),
			))
			return fmt.Errorf("expected tx hash %v, get %v, nonce %d, to %s, value %s, gas %d, gasPrice %s, data %s", expectedHash.String(), actualTx.Hash().String(),
				expectedTx.GetNonce(),
				expectedTx.GetTo().String(),
				expectedTx.GetValue().String(),
				expectedTx.GetGas(),
				expectedTx.GetPrice().String(),
				hex.EncodeToString(expectedTx.GetData()),
			)
		}
		expectedTx = actualTx
		// move to next
		*receivedTxs = (*receivedTxs)[1:]
	}
	state.Prepare(expectedTx.Hash(), common.Hash{}, len(*txs))
	gasUsed, _, err := core.SysCallContract(from, to, data, *p.chainConfig, state, header, p)
	*txs = append(*txs, expectedTx)
	*usedGas += gasUsed
	receipt := types.NewReceipt(false, *usedGas)

	receipt.TxHash = expectedTx.Hash()
	receipt.GasUsed = gasUsed
	receipt.PostState = []byte{}

	// Set the receipt logs and create a bloom for filtering
	receipt.Logs = state.GetLogs(expectedTx.Hash())
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	receipt.BlockHash = header.Hash()
	receipt.BlockNumber = header.Number
	receipt.TransactionIndex = uint(state.TxIndex())
	*receipts = append(*receipts, receipt)
	state.SetNonce(from, nonce+1)
	return nil
}

// chain context
type chainContext struct {
	Chain  consensus.ChainHeaderReader
	parlia consensus.Engine
}

func (c chainContext) Engine() consensus.Engine {
	return c.parlia
}

func (c chainContext) GetHeader(hash common.Hash, number uint64) *types.Header {
	return c.Chain.GetHeader(hash, number)
}
