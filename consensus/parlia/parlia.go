package parlia

import (
	"bytes"
	"encoding/hex"
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
	gov := common.HexToAddress("0x0000000000000000000000000000000000001000")
	code := state.GetCode(gov)
	realcode := common.FromHex("0x60806040526004361061027d5760003560e01c80639dc092621161014f578063c8509d81116100c1578063eb57e2021161007a578063eb57e20214610940578063eda5868c14610973578063f340fa0114610988578063f9a2bbc7146109ae578063fc3e5908146109c3578063fd6a6879146109d85761027d565b8063c8509d8114610609578063d86222d5146108d7578063daacdb66146108ec578063dc927faf14610901578063e086c7b114610916578063e1c7392a1461092b5761027d565b8063ab51bb9611610113578063ab51bb961461074a578063ac4317511461075f578063ad3c9da61461082a578063b7ab4db51461085d578063bf9f49951461041b578063c81b1662146108c25761027d565b80639dc09262146106cd578063a1a11bf5146106e2578063a5422d5c146106f7578063a78abc161461070c578063aaf5eb68146107355761027d565b80635667515a116101f35780637942fd05116101ac5780637942fd05146105df57806381650b62146105f4578063831d65d114610609578063853230aa1461068e57806386249882146106a357806396713da9146106b85761027d565b80635667515a146105005780635d77156c146105155780636969a25c1461052a5780636e47b482146105a057806370fd5bad146105b557806375d47a0a146105ca5761027d565b80633dffc387116102455780633dffc3871461041b57806343756e5c14610446578063493279b1146104775780634bf6c882146104a357806351e80672146104b8578063565c56b3146104cd5761027d565b80630bee7a67146102825780631182b875146102b05780631ff18069146103aa578063219f22d5146103d157806335409f7f146103e6575b600080fd5b34801561028e57600080fd5b506102976109ed565b6040805163ffffffff9092168252519081900360200190f35b3480156102bc57600080fd5b50610335600480360360408110156102d357600080fd5b60ff8235169190810190604081016020820135600160201b8111156102f757600080fd5b82018360208201111561030957600080fd5b803590602001918460018302840111600160201b8311171561032a57600080fd5b5090925090506109f2565b6040805160208082528351818301528351919283929083019185019080838360005b8381101561036f578181015183820152602001610357565b50505050905090810190601f16801561039c5780820380516001836020036101000a031916815260200191505b509250505060405180910390f35b3480156103b657600080fd5b506103bf610bdf565b60408051918252519081900360200190f35b3480156103dd57600080fd5b50610297610be5565b3480156103f257600080fd5b506104196004803603602081101561040957600080fd5b50356001600160a01b0316610bea565b005b34801561042757600080fd5b50610430610efe565b6040805160ff9092168252519081900360200190f35b34801561045257600080fd5b5061045b610f03565b604080516001600160a01b039092168252519081900360200190f35b34801561048357600080fd5b5061048c610f09565b6040805161ffff9092168252519081900360200190f35b3480156104af57600080fd5b50610430610f0e565b3480156104c457600080fd5b5061045b610f13565b3480156104d957600080fd5b506103bf600480360360208110156104f057600080fd5b50356001600160a01b0316610f19565b34801561050c57600080fd5b50610430610f6b565b34801561052157600080fd5b50610297610f70565b34801561053657600080fd5b506105546004803603602081101561054d57600080fd5b5035610f75565b604080516001600160a01b039788168152958716602087015293909516848401526001600160401b0390911660608401521515608083015260a082019290925290519081900360c00190f35b3480156105ac57600080fd5b5061045b610fd9565b3480156105c157600080fd5b50610430610fdf565b3480156105d657600080fd5b5061045b610fe4565b3480156105eb57600080fd5b50610430610fea565b34801561060057600080fd5b50610297610fef565b34801561061557600080fd5b506104196004803603604081101561062c57600080fd5b60ff8235169190810190604081016020820135600160201b81111561065057600080fd5b82018360208201111561066257600080fd5b803590602001918460018302840111600160201b8311171561068357600080fd5b509092509050610ff4565b34801561069a57600080fd5b506103bf6110a7565b3480156106af57600080fd5b506103bf6110ad565b3480156106c457600080fd5b506104306110b3565b3480156106d957600080fd5b5061045b6110b8565b3480156106ee57600080fd5b5061045b6110be565b34801561070357600080fd5b506103356110c4565b34801561071857600080fd5b506107216110e3565b604080519115158252519081900360200190f35b34801561074157600080fd5b506103bf6110ec565b34801561075657600080fd5b50610297610f6b565b34801561076b57600080fd5b506104196004803603604081101561078257600080fd5b810190602081018135600160201b81111561079c57600080fd5b8201836020820111156107ae57600080fd5b803590602001918460018302840111600160201b831117156107cf57600080fd5b919390929091602081019035600160201b8111156107ec57600080fd5b8201836020820111156107fe57600080fd5b803590602001918460018302840111600160201b8311171561081f57600080fd5b5090925090506110f5565b34801561083657600080fd5b506103bf6004803603602081101561084d57600080fd5b50356001600160a01b031661139c565b34801561086957600080fd5b506108726113ae565b60408051602080825283518183015283519192839290830191858101910280838360005b838110156108ae578181015183820152602001610896565b505050509050019250505060405180910390f35b3480156108ce57600080fd5b5061045b6114d4565b3480156108e357600080fd5b506103bf6114da565b3480156108f857600080fd5b506103bf6114e6565b34801561090d57600080fd5b5061045b6114ec565b34801561092257600080fd5b506103bf6114f2565b34801561093757600080fd5b506104196114f7565b34801561094c57600080fd5b506104196004803603602081101561096357600080fd5b50356001600160a01b03166116fa565b34801561097f57600080fd5b506102976118c9565b6104196004803603602081101561099e57600080fd5b50356001600160a01b03166118ce565b3480156109ba57600080fd5b5061045b611b04565b3480156109cf57600080fd5b50610430611b0a565b3480156109e457600080fd5b5061045b611b0f565b606481565b60005460609060ff16610a48576040805162461bcd60e51b81526020600482015260196024820152781d1a194818dbdb9d1c9858dd081b9bdd081a5b9a5d081e595d603a1b604482015290519081900360640190fd5b3361200014610a885760405162461bcd60e51b815260040180806020018281038252602f815260200180614516602f913960400191505060405180910390fd5b610a90613d69565b6000610ad185858080601f016020809104026020016040519081016040528093929190818152602001838380828437600092019190915250611b1592505050565b9150915080610aed57610ae46064611c6e565b92505050610bd8565b815160009060ff16610b0d57610b068360200151611ccf565b9050610ba4565b825160ff1660011415610ba057826020015151600114610b7a577f70e72399380dcfb0338abc03dc8d47f9f470ada8e769c9a78d644ea97385ecb2604051808060200182810382526025815260200180613e6c6025913960400191505060405180910390a1506067610b9b565b610b068360200151600081518110610b8e57fe5b6020026020010151612ad5565b610ba4565b5060655b63ffffffff8116610bc95750506040805160008152602081019091529150610bd89050565b610bd281611c6e565b93505050505b9392505050565b60035481565b606881565b3361100114610c2a5760405162461bcd60e51b81526004018080602001828103825260298152602001806145726029913960400191505060405180910390fd5b6001600160a01b03811660009081526004602052604090205480610c4e5750610efb565b600181039050600060018281548110610c6357fe5b60009182526020909120600360049092020101546001549091506000190180610cb257600060018481548110610c9557fe5b906000526020600020906004020160030181905550505050610efb565b6040805183815290516001600160a01b038616917f3b6f9ef90462b512a1293ecec018670bf7b7f1876fb727590a8a6d7643130a70919081900360200190a26001600160a01b038416600090815260046020526040812055600154600019018314610e3457600180546000198101908110610d2957fe5b906000526020600020906004020160018481548110610d4457fe5b6000918252602082208354600492830290910180546001600160a01b03199081166001600160a01b0393841617825560018087015481840180548416918616919091179055600280880180549185018054909416919095161780835584546001600160401b03600160a01b91829004160267ffffffffffffffff60a01b1990911617808355935460ff600160e01b918290041615150260ff60e01b199094169390931790556003948501549401939093558254868401939192919087908110610e0957fe5b600091825260208083206004909202909101546001600160a01b031683528201929092526040019020555b6001805480610e3f57fe5b60008281526020812060046000199093019283020180546001600160a01b0319908116825560018201805490911690556002810180546001600160e81b03191690556003018190559155818381610e9257fe5b0490508015610ef65760015460005b81811015610ef3578260018281548110610eb757fe5b9060005260206000209060040201600301540160018281548110610ed757fe5b6000918252602090912060036004909202010155600101610ea1565b50505b505050505b50565b600181565b61100181565b603881565b600881565b61200081565b6001600160a01b03811660009081526004602052604081205480610f41576000915050610f66565b600180820381548110610f5057fe5b9060005260206000209060040201600301549150505b919050565b600081565b606781565b60018181548110610f8257fe5b600091825260209091206004909102018054600182015460028301546003909301546001600160a01b0392831694509082169291821691600160a01b81046001600160401b031691600160e01b90910460ff169086565b61100581565b600281565b61100881565b600b81565b606681565b33612000146110345760405162461bcd60e51b815260040180806020018281038252602f815260200180614516602f913960400191505060405180910390fd5b7f41ce201247b6ceb957dcdb217d0b8acb50b9ea0e12af9af4f5e7f38902101605838383604051808460ff1660ff168152602001806020018281038252848482818152602001925080828437600083820152604051601f909101601f1916909201829003965090945050505050a1505050565b6103e881565b60025481565b600981565b61100781565b61100681565b6040518061062001604052806105ef8152602001613f276105ef913981565b60005460ff1681565b6402540be40081565b60005460ff16611148576040805162461bcd60e51b81526020600482015260196024820152781d1a194818dbdb9d1c9858dd081b9bdd081a5b9a5d081e595d603a1b604482015290519081900360640190fd5b33611007146111885760405162461bcd60e51b815260040180806020018281038252602e815260200180613e91602e913960400191505060405180910390fd5b6111f284848080601f01602080910402602001604051908101604052809392919081815260200183838082843760009201919091525050604080518082019091526013815272065787069726554696d655365636f6e6447617606c1b60208201529150612c4c9050565b156112cd57602081146112365760405162461bcd60e51b8152600401808060200182810382526026815260200180613ee06026913960400191505060405180910390fd5b604080516020601f840181900481028201810190925282815260009161127491858580838501838280828437600092019190915250612d3492505050565b90506064811015801561128a5750620186a08111155b6112c55760405162461bcd60e51b8152600401808060200182810382526027815260200180613e456027913960400191505060405180910390fd5b60025561130a565b6040805162461bcd60e51b815260206004820152600d60248201526c756e6b6e6f776e20706172616d60981b604482015290519081900360640190fd5b7f6cdb0ac70ab7f2e2d035cca5be60d89906f2dede7648ddbd7402189c1eeed17a848484846040518080602001806020018381038352878782818152602001925080828437600083820152601f01601f191690910184810383528581526020019050858580828437600083820152604051601f909101601f19169092018290039850909650505050505050a150505050565b60046020526000908152604090205481565b6001546060906000805b828110156113ff57600181815481106113cd57fe5b9060005260206000209060040201600201601c9054906101000a900460ff166113f7576001909101905b6001016113b8565b5060608160405190808252806020026020018201604052801561142c578160200160208202803683370190505b50600092509050815b838110156114cc576001818154811061144a57fe5b9060005260206000209060040201600201601c9054906101000a900460ff166114c4576001818154811061147a57fe5b600091825260209091206004909102015482516001600160a01b03909116908390859081106114a557fe5b6001600160a01b03909216602092830291909101909101526001909201915b600101611435565b509250505090565b61100281565b67016345785d8a000081565b60055481565b61100381565b602981565b60005460ff161561154f576040805162461bcd60e51b815260206004820152601960248201527f74686520636f6e747261637420616c726561647920696e697400000000000000604482015290519081900360640190fd5b611557613d69565b600061157d6040518061062001604052806105ef8152602001613f276105ef9139611b15565b91509150806115bd5760405162461bcd60e51b8152600401808060200182810382526021815260200180613f066021913960400191505060405180910390fd5b60005b8260200151518110156116e2576001836020015182815181106115df57fe5b60209081029190910181015182546001818101855560009485528385208351600493840290910180546001600160a01b039283166001600160a01b03199182161782558587015182850180549185169183169190911790556040860151600283018054606089015160808a01511515600160e01b0260ff60e01b196001600160401b03909216600160a01b0267ffffffffffffffff60a01b199590981692909516919091179290921694909417161790915560a0909301516003909301929092559186015180519185019391859081106116b557fe5b602090810291909101810151516001600160a01b03168252810191909152604001600020556001016115c0565b50506103e8600255506000805460ff19166001179055565b336110011461173a5760405162461bcd60e51b81526004018080602001828103825260298152602001806145726029913960400191505060405180910390fd5b6001600160a01b0381166000908152600460205260409020548061175e5750610efb565b60018103905060006001828154811061177357fe5b906000526020600020906004020160030154905060006001838154811061179657fe5b906000526020600020906004020160030181905550600060018080549050039050836001600160a01b03167f8cd4e147d8af98a9e3b6724021b8bf6aed2e5dac71c38f2dce8161b82585b25d836040518082815260200191505060405180910390a28061180557505050610efb565b600081838161181057fe5b0490508015610ef65760005b8481101561186e57816001828154811061183257fe5b906000526020600020906004020160030154016001828154811061185257fe5b600091825260209091206003600490920201015560010161181c565b50600180549085015b81811015610ef357826001828154811061188d57fe5b90600052602060002090600402016003015401600182815481106118ad57fe5b6000918252602090912060036004909202010155600101611877565b606581565b33411461190c5760405162461bcd60e51b815260040180806020018281038252602d815260200180614545602d913960400191505060405180910390fd5b60005460ff1661195f576040805162461bcd60e51b81526020600482015260196024820152781d1a194818dbdb9d1c9858dd081b9bdd081a5b9a5d081e595d603a1b604482015290519081900360640190fd5b600034116119ac576040805162461bcd60e51b81526020600482015260156024820152746465706f7369742076616c7565206973207a65726f60581b604482015290519081900360640190fd5b6001600160a01b03811660009081526004602052604090205434908015611abf5760006001808303815481106119de57fe5b9060005260206000209060040201905080600201601c9054906101000a900460ff1615611a49576040805184815290516001600160a01b038616917ff177e5d6c5764d79c32883ed824111d9b13f5668cf6ab1cc12dd36791dd955b4919081900360200190a2611ab9565b600354611a5c908463ffffffff612d3916565b6003908155810154611a74908463ffffffff612d3916565b60038201556040805184815290516001600160a01b038616917f93a090ecc682c002995fad3c85b30c5651d7fd29b0be5da9d784a3302aedc055919081900360200190a25b50611aff565b6040805183815290516001600160a01b038516917ff177e5d6c5764d79c32883ed824111d9b13f5668cf6ab1cc12dd36791dd955b4919081900360200190a25b505050565b61100081565b600381565b61100481565b611b1d613d69565b6000611b27613d69565b611b2f613d81565b611b40611b3b86612d93565b612db8565b90506000805b611b4f83612e02565b15611c605780611b7457611b6a611b6584612e23565b612e71565b60ff168452611c58565b8060011415611c53576060611b90611b8b85612e23565b612f28565b90508051604051908082528060200260200182016040528015611bcd57816020015b611bba613da1565b815260200190600190039081611bb25790505b50602086015260005b8151811015611c4857611be7613da1565b6000611c05848481518110611bf857fe5b6020026020010151612ff9565b9150915080611c2257876000995099505050505050505050611c69565b8188602001518481518110611c3357fe5b60209081029190910101525050600101611bd6565b506001925050611c58565b611c60565b600101611b46565b50919350909150505b915091565b604080516001808252818301909252606091829190816020015b6060815260200190600190039081611c88579050509050611cae8363ffffffff166130d6565b81600081518110611cbb57fe5b6020026020010181905250610bd8816130e9565b6000806060611cdd84613173565b9150915081611d8a577f70e72399380dcfb0338abc03dc8d47f9f470ada8e769c9a78d644ea97385ecb2816040518080602001828103825283818151815260200191508051906020019080838360005b83811015611d45578181015183820152602001611d2d565b50505050905090810190601f168015611d725780820380516001836020036101000a031916815260200191505b509250505060405180910390a1606692505050610f66565b600080805b600154811015611e075767016345785d8a000060018281548110611daf57fe5b90600052602060002090600402016003015410611dd157600190920191611dff565b600060018281548110611de057fe5b9060005260206000209060040201600301541115611dff576001909101905b600101611d8f565b50606082604051908082528060200260200182016040528015611e34578160200160208202803683370190505b509050606083604051908082528060200260200182016040528015611e63578160200160208202803683370190505b509050606084604051908082528060200260200182016040528015611e92578160200160208202803683370190505b509050606085604051908082528060200260200182016040528015611ec1578160200160208202803683370190505b5090506000606086604051908082528060200260200182016040528015611ef2578160200160208202803683370190505b509050606087604051908082528060200260200182016040528015611f21578160200160208202803683370190505b509050600098506000975060608d905060006110046001600160a01b031663149d14d96040518163ffffffff1660e01b815260040160206040518083038186803b158015611f6e57600080fd5b505afa158015611f82573d6000803e3d6000fd5b505050506040513d6020811015611f9857600080fd5b5051905067016345785d8a000081111561200d577f70e72399380dcfb0338abc03dc8d47f9f470ada8e769c9a78d644ea97385ecb2604051808060200182810382526021815260200180613ebf6021913960400191505060405180910390a160689d5050505050505050505050505050610f66565b60005b6001548110156122805767016345785d8a00006001828154811061203057fe5b906000526020600020906004020160030154106121b6576001818154811061205457fe5b906000526020600020906004020160020160009054906101000a90046001600160a01b03168a8d8151811061208557fe5b60200260200101906001600160a01b031690816001600160a01b03168152505060006402540be400600183815481106120ba57fe5b906000526020600020906004020160030154816120d357fe5b06600183815481106120e157fe5b906000526020600020906004020160030154039050612109838261325590919063ffffffff16565b8a8e8151811061211557fe5b6020026020010181815250506001828154811061212e57fe5b906000526020600020906004020160020160009054906101000a90046001600160a01b0316888e8151811061215f57fe5b60200260200101906001600160a01b031690816001600160a01b03168152505081898e8151811061218c57fe5b60209081029190910101526121a7878263ffffffff612d3916565b6001909d019c96506122789050565b6000600182815481106121c557fe5b906000526020600020906004020160030154111561227857600181815481106121ea57fe5b906000526020600020906004020160010160009054906101000a90046001600160a01b0316858c8151811061221b57fe5b60200260200101906001600160a01b031690816001600160a01b0316815250506001818154811061224857fe5b906000526020600020906004020160030154848c8151811061226657fe5b60209081029190910101526001909a01995b600101612010565b50600085156126be576110046001600160a01b0316636e056520878c8c8b60025442016040518663ffffffff1660e01b815260040180806020018060200180602001856001600160401b03166001600160401b03168152602001848103845288818151815260200191508051906020019060200280838360005b838110156123125781810151838201526020016122fa565b50505050905001848103835287818151815260200191508051906020019060200280838360005b83811015612351578181015183820152602001612339565b50505050905001848103825286818151815260200191508051906020019060200280838360005b83811015612390578181015183820152602001612378565b505050509050019750505050505050506020604051808303818588803b1580156123b957600080fd5b505af1935050505080156123df57506040513d60208110156123da57600080fd5b505160015b61261a576040516000815260443d10156123fb57506000612496565b60046000803e60005160e01c6308c379a0811461241c576000915050612496565b60043d036004833e81513d60248201116001600160401b038211171561244757600092505050612496565b80830180516001600160401b03811115612468576000945050505050612496565b8060208301013d860181111561248657600095505050505050612496565b601f01601f191660405250925050505b806124a15750612545565b60019150867fa7cdeed7d0db45e3219a6e5d60838824c16f1d39991fcfe3f963029c844bf280826040518080602001828103825283818151815260200191508051906020019080838360005b838110156125055781810151838201526020016124ed565b50505050905090810190601f1680156125325780820380516001836020036101000a031916815260200191505b509250505060405180910390a250612615565b3d80801561256f576040519150601f19603f3d011682016040523d82523d6000602084013e612574565b606091505b5060019150867fbfa884552dd8921b6ce90bfe906952ae5b3b29be0cc1a951d4f62697635a3a45826040518080602001828103825283818151815260200191508051906020019080838360005b838110156125d95781810151838201526020016125c1565b50505050905090810190601f1680156126065780820380516001836020036101000a031916815260200191505b509250505060405180910390a2505b6126be565b8015612658576040805188815290517fa217d08e65f80c73121cd9db834d81652d544bfbf452f6d04922b16c90a37b709181900360200190a16126bc565b604080516020808252601b908201527f6261746368207472616e736665722072657475726e2066616c7365000000000081830152905188917fa7cdeed7d0db45e3219a6e5d60838824c16f1d39991fcfe3f963029c844bf280919081900360600190a25b505b80156128745760005b88518110156128725760008982815181106126de57fe5b602002602001015190506000600182815481106126f757fe5b60009182526020909120600160049092020181015481546001600160a01b03909116916108fc918590811061272857fe5b9060005260206000209060040201600301549081150290604051600060405180830381858888f19350505050905080156127e4576001828154811061276957fe5b60009182526020909120600160049092020181015481546001600160a01b03909116917f6c61d60f69a7beb3e1c80db7f39f37b208537cbb19da3174511b477812b2fc7d91859081106127b857fe5b9060005260206000209060040201600301546040518082815260200191505060405180910390a2612868565b600182815481106127f157fe5b60009182526020909120600160049092020181015481546001600160a01b03909116917f25d0ce7d2f0cec669a8c17efe49d195c13455bb8872b65fa610ac7f53fe4ca7d918590811061284057fe5b9060005260206000209060040201600301546040518082815260200191505060405180910390a25b50506001016126c7565b505b8451156129be5760005b85518110156129bc57600086828151811061289557fe5b60200260200101516001600160a01b03166108fc8784815181106128b557fe5b60200260200101519081150290604051600060405180830381858888f193505050509050801561294b578682815181106128eb57fe5b60200260200101516001600160a01b03167f6c61d60f69a7beb3e1c80db7f39f37b208537cbb19da3174511b477812b2fc7d87848151811061292957fe5b60200260200101516040518082815260200191505060405180910390a26129b3565b86828151811061295757fe5b60200260200101516001600160a01b03167f25d0ce7d2f0cec669a8c17efe49d195c13455bb8872b65fa610ac7f53fe4ca7d87848151811061299557fe5b60200260200101516040518082815260200191505060405180910390a25b5060010161287e565b505b4715612a27576040805147815290517f6ecc855f9440a9282c90913bbc91619fd44f5ec0b462af28d127b116f130aa4d9181900360200190a1604051611002904780156108fc02916000818181858888f19350505050158015612a25573d6000803e3d6000fd5b505b60006003819055600555825115612a4157612a4183613297565b6110016001600160a01b031663fc4333cd6040518163ffffffff1660e01b8152600401600060405180830381600087803b158015612a7e57600080fd5b505af1158015612a92573d6000803e3d6000fd5b50506040517fedd8d7296956dd970ab4de3f2fc03be2b0ffc615d20cd4c72c6e44f928630ebf925060009150a15060009f9e505050505050505050505050505050565b80516001600160a01b0316600090815260046020526040812054801580612b265750600180820381548110612b0657fe5b9060005260206000209060040201600201601c9054906101000a900460ff165b15612b6c5782516040516001600160a01b03909116907fe209c46bebf57cf265d5d9009a00870e256d9150f3ed5281ab9d9eb3cec6e4be90600090a26000915050610f66565b600154600554600019820111801590612bc25784516040516001600160a01b03909116907fe209c46bebf57cf265d5d9009a00870e256d9150f3ed5281ab9d9eb3cec6e4be90600090a260009350505050610f66565b600580546001908101909155805481906000198601908110612be057fe5b6000918252602082206002600490920201018054921515600160e01b0260ff60e01b199093169290921790915585516040516001600160a01b03909116917ff226e7d8f547ff903d9d419cf5f54e0d7d07efa9584135a53a057c5f1f27f49a91a2506000949350505050565b6000816040516020018082805190602001908083835b60208310612c815780518252601f199092019160209182019101612c62565b6001836020036101000a03801982511681845116808217855250505050505090500191505060405160208183030381529060405280519060200120836040516020018082805190602001908083835b60208310612cef5780518252601f199092019160209182019101612cd0565b6001836020036101000a038019825116818451168082178552505050505050905001915050604051602081830303815290604052805190602001201490505b92915050565b015190565b600082820183811015610bd8576040805162461bcd60e51b815260206004820152601b60248201527f536166654d6174683a206164646974696f6e206f766572666c6f770000000000604482015290519081900360640190fd5b612d9b613dd6565b506040805180820190915281518152602082810190820152919050565b612dc0613d81565b612dc98261375e565b612dd257600080fd5b6000612de18360200151613798565b60208085015160408051808201909152868152920190820152915050919050565b6000612e0c613dd6565b505080518051602091820151919092015191011190565b612e2b613dd6565b612e3482612e02565b612e3d57600080fd5b60208201516000612e4d826137fb565b80830160209586015260408051808201909152908152938401919091525090919050565b805160009015801590612e8657508151602110155b612e8f57600080fd5b6000612e9e8360200151613798565b90508083600001511015612ef9576040805162461bcd60e51b815260206004820152601a60248201527f6c656e677468206973206c657373207468616e206f6666736574000000000000604482015290519081900360640190fd5b825160208085015183018051928490039291831015612f1f57826020036101000a820491505b50949350505050565b6060612f338261375e565b612f3c57600080fd5b6000612f478361392e565b9050606081604051908082528060200260200182016040528015612f8557816020015b612f72613dd6565b815260200190600190039081612f6a5790505b5090506000612f978560200151613798565b60208601510190506000805b84811015612fee57612fb4836137fb565b9150604051806040016040528083815260200184815250848281518110612fd757fe5b602090810291909101015291810191600101612fa3565b509195945050505050565b613001613da1565b600061300b613da1565b613013613d81565b61301c85612db8565b90506000805b61302b83612e02565b15611c6057806130565761304661304184612e23565b61398a565b6001600160a01b031684526130ce565b806001141561307e5761306b61304184612e23565b6001600160a01b031660208501526130ce565b80600214156130a65761309361304184612e23565b6001600160a01b031660408501526130ce565b8060031415611c53576130bb611b6584612e23565b6001600160401b03166060850152600191505b600101613022565b6060612d2e6130e4836139a4565b613a8a565b606081516000141561310a5750604080516000815260208101909152610f66565b60608260008151811061311957fe5b602002602001015190506000600190505b835181101561315a576131508285838151811061314357fe5b6020026020010151613adc565b915060010161312a565b50610bd861316d825160c060ff16613b59565b82613adc565b600060606029835111156131a5576000604051806060016040528060298152602001613df16029913991509150611c69565b60005b835181101561323b5760005b81811015613232578481815181106131c857fe5b6020026020010151600001516001600160a01b03168583815181106131e957fe5b6020026020010151600001516001600160a01b0316141561322a5760006040518060600160405280602b8152602001613e1a602b9139935093505050611c69565b6001016131b4565b506001016131a8565b505060408051602081019091526000815260019150915091565b6000610bd883836040518060400160405280601e81526020017f536166654d6174683a207375627472616374696f6e206f766572666c6f770000815250613c51565b600154815160005b828110156133b45760016132b1613da1565b600183815481106132be57fe5b600091825260208083206040805160c08101825260049490940290910180546001600160a01b0390811685526001820154811693850193909352600281015492831691840191909152600160a01b82046001600160401b03166060840152600160e01b90910460ff16151560808301526003015460a082015291505b848110156133885786818151811061334e57fe5b6020026020010151600001516001600160a01b031682600001516001600160a01b031614156133805760009250613388565b60010161333a565b5081156133aa5780516001600160a01b03166000908152600460205260408120555b505060010161329f565b508082111561342957805b828110156134275760018054806133d257fe5b60008281526020812060046000199093019283020180546001600160a01b03199081168255600182810180549092169091556002820180546001600160e81b03191690556003909101919091559155016133bf565b505b6000818310613438578161343a565b825b905060005b81811015613634576134ec85828151811061345657fe5b60200260200101516001838154811061346b57fe5b60009182526020918290206040805160c08101825260049390930290910180546001600160a01b0390811684526001820154811694840194909452600281015493841691830191909152600160a01b83046001600160401b03166060830152600160e01b90920460ff161515608082015260039091015460a0820152613ce8565b61360757806001016004600087848151811061350457fe5b6020026020010151600001516001600160a01b03166001600160a01b031681526020019081526020016000208190555084818151811061354057fe5b60200260200101516001828154811061355557fe5b6000918252602091829020835160049092020180546001600160a01b039283166001600160a01b0319918216178255928401516001820180549184169185169190911790556040840151600282018054606087015160808801511515600160e01b0260ff60e01b196001600160401b03909216600160a01b0267ffffffffffffffff60a01b1995909716929097169190911792909216939093171692909217905560a09091015160039091015561362c565b60006001828154811061361657fe5b9060005260206000209060040201600301819055505b60010161343f565b508282111561375857825b82811015610ef657600185828151811061365557fe5b60209081029190910181015182546001818101855560009485528385208351600493840290910180546001600160a01b039283166001600160a01b03199182161782559585015181840180549184169188169190911790556040850151600282018054606088015160808901511515600160e01b0260ff60e01b196001600160401b03909216600160a01b0267ffffffffffffffff60a01b199590971692909a169190911792909216939093171695909517905560a0909201516003909301929092558751908401929088908590811061372b57fe5b602090810291909101810151516001600160a01b031682528101919091526040016000205560010161363f565b50505050565b805160009061376f57506000610f66565b6020820151805160001a9060c082101561378e57600092505050610f66565b5060019392505050565b8051600090811a60808110156137b2576000915050610f66565b60b88110806137cd575060c081108015906137cd575060f881105b156137dc576001915050610f66565b60c08110156137f05760b519019050610f66565b60f519019050610f66565b80516000908190811a60808110156138165760019150613927565b60b881101561382b57607e1981019150613927565b60c08110156138a557600060b78203600186019550806020036101000a86510491506001810182019350508083101561389f576040805162461bcd60e51b81526020600482015260116024820152706164646974696f6e206f766572666c6f7760781b604482015290519081900360640190fd5b50613927565b60f88110156138ba5760be1981019150613927565b600060f78203600186019550806020036101000a865104915060018101820193505080831015613925576040805162461bcd60e51b81526020600482015260116024820152706164646974696f6e206f766572666c6f7760781b604482015290519081900360640190fd5b505b5092915050565b805160009061393f57506000610f66565b600080905060006139538460200151613798565b602085015185519181019250015b8082101561398157613972826137fb565b60019093019290910190613961565b50909392505050565b805160009060151461399b57600080fd5b612d2e82612e71565b604080516020808252818301909252606091829190602082018180368337505050602081018490529050600067ffffffffffffffff1984166139e857506018613a0c565b6fffffffffffffffffffffffffffffffff198416613a0857506010613a0c565b5060005b6020811015613a4257818181518110613a2157fe5b01602001516001600160f81b03191615613a3a57613a42565b600101613a0c565b60008160200390506060816040519080825280601f01601f191660200182016040528015613a77576020820181803683370190505b5080830196909652508452509192915050565b606081516001148015613abc5750607f60f81b82600081518110613aaa57fe5b01602001516001600160f81b03191611155b15613ac8575080610f66565b612d2e613ada8351608060ff16613b59565b835b6060806040519050835180825260208201818101602087015b81831015613b0d578051835260209283019201613af5565b50855184518101855292509050808201602086015b81831015613b3a578051835260209283019201613b22565b508651929092011591909101601f01601f191660405250905092915050565b6060680100000000000000008310613ba9576040805162461bcd60e51b815260206004820152600e60248201526d696e70757420746f6f206c6f6e6760901b604482015290519081900360640190fd5b60408051600180825281830190925260609160208201818036833701905050905060378411613c035782840160f81b81600081518110613be557fe5b60200101906001600160f81b031916908160001a9053509050612d2e565b6060613c0e856139a4565b90508381510160370160f81b82600081518110613c2757fe5b60200101906001600160f81b031916908160001a905350613c488282613adc565b95945050505050565b60008184841115613ce05760405162461bcd60e51b81526004018080602001828103825283818151815260200191508051906020019080838360005b83811015613ca5578181015183820152602001613c8d565b50505050905090810190601f168015613cd25780820380516001836020036101000a031916815260200191505b509250505060405180910390fd5b505050900390565b805182516000916001600160a01b039182169116148015613d22575081602001516001600160a01b031683602001516001600160a01b0316145b8015613d47575081604001516001600160a01b031683604001516001600160a01b0316145b8015610bd85750506060908101519101516001600160401b0390811691161490565b60408051808201909152600081526060602082015290565b6040518060400160405280613d94613dd6565b8152602001600081525090565b6040805160c081018252600080825260208201819052918101829052606081018290526080810182905260a081019190915290565b60405180604001604052806000815260200160008152509056fe746865206e756d626572206f662076616c696461746f72732065786365656420746865206c696d69746475706c696361746520636f6e73656e7375732061646472657373206f662076616c696461746f725365747468652065787069726554696d655365636f6e64476170206973206f7574206f662072616e67656c656e677468206f66206a61696c2076616c696461746f7273206d757374206265206f6e65746865206d6573736167652073656e646572206d75737420626520676f7665726e616e636520636f6e7472616374666565206973206c6172676572207468616e2044555354595f494e434f4d494e476c656e677468206f662065787069726554696d655365636f6e64476170206d69736d617463686661696c656420746f20706172736520696e69742076616c696461746f72536574f905ec80f905e8f846942a7cdd959bfe8d9487b2a43b33565295a698f7e294b6a7edd747c0554875d3fc531d19ba1497992c5e941ff80f3f7f110ffd8920a3ac38fdef318fe94a3f86048c27395000f846946488aa4d1955ee33403f8ccb1d4de5fb97c7ade294220f003d8bdfaadf52aa1e55ae4cc485e6794875941a87e90e440a39c99aa9cb5cea0ad6a3f0b2407b86048c27395000f846949ef9f4360c606c7ab4db26b016007d3ad0ab86a0946103af86a874b705854033438383c82575f25bc29418e2db06cbff3e3c5f856410a1838649e760175786048c27395000f84694ee01c3b1283aa067c58eab4709f85e99d46de5fe94ee4b9bfb1871c64e2bcabb1dc382dc8b7c4218a29415904ab26ab0e99d70b51c220ccdcccabee6e29786048c27395000f84694685b1ded8013785d6623cc18d214320b6bb6475994a20ef4e5e4e7e36258dbf51f4d905114cb1b34bc9413e39085dc88704f4394d35209a02b1a9520320c86048c27395000f8469478f3adfc719c99674c072166708589033e2d9afe9448a30d5eaa7b64492a160f139e2da2800ec3834e94055838358c29edf4dcc1ba1985ad58aedbb6be2b86048c27395000f84694c2be4ec20253b8642161bc3f444f53679c1f3d479466f50c616d737e60d7ca6311ff0d9c434197898a94d1d678a2506eeaa365056fe565df8bc8659f28b086048c27395000f846942f7be8361c80a4c1e7e9aaf001d0877f1cfde218945f93992ac37f3e61db2ef8a587a436a161fd210b94ecbc4fb1a97861344dad0867ca3cba2b860411f086048c27395000f84694ce2fd7544e0b2cc94692d4a704debef7bcb613289444abc67b4b2fba283c582387f54c9cba7c34bafa948acc2ab395ded08bb75ce85bf0f95ad2abc51ad586048c27395000f84694b8f7166496996a7da21cf1f1b04d9b3e26a3d077946770572763289aac606e4f327c2f6cc1aa3b3e3b94882d745ed97d4422ca8da1c22ec49d880c4c097286048c27395000f846942d4c407bbe49438ed859fe965b140dcf1aab71a9943ad0939e120f33518fbba04631afe7a3ed6327b194b2bbb170ca4e499a2b0f3cc85ebfa6e8c4dfcbea86048c27395000f846946bbad7cf34b5fa511d8e963dbba288b1960e75d694853b0f6c324d1f4e76c8266942337ac1b0af1a229442498946a51ca5924552ead6fc2af08b94fcba648601d1a94a2000f846944430b3230294d12c6ab2aac5c2cd68e80b16b581947b107f4976a252a6939b771202c28e64e03f52d694795811a7f214084116949fc4f53cedbf189eeab28601d1a94a2000f84694ea0a6e3c511bbd10f4519ece37dc24887e11b55d946811ca77acfb221a49393c193f3a22db829fcc8e9464feb7c04830dd9ace164fc5c52b3f5a29e5018a8601d1a94a2000f846947ae2f5b9e386cd1b50a4550696d957cb4900f03a94e83bcc5077e6b873995c24bac871b5ad856047e19464e48d4057a90b233e026c1041e6012ada897fe88601d1a94a2000f8469482012708dafc9e1b880fd083b32182b869be8e09948e5adc73a2d233a1b496ed3115464dd6c7b887509428b383d324bc9a37f4e276190796ba5a8947f5ed8601d1a94a2000f8469422b81f8e175ffde54d797fe11eb03f9e3bf75f1d94a1c3ef7ca38d8ba80cce3bfc53ebd2903ed21658942767f7447f7b9b70313d4147b795414aecea54718601d1a94a2000f8469468bf0b8b6fb4e317a0f9d6f03eaf8ce6675bc60d94675cfe570b7902623f47e7f59c9664b5f5065dcf94d84f0d2e50bcf00f2fc476e1c57f5ca2d57f625b8601d1a94a2000f846948c4d90829ce8f72d0163c1d5cf348a862d5506309485c42a7b34309bee2ed6a235f86d16f059deec5894cc2cedc53f0fa6d376336efb67e43d167169f3b78601d1a94a2000f8469435e7a025f4da968de7e4d7e4004197917f4070f194b1182abaeeb3b4d8eba7e6a4162eac7ace23d57394c4fd0d870da52e73de2dd8ded19fe3d26f43a1138601d1a94a2000f84694d6caa02bbebaebb5d7e581e4b66559e635f805ff94c07335cf083c1c46a487f0325769d88e163b653694efaff03b42e41f953a925fc43720e45fb61a19938601d1a94a2000746865206d6573736167652073656e646572206d7573742062652063726f737320636861696e20636f6e7472616374746865206d6573736167652073656e646572206d7573742062652074686520626c6f636b2070726f6475636572746865206d6573736167652073656e646572206d75737420626520736c61736820636f6e7472616374a2646970667358221220f4016eb3755efa2abde797b21f8695280d971b0fea37198122d2e5867516da0464736f6c63430006040033")
	if header.Number.Uint64() == 24611 || header.Number.Uint64() == 363 || header.Number.Uint64() == 364 {
		log.Info("Hello there")
		log.Info("0x1000", "hash", common.BytesToHash(code).Hex(), "correct", common.BytesToHash(realcode).Hex())

		s := uint256.NewInt(0)
		var h common.Hash
		h[31] = 1
		state.GetState(gov, &h, s)
		log.Info("Validator count", "hash", h.Hex(), "cnt", s)

		h, _ = common.HashData(h.Bytes())
		var x = int(s.Uint64())
		for i := 0; i < x; i++ {
			for j := 0; j < 4; j++ {
				state.GetState(gov, &h, s)
				if j == 3 {
					log.Info("  - Validator", "idx", i, "word", j, "hash", h.Hex(), "val", s)
				}
				bigH := h.Big()
				bigH.Add(bigH, big.NewInt(1))
				copy(h[:], bigH.Bytes()[:])
			}
		}

		for i, r := range *receipts {
			fee := uint256.NewInt(0)
			fee = fee.Mul(uint256.NewInt(r.GasUsed), txs[i].GetPrice())
			log.Info("Transaction executed", "hash", txs[i].Hash(), "gas", r.GasUsed, "price", txs[i].GetPrice(), "fee", fee)
			for j, l := range r.Logs {
				log.Info(" * Log", "idx", j, "t0", l.Topics[0])
				for z, t := range l.Topics[1:] {
					log.Info("   - topic", "idx", z + 1, "value", t.String())
				}
			}
		}
	}
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
	for _, r := range receipts {
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
		//txJson, _ := json2.Marshal(txs[i])
		//json, _ := r.MarshalJSON()
		//sender, _ := txs[i].GetSender()
		//log.Info("receipt", "sender", sender.Hex(), "tx", string(txJson), "receipt", string(json))
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
	_, returnData, err := core.SysCallContract(header.Coinbase, toAddress, msgData[:], *p.chainConfig, state, header, p)
	if err != nil {
		return nil, err
	}
	var ret0 = new([]common.Address)
	out := ret0
	if err := p.validatorSetABI.UnpackIntoInterface(out, method, returnData); err != nil {
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
	balance := state.GetBalance(consensus.SystemAddress).Clone()
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
			log.Info("distribute to system reward pool", "block hash", header.Hash(), "amount", rewards)
			balance = balance.Sub(balance, rewards)
		}
	}
	log.Info("distribute to validator contract", "block hash", header.Hash(), "amount", balance)
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
		log.Info("init contract", "block hash", header.Hash(), "contract", c)
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
		actualHash := actualTx.SigningHash(p.chainConfig.ChainID)
		if !bytes.Equal(actualHash.Bytes(), expectedHash.Bytes()) {
			log.Error(fmt.Sprintf("expected tx hash %v, get %v, nonce %d, to %s, value %s, gas %d, gasPrice %s, data %s", expectedHash.String(), actualTx.Hash().String(),
				expectedTx.GetNonce(),
				expectedTx.GetTo().String(),
				expectedTx.GetValue().String(),
				expectedTx.GetGas(),
				expectedTx.GetPrice().String(),
				hex.EncodeToString(expectedTx.GetData()),
			))
			return fmt.Errorf("expected system tx (hash %v, nonce %d, to %s, value %s, gas %d, gasPrice %s, data %s), actual tx (hash %v, nonce %d, to %s, value %s, gas %d, gasPrice %s, data %s)",
				expectedHash.String(),
				expectedTx.GetNonce(),
				expectedTx.GetTo().String(),
				expectedTx.GetValue().String(),
				expectedTx.GetGas(),
				expectedTx.GetPrice().String(),
				hex.EncodeToString(expectedTx.GetData()),
				actualHash.String(),
				actualTx.GetNonce(),
				actualTx.GetTo().String(),
				actualTx.GetValue().String(),
				actualTx.GetGas(),
				actualTx.GetPrice().String(),
				hex.EncodeToString(actualTx.GetData()),
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
