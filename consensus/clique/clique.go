// Copyright 2017 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package clique implements the proof-of-authority consensus engine.
package clique

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"math/rand"
	"sync"
	"time"

	"github.com/goccy/go-json"
	lru "github.com/hashicorp/golang-lru"
	"golang.org/x/crypto/sha3"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

const (
	epochLength          = uint64(30000)          // Default number of blocks after which to checkpoint and reset the pending votes
	extraVanity          = 32                     // Fixed number of extra-data prefix bytes reserved for signer vanity
	extraSeal            = crypto.SignatureLength // Fixed number of extra-data suffix bytes reserved for signer seal
	warmupCacheSnapshots = 20

	wiggleTime = 500 * time.Millisecond // Random delay (per signer) to allow concurrent signers
)

// Clique proof-of-authority protocol constants.
var (
	nonceAuthVote = hexutil.MustDecode("0xffffffffffffffff") // Magic nonce number to vote on adding a new signer
	nonceDropVote = hexutil.MustDecode("0x0000000000000000") // Magic nonce number to vote on removing a signer.

	uncleHash = types.CalcUncleHash(nil) // Always Keccak256(RLP([])) as uncles are meaningless outside of PoW.

	diffInTurn = big.NewInt(2) // Block difficulty for in-turn signatures
	diffNoTurn = big.NewInt(1) // Block difficulty for out-of-turn signatures
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
	errInvalidCheckpointBeneficiary = errors.New("beneficiary in checkpoint block non-zero")

	// errInvalidVote is returned if a nonce value is something else that the two
	// allowed constants of 0x00..0 or 0xff..f.
	errInvalidVote = errors.New("vote nonce not 0x00..0 or 0xff..f")

	// errInvalidCheckpointVote is returned if a checkpoint/epoch transition block
	// has a vote nonce set to non-zeroes.
	errInvalidCheckpointVote = errors.New("vote nonce in checkpoint block non-zero")

	// errMissingVanity is returned if a block's extra-data section is shorter than
	// 32 bytes, which is required to store the signer vanity.
	errMissingVanity = errors.New("extra-data 32 byte vanity prefix missing")

	// errMissingSignature is returned if a block's extra-data section doesn't seem
	// to contain a 65 byte secp256k1 signature.
	errMissingSignature = errors.New("extra-data 65 byte signature suffix missing")

	// errExtraSigners is returned if non-checkpoint block contain signer data in
	// their extra-data fields.
	errExtraSigners = errors.New("non-checkpoint block contains extra signer list")

	// errInvalidCheckpointSigners is returned if a checkpoint block contains an
	// invalid list of signers (i.e. non divisible by 20 bytes).
	errInvalidCheckpointSigners = errors.New("invalid signer list on checkpoint block")

	// errMismatchingCheckpointSigners is returned if a checkpoint block contains a
	// list of signers different than the one the local node calculated.
	errMismatchingCheckpointSigners = errors.New("mismatching signer list on checkpoint block")

	// errInvalidMixDigest is returned if a block's mix digest is non-zero.
	errInvalidMixDigest = errors.New("non-zero mix digest")

	// errInvalidUncleHash is returned if a block contains an non-empty uncle list.
	errInvalidUncleHash = errors.New("non empty uncle hash")

	// errInvalidDifficulty is returned if the difficulty of a block neither 1 or 2.
	errInvalidDifficulty = errors.New("invalid difficulty")

	// errWrongDifficulty is returned if the difficulty of a block doesn't match the
	// turn of the signer.
	errWrongDifficulty = errors.New("wrong difficulty")

	// errInvalidTimestamp is returned if the timestamp of a block is lower than
	// the previous block's timestamp + the minimum block period.
	errInvalidTimestamp = errors.New("invalid timestamp")

	// errInvalidVotingChain is returned if an authorization list is attempted to
	// be modified via out-of-range or non-contiguous headers.
	errInvalidVotingChain = errors.New("invalid voting chain")

	// errUnauthorizedSigner is returned if a header is signed by a non-authorized entity.
	errUnauthorizedSigner = errors.New("unauthorized signer")

	// errRecentlySigned is returned if a header is signed by an authorized entity
	// that already signed a header recently, thus is temporarily not allowed to.
	errRecentlySigned = errors.New("recently signed")
)

// SignerFn hashes and signs the data to be signed by a backing account.
type SignerFn func(signer common.Address, mimeType string, message []byte) ([]byte, error)

// ecrecover extracts the Ethereum account address from a signed header.
func ecrecover(header *types.Header, sigcache *lru.ARCCache) (common.Address, error) {
	// If the signature's already cached, return that
	hash := header.Hash()

	// hitrate while straight-forward sync is from 0.5 to 0.65
	if address, known := sigcache.Peek(hash); known {
		return address.(common.Address), nil
	}

	// Retrieve the signature from the header extra-data
	if len(header.Extra) < extraSeal {
		return common.Address{}, errMissingSignature
	}
	signature := header.Extra[len(header.Extra)-extraSeal:]

	// Recover the public key and the Ethereum address
	pubkey, err := crypto.Ecrecover(SealHash(header).Bytes(), signature)
	if err != nil {
		return common.Address{}, err
	}

	var signer common.Address
	copy(signer[:], crypto.Keccak256(pubkey[1:])[12:])

	sigcache.Add(hash, signer)
	return signer, nil
}

func ecrecovers(hs []*types.Header, sigcache *lru.ARCCache) ([]common.Address, error) {
	res := make([]common.Address, 0, len(hs))

	for _, h := range hs {
		addr, err := ecrecover(h, sigcache)
		if err != nil {
			return nil, err
		}

		res = append(res, addr)
	}

	return res, nil
}

// Clique is the proof-of-authority consensus engine proposed to support the
// Ethereum testnet following the Ropsten attacks.
type Clique struct {
	chainConfig    *params.ChainConfig
	config         *params.CliqueConfig   // Consensus engine configuration parameters
	snapshotConfig *params.SnapshotConfig // Consensus engine configuration parameters
	db             ethdb.Database         // Database to store and retrieve snapshot checkpoints

	signatures     *lru.ARCCache // Signatures of recent blocks to speed up mining
	recents        *lru.ARCCache // Snapshots for recent block to speed up reorgs
	recentsNum     *lru.ARCCache // Snapshots for recent block to speed up reorgs
	snapshotBlocks *lru.ARCCache // blockNum -> hash

	proposals map[common.Address]bool // Current list of proposals we are pushing

	signer common.Address // Ethereum address of the signing key
	signFn SignerFn       // Signer function to authorize hashes with
	lock   sync.RWMutex   // Protects the signer fields

	// The fields below are for testing only
	fakeDiff bool // Skip difficulty verifications

	snapStorage *storage
	exitCh      chan struct{}
}

// New creates a Clique proof-of-authority consensus engine with the initial
// signers set to the ones provided by the user.
func New(cfg *params.ChainConfig, snapshotConfig *params.SnapshotConfig, cliqueDB ethdb.Database) *Clique {
	config := cfg.Clique

	// Set any missing consensus parameters to their defaults
	conf := *config
	if conf.Epoch == 0 {
		conf.Epoch = epochLength
	}
	// Allocate the snapshot caches and create the engine
	recents, _ := lru.NewARC(snapshotConfig.InmemorySnapshots)
	recentsNum, _ := lru.NewARC(snapshotConfig.InmemorySnapshots)
	snapshotBlocks, _ := lru.NewARC(snapshotConfig.InmemorySnapshots)
	signatures, _ := lru.NewARC(snapshotConfig.InmemorySignatures)

	exitCh := make(chan struct{})

	c := &Clique{
		chainConfig:    cfg,
		config:         &conf,
		snapshotConfig: snapshotConfig,
		db:             cliqueDB,
		recents:        recents,
		recentsNum:     recentsNum,
		snapshotBlocks: snapshotBlocks,
		signatures:     signatures,
		proposals:      make(map[common.Address]bool),
		snapStorage:    newStorage(cliqueDB, exitCh),
		exitCh:         exitCh,
	}

	// warm the cache
	snapNum, err := lastSnapshot(cliqueDB)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			log.Error("on Clique init while getting latest snapshot", "error", err)
		}
	} else {
		snaps, err := c.snapshots(snapNum, warmupCacheSnapshots)
		if err != nil {
			log.Error("on Clique init", "error", err)
		}

		for _, sn := range snaps {
			c.recentsAdd(sn.Number, sn.Hash, sn)
			c.snapshotBlocks.Add(sn.Number, sn.Hash)
		}
	}

	return c
}

// Author implements consensus.Engine, returning the Ethereum address recovered
// from the signature in the header's extra-data section.
func (c *Clique) Author(header *types.Header) (common.Address, error) {
	return ecrecover(header, c.signatures)
}

// VerifyHeader checks whether a header conforms to the consensus rules.
func (c *Clique) VerifyHeader(chain consensus.ChainHeaderReader, header *types.Header, _ bool) error {
	snap, err := c.snapshot(chain, header.Number.Uint64(), header.Hash(), header.ParentHash)
	if err != nil {
		return err
	}
	return c.verifyHeaderBySnapshot(chain, header, snap)
}

// VerifyHeaders is similar to VerifyHeader, but verifies a batch of headers. The
// method returns a quit channel to abort the operations and a results channel to
// retrieve the async verifications (the order is that of the input slice).
func (c *Clique) VerifyHeaders(chain consensus.ChainHeaderReader, headers []*types.Header, _ []bool) (func(), <-chan error) {
	abort := make(chan struct{})
	results := make(chan error, len(headers))

	cancel := func() {
		close(abort)
	}

	go func() {
		var doneCount int

		for i, header := range headers {
			err := c.verifyHeader(chain, header, headers[:i])

			select {
			case <-abort:
				return
			case results <- err:
				doneCount++
				if doneCount == len(headers) {
					close(results)
				}
			}
		}
	}()

	return cancel, results
}

func (c *Clique) recentsAdd(num uint64, hash common.Hash, s *Snapshot) {
	c.recents.Add(hash, s.Copy())
	c.recentsNum.Add(num, struct{}{})
}

func (c *Clique) recentsGet(hash common.Hash) (*Snapshot, bool) {
	s, ok := c.recents.Get(hash)
	if !ok {
		return nil, false
	}
	sn := s.(*Snapshot)
	if sn == nil {
		return nil, false
	}
	return sn.Copy(), true
}

func (c *Clique) recentsHas(hash common.Hash) bool {
	return c.recents.Contains(hash)
}

func (c *Clique) applyAndStoreSnapshot(snap *Snapshot, check bool, headers ...*types.Header) error {
	if snap == nil {
		return fmt.Errorf("can't create a new snapshot, a previous one is nil: %w", ErrNotFound)
	}

	num := snap.Number + uint64(len(headers))

	hash := snap.Hash
	if len(headers) > 0 {
		hash = headers[len(headers)-1].Hash()
	}

	if hash != (common.Hash{}) && check {
		s, ok := c.getSnapshot(num, &hash)
		if ok {
			*snap = *s
			return nil
		}
	}

	if len(headers) > 0 && headers[len(headers)-1].Number.Uint64() > snap.Number {
		if err := snap.apply(c.signatures, headers...); err != nil {
			return err
		}
	}

	c.recentsAdd(snap.Number, snap.Hash, snap)

	c.snapshotBlocks.Add(snap.Number, snap.Hash)

	// If we've generated a new checkpoint snapshot, save to disk
	if isSnapshot(snap.Number, c.config.Epoch, c.snapshotConfig.CheckpointInterval) {
		if err := snap.store(); err != nil {
			return err
		}
		log.Trace("Stored a snapshot to disk", "number", snap.Number, "hash", snap.Hash)
	}

	return nil
}

// VerifyUncles implements consensus.Engine, always returning an error for any
// uncles as this consensus mechanism doesn't permit uncles.
func (c *Clique) VerifyUncles(_ consensus.ChainReader, block *types.Block) error {
	if len(block.Uncles()) > 0 {
		return errors.New("uncles not allowed")
	}
	return nil
}

// VerifySeal implements consensus.Engine, checking whether the signature contained
// in the header satisfies the consensus protocol requirements.
func (c *Clique) VerifySeal(chain consensus.ChainHeaderReader, header *types.Header) error {
	snap, err := c.snapshot(chain, header.Number.Uint64(), header.Hash(), header.ParentHash)
	if err != nil {
		return err
	}
	return c.verifySeal(chain, header, snap)
}

// Prepare implements consensus.Engine, preparing all the consensus fields of the
// header for running the transactions on top.
func (c *Clique) Prepare(chain consensus.ChainHeaderReader, header *types.Header) error {
	// If the block isn't a checkpoint, cast a random vote (good enough for now)
	header.Coinbase = common.Address{}
	header.Nonce = types.BlockNonce{}

	number := header.Number.Uint64()
	// Assemble the voting snapshot to check which votes make sense
	snap, err := c.snapshot(chain, number-1, header.ParentHash, common.Hash{})
	if err != nil {
		return err
	}
	if number%c.config.Epoch != 0 {
		c.lock.RLock()

		// Gather all the proposals that make sense voting on
		addresses := make([]common.Address, 0, len(c.proposals))
		for address, authorize := range c.proposals {
			if snap.validVote(address, authorize) {
				addresses = append(addresses, address)
			}
		}
		// If there's pending proposals, cast a vote on them
		if len(addresses) > 0 {
			header.Coinbase = addresses[rand.Intn(len(addresses))]
			if c.proposals[header.Coinbase] {
				copy(header.Nonce[:], nonceAuthVote)
			} else {
				copy(header.Nonce[:], nonceDropVote)
			}
		}
		c.lock.RUnlock()
	}
	// Set the correct difficulty
	header.Difficulty = calcDifficulty(snap, c.signer)

	// Ensure the extra data has all its components
	if len(header.Extra) < extraVanity {
		header.Extra = append(header.Extra, bytes.Repeat([]byte{0x00}, extraVanity-len(header.Extra))...)
	}
	header.Extra = header.Extra[:extraVanity]

	if number%c.config.Epoch == 0 {
		for _, signer := range snap.signers() {
			header.Extra = append(header.Extra, signer[:]...)
		}
	}
	header.Extra = append(header.Extra, make([]byte, extraSeal)...)

	// Mix digest is reserved for now, set to empty
	header.MixDigest = common.Hash{}

	// Ensure the timestamp has the correct delay
	parent := chain.GetHeader(header.ParentHash, number-1)
	if parent == nil {
		return consensus.ErrUnknownAncestor
	}
	header.Time = parent.Time + c.config.Period

	now := uint64(time.Now().Unix())
	if header.Time < now {
		header.Time = now
	}

	return nil
}

// Finalize implements consensus.Engine, ensuring no uncles are set, nor block
// rewards given.
func (c *Clique) Finalize(_ *params.ChainConfig, header *types.Header, state *state.IntraBlockState, txs []*types.Transaction, uncles []*types.Header) {
	// No block rewards in PoA, so the state remains as is and uncles are dropped
	header.UncleHash = types.CalcUncleHash(nil)
}

// FinalizeAndAssemble implements consensus.Engine, ensuring no uncles are set,
// nor block rewards given, and returns the final block.
func (c *Clique) FinalizeAndAssemble(chainConfig *params.ChainConfig, header *types.Header, state *state.IntraBlockState, txs []*types.Transaction, uncles []*types.Header, receipts []*types.Receipt) (*types.Block, error) {
	// No block rewards in PoA, so the state remains as is and uncles are dropped
	header.UncleHash = types.CalcUncleHash(nil)

	// Assemble and return the final block for sealing
	return types.NewBlock(header, txs, nil, receipts), nil
}

// Authorize injects a private key into the consensus engine to mint new blocks
// with.
func (c *Clique) Authorize(signer common.Address, signFn SignerFn) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.signer = signer
	c.signFn = signFn
}

// Seal implements consensus.Engine, attempting to create a sealed block using
// the local signing credentials.
func (c *Clique) Seal(chain consensus.ChainHeaderReader, block *types.Block, results chan<- *types.Block, stop <-chan struct{}) error {
	header := block.Header()

	// Sealing the genesis block is not supported
	number := header.Number.Uint64()
	if number == 0 {
		return errUnknownBlock
	}
	// For 0-period chains, refuse to seal empty blocks (no reward but would spin sealing)
	if c.config.Period == 0 && len(block.Transactions()) == 0 {
		log.Info("Sealing paused, waiting for transactions")
		return nil
	}
	// Don't hold the signer fields for the entire sealing procedure
	c.lock.RLock()
	signer, signFn := c.signer, c.signFn
	c.lock.RUnlock()

	// Bail out if we're unauthorized to sign a block
	snap, err := c.snapshot(chain, number-1, header.ParentHash, common.Hash{})
	if err != nil {
		return err
	}
	if _, authorized := snap.Signers[signer]; !authorized {
		return errUnauthorizedSigner
	}
	// If we're amongst the recent signers, wait for the next block
	for seen, recent := range snap.Recents {
		if recent == signer {
			// Signer is among RecentsRLP, only wait if the current block doesn't shift it out
			if limit := uint64(len(snap.Signers)/2 + 1); number < limit || seen > number-limit {
				log.Info("Signed recently, must wait for others")
				return nil
			}
		}
	}
	// Sweet, the protocol permits us to sign the block, wait for our time
	delay := time.Unix(int64(header.Time), 0).Sub(time.Now()) // nolint: gosimple
	if header.Difficulty.Cmp(diffNoTurn) == 0 {
		// It's not our turn explicitly to sign, delay it a bit
		wiggle := time.Duration(len(snap.Signers)/2+1) * wiggleTime
		delay += time.Duration(rand.Int63n(int64(wiggle)))

		log.Trace("Out-of-turn signing requested", "wiggle", common.PrettyDuration(wiggle))
	}
	// Sign all the things!
	sighash, err := signFn(signer, accounts.MimetypeClique, CliqueRLP(header))
	if err != nil {
		return err
	}
	copy(header.Extra[len(header.Extra)-extraSeal:], sighash)
	// Wait until sealing is terminated or delay timeout.
	log.Trace("Waiting for slot to sign and propagate", "delay", common.PrettyDuration(delay))
	go func() {
		select {
		case <-stop:
			return
		case <-time.After(delay):
		}

		select {
		case results <- block.WithSeal(header):
		default:
			log.Warn("Sealing result is not read by miner", "sealhash", SealHash(header))
		}
	}()

	return nil
}

// CalcDifficulty is the difficulty adjustment algorithm. It returns the difficulty
// that a new block should have:
// * DIFF_NOTURN(2) if BLOCK_NUMBER % SIGNER_COUNT != SIGNER_INDEX
// * DIFF_INTURN(1) if BLOCK_NUMBER % SIGNER_COUNT == SIGNER_INDEX
func (c *Clique) CalcDifficulty(chain consensus.ChainHeaderReader, _, _ uint64, _, parentNumber *big.Int, parentHash, _ common.Hash) *big.Int {
	snap, err := c.snapshot(chain, parentNumber.Uint64(), parentHash, common.Hash{})
	if err != nil {
		return nil
	}
	return calcDifficulty(snap, c.signer)
}

func calcDifficulty(snap *Snapshot, signer common.Address) *big.Int {
	if snap.inturn(snap.Number+1, signer) {
		return new(big.Int).Set(diffInTurn)
	}
	return new(big.Int).Set(diffNoTurn)
}

// SealHash returns the hash of a block prior to it being sealed.
func (c *Clique) SealHash(header *types.Header) common.Hash {
	return SealHash(header)
}

// Close implements consensus.Engine. It's a noop for clique as there are no background threads.
func (c *Clique) Close() error {
	common.SafeClose(c.exitCh)
	c.snapStorage.Close()
	return nil
}

// APIs implements consensus.Engine, returning the user facing RPC API to allow
// controlling the signer voting.
func (c *Clique) APIs(chain consensus.ChainHeaderReader) []rpc.API {
	return []rpc.API{{
		Namespace: "clique",
		Version:   "1.0",
		Service:   &API{chain: chain, clique: c},
		Public:    false,
	}}
}

// SealHash returns the hash of a block prior to it being sealed.
func SealHash(header *types.Header) (hash common.Hash) {
	hasher := sha3.NewLegacyKeccak256()
	encodeSigHeader(hasher, header)
	hasher.Sum(hash[:0])
	return hash
}

// CliqueRLP returns the rlp bytes which needs to be signed for the proof-of-authority
// sealing. The RLP to sign consists of the entire header apart from the 65 byte signature
// contained at the end of the extra data.
//
// Note, the method requires the extra data to be at least 65 bytes, otherwise it
// panics. This is done to avoid accidentally using both forms (signature present
// or not), which could be abused to produce different hashes for the same header.
func CliqueRLP(header *types.Header) []byte {
	b := new(bytes.Buffer)
	encodeSigHeader(b, header)
	return b.Bytes()
}

func encodeSigHeader(w io.Writer, header *types.Header) {
	err := rlp.Encode(w, []interface{}{
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
		header.Extra[:len(header.Extra)-crypto.SignatureLength], // Yes, this will panic if extra is too short
		header.MixDigest,
		header.Nonce,
	})
	if err != nil {
		panic("can't encode: " + err.Error())
	}
}

func (c *Clique) checkSnapshot(num uint64, hash *common.Hash) bool {
	ok, _ := hasSnapshot(c.db, num)
	if !ok {
		return false
	}

	if ok := c.findSnapshot(num, hash); !ok {
		return c.lookupSnapshot(num)
	}

	return true
}

func (c *Clique) findSnapshot(num uint64, hash *common.Hash) bool {
	var (
		h        interface{}
		ok       bool
		snapHash common.Hash
		err      error
	)

	if h, ok = c.snapshotBlocks.Peek(num); ok {
		snapHash, ok = h.(common.Hash)
		if ok {
			if hash != nil && *hash != snapHash {
				ok = false
			} else {
				// If an in-memory snapshot was found, use that
				ok = c.recentsHas(snapHash)
			}
		}
	}

	if !ok && hash != nil {
		// If an on-disk checkpoint snapshot can be found, use that
		ok, err = hasSnapshotData(c.db, num, *hash)
		if err != nil {
			ok = false
		}
	}

	return ok
}

func (c *Clique) getSnapshot(num uint64, hash *common.Hash) (*Snapshot, bool) {
	var (
		h        interface{}
		s        *Snapshot
		ok       bool
		snapHash common.Hash
		err      error
	)

	if h, ok = c.snapshotBlocks.Peek(num); ok {
		snapHash, ok = h.(common.Hash)
		if ok {
			if hash != nil && *hash != snapHash {
				ok = false
			} else {
				// If an in-memory snapshot was found, use that
				s, ok = c.recentsGet(snapHash)
			}
		}
	}

	if !ok && hash != nil {
		// If an on-disk checkpoint snapshot can be found, use that
		s, err = loadSnapshot(c.db, num, *hash)
		if err != nil {
			ok = false
		}
	}

	return s, ok
}

func (c *Clique) lookupSnapshot(num uint64) bool {

	prefix := dbutils.EncodeBlockNumber(num)
	var tx ethdb.Tx
	if dbtx, err := c.db.Begin(context.Background(), ethdb.RO); err == nil {
		defer dbtx.Rollback()
		tx = dbtx.(ethdb.HasTx).Tx()
	} else {
		log.Error("Lookup snapshot - opening RO tx", "error", err)
		return false
	}

	cur, err := tx.Cursor(dbutils.CliqueSeparateBucket)
	if err != nil {
		log.Error("Lookup snapshot - opening cursor", "error", err)
		return false
	}
	defer cur.Close()

	k, _, err1 := cur.Seek(prefix)
	if err1 != nil {
		log.Error("Lookup snapshot - seek", "error", err1)
		return false
	}

	return bytes.HasPrefix(k, prefix)
}

func (c *Clique) snapshots(latest uint64, total int) ([]*Snapshot, error) {
	if total <= 0 {
		return nil, nil
	}

	blockEncoded := dbutils.EncodeBlockNumber(latest)

	var tx ethdb.Tx
	if dbtx, err := c.db.Begin(context.Background(), ethdb.RO); err == nil {
		defer dbtx.Rollback()
		tx = dbtx.(ethdb.HasTx).Tx()
	} else {
		return nil, err
	}
	cur, err1 := tx.Cursor(dbutils.CliqueSeparateBucket)
	if err1 != nil {
		return nil, err1
	}
	defer cur.Close()

	res := make([]*Snapshot, 0, total)
	for k, v, err := cur.Seek(blockEncoded); k != nil; k, v, err = cur.Prev() {
		if err != nil {
			return nil, err
		}

		s := new(Snapshot)
		err = json.Unmarshal(v, s)
		if err != nil {
			return nil, err
		}

		s.config = c.config
		s.snapStorage = c.snapStorage

		res = append(res, s)

		total--
		if total == 0 {
			break
		}
	}

	return res, nil
}
