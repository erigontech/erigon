package clique

import (
	"bytes"
	"sort"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/consensus/misc"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
)

type Verifier struct {
	*Clique
}

func NewCliqueVerifier(c *Clique) *Verifier {
	return &Verifier{c}
}

func (c *Verifier) Verify(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header, _ bool, _ bool) error {
	return c.verifyHeader(header, parents, chain.Config())
}

func (c *Verifier) NeededForVerification(header *types.Header) int {
	return c.findPrevCheckpoint(header.Number.Uint64())
}

const isDebug = true

// verifyHeader checks whether a header conforms to the consensus rules.The
// caller may optionally pass in a batch of parents (ascending order) to avoid
// looking those up from the database. This is useful for concurrently verifying
// a batch of new headers.
func (c *Verifier) verifyHeader(header *types.Header, parents []*types.Header, config *params.ChainConfig) error {
	if header.Number == nil {
		return errUnknownBlock
	}
	number := header.Number.Uint64()

	now := time.Now()
	nowUnix := now.Unix()

	// Don't waste time checking blocks from the future
	if header.Time > uint64(nowUnix) {
		return consensus.ErrFutureBlock
	}

	// Checkpoint blocks need to enforce zero beneficiary
	checkpoint := (number % c.config.Epoch) == 0
	if checkpoint && header.Coinbase != (common.Address{}) {
		return errInvalidCheckpointBeneficiary
	}

	// Nonces must be 0x00..0 or 0xff..f, zeroes enforced on checkpoints
	if !bytes.Equal(header.Nonce[:], nonceAuthVote) && !bytes.Equal(header.Nonce[:], nonceDropVote) {
		return errInvalidVote
	}

	if checkpoint && !bytes.Equal(header.Nonce[:], nonceDropVote) {
		return errInvalidCheckpointVote
	}

	// Check that the extra-data contains both the vanity and signature
	if len(header.Extra) < extraVanity {
		return errMissingVanity
	}
	if len(header.Extra) < extraVanity+extraSeal {
		return errMissingSignature
	}
	// Ensure that the extra-data contains a signer list on checkpoint, but none otherwise
	signersBytes := len(header.Extra) - extraVanity - extraSeal
	if !checkpoint && signersBytes != 0 {
		return errExtraSigners
	}
	if checkpoint && signersBytes%common.AddressLength != 0 {
		return errInvalidCheckpointSigners
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
		if header.Difficulty == nil || (header.Difficulty.Cmp(diffInTurn) != 0 && header.Difficulty.Cmp(diffNoTurn) != 0) {
			return errInvalidDifficulty
		}
	}

	// If all checks passed, validate any special fields for hard forks
	if err := misc.VerifyForkHashes(config, header, false); err != nil {
		return err
	}

	// All basic checks passed, verify cascading fields
	return c.verifyCascadingFields(header, parents)
}

// verifyCascadingFields verifies all the header fields that are not standalone,
// rather depend on a batch of previous headers. The caller may optionally pass
// in a batch of parents (ascending order) to avoid looking those up from the
// database. This is useful for concurrently verifying a batch of new headers.
func (c *Verifier) verifyCascadingFields(header *types.Header, parents []*types.Header) error {
	// The genesis block is the always valid dead-end
	number := header.Number.Uint64()

	// Retrieve the snapshot needed to verify this header and cache it
	snap, err := c.snapshot(parents)
	if err != nil {
		return err
	}

	// Ensure that the block's timestamp isn't too close to its parent
	parent := new(types.Header)
	highestParentNum := int64(-1)
	for _, p := range parents {
		if p.Number.Int64() > highestParentNum {
			*parent = *p
			highestParentNum = parent.Number.Int64()
		}
	}

	if highestParentNum == -1 || parent.Number.Uint64() != number-1 || parent.Hash() != header.ParentHash {
		return consensus.ErrUnknownAncestor
	}
	if parent.Time+c.config.Period > header.Time {
		return errInvalidTimestamp
	}

	// If the block is a checkpoint block, verify the signer list
	if number%c.config.Epoch == 0 {
		signers := make([]byte, len(snap.Signers)*common.AddressLength)
		for i, signer := range snap.signers() {
			copy(signers[i*common.AddressLength:], signer[:])
		}

		extraSuffix := len(header.Extra) - extraSeal
		if !bytes.Equal(header.Extra[extraVanity:extraSuffix], signers) {
			return errMismatchingCheckpointSigners
		}
	}

	// All basic checks passed, verify the seal and return
	err = c.verifySeal(header, snap)
	if err == nil {
		if err = c.applyAndStoreSnapshot(snap, header); err != nil {
			log.Error("can't store a snapshot", "block", header.Number.Uint64(), "hash", header.Hash().String(), "err", err)
		}
	}

	return err
}

// returns sort.less function
func headersSortFunc(headers []*types.Header) func(i, j int) bool {
	return func(i, j int) bool {
		return headers[i].Number.Cmp(headers[j].Number) == -1
	}
}

// Search for a snapshot in memory or on disk for checkpoints
// snapshot retrieves the authorization snapshot at a given point in time.
func (c *Verifier) snapshot(parents []*types.Header) (*Snapshot, error) {
	var snap *Snapshot
	var err error

	if !sort.SliceIsSorted(parents, headersSortFunc(parents)) {
		sort.Slice(parents, headersSortFunc(parents))
	}

	var i int
	var s *Snapshot
	for i = len(parents) - 1; i >= 0; i-- {
		p := parents[i]
		number := parents[i].Number.Uint64()

		// If an in-memory snapshot was found, use that
		if s, ok := c.recents.Get(p.Hash()); ok {
			snap = s.(*Snapshot)
			break
		}

		// If an on-disk checkpoint snapshot can be found, use that
		if isSnapshot(number, c.config.Epoch) {
			if s, err = loadAndFillSnapshot(c.db, p.Number.Uint64(), p.Hash(), c.config, c.snapStorage, c.signatures); err == nil {
				log.Trace("Loaded voting snapshot from disk", "number", p.Number, "hash", p.Hash())
				snap = s
				break
			} else {
				log.Trace("can't load and update a snapshot", "num", number, "block", p.Number.Uint64(), "hash", p.Hash().String(), "error", err)
			}
		}

		// If we're at the genesis, snapshot the initial state.
		if p.Number.Int64() == 0 {
			snap, err = c.storeGenesisSnapshot(p)
			if err != nil {
				return nil, err
			}

			log.Info("Stored checkpoint snapshot to disk1", "number", p.Number, "hash", p.Hash())
			break
		}
	}

	if snap == nil {
		return nil, consensus.ErrUnknownAncestor
	}

	if len(parents) > 0 {
		m := make(map[int64]struct{})
		for _, p := range parents {
			_, ok := m[p.Number.Int64()]
			if ok {
				continue
			}
			m[p.Number.Int64()] = struct{}{}
		}
	}

	if i+1 <= len(parents) {
		parents = parents[i+1:]
	} else {
		parents = []*types.Header{}
	}

	err = c.applyAndStoreSnapshot(snap, parents...)
	if err != nil {
		return nil, err
	}

	return snap, nil
}

func (c *Verifier) storeGenesisSnapshot(h *types.Header) (*Snapshot, error) {
	signers := make([]common.Address, (len(h.Extra)-extraVanity-extraSeal)/common.AddressLength)
	for i := 0; i < len(signers); i++ {
		copy(signers[i][:], h.Extra[extraVanity+i*common.AddressLength:])
	}

	snap := newSnapshot(c.config, c.snapStorage, c.signatures, h.Number.Uint64(), h.Hash(), signers)
	if err := c.applyAndStoreSnapshot(snap); err != nil {
		return nil, err
	}

	return snap, nil
}

// verifySeal checks whether the signature contained in the header satisfies the
// consensus protocol requirements. The method accepts an optional list of parent
// headers that aren't yet part of the local blockchain to generate the snapshots
// from.
func (c *Verifier) verifySeal(header *types.Header, snap *Snapshot) error {
	// Verifying the genesis block is not supported
	number := header.Number.Uint64()
	if number == 0 {
		return errUnknownBlock
	}

	// Resolve the authorization key and check against signers
	signer, err := ecrecover(header, c.signatures)
	if err != nil {
		return err
	}

	if _, ok := snap.Signers[signer]; !ok {
		return errUnauthorizedSigner
	}

	for seen, recent := range snap.Recents {
		if recent == signer {
			// Signer is among recents, only fail if the current block doesn't shift it out
			if limit := uint64(len(snap.Signers)/2 + 1); seen > number-limit {
				return errRecentlySigned
			}
		}
	}

	// Ensure that the difficulty corresponds to the turn-ness of the signer
	if !c.fakeDiff {
		inturn := snap.inturn(header.Number.Uint64(), signer)
		if inturn && header.Difficulty.Cmp(diffInTurn) != 0 {
			return errWrongDifficulty
		}
		if !inturn && header.Difficulty.Cmp(diffNoTurn) != 0 {
			return errWrongDifficulty
		}
	}

	return nil
}

func (c *Verifier) findPrevCheckpoint(num uint64) int {
	if num == 0 {
		// If we're at the genesis, snapshot the initial state.
		return 0
	}

	var n int
	for n = int(num); n >= 0; n-- {
		if ok := c.checkSnapshot(uint64(n)); ok {
			break
		}
	}

	if n < 0 {
		n = 0
	}

	ancestors := int(num) - n
	if ancestors <= 0 {
		ancestors = 1 // we need at least 1 parent for verification
	}

	return ancestors
}

func (c *Verifier) checkSnapshot(num uint64) bool {
	var (
		h        interface{}
		ok       bool
		snapHash common.Hash
		err      error
	)

	if h, ok = c.snapshotBlocks.Get(num); ok {
		snapHash, ok = h.(common.Hash)
		if ok {
			// If an in-memory snapshot was found, use that
			_, ok = c.recents.Get(snapHash)
			if ok {
				return true
			}

			// If an on-disk checkpoint snapshot can be found, use that
			ok, err = hasSnapshotData(c.db, num, snapHash)
			if err != nil {
				log.Error("while getting a shapshot", "block", num, "snapHash", snapHash, "err", err)
				ok = false
			}

			if ok {
				return true
			}
		}
	}

	if !ok {
		err = c.db.Walk(dbutils.CliqueBucket, dbutils.EncodeBlockNumber(num), dbutils.NumberLength*8, func(k, v []byte) (bool, error) {
			_, _, err = dbutils.DecodeBlockBodyKey(k)
			if err != nil {
				return false, err
			}

			ok = true
			return false, nil
		})

		if err != nil {
			log.Error("while getting a shapshot", "block", num, "err", err)
			return false
		}
	}

	return ok
}

func isSnapshot(number uint64, epoch uint64) bool {
	return number == 0 || number%checkpointInterval == 0 || number%epoch == 0
}
