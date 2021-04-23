package clique

import (
	"bytes"
	"fmt"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/consensus/misc"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/log"
)

// verifyHeader checks whether a header conforms to the consensus rules.The
// caller may optionally pass in a batch of parents (ascending order) to avoid
// looking those up from the database. This is useful for concurrently verifying
// a batch of new headers.
func (c *Clique) verifyHeader(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header) error {
	if err := c.verifyHeaderData(header); err != nil {
		return err
	}

	// All basic checks passed, verify cascading fields
	return c.verifyCascadingFieldsByAncestors(chain, header, parents)
}

func (c *Clique) verifyHeaderBySnapshot(chain consensus.ChainHeaderReader, header *types.Header, snap *Snapshot) error {
	if err := c.verifyHeaderData(header); err != nil {
		return err
	}

	// All basic checks passed, verify cascading fields
	return c.verifyCascadingFieldsBySnapshot(chain, header, snap)
}

func (c *Clique) verifyHeaderData(header *types.Header) error {
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
	return misc.VerifyForkHashes(c.chainConfig, header, false)
}

// verifyCascadingFields verifies all the header fields that are not standalone,
// rather depend on a batch of previous headers. The caller may optionally pass
// in a batch of parents (ascending order) to avoid looking those up from the
// database. This is useful for concurrently verifying a batch of new headers.
func (c *Clique) verifyCascadingFieldsByAncestors(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header) error {
	// The genesis block is the always valid dead-end
	number := header.Number.Uint64()
	if number == 0 {
		return nil
	}

	// Retrieve the snapshot needed to verify this header and cache it
	var parent *types.Header
	if len(parents) > 0 {
		parent = parents[len(parents)-1]
	} else {
		parent = chain.GetHeader(header.ParentHash, number-1)
		parents = []*types.Header{parent}
	}

	snap, err := c.snapshotFromAncestors(&parents)
	if err != nil {
		return err
	}

	// Ensure that the block's timestamp isn't too close to its parent
	if parent.Number.Uint64() != number-1 || parent.Hash() != header.ParentHash {
		return fmt.Errorf("verifyCascadingFields num=%d err=%w %v", header.Number.Uint64(), consensus.ErrUnknownAncestor, len(parents))
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
	err = c.verifySeal(chain, header, snap)
	if err == nil {
		if err = c.applyAndStoreSnapshot(snap, true, header); err != nil {
			log.Error("can't store a snapshot", "block", header.Number.Uint64(), "hash", header.Hash().String(), "err", err, len(parents))
		}
	}

	return err
}

func (c *Clique) verifyCascadingFieldsBySnapshot(chain consensus.ChainHeaderReader, header *types.Header, snap *Snapshot) error {
	// The genesis block is the always valid dead-end
	number := header.Number.Uint64()
	if number == 0 {
		return nil
	}

	// Ensure that the block's timestamp isn't too close to its parent
	if snap.Number != number-1 || snap.Hash != header.ParentHash {
		return fmt.Errorf("verifyCascadingFields num=%d err=%w", header.Number.Uint64(), consensus.ErrUnknownAncestor)
	}
	if snap.Time+c.config.Period > header.Time {
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
	if err := c.verifySeal(chain, header, snap); err != nil {
		return err
	}
	if err := c.applyAndStoreSnapshot(snap, true, header); err != nil {
		log.Error("can't store a snapshot", "block", header.Number.Uint64(), "hash", header.Hash().String(), "err", err)
		return err
	}
	return nil
}

func (c *Clique) snapshot(chain consensus.ChainHeaderReader, num uint64, hash common.Hash, parentHash common.Hash) (*Snapshot, error) {
	if parentHash == (common.Hash{}) {
		parent := chain.GetHeader(hash, num)
		if parent == nil {
			return nil, fmt.Errorf("header not found: %d(%q)", num, hash.Hex())
		}
		parentHash = parent.Hash()
	}

	return c.getHeaderSnapshot(chain, num, hash, parentHash)
}

// Search for a snapshot in memory or on disk for checkpoints
// snapshot retrieves the authorization snapshot at a given point in time.
func (c *Clique) snapshotFromAncestors(parentsRef *[]*types.Header) (*Snapshot, error) {
	var snap *Snapshot
	var err error

	parents := *parentsRef

	var (
		i int
		s *Snapshot
		p *types.Header
	)

	for i = len(parents) - 1; i >= 0; i-- {
		p = parents[i]
		number := parents[i].Number.Uint64()

		// If an in-memory snapshot was found, use that
		var ok bool
		if s, ok = c.recentsGet(p.Hash()); ok {
			snap = s
			break
		}

		// If an on-disk checkpoint snapshot can be found, use that
		if isSnapshot(number, c.config.Epoch, c.snapshotConfig.CheckpointInterval) {
			if s, err = loadAndFillSnapshot(c.db, p.Number.Uint64(), p.Hash(), c.config, c.snapStorage); err == nil {
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

			log.Info("Stored genesis checkpoint snapshot to disk", "number", p.Number, "hash", p.Hash())
			break
		}
	}

	if snap == nil {
		if len(parents) > 0 {
			if i < 0 {
				i = 0
			}

			block := parents[0].Number.Uint64() + 1
			return nil, fmt.Errorf("a nil snap for %d block, parents from %d to %d(stop on %d - index %d): %w\n%s",
				block,
				parents[len(parents)-1].Number.Uint64(),
				parents[0].Number.Uint64(),
				parents[i].Number.Uint64(),
				i,
				consensus.ErrUnknownAncestor,
				debug.Callers(10))
		}

		return nil, fmt.Errorf("a nil snap for %d ancestors: %w", len(parents), consensus.ErrUnknownAncestor)
	}

	// we always need at least 1 parent for further validation
	if i != len(parents)-1 {
		if i+1 <= len(parents)+1 {
			parents = parents[i+1:]
		} else {
			parents = []*types.Header{}
		}
	} else {
		parents = parents[i:]
	}
	*parentsRef = parents

	err = c.applyAndStoreSnapshot(snap, true, parents...)
	if err != nil {
		return nil, err
	}

	return snap, nil
}

func (c *Clique) snapshotByHeader(num uint64, hash common.Hash) *Snapshot {
	if s, ok := c.recentsGet(hash); ok {
		return s
	}

	if isSnapshot(num, c.config.Epoch, c.snapshotConfig.CheckpointInterval) {
		if s, err := loadAndFillSnapshot(c.db, num, hash, c.config, c.snapStorage); err == nil {
			return s
		}
	}
	fmt.Printf("snapshotByHeader %d (epoch %d, checkpoint interval %d) %x returns nil\n", num, c.config.Epoch, c.snapshotConfig.CheckpointInterval, hash)

	return nil
}

func (c *Clique) storeGenesisSnapshot(h *types.Header) (*Snapshot, error) {
	signers := make([]common.Address, (len(h.Extra)-extraVanity-extraSeal)/common.AddressLength)
	for i := 0; i < len(signers); i++ {
		copy(signers[i][:], h.Extra[extraVanity+i*common.AddressLength:])
	}

	snap := newSnapshot(c.config, c.snapStorage, h.Number.Uint64(), h.Hash(), h.Time, signers)
	if err := c.applyAndStoreSnapshot(snap, false); err != nil {
		return nil, err
	}

	return snap, nil
}

// verifySeal checks whether the signature contained in the header satisfies the
// consensus protocol requirements. The method accepts an optional list of parent
// headers that aren't yet part of the local blockchain to generate the snapshots
// from.
func (c *Clique) verifySeal(chain consensus.ChainHeaderReader, header *types.Header, snap *Snapshot) error {
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
			// Signer is among RecentsRLP, only fail if the current block doesn't shift it out
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

func (c *Clique) getHeaderSnapshot(chain consensus.ChainHeaderReader, num uint64, hash common.Hash, parentHash common.Hash) (*Snapshot, error) {
	ancestorsNum := uint64(c.findPrevCheckpoint(num, hash, parentHash))
	if ancestorsNum == 0 {
		snap := c.snapshotByHeader(num, hash)
		if snap != nil {
			return snap, nil
		}
	}

	ancestors := make([]*types.Header, 0, ancestorsNum)

	for n := int(num - 1); n >= int(num)-int(ancestorsNum); n-- {
		ans := chain.GetHeader(parentHash, uint64(n))
		ancestors = append(ancestors, ans)
		parentHash = ans.ParentHash
	}

	snap, err := c.snapshotFromAncestors(&ancestors)
	if err != nil {
		return nil, err
	}

	return snap, nil
}

func (c *Clique) getAncestors(chain consensus.ChainHeaderReader, num uint64, hash common.Hash, parentHash common.Hash) []*types.Header {
	ancestorsNum := uint64(c.findPrevCheckpoint(num, hash, parentHash))
	if ancestorsNum == 0 {
		return nil
	}

	ancestors := make([]*types.Header, ancestorsNum)

	i := ancestorsNum - 1
	for n := int(num - 1); n >= int(num)-int(ancestorsNum); n-- {
		ans := chain.GetHeader(parentHash, uint64(n))
		ancestors[i] = ans
		parentHash = ans.ParentHash
		i--
	}

	return ancestors
}

func (c *Clique) findPrevCheckpoint(num uint64, hash common.Hash, parentHash common.Hash) int {
	if num == 0 {
		// If we're at the genesis, snapshot the initial state.
		return 0
	}

	var (
		n       int
		ok      bool
		highest = num
	)

	for n = int(highest); n >= 0; n-- {
		ok = c.recentsNum.Contains(uint64(n))
		if ok {
			break
		}
	}

	if !ok {
		last, _ := lastSnapshot(c.db)
		if last < num {
			highest = last
		}

		for n = int(highest); n >= 0; n-- {
			ok = c.checkClosestSnapshot(uint64(n), hash, parentHash, num)
			if ok {
				break
			}
		}
	}

	if n < 0 {
		n = 0
	}

	ancestors := int(num) - n

	if ancestors < 0 {
		ancestors = 1 // we need at least 1 parent for verification
	}

	return ancestors
}

func (c *Clique) checkClosestSnapshot(num uint64, hash common.Hash, parentHash common.Hash, maxNum uint64) bool {
	var snapshotHash *common.Hash

	switch num {
	case maxNum:
		if hash != (common.Hash{}) {
			snapshotHash = &hash
		}
	case maxNum - 1:
		if parentHash != (common.Hash{}) {
			snapshotHash = &parentHash
		}
	default:
		// nothing to do
	}

	return c.checkSnapshot(num, snapshotHash)
}

func isSnapshot(number uint64, epoch, checkpointInterval uint64) bool {
	return number == 0 || number%checkpointInterval == 0 || number%epoch == 0
}
