package clique

import (
	"bytes"
	"sort"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/consensus/misc"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/log"
)

type CliqueVerifier struct {
	*Clique
}

func NewCliqueVerifier(c *Clique) *CliqueVerifier {
	return &CliqueVerifier{c}
}

func (c *CliqueVerifier) Verify(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header, _ bool, _ bool) error {
	return c.verifyHeader(chain, header, parents)
}

func (c *CliqueVerifier) NeededForVerification(header *types.Header) int {
	return int(header.Number.Int64()) % checkpointInterval
}

// verifyHeader checks whether a header conforms to the consensus rules.The
// caller may optionally pass in a batch of parents (ascending order) to avoid
// looking those up from the database. This is useful for concurrently verifying
// a batch of new headers.
func (c *CliqueVerifier) verifyHeader(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header) error {
	if header.Number == nil {
		return errUnknownBlock
	}
	number := header.Number.Uint64()

	// Don't waste time checking blocks from the future
	if header.Time > uint64(time.Now().Unix()) {
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
	if err := misc.VerifyForkHashes(chain.Config(), header, false); err != nil {
		return err
	}
	// All basic checks passed, verify cascading fields
	return c.verifyCascadingFields(chain, header, parents)
}

// verifyCascadingFields verifies all the header fields that are not standalone,
// rather depend on a batch of previous headers. The caller may optionally pass
// in a batch of parents (ascending order) to avoid looking those up from the
// database. This is useful for concurrently verifying a batch of new headers.
func (c *CliqueVerifier) verifyCascadingFields(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header) error {
	// The genesis block is the always valid dead-end
	number := header.Number.Uint64()
	if number == 0 {
		return nil
	}
	// Ensure that the block's timestamp isn't too close to its parent
	var parent *types.Header
	if len(parents) > 0 {
		parent = parents[len(parents)-1]
	}

	if parent == nil || parent.Number.Uint64() != number-1 || parent.Hash() != header.ParentHash {
		return consensus.ErrUnknownAncestor
	}
	if parent.Time+c.config.Period > header.Time {
		return errInvalidTimestamp
	}

	// TODO: check that parents are a chain!!!

	// Retrieve the snapshot needed to verify this header and cache it
	snap, err := c.snapshot(number-1, parents)
	if err != nil {
		return err
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
	return c.verifySeal(header, parents)
}

// Search for a snapshot in memory or on disk for checkpoints
// snapshot retrieves the authorization snapshot at a given point in time.
func (c *CliqueVerifier) snapshot(number uint64, parents []*types.Header) (*Snapshot, error) {
	var snap *Snapshot

	sort.Slice(parents, func(i, j int) bool {
		return parents[i].Number.Cmp(parents[j].Number) == -1
	})

	var i int64
	for i = parents[len(parents)-1].Number.Int64(); i >= parents[0].Number.Int64(); i-- {
		p := parents[i]

		// If an in-memory snapshot was found, use that
		if s, ok := c.recents.Get(p.Hash()); ok {
			snap = s.(*Snapshot)
			break
		}

		// If an on-disk checkpoint snapshot can be found, use that
		if number%checkpointInterval == 0 {
			if s, err := loadSnapshot(c.config, c.signatures, c.db, p.Hash()); err == nil {
				log.Trace("Loaded voting snapshot from disk", "number", p.Number, "hash", p.Hash())
				snap = s
				break
			}
		}

		// If we're at the genesis, snapshot the initial state.
		if p.Number.Int64() == 0 {
			signers := make([]common.Address, (len(p.Extra)-extraVanity-extraSeal)/common.AddressLength)
			for i := 0; i < len(signers); i++ {
				copy(signers[i][:], p.Extra[extraVanity+i*common.AddressLength:])
			}

			snap = newSnapshot(c.config, c.signatures, number, p.Hash(), signers)
			if err := snap.store(c.db); err != nil {
				return nil, err
			}

			log.Info("Stored checkpoint snapshot to disk", "number", p.Number, "hash", p.Hash())
			break
		}
	}

	if snap == nil {
		return nil, consensus.ErrUnknownAncestor
	}

	parents = parents[:i]

	// headers should be in forward order
	snap, err := snap.apply(parents)
	if err != nil {
		return nil, err
	}

	c.recents.Add(snap.Hash, snap)

	// If we've generated a new checkpoint snapshot, save to disk
	if snap.Number%checkpointInterval == 0 && len(parents) > 0 {
		if err = snap.store(c.db); err != nil {
			return nil, err
		}

		log.Trace("Stored voting snapshot to disk", "number", snap.Number, "hash", snap.Hash)
	}

	return snap, err
}

// verifySeal checks whether the signature contained in the header satisfies the
// consensus protocol requirements. The method accepts an optional list of parent
// headers that aren't yet part of the local blockchain to generate the snapshots
// from.
func (c *CliqueVerifier) verifySeal(header *types.Header, parents []*types.Header) error {
	// Verifying the genesis block is not supported
	number := header.Number.Uint64()
	if number == 0 {
		return errUnknownBlock
	}

	// Retrieve the snapshot needed to verify this header and cache it
	snap, err := c.snapshot(number-1, parents)
	if err != nil {
		return err
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
