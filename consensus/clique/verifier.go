package clique

import (
	"bytes"
	"fmt"
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

type CliqueVerifier struct {
	*Clique
}

func NewCliqueVerifier(c *Clique) *CliqueVerifier {
	return &CliqueVerifier{c}
}

func (c *CliqueVerifier) Verify(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header, _ bool, _ bool) error {
	err := c.verifyHeader(header, parents, chain.Config())
	fmt.Println("XXX-Verify-DONE", header.Number.Uint64(), len(parents), err)
	return err
}

func (c *CliqueVerifier) NeededForVerification(header *types.Header) int {
	n := c.findPrevCheckpoint(header.Number.Uint64())
	fmt.Printf("-----NeededForVerification %d %d\n", header.Number.Uint64(), n)
	return n
}

// verifyHeader checks whether a header conforms to the consensus rules.The
// caller may optionally pass in a batch of parents (ascending order) to avoid
// looking those up from the database. This is useful for concurrently verifying
// a batch of new headers.
func (c *CliqueVerifier) verifyHeader(header *types.Header, parents []*types.Header, config *params.ChainConfig) error {
	fmt.Println("===verifyHeader-1", header.Number.Uint64(), len(parents))
	t := time.Now()
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
	fmt.Println("===verifyHeader-2", header.Number.Uint64(), time.Since(t))
	t = time.Now()
	// If all checks passed, validate any special fields for hard forks
	if err := misc.VerifyForkHashes(config, header, false); err != nil {
		return err
	}
	fmt.Println("===verifyHeader-3", header.Number.Uint64(), time.Since(t))
	t = time.Now()
	// All basic checks passed, verify cascading fields
	err := c.verifyCascadingFields(header, parents)
	fmt.Println("===verifyHeader-4", header.Number.Uint64(), time.Since(t), err)
	return err
}

// verifyCascadingFields verifies all the header fields that are not standalone,
// rather depend on a batch of previous headers. The caller may optionally pass
// in a batch of parents (ascending order) to avoid looking those up from the
// database. This is useful for concurrently verifying a batch of new headers.
func (c *CliqueVerifier) verifyCascadingFields(header *types.Header, parents []*types.Header) error {
	t := time.Now()
	// The genesis block is the always valid dead-end
	number := header.Number.Uint64()

	// Retrieve the snapshot needed to verify this header and cache it
	snap, err := c.snapshot(parents)
	if err != nil {
		return err
	}
	fmt.Println("verifyCascadingFields-1", time.Since(t), header.Number.Uint64())
	t = time.Now()

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
	fmt.Println("verifyCascadingFields-2", time.Since(t), header.Number.Uint64())
	t = time.Now()

	// All basic checks passed, verify the seal and return
	err = c.verifySeal(header, snap)
	fmt.Println("verifyCascadingFields-3", time.Since(t), header.Number.Uint64(), err)
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
func (c *CliqueVerifier) snapshot(parents []*types.Header) (*Snapshot, error) {
	fmt.Println("+++snapshot-0")
	var snap *Snapshot
	var err error

	t := time.Now()

	if !sort.SliceIsSorted(parents, headersSortFunc(parents)) {
		sort.Slice(parents, headersSortFunc(parents))
	}

	fmt.Println("+++snapshot-1", time.Since(t), "from", 0, "to", len(parents)-1)

	var i int
	for i = len(parents) - 1; i >= 0; i-- {
		p := parents[i]
		number := parents[i].Number.Uint64()

		// If an in-memory snapshot was found, use that
		t = time.Now()
		if s, ok := c.recents.Get(p.Hash()); ok {
			snap = s.(*Snapshot)
			fmt.Println("+++snapshot-2.1", time.Since(t), number)
			break
		}

		fmt.Println("+++snapshot-2.2", isSnapshot(number, c.config.Epoch), number)
		t = time.Now()
		// If an on-disk checkpoint snapshot can be found, use that
		if isSnapshot(number, c.config.Epoch) {
			fmt.Println("+++snapshot-2.3", number)
			//fmt.Printf("BEFORE2 loadAndFillSnapshot for block %d(%q)\n", p.Number.Uint64(), p.Hash().String())
			if s, err := loadAndFillSnapshot(c.db, p.Number.Uint64(), p.Hash(), c.config, c.signatures); err == nil {
				log.Trace("Loaded voting snapshot from disk", "number", p.Number, "hash", p.Hash())
				snap = s
				fmt.Println("+++snapshot-2.4", number)
				break
			} else {
				log.Info("can't load and update a snapshot", "num", number, "block", p.Number.Uint64(), "hash", p.Hash().String(), "error", err)
			}

			fmt.Println("+++snapshot-2.5", number)
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

	fmt.Println("+++snapshot-2.6", time.Since(t), parents[i].Number.Uint64())

	if snap == nil {
		return nil, consensus.ErrUnknownAncestor
	}

	fmt.Println("+++snapshot-3", parents[i].Number.Uint64())
	t = time.Now()

	if len(parents) > 0 {
		m := make(map[int64]struct{})
		for _, p := range parents {
			_, ok := m[p.Number.Int64()]
			if ok {
				//fmt.Println("snapshot-ERR: got duplicate on", p.Number.Int64())
			}
			m[p.Number.Int64()] = struct{}{}
		}
		//fmt.Println("snapshot1", parents[0].Number.Int64(), parents[len(parents)-1].Number.Int64(), len(parents), i)
	}

	//fmt.Printf("snapshotXXX-1: first parent %v; last parent %v; parents %v, found index %v, next after snap %v\n",
	//	parents[0].Number.Int64(), parents[len(parents)-1].Number.Int64(), len(parents), i, snap.Number+1)
	if i+1 <= len(parents) {
		parents = parents[i+1:]
	} else {
		parents = []*types.Header{}
	}

	fmt.Println("+++snapshot-4", time.Since(t), snap.Number)
	t = time.Now()

	err = snap.apply(parents)
	if err != nil {
		return nil, err
	}

	fmt.Println("+++snapshot-5", time.Since(t), snap.Number)
	t = time.Now()

	c.recents.Add(snap.Hash, snap)
	fmt.Println("+++snapshot-6", time.Since(t), snap.Number)
	t = time.Now()

	c.snapshotBlocks.Add(snap.Number, snap.Hash)
	fmt.Println("+++snapshot-7.1", time.Since(t), snap.Number, isSnapshot(snap.Number, c.config.Epoch), len(parents) > 0)
	t = time.Now()

	// If we've generated a new checkpoint snapshot, save to disk
	if isSnapshot(snap.Number, c.config.Epoch) {
		if err = snap.store(c.db, snap.Number == 0); err != nil {
			return nil, err
		}
		fmt.Println("+++snapshot-8", time.Since(t), snap.Number)

		log.Trace("Stored voting snapshot to disk", "number", snap.Number, "hash", snap.Hash)
	}

	return snap, err
}

func (c *CliqueVerifier) storeGenesisSnapshot(h *types.Header) (*Snapshot, error) {
	signers := make([]common.Address, (len(h.Extra)-extraVanity-extraSeal)/common.AddressLength)
	for i := 0; i < len(signers); i++ {
		copy(signers[i][:], h.Extra[extraVanity+i*common.AddressLength:])
	}

	snap := newSnapshot(c.config, c.signatures, h.Number.Uint64(), h.Hash(), signers)
	if err := snap.store(c.db, true); err != nil {
		return nil, err
	}

	return snap, nil
}

// verifySeal checks whether the signature contained in the header satisfies the
// consensus protocol requirements. The method accepts an optional list of parent
// headers that aren't yet part of the local blockchain to generate the snapshots
// from.
func (c *CliqueVerifier) verifySeal(header *types.Header, snap *Snapshot) error {
	// Verifying the genesis block is not supported
	number := header.Number.Uint64()
	if number == 0 {
		return errUnknownBlock
	}

	t := time.Now()

	// Resolve the authorization key and check against signers
	signer, err := ecrecover(header, c.signatures)
	if err != nil {
		return err
	}
	fmt.Println("verifySeal-1", time.Since(t), header.Number.Uint64())
	t = time.Now()

	if _, ok := snap.Signers[signer]; !ok {
		//fmt.Printf("errUnauthorizedSigner-4 for block %d(%q) - snap %d(%s): %v\n", number, signer.Hash().String(), snap.Number, snap.Hash.String(), spew.Sdump(snap.Signers))
		return errUnauthorizedSigner
	}

	for seen, recent := range snap.Recents {
		if recent == signer {
			// Signer is among recents, only fail if the current block doesn't shift it out
			if limit := uint64(len(snap.Signers)/2 + 1); seen > number-limit {
				//fmt.Println("recently signed3")
				return errRecentlySigned
			}
		}
	}
	fmt.Println("verifySeal-2", time.Since(t), header.Number.Uint64())
	t = time.Now()

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
	fmt.Println("verifySeal-3", time.Since(t), header.Number.Uint64())

	return nil
}

func (c *CliqueVerifier) findPrevCheckpoint(num uint64) int {
	// If we're at the genesis, snapshot the initial state.
	if num == 0 {
		fmt.Printf("NeededForVerification-0 num=%d\n", num)
		return 0
	}

	var n int
	for n = int(num); n >= 0; n-- {
		// fixme add hash
		if ok := c.checkSnapshot(uint64(n)); ok {
			fmt.Printf("NeededForVerification-0.1 GOT a SNAP num=%d\n", num)
			break
		}
	}

	if n < 0 {
		fmt.Printf("NeededForVerification-0.2 NEGATIVE num=%d n=%d\n", num, n)
		n = 0
	}

	ancestors := int(num) - n
	if ancestors <= 0 {
		ancestors = 1 // we need at least 1 parent for verification
	}

	fmt.Printf("NeededForVerification-1 num=%d n=%d ancestors=%d\n", num, n, ancestors)

	return ancestors
}

func (c *CliqueVerifier) checkSnapshot(num uint64) bool {
	//fmt.Println("====checkSnapshot-0", num)
	var (
		ok       bool
		snapHash common.Hash
		err      error
	)

	if h, ok := c.snapshotBlocks.Get(num); ok {
		//fmt.Println("====checkSnapshot-1", num)
		snapHash, ok = h.(common.Hash)
		if ok {
			// If an in-memory snapshot was found, use that
			_, ok = c.recents.Get(snapHash)
			if ok {
				//fmt.Println("====checkSnapshot-1.0", num)
				fmt.Printf("checkSnapshot for %d for snap in memory\n", num)
				return true
			}

			// If an on-disk checkpoint snapshot can be found, use that
			ok, err = hasSnapshotData(c.db, num, snapHash)
			if err != nil {
				//fmt.Println("====checkSnapshot-1.1", num)
				ok = false
			}

			if ok {
				fmt.Printf("checkSnapshot for %d for snap in db %v %v\n", num, ok, err)
				//fmt.Println("====checkSnapshot-1.2", num)
				return true
			}
		}
	}
	//fmt.Println("====checkSnapshot-2")

	if !ok {
		//fmt.Println("====checkSnapshot-3")
		c.db.Walk(dbutils.CliqueBucket, dbutils.EncodeBlockNumber(num), 8*8, func(k, v []byte) (bool, error) {
			//fmt.Println("====checkSnapshot-3.1")
			number, hash, err := dbutils.DecodeBlockBodyKey(k)
			fmt.Println("=====", number, hash, err)
			return true, nil
		})
	}

	// If an on-disk checkpoint snapshot can be found, use that
	// fixme remove
	ok, err = hasSnapshotByBlock(c.db, num)
	if ok {
		//fmt.Println("====checkSnapshot-4")
		fmt.Printf("checkSnapshot for %d for snap in db %v %v\n", num, ok, err)
	}
	//fmt.Println("====checkSnapshot-5")
	return ok && err == nil
}

func isSnapshot(number uint64, epoch uint64) bool {
	return number == 0 || number%checkpointInterval == 0 || number%epoch == 0
}
