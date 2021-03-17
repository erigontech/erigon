package clique

import (
	"bytes"
	"fmt"
	"math/rand"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
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
	t := time.Now()
	defer func() {
		debugLog("in Verifier.verifyByRequest", header.Number.Uint64(), time.Since(t))
	}()

	types.SortHeadersAsc(parents)

	snapID := rand.Uint64()

	return c.verifyHeader(header, parents, chain.Config(), snapID)
}

func (c *Verifier) AncestorsNeededForVerification(header *types.Header) int {
	debugLog("AncestorsNeededForVerification")
	return c.findPrevCheckpoint(header.Number.Uint64(), header.Hash(), header.ParentHash, rand.Uint64())
}

// verifyHeader checks whether a header conforms to the consensus rules.The
// caller may optionally pass in a batch of parents (ascending order) to avoid
// looking those up from the database. This is useful for concurrently verifying
// a batch of new headers.
func (c *Verifier) verifyHeader(header *types.Header, parents []*types.Header, config *params.ChainConfig, snapID uint64) error {
	t := time.Now()
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

	debugLog("verifyHeader-1", header.Number.Uint64(), time.Since(t))
	t = time.Now()

	// If all checks passed, validate any special fields for hard forks
	if err := misc.VerifyForkHashes(config, header, false); err != nil {
		return err
	}
	debugLog("verifyHeader-1-VerifyForkHashes", header.Number.Uint64(), time.Since(t))
	t = time.Now()

	// All basic checks passed, verify cascading fields
	err := c.verifyCascadingFields(header, parents, snapID)
	debugLog("verifyHeader-2-verifyCascadingFields", header.Number.Uint64(), time.Since(t))
	return err
}

// verifyCascadingFields verifies all the header fields that are not standalone,
// rather depend on a batch of previous headers. The caller may optionally pass
// in a batch of parents (ascending order) to avoid looking those up from the
// database. This is useful for concurrently verifying a batch of new headers.
func (c *Verifier) verifyCascadingFields(header *types.Header, parents []*types.Header, snapID uint64) error {
	// The genesis block is the always valid dead-end
	number := header.Number.Uint64()

	t := time.Now()
	// Retrieve the snapshot needed to verify this header and cache it
	snap, err := c.snapshot(&parents, snapID)
	if len(parents) > 0 && snap != nil {
		debugLogf("verifyCascadingFields-0.1-snapshot snapID=%d header=%d from=%d to=%d parentsLen=%d snapNum=%d %s err=%v\n",
			snapID, header.Number, parents[0].Number.Uint64(), parents[len(parents)-1].Number.Uint64(), len(parents), snap.Number, len(parents), err)
	}
	if len(parents) == 0 && snap != nil {
		debugLogf("verifyCascadingFields-0.2-snapshot snapID=%d header=%d parentsLen=%d snapNum=%d %s err=%v\n",
			snapID, header.Number, len(parents), snap.Number, len(parents), err)
	}
	if snap == nil {
		debugLogf("verifyCascadingFields-0.3-snapshot snapID=%d header=%d from=%d to=%d parentsLen=%d snapNIL=%d %s err=%v\n",
			snapID, header.Number, parents[0].Number.Uint64(), parents[len(parents)-1].Number.Uint64(), len(parents), snap == nil, len(parents), err)
	}

	if err != nil {
		return err
	}
	debugLog("verifyCascadingFields-1-snapshot", snapID, time.Since(t))

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

	if highestParentNum == -1 || parent.Number.Uint64() != number-1 || parent.HashCache() != header.ParentHash {
		if parent.Number != nil {
			debugLogf("verifyCascadingFields-2-snapshot snapID=%d highestParentNum=%v parent.Number=%v parent.HashCacheCMP=%v parent.HashCache=%v %s\n",
				snapID,
				highestParentNum == -1,
				parent.Number.Uint64() != number-1,
				parent.HashCache() != header.ParentHash,
				parent.HashCache().String(),
				len(parents))
		} else {
			debugLogf("verifyCascadingFields-2-snapshot snapID=%d highestParentNum=%v parentr=%v %s\n",
				snapID,
				highestParentNum == -1,
				parent == nil,
				len(parents))
		}
		return fmt.Errorf("verifyCascadingFields %d num=%d err=%w %v", snapID, header.Number.Uint64(), consensus.ErrUnknownAncestor, len(parents))
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

	debugLog("verifyCascadingFields-2", snapID, time.Since(t))
	t = time.Now()

	// All basic checks passed, verify the seal and return
	err = c.verifySeal(header, snap)
	debugLog("verifyCascadingFields-3-verifySeal", snapID, time.Since(t))
	t = time.Now()

	if err == nil {
		debugLog("applyAndStoreSnapshot-3.1-START", snapID, "snap=", snap.Number, header.Number, len(parents))
		if err = c.applyAndStoreSnapshot(snap, snapID, true, header); err != nil {
			debugLog("applyAndStoreSnapshot-3.1-ERR", snapID, "snap=", snap.Number, header.Number, err)

			log.Error("can't store a snapshot", "block", snapID, header.Number.Uint64(), "hash", header.HashCache().String(), "err", err, len(parents))
		}
		debugLog("applyAndStoreSnapshot-3.1-END", snapID, "snap=", snap.Number, header.Number)
	}
	debugLog("verifyCascadingFields-4-applyAndStoreSnapshot", snapID, time.Since(t))

	return err
}

// Search for a snapshot in memory or on disk for checkpoints
// snapshot retrieves the authorization snapshot at a given point in time.
func (c *Verifier) snapshot(parentsRef *[]*types.Header, snapID uint64) (*Snapshot, error) {
	var snap *Snapshot
	var err error

	parents := *parentsRef

	t := time.Now()
	debugLog("snapshot-1", snapID, parents[0].Number.Uint64(), parents[len(parents)-1].Number.Uint64(), time.Since(t))
	t = time.Now()

	var (
		i int
		s *Snapshot
		p *types.Header
	)

	debugLog("snap.0", snapID, parents[len(parents)-1].Number.Uint64(), parents[0].Number.Uint64(), len(parents))

	for i = len(parents) - 1; i >= 0; i-- {

		debugLog("snap.1", snapID, i, parents[i].Number.Uint64())

		p = parents[i]
		number := parents[i].Number.Uint64()

		// If an in-memory snapshot was found, use that
		var ok bool
		if s, ok = c.recentsGet(p.HashCache()); ok {
			snap = s
			debugLog("snap-break-1", snapID, i, snap.Number, p.Number.Uint64())
			break
		}

		// If an on-disk checkpoint snapshot can be found, use that
		if isSnapshot(number, c.config.Epoch, c.snapshotConfig.CheckpointInterval) {
			if s, err = loadAndFillSnapshot(c.db, p.Number.Uint64(), p.HashCache(), c.config, c.snapStorage); err == nil {
				log.Trace("Loaded voting snapshot from disk", "number", p.Number, "hash", p.HashCache())
				snap = s
				debugLog("snap-break-2", snapID, i, snap.Number, p.Number.Uint64())
				break
			} else {
				log.Trace("can't load and update a snapshot", "num", number, "block", p.Number.Uint64(), "hash", p.HashCache().String(), "error", err)
			}
		}

		// If we're at the genesis, snapshot the initial state.
		if p.Number.Int64() == 0 {
			snap, err = c.storeGenesisSnapshot(p, snapID)
			if err != nil {
				debugLog("snap-break-3-error!!!!", snapID, i, p.Number.Uint64(), err)
				return nil, err
			}

			log.Info("Stored genesis checkpoint snapshot to disk", "number", p.Number, "hash", p.HashCache())
			debugLog("snap-break-4", snapID, i, snap.Number, p.Number.Uint64())
			break
		}
	}
	debugLog("snapshot-2", snapID, time.Since(t))
	t = time.Now()

	if snap == nil {
		return nil, fmt.Errorf("a nil snap for %d block: %w", parents[i].Number.Uint64(), consensus.ErrUnknownAncestor)
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

	if len(parents) > 0 {
		debugLogf("applyAndStoreSnapshot-PRE-slice snapID=%d snapNum=%d partntsLen=%d i=%d fromNum=%d toNum=%d %d\n",
			snapID, snap.Number, len(parents), i, parents[0].Number.Uint64(), parents[len(parents)-1].Number.Uint64(), len(parents))
	}

	//snapID=6399527266456256611 snapNum=0 partntsLen=1 i=0 fromNum=0 toNum=0 "parents: '0 '"
	// 0 1 2 3 4
	//     i
	// we always need at least 1 parent for further validation
	if i != len(parents)-1 {
		if i+1 <= len(parents)+1 {
			debugLog("applyAndStoreSnapshot-slice-case-1", snapID, snap.Number, len(parents), i, len(parents))
			parents = parents[i+1:]
		} else {
			debugLog("applyAndStoreSnapshot-slice-case-2", snapID, snap.Number, len(parents), i, len(parents))
			parents = []*types.Header{}
		}
	} else {
		parents = parents[i:]
	}
	*parentsRef = parents

	if len(parents) > 0 {
		debugLogf("applyAndStoreSnapshot-POST-1-slice snapID=%d snapNum=%d partntsLen=%d i=%d fromNum=%d toNum=%d %d\n",
			snapID, snap.Number, len(parents), i, parents[0].Number.Uint64(), parents[len(parents)-1].Number.Uint64(), len(parents))
	}
	debugLogf("applyAndStoreSnapshot-POST-2-slice snapID=%d snapNum=%d partntsLen=%d i=%d %d\n",
		snapID, snap.Number, len(parents), i, len(parents))

	debugLog("snapshot-3", snapID, time.Since(t))

	debugLog("GOT-3!!!", snapID, snap.Number)
	t = time.Now()

	debugLog("applyAndStoreSnapshot-1-START", snapID, snap.Number, len(parents), len(parents))
	err = c.applyAndStoreSnapshot(snap, snapID, true, parents...)
	if err != nil {
		debugLog("applyAndStoreSnapshot-1-END", snapID, snap.Number, len(parents), len(parents))
		return nil, err
	}
	debugLog("applyAndStoreSnapshot-1-SUCC", snapID, snap.Number, len(parents), len(parents))
	debugLog("snapshot-4-applyAndStoreSnapshot", snapID, snap.Number, len(parents), time.Since(t))

	return snap, nil
}

func (c *Verifier) storeGenesisSnapshot(h *types.Header, snapID uint64) (*Snapshot, error) {
	signers := make([]common.Address, (len(h.Extra)-extraVanity-extraSeal)/common.AddressLength)
	for i := 0; i < len(signers); i++ {
		copy(signers[i][:], h.Extra[extraVanity+i*common.AddressLength:])
	}

	snap := newSnapshot(c.config, c.snapStorage, h.Number.Uint64(), h.HashCache(), signers)
	debugLog("applyAndStoreSnapshot-2-START", snapID, snap.Number)
	if err := c.applyAndStoreSnapshot(snap, snapID, false); err != nil {
		debugLog("applyAndStoreSnapshot-2-END", snapID, snap.Number)
		return nil, err
	}

	debugLog("GOT!!!-2", snapID, snap.Number)

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
	signer, err := c.recoverSig.ecrecover(header)
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

func (c *Verifier) findPrevCheckpoint(num uint64, hash common.Hash, parentHash common.Hash, snapID uint64) int {
	debugLog("findPrevCheckpoint")
	if num == 0 {
		// If we're at the genesis, snapshot the initial state.
		return 0
	}

	var (
		n       int
		ok      bool
		highest = num
	)

	/*
	n = sort.Search(int(highest)+1, func(blockNum int) bool {
		_, ok = c.recentsNum.Get(uint64(blockNum))
		debugLog("lastSnapshot-555", c.recents.Len())
		return ok
	})
	*/
	for n = int(highest); n >= 0; n-- {
		_, ok = c.recentsNum.Get(uint64(n))
		if ok {
			break
		}
	}
	debugLog("lastSnapshot-777", n, ok)

	if !ok {
		last, err := lastSnapshot(c.db)
		debugLog("lastSnapshot-999", last, err)
		if last < num {
			highest = last
		}

		/*
			n = sort.Search(int(highest)+1, func(blockNum int) bool {
				return !c.checkClosestSnapshot(uint64(blockNum), hash, parentHash, num, snapID)
			})
		*/

		/*
		n = Search(int(highest)+1, func(currentN int) bool {
			return !c.checkClosestSnapshot(uint64(currentN), hash, parentHash, num, snapID)
		})
		 */

		for n = int(highest); n >= 0; n-- {
			ok = c.checkClosestSnapshot(uint64(n), hash, parentHash, num, snapID)
			if ok {
				break
			}
		}

		if n <= int(highest) {
			ok = true
			debugLog("xxx-1", n, last)
		} else {
			n = 0
			debugLog("xxx-2", n, last)
		}
	}

	if n < 0 {
		n = 0
	}

	ancestors := int(num) - n
	debugLog("ancestors", ancestors)

	if ancestors <= 0 {
		ancestors = 1 // we need at least 1 parent for verification
	}

	return ancestors
}

func Search(length int, f func(int) bool) int {
	// Define f(-1) == false and f(n) == true.
	// Invariant: f(i-1) == false, f(j) == true.
	i, j := 0, length
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		debugLog("xxx-0", h)
		if !f(h) {
			i = h + 1 // preserves f(i-1) == false
		} else {
			j = h // preserves f(j) == true
		}
	}
	// i == j, f(i-1) == false, and f(j) (= f(i)) == true  =>  answer is i.
	return i
}

func (c *Verifier) checkClosestSnapshot(num uint64, hash common.Hash, parentHash common.Hash, maxNum uint64, snapID uint64) bool {
	debugLog("findPrevCheckpoint-1")
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

	return c.checkSnapshot(num, snapshotHash, snapID)
}

func isSnapshot(number uint64, epoch, checkpointInterval uint64) bool {
	return number == 0 || number%checkpointInterval == 0 || number%epoch == 0
}

//debug
func parentsToString(parents []*types.Header) string {
	parStr := "parents: '"
	for _, par := range parents {
		parStr += fmt.Sprintf("%d ", par.Number.Uint64())
	}

	return parStr + "'"
}
