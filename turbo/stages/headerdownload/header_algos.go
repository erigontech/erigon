package headerdownload

import (
	"bytes"
	"compress/gzip"
	"container/heap"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/interfaces"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/params/networkname"
	"github.com/ledgerwatch/erigon/rlp"
)

// Implements sort.Interface so we can sort the incoming header in the message by block height
type HeadersByHeightAndHash []ChainSegmentHeader

func (h HeadersByHeightAndHash) Len() int {
	return len(h)
}

func (h HeadersByHeightAndHash) Less(i, j int) bool {
	// Note - the ordering is the inverse ordering of the block heights
	if h[i].Number != h[j].Number {
		return h[i].Number > h[j].Number
	}
	return bytes.Compare(h[i].Hash[:], h[j].Hash[:]) > 0
}

func (h HeadersByHeightAndHash) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

// SplitIntoSegments converts message containing headers into a collection of chain segments
func (hd *HeaderDownload) SplitIntoSegments(csHeaders []ChainSegmentHeader) ([]ChainSegment, Penalty, error) {
	hd.lock.RLock()
	defer hd.lock.RUnlock()
	sort.Sort(HeadersByHeightAndHash(csHeaders))
	// Now all headers are order from the highest block height to the lowest
	var segments []ChainSegment                          // Segments being built
	segmentMap := make(map[common.Hash]int)              // Mapping of the header hash to the index of the chain segment it belongs
	childrenMap := make(map[common.Hash][]*types.Header) // Mapping parent hash to the children

	number := uint64(math.MaxUint64)
	var hash common.Hash
	for _, h := range csHeaders {
		// Headers are sorted by number, then by hash, so any dups will be consecutive
		if h.Number == number && h.Hash == hash {
			return nil, DuplicateHeaderPenalty, nil
		}
		number = h.Number
		hash = h.Hash

		if _, bad := hd.badHeaders[hash]; bad {
			return nil, BadBlockPenalty, nil
		}
		var segmentIdx int
		children := childrenMap[hash]
		for _, child := range children {
			if valid, penalty := hd.childParentValid(child, h.Header); !valid {
				return nil, penalty, nil
			}
		}
		if len(children) == 1 {
			// Single child, extract segmentIdx
			segmentIdx = segmentMap[hash]
		} else {
			// No children, or more than one child, create new segment
			segmentIdx = len(segments)
			segments = append(segments, ChainSegment{})
		}
		segments[segmentIdx] = append(segments[segmentIdx], h)
		segmentMap[h.Header.ParentHash] = segmentIdx
		siblings := childrenMap[h.Header.ParentHash]
		siblings = append(siblings, h.Header)
		childrenMap[h.Header.ParentHash] = siblings
	}
	return segments, NoPenalty, nil
}

// Checks whether child-parent relationship between two headers is correct
// (excluding Proof Of Work validity)
func (hd *HeaderDownload) childParentValid(child, parent *types.Header) (bool, Penalty) {
	if parent.Number.Uint64()+1 != child.Number.Uint64() {
		return false, WrongChildBlockHeightPenalty
	}
	return true, NoPenalty
}

// SingleHeaderAsSegment converts message containing 1 header into one singleton chain segment
func (hd *HeaderDownload) SingleHeaderAsSegment(headerRaw []byte, header *types.Header) ([]ChainSegment, Penalty, error) {
	hd.lock.RLock()
	defer hd.lock.RUnlock()

	headerHash := types.RawRlpHash(headerRaw)
	if _, bad := hd.badHeaders[headerHash]; bad {
		return nil, BadBlockPenalty, nil
	}
	h := ChainSegmentHeader{
		Header:    header,
		HeaderRaw: headerRaw,
		Hash:      headerHash,
		Number:    header.Number.Uint64(),
	}
	return []ChainSegment{{h}}, NoPenalty, nil
}

// ReportBadHeader -
func (hd *HeaderDownload) ReportBadHeader(headerHash common.Hash) {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	hd.badHeaders[headerHash] = struct{}{}
	// Find the link, remove it and all its descendands from all the queues
	if link, ok := hd.links[headerHash]; ok {
		removeList := []*Link{link}
		for len(removeList) > 0 {
			removal := removeList[len(removeList)-1]
			hd.moveLinkToQueue(removal, NoQueue)
			delete(hd.links, removal.hash)
			removeList = append(removeList[:len(removeList)-1], removal.next...)
		}
	}
}

func (hd *HeaderDownload) IsBadHeader(headerHash common.Hash) bool {
	hd.lock.RLock()
	defer hd.lock.RUnlock()
	_, ok := hd.badHeaders[headerHash]
	return ok
}

// findAnchor attempts to find anchor to which given chain segment can be attached to
func (hd *HeaderDownload) findAnchor(segment ChainSegment) (found bool, anchor *Anchor, start int) {
	// Walk the segment from children towards parents
	for i, h := range segment {
		// Check if the header can be attached to an anchor of a working tree
		if anchor, attaching := hd.anchors[h.Hash]; attaching {
			return true, anchor, i
		}
	}
	return false, nil, 0
}

// FindLink attempts to find a non-persisted link that given chain segment can be attached to.
func (hd *HeaderDownload) findLink(segment ChainSegment, start int) (found bool, link *Link, end int) {
	if _, duplicate := hd.getLink(segment[start].Hash); duplicate {
		return false, nil, 0
	}
	// Walk the segment from children towards parents
	for i, h := range segment[start:] {
		// Check if the header can be attached to any links
		if link, attaching := hd.getLink(h.Header.ParentHash); attaching {
			return true, link, start + i + 1
		}
	}
	return false, nil, len(segment)
}

func (hd *HeaderDownload) removeUpwards(toRemove []*Link) {
	for len(toRemove) > 0 {
		removal := toRemove[len(toRemove)-1]
		toRemove = toRemove[:len(toRemove)-1]
		delete(hd.links, removal.hash)
		hd.moveLinkToQueue(removal, NoQueue)
		toRemove = append(toRemove, removal.next...)
	}
}

func (hd *HeaderDownload) MarkPreverified(link *Link) {
	// Go through all parent links that are not preverified and mark them too
	for link != nil && !link.persisted {
		if !link.verified {
			link.verified = true
			hd.moveLinkToQueue(link, InsertQueueID)
		}
		link = hd.links[link.header.ParentHash]
	}
}

func (hd *HeaderDownload) MarkAllPreverified() {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	for hd.verifyQueue.Len() > 0 {
		link := hd.verifyQueue[0]
		if !link.verified {
			link.verified = true
			hd.moveLinkToQueue(link, InsertQueueID)
		} else {
			panic("verified link in verifyQueue")
		}
	}
	for hd.linkQueue.Len() > 0 {
		link := hd.linkQueue[0]
		if !link.verified {
			link.verified = true
			hd.moveLinkToQueue(link, InsertQueueID)
		} else {
			panic("verified link in linkQueue")
		}
	}
}

// ExtendUp extends a working tree up from the link, using given chain segment
func (hd *HeaderDownload) extendUp(segment ChainSegment, attachmentLink *Link) {
	// Iterate over headers backwards (from parents towards children)
	prevLink := attachmentLink
	for i := len(segment) - 1; i >= 0; i-- {
		link := hd.addHeaderAsLink(segment[i], false /* persisted */)
		if prevLink.persisted {
			// If we are attching to already persisted link, schedule for insertion (persistence)
			if link.verified {
				hd.moveLinkToQueue(link, InsertQueueID)
			} else {
				hd.moveLinkToQueue(link, VerifyQueueID)
			}
		}
		prevLink.next = append(prevLink.next, link)
		prevLink = link
		if _, ok := hd.preverifiedHashes[link.hash]; ok {
			hd.MarkPreverified(link)
		}
	}
}

// ExtendDown extends some working trees down from the anchor, using given chain segment
// it creates a new anchor and collects all the links from the attached anchors to it
func (hd *HeaderDownload) extendDown(segment ChainSegment, anchor *Anchor) bool {
	// Find attachment anchor again

	anchorPreverified := false
	for _, link := range anchor.links {
		if link.verified {
			anchorPreverified = true
			break
		}
	}
	newAnchorH := segment[len(segment)-1]
	newAnchorHeader := newAnchorH.Header
	var newAnchor *Anchor
	newAnchor, preExisting := hd.anchors[newAnchorHeader.ParentHash]
	if !preExisting {
		newAnchor = &Anchor{
			parentHash:    newAnchorHeader.ParentHash,
			nextRetryTime: 0, // Will ensure this anchor will be top priority
			peerID:        anchor.peerID,
			blockHeight:   newAnchorH.Number,
		}
		if newAnchor.blockHeight > 0 {
			hd.anchors[newAnchorHeader.ParentHash] = newAnchor
			heap.Push(hd.anchorQueue, newAnchor)
		}
	}
	hd.removeAnchor(anchor)
	// Add all headers in the segments as links to this anchor
	var prevLink *Link
	for i := len(segment) - 1; i >= 0; i-- {
		link := hd.addHeaderAsLink(segment[i], false /* pesisted */)
		if prevLink == nil {
			newAnchor.links = append(newAnchor.links, link)
		} else {
			prevLink.next = append(prevLink.next, link)
		}
		prevLink = link
		if _, ok := hd.preverifiedHashes[link.hash]; ok {
			hd.MarkPreverified(link)
		}
	}
	prevLink.next = anchor.links
	anchor.links = nil
	if anchorPreverified {
		// Mark the entire segment as preverified
		hd.MarkPreverified(prevLink)
	}
	return !preExisting
}

// Connect connects some working trees using anchors of some, and a link of another
func (hd *HeaderDownload) connect(segment ChainSegment, attachmentLink *Link, anchor *Anchor) []PenaltyItem {
	anchorPreverified := false
	for _, link := range anchor.links {
		if link.verified {
			anchorPreverified = true
			break
		}
	}
	hd.removeAnchor(anchor)
	// Iterate over headers backwards (from parents towards children)
	prevLink := attachmentLink
	for i := len(segment) - 1; i >= 0; i-- {
		link := hd.addHeaderAsLink(segment[i], false /* persisted */)
		// If we attach to already persisted link, mark this one for insertion
		if prevLink.persisted {
			if link.verified {
				hd.moveLinkToQueue(link, InsertQueueID)
			} else {
				hd.moveLinkToQueue(link, VerifyQueueID)
			}
		}
		prevLink.next = append(prevLink.next, link)
		prevLink = link
		if _, ok := hd.preverifiedHashes[link.hash]; ok {
			hd.MarkPreverified(link)
		}
	}
	prevLink.next = anchor.links
	anchor.links = nil
	if anchorPreverified {
		// Mark the entire segment as preverified
		hd.MarkPreverified(prevLink)
	}
	var penalties []PenaltyItem
	if _, bad := hd.badHeaders[attachmentLink.hash]; bad {
		hd.invalidateAnchor(anchor, "descendant of a known bad block")
		penalties = append(penalties, PenaltyItem{Penalty: AbandonedAnchorPenalty, PeerID: anchor.peerID})
	}
	return penalties
}

func (hd *HeaderDownload) removeAnchor(anchor *Anchor) {
	// Anchor is removed from the map, and from the priority queue
	delete(hd.anchors, anchor.parentHash)
	heap.Remove(hd.anchorQueue, anchor.idx)
	anchor.idx = -1
}

// if anchor will be abandoned - given peerID will get Penalty
func (hd *HeaderDownload) newAnchor(segment ChainSegment, peerID enode.ID) bool {
	anchorH := segment[len(segment)-1]
	anchorHeader := anchorH.Header

	var anchor *Anchor
	anchor, preExisting := hd.anchors[anchorHeader.ParentHash]
	if !preExisting {
		if anchorH.Number < hd.highestInDb {
			log.Debug(fmt.Sprintf("new anchor too far in the past: %d, latest header in db: %d", anchorH.Number, hd.highestInDb))
			return false
		}
		if len(hd.anchors) >= hd.anchorLimit {
			log.Debug(fmt.Sprintf("too many anchors: %d, limit %d", len(hd.anchors), hd.anchorLimit))
			return false
		}
		anchor = &Anchor{
			parentHash:    anchorHeader.ParentHash,
			peerID:        peerID,
			nextRetryTime: 0, // Will ensure this anchor will be top priority
			blockHeight:   anchorH.Number,
		}
		hd.anchors[anchorHeader.ParentHash] = anchor
		heap.Push(hd.anchorQueue, anchor)
	}
	// Iterate over headers backwards (from parents towards children)
	var prevLink *Link
	for i := len(segment) - 1; i >= 0; i-- {
		link := hd.addHeaderAsLink(segment[i], false /* persisted */)
		if prevLink == nil {
			anchor.links = append(anchor.links, link)
		} else {
			prevLink.next = append(prevLink.next, link)
		}
		prevLink = link
		if _, ok := hd.preverifiedHashes[link.hash]; ok {
			hd.MarkPreverified(link)
		}
	}
	return !preExisting
}

func (hd *HeaderDownload) pruneLinkQueue() {
	for hd.linkQueue.Len() > hd.linkLimit {
		link := heap.Pop(&hd.linkQueue).(*Link)
		delete(hd.links, link.hash)
		if parentLink, ok := hd.links[link.header.ParentHash]; ok {
			for i, n := range parentLink.next {
				if n == link {
					if i == len(parentLink.next)-1 {
						parentLink.next = parentLink.next[:i]
					} else {
						parentLink.next = append(parentLink.next[:i], parentLink.next[i+1:]...)
					}
					break
				}
			}
		}
		if anchor, ok := hd.anchors[link.header.ParentHash]; ok {
			for i, n := range anchor.links {
				if n == link {
					if i == len(anchor.links)-1 {
						anchor.links = anchor.links[:i]
					} else {
						anchor.links = append(anchor.links[:i], anchor.links[i+1:]...)
					}
					break
				}
			}
		}
	}
}

func (hd *HeaderDownload) AnchorState() string {
	hd.lock.RLock()
	defer hd.lock.RUnlock()
	return hd.anchorState()
}

func (hd *HeaderDownload) anchorState() string {
	//nolint:prealloc
	var ss []string
	for anchorParent, anchor := range hd.anchors {
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("{%8d", anchor.blockHeight))
		// Try to figure out end
		var end uint64
		var searchList []*Link
		searchList = append(searchList, anchor.links...)
		var bs []int
		for len(searchList) > 0 {
			link := searchList[len(searchList)-1]
			if link.blockHeight > end {
				end = link.blockHeight
			}
			searchList = searchList[:len(searchList)-1]
			if len(link.next) > 0 {
				searchList = append(searchList, link.next...)
			}
			bs = append(bs, int(link.blockHeight))
		}
		var sbb strings.Builder
		sort.Ints(bs)
		for j, b := range bs {
			if j == 0 {
				sbb.WriteString(fmt.Sprintf("%d", b))
			} else if j == len(bs)-1 {
				if bs[j-1]+1 == b {
					// Close interval
					sbb.WriteString(fmt.Sprintf("-%d", b))
				} else {
					// Standalone
					sbb.WriteString(fmt.Sprintf(" %d", b))
				}
			} else {
				if bs[j-1] == b {
					// Skip
				} else if bs[j-1]+1 == b {
					if b+1 == bs[j+1] {
						// Skip
					} else {
						// Close interval
						sbb.WriteString(fmt.Sprintf("-%d", b))
					}
				} else {
					// Open interval or standalone
					sbb.WriteString(fmt.Sprintf(" %d", b))
				}
			}
		}
		sb.WriteString(fmt.Sprintf("-%d links=%d (%s)}", end, len(bs), sbb.String()))
		sb.WriteString(fmt.Sprintf(" => %x", anchorParent))
		ss = append(ss, sb.String())
	}
	sort.Strings(ss)
	return strings.Join(ss, "\n")
}

func InitPreverifiedHashes(chain string) (map[common.Hash]struct{}, uint64) {
	var encodings []string
	var height uint64
	switch chain {
	case networkname.MainnetChainName:
		encodings = mainnetPreverifiedHashes
		height = mainnetPreverifiedHeight
	case networkname.RopstenChainName:
		encodings = ropstenPreverifiedHashes
		height = ropstenPreverifiedHeight
	default:
		log.Debug("Preverified hashes not found for", "chain", chain)
		return nil, 0
	}
	return DecodeHashes(encodings), height
}

func DecodeHashes(encodings []string) map[common.Hash]struct{} {
	hashes := make(map[common.Hash]struct{}, len(encodings))

	for _, encoding := range encodings {
		hashes[common.HexToHash(encoding)] = struct{}{}
	}

	return hashes
}

func (hd *HeaderDownload) SetPreverifiedHashes(preverifiedHashes map[common.Hash]struct{}, preverifiedHeight uint64) {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	hd.preverifiedHashes = preverifiedHashes
	hd.preverifiedHeight = preverifiedHeight
}

func (hd *HeaderDownload) RecoverFromDb(db kv.RoDB) error {
	hd.lock.Lock()
	defer hd.lock.Unlock()

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	// Drain persistedLinksQueue and remove links
	for hd.persistedLinkQueue.Len() > 0 {
		link := heap.Pop(&hd.persistedLinkQueue).(*Link)
		delete(hd.links, link.hash)
	}
	err := db.View(context.Background(), func(tx kv.Tx) error {
		c, err := tx.Cursor(kv.Headers)
		if err != nil {
			return err
		}
		// Take hd.persistedLinkLimit headers (with the highest heights) as links
		for k, v, err := c.Last(); k != nil && hd.persistedLinkQueue.Len() < hd.persistedLinkLimit; k, v, err = c.Prev() {
			if err != nil {
				return err
			}
			var header types.Header
			if err = rlp.DecodeBytes(v, &header); err != nil {
				return err
			}
			h := ChainSegmentHeader{
				HeaderRaw: v,
				Header:    &header,
				Hash:      types.RawRlpHash(v),
				Number:    header.Number.Uint64(),
			}
			hd.addHeaderAsLink(h, true /* persisted */)

			select {
			case <-logEvery.C:
				log.Info("recover headers from db", "left", hd.persistedLinkLimit-hd.persistedLinkQueue.Len())
			default:
			}
		}
		hd.highestInDb, err = stages.GetStageProgress(tx, stages.Headers)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// ReadProgressFromDb updates highestInDb field according to the information
// in the database. It is useful in the situations when transaction was
// aborted and highestInDb became out-of-sync
func (hd *HeaderDownload) ReadProgressFromDb(tx kv.RwTx) (err error) {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	hd.highestInDb, err = stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return err
	}
	return nil
}

func (hd *HeaderDownload) invalidateAnchor(anchor *Anchor, reason string) {
	log.Warn("Invalidating anchor", "height", anchor.blockHeight, "hash", anchor.parentHash, "reason", reason)
	hd.removeAnchor(anchor)
	hd.removeUpwards(anchor.links)
}

func (hd *HeaderDownload) RequestMoreHeaders(currentTime uint64) (*HeaderRequest, []PenaltyItem) {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	var penalties []PenaltyItem
	if hd.anchorQueue.Len() == 0 {
		log.Trace("Empty anchor queue")
		return nil, penalties
	}
	for hd.anchorQueue.Len() > 0 {
		anchor := (*hd.anchorQueue)[0]
		// Only process the anchors for which the nextRetryTime has already come
		if anchor.nextRetryTime > currentTime {
			return nil, penalties
		}
		if anchor.timeouts < 10 {
			// Produce a header request that would extend this anchor (add parent, parent of parent, etc.)
			return &HeaderRequest{
				Anchor:  anchor,
				Hash:    anchor.parentHash,
				Number:  anchor.blockHeight - 1,
				Length:  192,
				Skip:    0,
				Reverse: true,
			}, penalties
		}
		// Ancestors of this anchor seem to be unavailable, invalidate and move on
		hd.invalidateAnchor(anchor, "suspected unavailability")
		penalties = append(penalties, PenaltyItem{Penalty: AbandonedAnchorPenalty, PeerID: anchor.peerID})
	}
	return nil, penalties
}

func (hd *HeaderDownload) RequestMoreHeadersForPOS() HeaderRequest {
	hd.lock.RLock()
	defer hd.lock.RUnlock()
	// Assemble the request
	return HeaderRequest{
		Hash:    hd.hashToDownloadPoS,
		Number:  hd.heightToDownloadPoS,
		Length:  192,
		Skip:    0,
		Reverse: true,
	}
}

func (hd *HeaderDownload) UpdateRetryTime(req *HeaderRequest, currentTime, timeout uint64) {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	if req.Anchor.idx == -1 {
		// Anchor has already been deleted
		return
	}
	req.Anchor.timeouts++
	req.Anchor.nextRetryTime = currentTime + timeout
	heap.Fix(hd.anchorQueue, req.Anchor.idx)
}

func (hd *HeaderDownload) RequestSkeleton() *HeaderRequest {
	hd.lock.RLock()
	defer hd.lock.RUnlock()
	log.Trace("Request skeleton", "anchors", len(hd.anchors), "top seen height", hd.topSeenHeightPoW, "highestInDb", hd.highestInDb)
	stride := uint64(8 * 192)
	strideHeight := hd.highestInDb + stride
	lowestAnchorHeight := hd.topSeenHeightPoW + 1 // Inclusive upper bound
	if lowestAnchorHeight <= strideHeight {
		return nil
	}
	// Determine the query range as the height of lowest anchor
	for _, anchor := range hd.anchors {
		if anchor.blockHeight > strideHeight && anchor.blockHeight < lowestAnchorHeight {
			lowestAnchorHeight = anchor.blockHeight // Exclusive upper bound
		}
	}
	length := (lowestAnchorHeight - strideHeight) / stride
	if length > 192 {
		length = 192
	}
	return &HeaderRequest{Number: strideHeight, Length: length, Skip: stride - 1, Reverse: false}
}

func (hd *HeaderDownload) VerifyHeader(header *types.Header) error {
	return hd.engine.VerifyHeader(hd.consensusHeaderReader, header, true /* seal */)
}

type FeedHeaderFunc = func(header *types.Header, headerRaw []byte, hash common.Hash, blockHeight uint64) (td *big.Int, err error)

// InsertHeaders attempts to insert headers into the database, verifying them first
// It returns true in the first return value if the system is "in sync"
func (hd *HeaderDownload) InsertHeaders(hf FeedHeaderFunc, terminalTotalDifficulty *big.Int, logPrefix string, logChannel <-chan time.Time) (bool, error) {
	hd.lock.Lock()
	defer hd.lock.Unlock()

	checkVerify := true
	checkInsert := true

	for checkVerify || checkInsert {
		if checkVerify {
			checkVerify = false
			// Perform verification if needed
			for hd.verifyQueue.Len() > 0 {
				link := hd.verifyQueue[0]
				select {
				case <-logChannel:
					log.Info(fmt.Sprintf("[%s] Verifying headers", logPrefix), "progress", hd.highestInDb)
				default:
				}
				skip := false
				if link.blockHeight <= hd.preverifiedHeight {
					if link.blockHeight <= hd.highestInDb {
						// There was preverified alternative to this link, drop
						skip = true
					} else {
						break // Wait to be mark as pre-verified or an alternative to be inserted
					}
				}
				if !skip {
					_, skip = hd.badHeaders[link.hash]
				}
				if !skip {
					_, skip = hd.badHeaders[link.header.ParentHash]
				}
				if !skip {
					if err := hd.VerifyHeader(link.header); err != nil {
						if errors.Is(err, consensus.ErrFutureBlock) {
							// This may become valid later
							log.Warn("Added future link", "hash", link.hash, "height", link.blockHeight, "timestamp", link.header.Time)
							break // prevent removal of the link from the hd.linkQueue
						} else {
							log.Warn("Verification failed for header", "hash", link.hash, "height", link.blockHeight, "error", err)
							skip = true
						}
					}
				}
				if skip {
					hd.moveLinkToQueue(link, NoQueue)
					delete(hd.links, link.hash)
					continue
				}
				hd.moveLinkToQueue(link, InsertQueueID)
				checkInsert = true
			}
		}
		if checkInsert {
			checkInsert = false
			// Check what we can insert without verification
			for hd.insertQueue.Len() > 0 && hd.insertQueue[0].blockHeight <= hd.highestInDb+1 {
				link := hd.insertQueue[0]
				_, bad := hd.badHeaders[link.hash]
				if !bad && !link.persisted {
					_, bad = hd.badHeaders[link.header.ParentHash]
				}
				if bad {
					// If the link or its parent is marked bad, throw it out
					hd.moveLinkToQueue(link, NoQueue)
					delete(hd.links, link.hash)
					continue
				}
				// Make sure long insertions do not appear as a stuck stage 1
				select {
				case <-logChannel:
					log.Info(fmt.Sprintf("[%s] Inserting headers", logPrefix), "progress", hd.highestInDb)
				default:
				}
				td, err := hf(link.header, link.headerRaw, link.hash, link.blockHeight)
				if err != nil {
					return false, err
				}
				if td != nil {
					// Check if transition to proof-of-stake happened and stop forward syncing
					if terminalTotalDifficulty != nil && td.Cmp(terminalTotalDifficulty) >= 0 {
						hd.highestInDb = link.blockHeight
						return true, nil
					}
				}

				if link.blockHeight > hd.highestInDb {
					hd.highestInDb = link.blockHeight
					checkVerify = true // highestInDb changes, so that there might be more links in verifyQueue to process
				}
				link.persisted = true
				link.header = nil // Drop header reference to free memory, as we won't need it anymore
				link.headerRaw = nil
				hd.moveLinkToQueue(link, PersistedQueueID)
				for _, nextLink := range link.next {
					if nextLink.verified {
						hd.moveLinkToQueue(nextLink, InsertQueueID)
					} else {
						hd.moveLinkToQueue(nextLink, VerifyQueueID)
						checkVerify = true
					}
				}
			}
			for hd.persistedLinkQueue.Len() > hd.persistedLinkLimit {
				link := heap.Pop(&hd.persistedLinkQueue).(*Link)
				delete(hd.links, link.hash)
			}
		}
	}
	return hd.highestInDb >= hd.preverifiedHeight && hd.topSeenHeightPoW > 0 && hd.highestInDb >= hd.topSeenHeightPoW, nil
}

func (hd *HeaderDownload) SetHashToDownloadPoS(hash common.Hash) {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	hd.hashToDownloadPoS = hash
}

func (hd *HeaderDownload) ProcessSegmentPOS(segment ChainSegment, tx kv.Getter) error {
	if len(segment) == 0 {
		return nil
	}
	hd.lock.Lock()
	defer hd.lock.Unlock()
	// Handle request after closing collectors
	if hd.headersCollector == nil {
		return nil
	}
	log.Trace("Collecting...", "from", segment[0].Number, "to", segment[len(segment)-1].Number, "len", len(segment))
	for _, segmentFragment := range segment {
		header := segmentFragment.Header
		if header.Hash() != hd.hashToDownloadPoS {
			return fmt.Errorf("unexpected hash %x (expected %x)", header.Hash(), hd.hashToDownloadPoS)
		}

		if err := hd.headersCollector.Collect(dbutils.HeaderKey(header.Number.Uint64(), header.Hash()), segmentFragment.HeaderRaw); err != nil {
			return err
		}

		hd.hashToDownloadPoS = header.ParentHash
		hd.heightToDownloadPoS = header.Number.Uint64() - 1

		hh, err := hd.headerReader.Header(context.Background(), tx, hd.hashToDownloadPoS, hd.heightToDownloadPoS)
		if err != nil {
			return err
		}
		if hh != nil {
			hd.synced = true
			return nil
		}

		if hd.heightToDownloadPoS == 0 {
			return errors.New("wrong genesis in PoS sync")
		}
	}
	return nil
}

// GrabAnnounces - returns all available announces and forget them
func (hd *HeaderDownload) GrabAnnounces() []Announce {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	res := hd.toAnnounce
	hd.toAnnounce = []Announce{}
	return res
}

func (hd *HeaderDownload) Progress() uint64 {
	hd.lock.RLock()
	defer hd.lock.RUnlock()
	if hd.posSync {
		return hd.heightToDownloadPoS
	} else {
		return hd.highestInDb
	}
}

func (hd *HeaderDownload) HasLink(linkHash common.Hash) bool {
	hd.lock.RLock()
	defer hd.lock.RUnlock()
	if _, ok := hd.getLink(linkHash); ok {
		return true
	}
	return false
}

// SaveExternalAnnounce - does mark hash as seen in external announcement
// only such hashes will broadcast further after
func (hd *HeaderDownload) SaveExternalAnnounce(hash common.Hash) {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	hd.seenAnnounces.Add(hash)
}

func (hd *HeaderDownload) getLink(linkHash common.Hash) (*Link, bool) {
	if link, ok := hd.links[linkHash]; ok {
		return link, true
	}
	return nil, false
}

// addHeaderAsLink wraps header into a link and adds it to either queue of persisted links or queue of non-persisted links
func (hd *HeaderDownload) addHeaderAsLink(h ChainSegmentHeader, persisted bool) *Link {
	link := &Link{
		blockHeight: h.Number,
		hash:        h.Hash,
		header:      h.Header,
		headerRaw:   h.HeaderRaw,
		persisted:   persisted,
	}
	if persisted {
		link.header = nil // Drop header reference to free memory, as we won't need it anymore
		link.headerRaw = nil
	}
	hd.links[h.Hash] = link
	if persisted {
		hd.moveLinkToQueue(link, PersistedQueueID)
	} else {
		hd.moveLinkToQueue(link, EntryQueueID)
	}
	return link
}

func (hi *HeaderInserter) NewFeedHeaderFunc(db kv.StatelessRwTx, headerReader interfaces.HeaderReader) FeedHeaderFunc {
	return func(header *types.Header, headerRaw []byte, hash common.Hash, blockHeight uint64) (*big.Int, error) {
		return hi.FeedHeaderPoW(db, headerReader, header, headerRaw, hash, blockHeight)
	}
}

// Find the forking point - i.e. the latest header on the canonical chain which is an ancestor of this one
// Most common case - forking point is the height of the parent header
func (hi *HeaderInserter) ForkingPoint(db kv.StatelessRwTx, header, parent *types.Header) (forkingPoint uint64, err error) {
	blockHeight := header.Number.Uint64()
	var ch common.Hash
	if fromCache, ok := hi.canonicalCache.Get(blockHeight - 1); ok {
		ch = fromCache.(common.Hash)
	} else {
		if ch, err = hi.headerReader.CanonicalHash(context.Background(), db, blockHeight-1); err != nil {
			return 0, fmt.Errorf("reading canonical hash for height %d: %w", blockHeight-1, err)
		}
	}
	if ch == header.ParentHash {
		forkingPoint = blockHeight - 1
	} else {
		// Going further back
		ancestorHash := parent.ParentHash
		ancestorHeight := blockHeight - 2
		// Look in the cache first
		for fromCache, ok := hi.canonicalCache.Get(ancestorHeight); ok; fromCache, ok = hi.canonicalCache.Get(ancestorHeight) {
			ch = fromCache.(common.Hash)
			if ch == ancestorHash {
				break
			}
			ancestor, err := hi.headerReader.Header(context.Background(), db, ancestorHash, ancestorHeight)
			if err != nil {
				return 0, err
			}
			ancestorHash = ancestor.ParentHash
			ancestorHeight--
		}
		// Now look in the DB
		for {
			ch, err := hi.headerReader.CanonicalHash(context.Background(), db, ancestorHeight)
			if err != nil {
				return 0, fmt.Errorf("[%s] reading canonical hash for %d: %w", hi.logPrefix, ancestorHeight, err)
			}
			if ch == ancestorHash {
				break
			}
			ancestor, err := hi.headerReader.Header(context.Background(), db, ancestorHash, ancestorHeight)
			if err != nil {
				return 0, err
			}
			ancestorHash = ancestor.ParentHash
			ancestorHeight--
		}
		// Loop above terminates when either err != nil (handled already) or ch == ancestorHash, therefore ancestorHeight is our forking point
		forkingPoint = ancestorHeight
	}
	return
}

func (hi *HeaderInserter) FeedHeaderPoW(db kv.StatelessRwTx, headerReader interfaces.HeaderReader, header *types.Header, headerRaw []byte, hash common.Hash, blockHeight uint64) (td *big.Int, err error) {
	if hash == hi.prevHash {
		// Skip duplicates
		return nil, nil
	}
	oldH, err := headerReader.Header(context.Background(), db, hash, blockHeight)
	if err != nil {
		return nil, err
	}
	if oldH != nil {
		// Already inserted, skip
		return nil, nil
	}
	// Load parent header
	parent, err := headerReader.Header(context.Background(), db, header.ParentHash, blockHeight-1)
	if err != nil {
		return nil, err
	}
	if parent == nil {
		// Fail on headers without parent
		return nil, fmt.Errorf("could not find parent with hash %x and height %d for header %x %d", header.ParentHash, blockHeight-1, hash, blockHeight)
	}
	// Parent's total difficulty
	parentTd, err := rawdb.ReadTd(db, header.ParentHash, blockHeight-1)
	if err != nil || parentTd == nil {
		return nil, fmt.Errorf("[%s] parent's total difficulty not found with hash %x and height %d for header %x %d: %v", hi.logPrefix, header.ParentHash, blockHeight-1, hash, blockHeight, err)
	}
	// Calculate total difficulty of this header using parent's total difficulty
	td = new(big.Int).Add(parentTd, header.Difficulty)
	// Now we can decide wether this header will create a change in the canonical head
	if td.Cmp(hi.localTd) > 0 {
		hi.newCanonical = true
		forkingPoint, err := hi.ForkingPoint(db, header, parent)
		if err != nil {
			return nil, err
		}
		hi.highest = blockHeight
		hi.highestHash = hash
		hi.highestTimestamp = header.Time
		hi.canonicalCache.Add(blockHeight, hash)
		// See if the forking point affects the unwindPoint (the block number to which other stages will need to unwind before the new canonical chain is applied)
		if forkingPoint < hi.unwindPoint {
			hi.unwindPoint = forkingPoint
			hi.unwind = true
		}
		// This makes sure we end up choosing the chain with the max total difficulty
		hi.localTd.Set(td)
	}
	if err = rawdb.WriteTd(db, hash, blockHeight, td); err != nil {
		return nil, fmt.Errorf("[%s] failed to WriteTd: %w", hi.logPrefix, err)
	}

	if err = db.Put(kv.Headers, dbutils.HeaderKey(blockHeight, hash), headerRaw); err != nil {
		return nil, fmt.Errorf("[%s] failed to store header: %w", hi.logPrefix, err)
	}

	hi.prevHash = hash
	return td, nil
}

func (hi *HeaderInserter) FeedHeaderPoS(db kv.GetPut, header *types.Header, hash common.Hash) error {
	blockHeight := header.Number.Uint64()
	// TODO(yperbasis): do we need to check if the header is already inserted (oldH)?

	parentTd, err := rawdb.ReadTd(db, header.ParentHash, blockHeight-1)
	if err != nil || parentTd == nil {
		return fmt.Errorf("[%s] parent's total difficulty not found with hash %x and height %d for header %x %d: %v", hi.logPrefix, header.ParentHash, blockHeight-1, hash, blockHeight, err)
	}
	td := new(big.Int).Add(parentTd, header.Difficulty)
	if err = rawdb.WriteTd(db, hash, blockHeight, td); err != nil {
		return fmt.Errorf("[%s] failed to WriteTd: %w", hi.logPrefix, err)
	}

	headerRaw, err := rlp.EncodeToBytes(header)
	if err != nil {
		return fmt.Errorf("[%s] failed to to RLP encode header %x %d: %v", hi.logPrefix, hash, blockHeight, err)
	}
	if err = db.Put(kv.Headers, dbutils.HeaderKey(blockHeight, hash), headerRaw); err != nil {
		return fmt.Errorf("[%s] failed to store header: %w", hi.logPrefix, err)
	}

	hi.highest = blockHeight
	hi.highestHash = hash
	hi.highestTimestamp = header.Time

	return nil
}

func (hi *HeaderInserter) GetHighest() uint64 {
	return hi.highest
}

func (hi *HeaderInserter) GetHighestHash() common.Hash {
	return hi.highestHash
}

func (hi *HeaderInserter) GetHighestTimestamp() uint64 {
	return hi.highestTimestamp
}

func (hi *HeaderInserter) UnwindPoint() uint64 {
	return hi.unwindPoint
}

func (hi *HeaderInserter) Unwind() bool {
	return hi.unwind
}

func (hi *HeaderInserter) BestHeaderChanged() bool {
	return hi.newCanonical
}

// ProcessSegment - handling single segment.
// If segment were processed by extendDown or newAnchor method, then it returns `requestMore=true`
// it allows higher-level algo immediately request more headers without waiting all stages precessing,
// speeds up visibility of new blocks
// It remember peerID - then later - if anchors created from segments will abandoned - this peerID gonna get Penalty
func (hd *HeaderDownload) ProcessSegment(segment ChainSegment, newBlock bool, peerID enode.ID) (requestMore bool, penalties []PenaltyItem) {
	lowestNum := segment[0].Number
	highest := segment[len(segment)-1]
	highestNum := highest.Number
	log.Trace("processSegment", "from", lowestNum, "to", highestNum)
	hd.lock.Lock()
	defer hd.lock.Unlock()
	foundAnchor, anchor, start := hd.findAnchor(segment)
	foundTip, link, end := hd.findLink(segment, start)
	if end == 0 {
		log.Trace("Duplicate segment")
		if foundAnchor {
			// If duplicate segment is extending from the anchor, the anchor needs to be deleted,
			// otherwise it will keep producing requests that will be found duplicate
			hd.removeAnchor(anchor)
		}
		return
	}
	if highestNum > hd.topSeenHeightPoW {
		if newBlock || hd.seenAnnounces.Seen(highest.Hash) {
			hd.topSeenHeightPoW = highestNum
		}
	}

	subSegment := segment[start:end]
	startNum := subSegment[0].Number
	endNum := subSegment[len(subSegment)-1].Number
	// There are 4 cases
	if foundAnchor {
		if foundTip {
			// Connect
			penalties = hd.connect(subSegment, link, anchor)
			log.Trace("Connected", "start", startNum, "end", endNum)
		} else {
			// ExtendDown
			requestMore = hd.extendDown(subSegment, anchor)
			log.Trace("Extended Down", "start", startNum, "end", endNum)
		}
	} else if foundTip {
		// ExtendUp
		hd.extendUp(subSegment, link)
		log.Trace("Extended Up", "start", startNum, "end", endNum)
	} else {
		// NewAnchor
		requestMore = hd.newAnchor(subSegment, peerID)
		log.Trace("NewAnchor", "start", startNum, "end", endNum)
	}
	//log.Info(hd.anchorState())
	log.Trace("Link queue", "size", hd.linkQueue.Len())
	if hd.linkQueue.Len() > hd.linkLimit {
		log.Trace("Too many links, cutting down", "count", hd.linkQueue.Len(), "tried to add", len(subSegment), "limit", hd.linkLimit)
		hd.pruneLinkQueue()
	}
	select {
	case hd.DeliveryNotify <- struct{}{}:
	default:
	}

	return hd.requestChaining && requestMore, penalties
}

func (hd *HeaderDownload) TopSeenHeight() uint64 {
	hd.lock.RLock()
	defer hd.lock.RUnlock()
	if hd.topSeenHeightPoW > hd.topSeenHeightPoS {
		return hd.topSeenHeightPoW
	} else {
		return hd.topSeenHeightPoS
	}
}

func (hd *HeaderDownload) UpdateTopSeenHeightPoS(blockHeight uint64) {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	if blockHeight > hd.topSeenHeightPoS {
		hd.topSeenHeightPoS = blockHeight
	}
}

func (hd *HeaderDownload) SetHeaderReader(headerReader consensus.ChainHeaderReader) {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	hd.consensusHeaderReader = headerReader
}

func (hd *HeaderDownload) EnableRequestChaining() {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	hd.requestChaining = true
}

func (hd *HeaderDownload) SetFetching(fetching bool) {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	hd.fetching = fetching
}

func (hd *HeaderDownload) SetHeightToDownloadPoS(heightToDownloadPoS uint64) {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	hd.heightToDownloadPoS = heightToDownloadPoS
}

func (hd *HeaderDownload) Unsync() {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	hd.synced = false
}

func (hd *HeaderDownload) SetHeadersCollector(collector *etl.Collector) {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	hd.headersCollector = collector
}

func (hd *HeaderDownload) SetPOSSync(posSync bool) {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	hd.posSync = posSync
}

func (hd *HeaderDownload) POSSync() bool {
	hd.lock.RLock()
	defer hd.lock.RUnlock()
	return hd.posSync
}

func (hd *HeaderDownload) Synced() bool {
	hd.lock.RLock()
	defer hd.lock.RUnlock()
	return hd.synced
}

func (hd *HeaderDownload) RequestChaining() bool {
	hd.lock.RLock()
	defer hd.lock.RUnlock()
	return hd.requestChaining
}

func (hd *HeaderDownload) Fetching() bool {
	hd.lock.RLock()
	defer hd.lock.RUnlock()
	return hd.fetching
}

func (hd *HeaderDownload) GetPendingPayloadStatus() common.Hash {
	hd.lock.RLock()
	defer hd.lock.RUnlock()
	return hd.pendingPayloadStatus
}

func (hd *HeaderDownload) SetPendingPayloadStatus(header common.Hash) {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	hd.pendingPayloadStatus = header
}

func (hd *HeaderDownload) ClearPendingPayloadStatus() {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	hd.pendingPayloadStatus = common.Hash{}
}

func (hd *HeaderDownload) AddMinedHeader(header *types.Header) error {
	buf := bytes.NewBuffer(nil)
	if err := header.EncodeRLP(buf); err != nil {
		return err
	}
	segments, _, err := hd.SingleHeaderAsSegment(buf.Bytes(), header)
	if err != nil {
		return err
	}

	peerID := enode.ID{'m', 'i', 'n', 'e', 'r'} // "miner"

	for _, segment := range segments {
		_, _ = hd.ProcessSegment(segment, false /* newBlock */, peerID)
	}
	return nil
}

func (hd *HeaderDownload) AddHeaderFromSnapshot(tx kv.Tx, n uint64, r interfaces.FullBlockReader) error {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	addPreVerifiedHashes := len(hd.preverifiedHashes) == 0
	if addPreVerifiedHashes && hd.preverifiedHashes == nil {
		hd.preverifiedHashes = map[common.Hash]struct{}{}
	}

	for i := n; i > 0 && hd.persistedLinkQueue.Len() < hd.persistedLinkLimit; i-- {
		header, err := r.HeaderByNumber(context.Background(), tx, i)
		if err != nil {
			return err
		}
		if header == nil {
			continue
		}
		v, err := rlp.EncodeToBytes(header)
		if err != nil {
			return err
		}
		h := ChainSegmentHeader{
			HeaderRaw: v,
			Header:    header,
			Hash:      types.RawRlpHash(v),
			Number:    header.Number.Uint64(),
		}
		link := hd.addHeaderAsLink(h, true /* persisted */)
		link.verified = true
		if addPreVerifiedHashes {
			hd.preverifiedHashes[h.Hash] = struct{}{}
		}
	}
	if hd.highestInDb < n {
		hd.highestInDb = n
	}
	if hd.preverifiedHeight < n {
		hd.preverifiedHeight = n
	}

	return nil
}

func DecodeTips(encodings []string) (map[common.Hash]HeaderRecord, error) {
	hardTips := make(map[common.Hash]HeaderRecord, len(encodings))

	var buf bytes.Buffer

	for i, encoding := range encodings {
		b, err := base64.RawStdEncoding.DecodeString(encoding)
		if err != nil {
			return nil, fmt.Errorf("decoding hard coded header on %d: %w", i, err)
		}

		if _, err = buf.Write(b); err != nil {
			return nil, fmt.Errorf("gzip write string on %d: %w", i, err)
		}

		zr, err := gzip.NewReader(&buf)
		if err != nil {
			return nil, fmt.Errorf("gzip reader on %d: %w %q", i, err, encoding)
		}

		res, err := io.ReadAll(zr)
		if err != nil {
			return nil, fmt.Errorf("gzip copy on %d: %w %q", i, err, encoding)
		}

		if err := zr.Close(); err != nil {
			return nil, fmt.Errorf("gzip close on %d: %w", i, err)
		}

		var h types.Header
		if err := rlp.DecodeBytes(res, &h); err != nil {
			return nil, fmt.Errorf("parsing hard coded header on %d: %w", i, err)
		}

		headerHash := types.RawRlpHash(res)
		hardTips[headerHash] = HeaderRecord{Raw: b, Header: &h}

		buf.Reset()
	}

	return hardTips, nil
}
