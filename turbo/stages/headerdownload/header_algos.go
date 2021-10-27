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
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"
)

// Implements sort.Interface so we can sort the incoming header in the message by block height
type HeadersByBlockHeight []*types.Header

func (h HeadersByBlockHeight) Len() int {
	return len(h)
}

func (h HeadersByBlockHeight) Less(i, j int) bool {
	// Note - the ordering is the inverse ordering of the block heights
	return h[i].Number.Cmp(h[j].Number) > 0
}

func (h HeadersByBlockHeight) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

// SplitIntoSegments converts message containing headers into a collection of chain segments
func (hd *HeaderDownload) SplitIntoSegments(headersRaw [][]byte, msg []*types.Header) ([]*ChainSegment, Penalty, error) {
	hd.lock.RLock()
	defer hd.lock.RUnlock()
	sort.Sort(HeadersByBlockHeight(msg))
	// Now all headers are order from the highest block height to the lowest
	var segments []*ChainSegment                         // Segments being built
	segmentMap := make(map[common.Hash]int)              // Mapping of the header hash to the index of the chain segment it belongs
	childrenMap := make(map[common.Hash][]*types.Header) // Mapping parent hash to the children
	dedupMap := make(map[common.Hash]struct{})           // Map used for detecting duplicate headers
	for i, header := range msg {
		headerHash := header.Hash()
		if _, bad := hd.badHeaders[headerHash]; bad {
			return nil, BadBlockPenalty, nil
		}
		if _, duplicate := dedupMap[headerHash]; duplicate {
			return nil, DuplicateHeaderPenalty, nil
		}
		dedupMap[headerHash] = struct{}{}
		var segmentIdx int
		children := childrenMap[headerHash]
		for _, child := range children {
			if valid, penalty := hd.childParentValid(child, header); !valid {
				return nil, penalty, nil
			}
		}
		if len(children) == 1 {
			// Single child, extract segmentIdx
			segmentIdx = segmentMap[headerHash]
		} else {
			// No children, or more than one child, create new segment
			segmentIdx = len(segments)
			segments = append(segments, &ChainSegment{})
		}
		segments[segmentIdx].Headers = append(segments[segmentIdx].Headers, header)
		segments[segmentIdx].HeadersRaw = append(segments[segmentIdx].HeadersRaw, headersRaw[i])
		segmentMap[header.ParentHash] = segmentIdx
		siblings := childrenMap[header.ParentHash]
		siblings = append(siblings, header)
		childrenMap[header.ParentHash] = siblings
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
func (hd *HeaderDownload) SingleHeaderAsSegment(headerRaw []byte, header *types.Header) ([]*ChainSegment, Penalty, error) {
	hd.lock.RLock()
	defer hd.lock.RUnlock()
	headerHash := header.Hash()
	if _, bad := hd.badHeaders[headerHash]; bad {
		return nil, BadBlockPenalty, nil
	}
	return []*ChainSegment{{HeadersRaw: [][]byte{headerRaw}, Headers: []*types.Header{header}}}, NoPenalty, nil
}

// ReportBadHeader -
func (hd *HeaderDownload) ReportBadHeader(headerHash common.Hash) {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	hd.badHeaders[headerHash] = struct{}{}
}

func (hd *HeaderDownload) IsBadHeader(headerHash common.Hash) bool {
	hd.lock.RLock()
	defer hd.lock.RUnlock()
	_, ok := hd.badHeaders[headerHash]
	return ok
}

// FindAnchors attempts to find anchors to which given chain segment can be attached to
func (hd *HeaderDownload) findAnchors(segment *ChainSegment) (found bool, start int) {
	// Walk the segment from children towards parents
	for i, header := range segment.Headers {
		// Check if the header can be attached to an anchor of a working tree
		if _, attaching := hd.anchors[header.Hash()]; attaching {
			return true, i
		}
	}
	return false, 0
}

// FindLink attempts to find a non-persisted link that given chain segment can be attached to.
func (hd *HeaderDownload) findLink(segment *ChainSegment, start int) (found bool, end int) {
	if _, duplicate := hd.getLink(segment.Headers[start].Hash()); duplicate {
		return false, 0
	}
	// Walk the segment from children towards parents
	for i, header := range segment.Headers[start:] {
		// Check if the header can be attached to any links
		if _, attaching := hd.getLink(header.ParentHash); attaching {
			return true, start + i + 1
		}
	}
	return false, len(segment.Headers)
}

func (hd *HeaderDownload) removeUpwards(toRemove []*Link) {
	for len(toRemove) > 0 {
		removal := toRemove[len(toRemove)-1]
		toRemove = toRemove[:len(toRemove)-1]
		delete(hd.links, removal.header.Hash())
		heap.Remove(hd.linkQueue, removal.idx)
		toRemove = append(toRemove, removal.next...)
	}
}

func (hd *HeaderDownload) markPreverified(link *Link) {
	// Go through all parent links that are not preveried and mark them too
	for link != nil && !link.persisted {
		link.preverified = true
		link = hd.links[link.header.ParentHash]
	}
}

// ExtendUp extends a working tree up from the link, using given chain segment
func (hd *HeaderDownload) extendUp(segment *ChainSegment, start, end int) error {
	// Find attachment link again
	linkHeader := segment.Headers[end-1]
	attachmentLink, attaching := hd.getLink(linkHeader.ParentHash)
	if !attaching {
		return fmt.Errorf("extendUp attachment link not found for %x", linkHeader.ParentHash)
	}
	if attachmentLink.preverified && len(attachmentLink.next) > 0 {
		return fmt.Errorf("cannot extendUp from preverified link %d with children", attachmentLink.blockHeight)
	}
	// Iterate over headers backwards (from parents towards children)
	prevLink := attachmentLink
	for i := end - 1; i >= start; i-- {
		link := hd.addHeaderAsLink(segment.Headers[i], false /* persisted */)
		prevLink.next = append(prevLink.next, link)
		prevLink = link
		if _, ok := hd.preverifiedHashes[link.hash]; ok {
			hd.markPreverified(link)
		}
	}

	if _, bad := hd.badHeaders[attachmentLink.hash]; !bad && attachmentLink.persisted {
		link := hd.links[linkHeader.Hash()]
		hd.insertList = append(hd.insertList, link)
	}
	return nil
}

// ExtendDown extends some working trees down from the anchor, using given chain segment
// it creates a new anchor and collects all the links from the attached anchors to it
func (hd *HeaderDownload) extendDown(segment *ChainSegment, start, end int) (bool, error) {
	// Find attachment anchor again
	anchorHeader := segment.Headers[start]
	if anchor, attaching := hd.anchors[anchorHeader.Hash()]; attaching {
		anchorPreverified := false
		for _, link := range anchor.links {
			if link.preverified {
				anchorPreverified = true
				break
			}
		}
		newAnchorHeader := segment.Headers[end-1]
		var newAnchor *Anchor
		newAnchor, preExisting := hd.anchors[newAnchorHeader.ParentHash]
		if !preExisting {
			newAnchor = &Anchor{
				parentHash:  newAnchorHeader.ParentHash,
				timestamp:   0,
				peerID:      anchor.peerID,
				blockHeight: newAnchorHeader.Number.Uint64(),
			}
			if newAnchor.blockHeight > 0 {
				hd.anchors[newAnchorHeader.ParentHash] = newAnchor
				heap.Push(hd.anchorQueue, newAnchor)
			}
		}

		// Anchor is removed from the map, but not from the anchorQueue
		// This is because it is hard to find the index under which the anchor is stored in the anchorQueue
		// But removal will happen anyway, in th function RequestMoreHeaders, if it disapppears from the map
		delete(hd.anchors, anchor.parentHash)
		// Add all headers in the segments as links to this anchor
		var prevLink *Link
		for i := end - 1; i >= start; i-- {
			link := hd.addHeaderAsLink(segment.Headers[i], false /* pesisted */)
			if prevLink == nil {
				newAnchor.links = append(newAnchor.links, link)
			} else {
				prevLink.next = append(prevLink.next, link)
			}
			prevLink = link
			if _, ok := hd.preverifiedHashes[link.hash]; ok {
				hd.markPreverified(link)
			}
		}
		prevLink.next = anchor.links
		anchor.links = nil
		if anchorPreverified {
			// Mark the entire segment as preverified
			hd.markPreverified(prevLink)
		}
		return !preExisting, nil
	}
	return false, fmt.Errorf("extendDown attachment anchors not found for %x", anchorHeader.Hash())
}

// Connect connects some working trees using anchors of some, and a link of another
func (hd *HeaderDownload) connect(segment *ChainSegment, start, end int) ([]PenaltyItem, error) {
	// Find attachment link again
	linkHeader := segment.Headers[end-1]
	// Find attachement anchors again
	anchorHeader := segment.Headers[start]
	attachmentLink, ok1 := hd.getLink(linkHeader.ParentHash)
	if !ok1 {
		return nil, fmt.Errorf("connect attachment link not found for %x", linkHeader.ParentHash)
	}
	if attachmentLink.preverified && len(attachmentLink.next) > 0 {
		return nil, fmt.Errorf("cannot connect to preverified link %d with children", attachmentLink.blockHeight)
	}
	anchor, ok2 := hd.anchors[anchorHeader.Hash()]
	if !ok2 {
		return nil, fmt.Errorf("connect attachment anchors not found for %x", anchorHeader.Hash())
	}
	anchorPreverified := false
	for _, link := range anchor.links {
		if link.preverified {
			anchorPreverified = true
			break
		}
	}
	// Anchor is removed from the map, but not from the anchorQueue
	// This is because it is hard to find the index under which the anchor is stored in the anchorQueue
	// But removal will happen anyway, in th function RequestMoreHeaders, if it disapppears from the map
	delete(hd.anchors, anchor.parentHash)
	// Iterate over headers backwards (from parents towards children)
	prevLink := attachmentLink
	for i := end - 1; i >= start; i-- {
		link := hd.addHeaderAsLink(segment.Headers[i], false /* persisted */)
		prevLink.next = append(prevLink.next, link)
		prevLink = link
		if _, ok := hd.preverifiedHashes[link.hash]; ok {
			hd.markPreverified(link)
		}
	}
	prevLink.next = anchor.links
	anchor.links = nil
	if anchorPreverified {
		// Mark the entire segment as preverified
		hd.markPreverified(prevLink)
	}
	var penalties []PenaltyItem
	if _, bad := hd.badHeaders[attachmentLink.hash]; bad {
		hd.invalidateAnchor(anchor)
		penalties = append(penalties, PenaltyItem{Penalty: AbandonedAnchorPenalty, PeerID: anchor.peerID})
	} else if attachmentLink.persisted {
		link := hd.links[linkHeader.Hash()]
		hd.insertList = append(hd.insertList, link)
	}
	return penalties, nil
}

func (hd *HeaderDownload) removeAnchor(segment *ChainSegment, start int) error {
	// Find attachement anchors again
	anchorHeader := segment.Headers[start]
	anchor, ok := hd.anchors[anchorHeader.Hash()]
	if !ok {
		return fmt.Errorf("connect attachment anchors not found for %x", anchorHeader.Hash())
	}
	// Anchor is removed from the map, but not from the anchorQueue
	// This is because it is hard to find the index under which the anchor is stored in the anchorQueue
	// But removal will happen anyway, in th function RequestMoreHeaders, if it disapppears from the map
	delete(hd.anchors, anchor.parentHash)
	return nil
}

// if anchor will be abandoned - given peerID will get Penalty
func (hd *HeaderDownload) newAnchor(segment *ChainSegment, start, end int, peerID string) (bool, error) {
	anchorHeader := segment.Headers[end-1]

	var anchor *Anchor
	anchor, preExisting := hd.anchors[anchorHeader.ParentHash]
	if !preExisting {
		if anchorHeader.Number.Uint64() < hd.highestInDb {
			return false, fmt.Errorf("new anchor too far in the past: %d, latest header in db: %d", anchorHeader.Number.Uint64(), hd.highestInDb)
		}
		if len(hd.anchors) >= hd.anchorLimit {
			return false, fmt.Errorf("too many anchors: %d, limit %d", len(hd.anchors), hd.anchorLimit)
		}
		anchor = &Anchor{
			parentHash:  anchorHeader.ParentHash,
			peerID:      peerID,
			timestamp:   0,
			blockHeight: anchorHeader.Number.Uint64(),
		}
		hd.anchors[anchorHeader.ParentHash] = anchor
		heap.Push(hd.anchorQueue, anchor)
	}
	// Iterate over headers backwards (from parents towards children)
	var prevLink *Link
	for i := end - 1; i >= start; i-- {
		header := segment.Headers[i]
		link := hd.addHeaderAsLink(header, false /* persisted */)
		if prevLink == nil {
			anchor.links = append(anchor.links, link)
		} else {
			prevLink.next = append(prevLink.next, link)
		}
		prevLink = link
		if _, ok := hd.preverifiedHashes[link.hash]; ok {
			hd.markPreverified(link)
		}
	}
	return !preExisting, nil
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
	case params.MainnetChainName:
		encodings = mainnetPreverifiedHashes
		height = mainnetPreverifiedHeight
	case params.RopstenChainName:
		encodings = ropstenPreverifiedHashes
		height = ropstenPreverifiedHeight
	default:
		log.Warn("Preverified hashes not found for", "chain", chain)
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
		link := heap.Pop(hd.persistedLinkQueue).(*Link)
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
			var h types.Header
			if err = rlp.DecodeBytes(v, &h); err != nil {
				return err
			}
			hd.addHeaderAsLink(&h, true /* persisted */)

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

func (hd *HeaderDownload) invalidateAnchor(anchor *Anchor) {
	log.Warn("Invalidating anchor for suspected unavailability", "height", anchor.blockHeight)
	delete(hd.anchors, anchor.parentHash)
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
		if _, ok := hd.anchors[anchor.parentHash]; ok {
			if anchor.timestamp > currentTime {
				// Anchor not ready for re-request yet
				return nil, penalties
			}
			if anchor.timeouts < 10 {
				return &HeaderRequest{Hash: anchor.parentHash, Number: anchor.blockHeight - 1, Length: 192, Skip: 0, Reverse: true}, penalties
			} else {
				// Ancestors of this anchor seem to be unavailable, invalidate and move on
				hd.invalidateAnchor(anchor)
				penalties = append(penalties, PenaltyItem{Penalty: AbandonedAnchorPenalty, PeerID: anchor.peerID})
			}
		}
		// Anchor disappeared or unavailable, pop from the queue and move on
		heap.Remove(hd.anchorQueue, 0)
	}
	return nil, penalties
}

func (hd *HeaderDownload) SentRequest(req *HeaderRequest, currentTime, timeout uint64) {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	anchor, ok := hd.anchors[req.Hash]
	if !ok {
		return
	}
	anchor.timeouts++
	anchor.timestamp = currentTime + timeout
	heap.Fix(hd.anchorQueue, anchor.idx)
}

func (hd *HeaderDownload) RequestSkeleton() *HeaderRequest {
	hd.lock.RLock()
	defer hd.lock.RUnlock()
	log.Trace("Request skeleton", "anchors", len(hd.anchors), "top seen height", hd.topSeenHeight, "highestInDb", hd.highestInDb)
	stride := uint64(8 * 192)
	queryRange := hd.topSeenHeight
	// Determine the query range as the height of lowest anchor
	for _, anchor := range hd.anchors {
		if anchor.blockHeight < queryRange {
			queryRange = anchor.blockHeight
		}
	}
	length := (queryRange - hd.highestInDb) / stride
	if length > 192 {
		length = 192
	}
	if length == 0 {
		return nil
	}
	return &HeaderRequest{Number: hd.highestInDb + stride, Length: length, Skip: stride, Reverse: false}
}

// InsertHeaders attempts to insert headers into the database, verifying them first
// It returns true in the first return value if the system is "in sync"
func (hd *HeaderDownload) InsertHeaders(hf func(header *types.Header, blockHeight uint64) error, logPrefix string, logChannel <-chan time.Time) (bool, error) {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	var linksInFuture []*Link // Here we accumulate links that fail validation as "in the future"
	for len(hd.insertList) > 0 {
		// Make sure long insertions do not appear as a stuck stage 1
		select {
		case <-logChannel:
			log.Info(fmt.Sprintf("[%s] Inserting headers", logPrefix), "progress", hd.highestInDb)
		default:
		}
		link := hd.insertList[len(hd.insertList)-1]
		if link.blockHeight <= hd.preverifiedHeight && !link.preverified {
			// Header should be preverified, but not yet, try again later
			break
		}
		hd.insertList = hd.insertList[:len(hd.insertList)-1]
		skip := false
		if !link.preverified {
			if _, bad := hd.badHeaders[link.hash]; bad {
				skip = true
			} else if err := hd.engine.VerifyHeader(hd.headerReader, link.header, true /* seal */); err != nil {
				log.Warn("Verification failed for header", "hash", link.header.Hash(), "height", link.blockHeight, "error", err)
				if errors.Is(err, consensus.ErrFutureBlock) {
					// This may become valid later
					linksInFuture = append(linksInFuture, link)
					log.Warn("Added future link", "hash", link.header.Hash(), "height", link.blockHeight, "timestamp", link.header.Time)
					continue // prevent removal of the link from the hd.linkQueue
				} else {
					skip = true
				}
			} else {
				if hd.seenAnnounces.Pop(link.hash) {
					hd.toAnnounce = append(hd.toAnnounce, Announce{Hash: link.hash, Number: link.blockHeight})
				}
			}
		}
		if _, ok := hd.links[link.hash]; ok {
			heap.Remove(hd.linkQueue, link.idx)
		}
		if skip {
			delete(hd.links, link.hash)
			continue
		}
		if err := hf(link.header, link.blockHeight); err != nil {
			return false, err
		}
		if link.blockHeight > hd.highestInDb {
			hd.highestInDb = link.blockHeight
		}
		link.persisted = true
		link.header = nil // Drop header reference to free memory, as we won't need it anymore
		heap.Push(hd.persistedLinkQueue, link)
		if len(link.next) > 0 {
			hd.insertList = append(hd.insertList, link.next...)
		}
	}
	for hd.persistedLinkQueue.Len() > hd.persistedLinkLimit {
		link := heap.Pop(hd.persistedLinkQueue).(*Link)
		delete(hd.links, link.hash)
	}
	if len(linksInFuture) > 0 {
		hd.insertList = append(hd.insertList, linksInFuture...)
		linksInFuture = nil //nolint
	}
	return hd.highestInDb >= hd.preverifiedHeight && hd.topSeenHeight > 0 && hd.highestInDb >= hd.topSeenHeight, nil
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
	return hd.highestInDb
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
func (hd *HeaderDownload) addHeaderAsLink(header *types.Header, persisted bool) *Link {
	height := header.Number.Uint64()
	linkHash := header.Hash()
	link := &Link{
		blockHeight: height,
		hash:        linkHash,
		header:      header,
		persisted:   persisted,
	}
	hd.links[linkHash] = link
	if persisted {
		heap.Push(hd.persistedLinkQueue, link)
	} else {
		heap.Push(hd.linkQueue, link)
	}
	return link
}

func (hi *HeaderInserter) FeedHeaderFunc(db kv.StatelessRwTx) func(header *types.Header, blockHeight uint64) error {
	return func(header *types.Header, blockHeight uint64) error {
		return hi.FeedHeader(db, header, blockHeight)
	}

}

func (hi *HeaderInserter) FeedHeader(db kv.StatelessRwTx, header *types.Header, blockHeight uint64) error {
	hash := header.Hash()
	if hash == hi.prevHash {
		// Skip duplicates
		return nil
	}
	if oldH := rawdb.ReadHeader(db, hash, blockHeight); oldH != nil {
		// Already inserted, skip
		return nil
	}
	// Load parent header
	parent := rawdb.ReadHeader(db, header.ParentHash, blockHeight-1)
	if parent == nil {
		// Fail on headers without parent
		return fmt.Errorf("could not find parent with hash %x and height %d for header %x %d", header.ParentHash, blockHeight-1, hash, blockHeight)
	}
	// Parent's total difficulty
	parentTd, err := rawdb.ReadTd(db, header.ParentHash, blockHeight-1)
	if err != nil || parentTd == nil {
		return fmt.Errorf("[%s] parent's total difficulty not found with hash %x and height %d for header %x %d: %v", hi.logPrefix, header.ParentHash, blockHeight-1, hash, blockHeight, err)
	}
	// Calculate total difficulty of this header using parent's total difficulty
	td := new(big.Int).Add(parentTd, header.Difficulty)
	// Now we can decide wether this header will create a change in the canonical head
	if td.Cmp(hi.localTd) > 0 {
		hi.newCanonical = true
		// Find the forking point - i.e. the latest header on the canonical chain which is an ancestor of this one
		// Most common case - forking point is the height of the parent header
		var forkingPoint uint64
		var ch common.Hash
		var err error
		if fromCache, ok := hi.canonicalCache.Get(blockHeight - 1); ok {
			ch = fromCache.(common.Hash)
		} else {
			if ch, err = rawdb.ReadCanonicalHash(db, blockHeight-1); err != nil {
				return fmt.Errorf("reading canonical hash for height %d: %w", blockHeight-1, err)
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
				ancestor := rawdb.ReadHeader(db, ancestorHash, ancestorHeight)
				ancestorHash = ancestor.ParentHash
				ancestorHeight--
			}
			// Now look in the DB
			for ch, err = rawdb.ReadCanonicalHash(db, ancestorHeight); err == nil && ch != ancestorHash; ch, err = rawdb.ReadCanonicalHash(db, ancestorHeight) {
				ancestor := rawdb.ReadHeader(db, ancestorHash, ancestorHeight)
				ancestorHash = ancestor.ParentHash
				ancestorHeight--
			}
			if err != nil {
				return fmt.Errorf("[%s] reading canonical hash for %d: %w", hi.logPrefix, ancestorHeight, err)
			}
			// Loop above terminates when either err != nil (handled already) or ch == ancestorHash, therefore ancestorHeight is our forking point
			forkingPoint = ancestorHeight
		}
		if err = rawdb.WriteHeadHeaderHash(db, hash); err != nil {
			return fmt.Errorf("[%s] marking head header hash as %x: %w", hi.logPrefix, hash, err)
		}
		if err = stages.SaveStageProgress(db, stages.Headers, blockHeight); err != nil {
			return fmt.Errorf("[%s] saving Headers progress: %w", hi.logPrefix, err)
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
		// This makes sure we end up chosing the chain with the max total difficulty
		hi.localTd.Set(td)
	}
	data, err2 := rlp.EncodeToBytes(header)
	if err2 != nil {
		return fmt.Errorf("[%s] failed to RLP encode header: %w", hi.logPrefix, err2)
	}
	if err = rawdb.WriteTd(db, hash, blockHeight, td); err != nil {
		return fmt.Errorf("[%s] failed to WriteTd: %w", hi.logPrefix, err)
	}
	if err = db.Put(kv.Headers, dbutils.HeaderKey(blockHeight, hash), data); err != nil {
		return fmt.Errorf("[%s] failed to store header: %w", hi.logPrefix, err)
	}
	hi.prevHash = hash
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
func (hd *HeaderDownload) ProcessSegment(segment *ChainSegment, newBlock bool, peerID string) (requestMore bool, penalties []PenaltyItem) {
	log.Trace("processSegment", "from", segment.Headers[0].Number.Uint64(), "to", segment.Headers[len(segment.Headers)-1].Number.Uint64())
	hd.lock.Lock()
	defer hd.lock.Unlock()
	foundAnchor, start := hd.findAnchors(segment)
	foundTip, end := hd.findLink(segment, start) // We ignore penalty because we will check it as part of PoW check
	if end == 0 {
		log.Trace("Duplicate segment")
		if foundAnchor {
			// If duplicate segment is extending from the anchor, the anchor needs to be deleted,
			// otherwise it will keep producing requests that will be found duplicate
			if err := hd.removeAnchor(segment, start); err != nil {
				log.Warn("removal of anchor failed", "error", err)
			}
		}
		return
	}
	height := segment.Headers[len(segment.Headers)-1].Number.Uint64()
	hash := segment.Headers[len(segment.Headers)-1].Hash()
	if newBlock || hd.seenAnnounces.Seen(hash) {
		if height > hd.topSeenHeight {
			hd.topSeenHeight = height
		}
	}
	startNum := segment.Headers[start].Number.Uint64()
	endNum := segment.Headers[end-1].Number.Uint64()
	// There are 4 cases
	if foundAnchor {
		if foundTip {
			// Connect
			var err error
			if penalties, err = hd.connect(segment, start, end); err != nil {
				log.Debug("Connect failed", "error", err)
				return
			}
			log.Trace("Connected", "start", startNum, "end", endNum)
		} else {
			// ExtendDown
			var err error
			if requestMore, err = hd.extendDown(segment, start, end); err != nil {
				log.Debug("ExtendDown failed", "error", err)
				return
			}
			log.Trace("Extended Down", "start", startNum, "end", endNum)
		}
	} else if foundTip {
		if end > 0 {
			// ExtendUp
			if err := hd.extendUp(segment, start, end); err != nil {
				log.Debug("ExtendUp failed", "error", err)
				return
			}
			log.Trace("Extended Up", "start", startNum, "end", endNum)
		}
	} else {
		// NewAnchor
		var err error
		if requestMore, err = hd.newAnchor(segment, start, end, peerID); err != nil {
			log.Debug("NewAnchor failed", "error", err)
			return
		}
		log.Trace("NewAnchor", "start", startNum, "end", endNum)
	}
	//log.Info(hd.anchorState())
	log.Trace("Link queue", "size", hd.linkQueue.Len())
	if hd.linkQueue.Len() > hd.linkLimit {
		log.Trace("Too many links, cutting down", "count", hd.linkQueue.Len(), "tried to add", end-start, "limit", hd.linkLimit)
	}
	for hd.linkQueue.Len() > hd.linkLimit {
		link := heap.Pop(hd.linkQueue).(*Link)
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
	select {
	case hd.DeliveryNotify <- struct{}{}:
	default:
	}

	return hd.requestChaining && requestMore, penalties
}

func (hd *HeaderDownload) TopSeenHeight() uint64 {
	hd.lock.RLock()
	defer hd.lock.RUnlock()
	return hd.topSeenHeight
}

func (hd *HeaderDownload) SetHeaderReader(headerReader consensus.ChainHeaderReader) {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	hd.headerReader = headerReader
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

func (hd *HeaderDownload) AddMinedBlock(block *types.Block) error {
	buf := bytes.NewBuffer(nil)
	if err := block.Header().EncodeRLP(buf); err != nil {
		return err
	}
	segments, _, err := hd.SingleHeaderAsSegment(buf.Bytes(), block.Header())
	if err != nil {
		return err
	}

	for _, segment := range segments {
		_, _ = hd.ProcessSegment(segment, false /* newBlock */, "miner")
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

		hardTips[h.Hash()] = HeaderRecord{Raw: b, Header: &h}

		buf.Reset()
	}

	return hardTips, nil
}
