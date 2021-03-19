package headerdownload

import (
	"container/heap"
	"context"
	"fmt"
	"math/big"
	"sort"
	"strings"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
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
	var prevLink *Link
	for link != nil && !link.preverified {
		link.preverified = true
		if prevLink != nil && len(link.next) > 1 {
			// Remove all non-canonical links
			var toRemove []*Link
			for _, n := range link.next {
				if n != prevLink {
					toRemove = append(toRemove, n)
				}
			}
			hd.removeUpwards(toRemove)
			link.next = append(link.next[:0], prevLink)
		}
		link = hd.links[link.header.ParentHash]
	}
}

// ExtendUp extends a working tree up from the link, using given chain segment
func (hd *HeaderDownload) extendUp(segment *ChainSegment, start, end int) error {
	// Find attachment link again
	linkHeader := segment.Headers[end-1]
	attachmentLink, attaching := hd.getLink(linkHeader.ParentHash)
	if attaching {
		if attachmentLink.preverified && len(attachmentLink.next) > 0 {
			return fmt.Errorf("cannot extendUp from preverified link %d with children", attachmentLink.blockHeight)
		}
		// Iterate over headers backwards (from parents towards children)
		prevLink := attachmentLink
		for i := end - 1; i >= start; i-- {
			header := segment.Headers[i]
			link := hd.addHeaderAsLink(header, false /* persisted */)
			prevLink.next = append(prevLink.next, link)
			prevLink = link
			if _, ok := hd.preverifiedHashes[header.Hash()]; ok {
				hd.markPreverified(link)
			}
		}
	} else {
		return fmt.Errorf("extendUp attachment link not found for %x", linkHeader.ParentHash)
	}
	if attachmentLink.persisted {
		link := hd.links[linkHeader.Hash()]
		hd.insertList = append(hd.insertList, link)
	}
	return nil
}

func (hd *HeaderDownload) Ready() (bool, uint64) {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	return hd.stageReady, hd.stageHeight
}

func (hd *HeaderDownload) StageReadyChannel() chan struct{} {
	return hd.stageReadyCh
}

// ExtendDown extends some working trees down from the anchor, using given chain segment
// it creates a new anchor and collects all the links from the attached anchors to it
func (hd *HeaderDownload) extendDown(segment *ChainSegment, start, end int) error {
	// Find attachement anchor again
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
		newAnchor := &Anchor{
			parentHash:  newAnchorHeader.ParentHash,
			timestamp:   0,
			blockHeight: newAnchorHeader.Number.Uint64(),
		}
		if newAnchor.blockHeight > 0 {
			hd.anchors[newAnchorHeader.ParentHash] = newAnchor
			heap.Push(hd.anchorQueue, newAnchor)
		}

		delete(hd.anchors, anchor.parentHash)
		// Add all headers in the segments as links to this anchor
		var prevLink *Link
		for i := end - 1; i >= start; i-- {
			header := segment.Headers[i]
			link := hd.addHeaderAsLink(header, false /* pesisted */)
			if prevLink == nil {
				newAnchor.links = append(newAnchor.links, link)
			} else {
				prevLink.next = append(prevLink.next, link)
			}
			prevLink = link
			if !anchorPreverified {
				if _, ok := hd.preverifiedHashes[header.Hash()]; ok {
					hd.markPreverified(link)
				}
			}
		}
		prevLink.next = anchor.links
		anchor.links = nil
		if anchorPreverified {
			// Mark the entire segment as preverified
			hd.markPreverified(prevLink)
		}
	} else {
		return fmt.Errorf("extendDown attachment anchors not found for %x", anchorHeader.Hash())
	}
	return nil
}

// Connect connects some working trees using anchors of some, and a link of another
func (hd *HeaderDownload) connect(segment *ChainSegment, start, end int) error {
	// Find attachment link again
	linkHeader := segment.Headers[end-1]
	// Find attachement anchors again
	anchorHeader := segment.Headers[start]
	attachmentLink, ok1 := hd.getLink(linkHeader.ParentHash)
	if !ok1 {
		return fmt.Errorf("connect attachment link not found for %x", linkHeader.ParentHash)
	}
	if attachmentLink.preverified && len(attachmentLink.next) > 0 {
		return fmt.Errorf("cannot connect to preverified link %d with children", attachmentLink.blockHeight)
	}
	anchor, ok2 := hd.anchors[anchorHeader.Hash()]
	if !ok2 {
		return fmt.Errorf("connect attachment anchors not found for %x", anchorHeader.Hash())
	}
	anchorPreverified := false
	for _, link := range anchor.links {
		if link.preverified {
			anchorPreverified = true
			break
		}
	}
	delete(hd.anchors, anchor.parentHash)
	// Iterate over headers backwards (from parents towards children)
	prevLink := attachmentLink
	for i := end - 1; i >= start; i-- {
		header := segment.Headers[i]
		link := hd.addHeaderAsLink(header, false /* persisted */)
		prevLink.next = append(prevLink.next, link)
		prevLink = link
		if !anchorPreverified {
			if _, ok := hd.preverifiedHashes[header.Hash()]; ok {
				hd.markPreverified(link)
			}
		}
	}
	prevLink.next = anchor.links
	anchor.links = nil
	if anchorPreverified {
		// Mark the entire segment as preverified
		hd.markPreverified(prevLink)
	}
	if attachmentLink.persisted {
		link := hd.links[linkHeader.Hash()]
		hd.insertList = append(hd.insertList, link)
	}
	return nil
}

func (hd *HeaderDownload) newAnchor(segment *ChainSegment, start, end int) error {
	anchorHeader := segment.Headers[end-1]

	if anchorHeader.Number.Uint64() < hd.highestInDb {
		return fmt.Errorf("new anchor too far in the past: %d, latest header in db: %d", anchorHeader.Number.Uint64(), hd.highestInDb)
	}
	if len(hd.anchors) >= hd.anchorLimit {
		return fmt.Errorf("too many anchors: %d, limit %d", len(hd.anchors), hd.anchorLimit)
	}
	anchor := &Anchor{
		parentHash:  anchorHeader.ParentHash,
		timestamp:   0,
		blockHeight: anchorHeader.Number.Uint64(),
	}
	hd.anchors[anchorHeader.ParentHash] = anchor
	heap.Push(hd.anchorQueue, anchor)
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
		if _, ok := hd.preverifiedHashes[header.Hash()]; ok {
			hd.markPreverified(link)
		}
	}
	return nil
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
	case "mainnet":
		encodings = mainnetPreverifiedHashes
		height = mainnetPreverifiedHeight
	case "ropsten":
		encodings = ropstenPreverifiedHashes
		height = ropstenPreverifiedHeight
	default:
		log.Error("Preverified hashes not found for", "chain", chain)
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

func (hd *HeaderDownload) RecoverFromDb(db ethdb.Database) error {
	err := db.(ethdb.HasKV).KV().View(context.Background(), func(tx ethdb.Tx) error {
		c := tx.Cursor(dbutils.HeaderPrefix)
		// Take hd.persistedLinkLimit headers (with the highest heights) as links
		for k, v, err := c.Last(); k != nil && hd.persistedLinkQueue.Len() < hd.persistedLinkLimit; k, v, err = c.Prev() {
			if err != nil {
				return err
			}
			if len(k) != 40 {
				continue
			}
			var h types.Header
			if err = rlp.DecodeBytes(v, &h); err != nil {
				return err
			}
			hd.addHeaderAsLink(&h, true /* persisted */)
		}
		return nil
	})
	if err != nil {
		return err
	}
	hd.highestInDb, err = stages.GetStageProgress(db, stages.Headers)
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

func (hd *HeaderDownload) RequestMoreHeaders(currentTime uint64) *HeaderRequest {
	hd.lock.Lock()
	defer hd.lock.Unlock()

	if hd.anchorQueue.Len() == 0 {
		log.Debug("Empty anchor queue")
		return nil
	}
	for hd.anchorQueue.Len() > 0 {
		anchor := (*hd.anchorQueue)[0]
		if _, ok := hd.anchors[anchor.parentHash]; ok {
			if anchor.timestamp <= currentTime {
				if anchor.timeouts < 10 {
					return &HeaderRequest{Hash: anchor.parentHash, Number: anchor.blockHeight - 1, Length: 192, Skip: 0, Reverse: true}
				} else {
					// Ancestors of this anchor seem to be unavailable, invalidate and move on
					hd.invalidateAnchor(anchor)
				}
			} else {
				// Anchor not ready for re-request yet
				return nil
			}
		}
		// Anchor disappered or unavailble, pop from the queue and move on
		heap.Remove(hd.anchorQueue, 0)
	}
	return nil
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
	heap.Fix(hd.anchorQueue, 0)
}

func (hd *HeaderDownload) RequestSkeleton() *HeaderRequest {
	hd.lock.RLock()
	defer hd.lock.RUnlock()
	if len(hd.anchors) > 16 {
		return nil // Need to be below anchor threshold to produce skeleton request
	}
	stride := uint64(8 * 192)
	return &HeaderRequest{Number: hd.highestInDb + stride, Length: 192, Skip: stride, Reverse: false}
}

func (hd *HeaderDownload) InsertHeaders(hf func(header *types.Header, blockHeight uint64) error) error {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	for len(hd.insertList) > 0 {
		link := hd.insertList[len(hd.insertList)-1]
		if link.blockHeight <= hd.preverifiedHeight && !link.preverified {
			// Header should be preverified, but not yet, try again later
			break
		}
		hd.insertList = hd.insertList[:len(hd.insertList)-1]
		if _, ok := hd.links[link.hash]; ok {
			heap.Remove(hd.linkQueue, link.idx)
		}
		if !link.preverified {
			if err := hd.engine.VerifyHeader(hd.headerReader, link.header, true /* seal */); err != nil {
				log.Error("Verification failed for header", "hash", link.header.Hash(), "height", link.blockHeight, "error", err)
				// skip this link and its children
				continue
			}
		}
		if err := hf(link.header, link.blockHeight); err != nil {
			return err
		}
		if link.blockHeight > hd.highestInDb {
			hd.highestInDb = link.blockHeight
		}
		link.persisted = true
		heap.Push(hd.persistedLinkQueue, link)
		if len(link.next) > 0 {
			hd.insertList = append(hd.insertList, link.next...)
		}
	}
	for hd.persistedLinkQueue.Len() > hd.persistedLinkLimit {
		link := heap.Pop(hd.persistedLinkQueue).(*Link)
		delete(hd.links, link.hash)
	}
	return nil
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

func (hi *HeaderInserter) FeedHeader(header *types.Header, blockHeight uint64) error {
	hash := header.Hash()
	if hash == hi.prevHash {
		// Skip duplicates
		return nil
	}
	if blockHeight < hi.prevHeight {
		return fmt.Errorf("[%s] headers are unexpectedly unsorted, got %d after %d", hi.logPrefix, blockHeight, hi.prevHeight)
	}
	if oldH := rawdb.ReadHeader(hi.batch, hash, blockHeight); oldH != nil {
		// Already inserted, skip
		return nil
	}
	// Load parent header
	parent := rawdb.ReadHeader(hi.batch, header.ParentHash, blockHeight-1)
	if parent == nil {
		log.Error(fmt.Sprintf("Could not find parent with hash %x and height %d for header %x %d", header.ParentHash, blockHeight-1, hash, blockHeight))
		// Skip headers without parents
		return nil
	}
	// Parent's total difficulty
	parentTd, err := rawdb.ReadTd(hi.batch, header.ParentHash, blockHeight-1)
	if err != nil {
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
		ch, err1 := rawdb.ReadCanonicalHash(hi.batch, blockHeight-1)
		if err1 != nil {
			return fmt.Errorf("reading canonical hash for height %d: %w", blockHeight-1, err1)
		}
		if ch == (common.Hash{}) || ch == header.ParentHash {
			forkingPoint = blockHeight - 1
		} else {
			// Going further back
			ancestorHash := parent.ParentHash
			ancestorHeight := blockHeight - 2
			for ch, err = rawdb.ReadCanonicalHash(hi.batch, ancestorHeight); err == nil && ch != ancestorHash; ch, err = rawdb.ReadCanonicalHash(hi.batch, ancestorHeight) {
				ancestor := rawdb.ReadHeader(hi.batch, ancestorHash, ancestorHeight)
				ancestorHash = ancestor.ParentHash
				ancestorHeight--
			}
			if err != nil {
				return fmt.Errorf("[%s] reading canonical hash for %d: %w", hi.logPrefix, ancestorHeight, err)
			}
			// Loop above terminates when either err != nil (handled already) or ch == ancestorHash, therefore ancestorHeight is our forking point
			forkingPoint = ancestorHeight
		}
		if err = rawdb.WriteHeadHeaderHash(hi.batch, hash); err != nil {
			return fmt.Errorf("[%s] marking head header hash as %x: %w", hi.logPrefix, hash, err)
		}
		hi.headerProgress = blockHeight
		if err = stages.SaveStageProgress(hi.batch, stages.Headers, blockHeight); err != nil {
			return fmt.Errorf("[%s] saving Headers progress: %w", hi.logPrefix, err)
		}
		// See if the forking point affects the unwindPoint (the block number to which other stages will need to unwind before the new canonical chain is applied)
		if forkingPoint < hi.unwindPoint {
			hi.unwindPoint = forkingPoint
		}
	}
	data, err2 := rlp.EncodeToBytes(header)
	if err2 != nil {
		return fmt.Errorf("[%s] failed to RLP encode header: %w", hi.logPrefix, err2)
	}
	if err = rawdb.WriteTd(hi.batch, hash, blockHeight, td); err != nil {
		return fmt.Errorf("[%s] failed to WriteTd: %w", hi.logPrefix, err)
	}
	if err = hi.batch.Put(dbutils.HeaderPrefix, dbutils.HeaderKey(blockHeight, hash), data); err != nil {
		return fmt.Errorf("[%s] failed to store header: %w", hi.logPrefix, err)
	}
	hi.prevHash = hash
	if blockHeight > hi.highest {
		hi.highest = blockHeight
		hi.highestHash = hash
	}
	return nil
}

func (hi *HeaderInserter) GetHighest() uint64 {
	return hi.highest
}

func (hi *HeaderInserter) GetHighestHash() common.Hash {
	return hi.highestHash
}

func (hi *HeaderInserter) UnwindPoint() uint64 {
	return hi.unwindPoint
}

func (hi *HeaderInserter) AnythingDone() bool {
	return hi.newCanonical
}

//nolint:interfacer
func (hd *HeaderDownload) ProcessSegment(segment *ChainSegment, newBlock bool) {
	log.Debug("processSegment", "from", segment.Headers[0].Number.Uint64(), "to", segment.Headers[len(segment.Headers)-1].Number.Uint64())
	hd.lock.Lock()
	defer hd.lock.Unlock()
	foundAnchor, start := hd.findAnchors(segment)
	foundTip, end := hd.findLink(segment, start) // We ignore penalty because we will check it as part of PoW check
	if end == 0 {
		log.Debug("Duplicate segment")
		return
	}
	if newBlock {
		height := segment.Headers[len(segment.Headers)-1].Number.Uint64()
		if height > hd.topSeenHeight {
			hd.topSeenHeight = segment.Headers[len(segment.Headers)-1].Number.Uint64()
		}
	}
	startNum := segment.Headers[start].Number.Uint64()
	endNum := segment.Headers[end-1].Number.Uint64()
	// There are 4 cases
	if foundAnchor {
		if foundTip {
			// Connect
			if err := hd.connect(segment, start, end); err != nil {
				log.Error("Connect failed", "error", err)
				return
			}
			log.Debug("Connected", "start", startNum, "end", endNum)
		} else {
			// ExtendDown
			if err := hd.extendDown(segment, start, end); err != nil {
				log.Error("ExtendDown failed", "error", err)
				return
			}
			log.Debug("Extended Down", "start", startNum, "end", endNum)
		}
	} else if foundTip {
		if end > 0 {
			// ExtendUp
			if err := hd.extendUp(segment, start, end); err != nil {
				log.Error("ExtendUp failed", "error", err)
				return
			}
			log.Debug("Extended Up", "start", startNum, "end", endNum)
		}
	} else {
		// NewAnchor
		if err := hd.newAnchor(segment, start, end); err != nil {
			log.Error("NewAnchor failed", "error", err)
			return
		}
		log.Debug("NewAnchor", "start", startNum, "end", endNum)
	}
	//log.Info(hd.anchorState())
	log.Debug("Link queue", "size", hd.linkQueue.Len())
	if hd.linkQueue.Len() > hd.linkLimit {
		log.Debug("Too many links, cutting down", "count", hd.linkQueue.Len(), "tried to add", end-start, "limit", hd.linkLimit)
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
}

func (hd *HeaderDownload) TopSeenHeight() uint64 {
	hd.lock.RLock()
	defer hd.lock.RUnlock()
	return hd.topSeenHeight
}

func (hd *HeaderDownload) InSync() bool {
	hd.lock.RLock()
	defer hd.lock.RUnlock()
	return hd.highestInDb >= hd.preverifiedHeight && hd.topSeenHeight > 0 && hd.highestInDb >= hd.topSeenHeight
}

func (hd *HeaderDownload) SetHeaderReader(headerReader consensus.ChainHeaderReader) {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	hd.headerReader = headerReader
}
