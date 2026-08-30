package forkchoice

import (
	"context"
	"errors"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

// IsBuilderBidCompatibleWithHead applies the gossip bid-parent rule using only bounded in-memory head data.
func (f *ForkChoiceStore) IsBuilderBidCompatibleWithHead(ctx context.Context, bid *cltypes.ExecutionPayloadBid) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if bid == nil {
		return false, errors.New("nil execution payload bid")
	}
	headNode, ok := f.cachedHeadNode()
	if !ok {
		return false, errors.New("head unavailable")
	}
	headRoot := headNode.Root
	headHeader, ok := f.GetHeader(headRoot)
	if !ok || headHeader == nil {
		return false, errors.New("head block header unavailable")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	headBlock, hasBlock := f.GetBlock(headRoot)
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !hasBlock || headBlock == nil || headBlock.Block == nil || headBlock.Block.Body == nil {
		return false, errors.New("head block unavailable")
	}
	finalHeadNode, ok := f.cachedHeadNode()
	if !ok || finalHeadNode != headNode {
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	signedHeadBid := headBlock.Block.Body.GetSignedExecutionPayloadBid()
	if signedHeadBid != nil && signedHeadBid.Message != nil {
		buildOnFull := f.ShouldBuildOnFull(finalHeadNode, bid.Slot)
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if latestHeadNode, ok := f.cachedHeadNode(); !ok || latestHeadNode != finalHeadNode {
			return false, nil
		}
		return BidCompatibleWithHead(bid, headRoot, headHeader, signedHeadBid.Message, buildOnFull), nil
	}
	headPayload := headBlock.Block.Body.ExecutionPayload
	if headPayload == nil {
		return false, errors.New("head payload unavailable")
	}
	return bid.ParentBlockRoot == headRoot && bid.ParentBlockHash == headPayload.BlockHash, nil
}

func (f *ForkChoiceStore) cachedHeadNode() (ForkChoiceNode, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.headHash == (common.Hash{}) {
		return ForkChoiceNode{}, false
	}
	return ForkChoiceNode{Root: f.headHash, PayloadStatus: f.headPayloadStatus}, true
}

// BidCompatibleWithHead reports whether a bid builds on the head or its parent under the Gloas gossip rule.
func BidCompatibleWithHead(bid *cltypes.ExecutionPayloadBid, headRoot common.Hash, headHeader *cltypes.BeaconBlockHeader, headBid *cltypes.ExecutionPayloadBid, buildOnFull bool) bool {
	if bid == nil || headHeader == nil || headBid == nil {
		return false
	}
	buildsOnParentBlock := bid.ParentBlockRoot == headHeader.ParentRoot
	buildsOnParentPayload := bid.ParentBlockHash == headBid.ParentBlockHash
	if buildsOnParentBlock && buildsOnParentPayload {
		return true
	}
	if bid.ParentBlockRoot != headRoot {
		return false
	}
	if buildOnFull {
		return bid.ParentBlockHash == headBid.BlockHash
	}
	return buildsOnParentPayload
}
