package forkchoice

import (
	"errors"
	"fmt"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

// IsBidCompatibleWithHead evaluates the gossip bid-parent rule against a coherent head root.
func (f *ForkChoiceStore) IsBidCompatibleWithHead(bid *cltypes.ExecutionPayloadBid) (bool, error) {
	if bid == nil {
		return false, errors.New("nil execution payload bid")
	}
	headNode, err := f.GetHeadNode()
	if err != nil {
		return false, fmt.Errorf("head unavailable: %w", err)
	}
	headRoot := headNode.Root
	headHeader, ok := f.GetHeader(headRoot)
	if !ok || headHeader == nil {
		return false, errors.New("head block header unavailable")
	}
	headBlock, hasBlock := f.GetBlock(headRoot)
	if hasBlock && headBlock != nil && headBlock.Block != nil && headBlock.Block.Body != nil {
		signedHeadBid := headBlock.Block.Body.GetSignedExecutionPayloadBid()
		if signedHeadBid != nil && signedHeadBid.Message != nil {
			return BidCompatibleWithHead(bid, headRoot, headHeader, signedHeadBid.Message, f.ShouldBuildOnFull(headNode, bid.Slot)), nil
		}
		if headPayload := headBlock.Block.Body.ExecutionPayload; headPayload != nil {
			return bid.ParentBlockRoot == headRoot && bid.ParentBlockHash == headPayload.BlockHash, nil
		}
	}
	headState, err := f.GetStateAtBlockRoot(headRoot, true)
	if err != nil {
		return false, fmt.Errorf("head state unavailable: %w", err)
	}
	if headState == nil {
		return false, errors.New("head state unavailable")
	}
	if headState.Version() >= clparams.GloasVersion {
		headBid := headState.GetLatestExecutionPayloadBid()
		if headBid == nil {
			return false, errors.New("head bid unavailable")
		}
		return BidCompatibleWithHead(bid, headRoot, headHeader, headBid, f.ShouldBuildOnFull(headNode, bid.Slot)), nil
	}
	headPayload := headState.LatestExecutionPayloadHeader()
	if headPayload == nil {
		return false, errors.New("head execution payload unavailable")
	}
	return bid.ParentBlockRoot == headRoot && bid.ParentBlockHash == headPayload.BlockHash, nil
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
