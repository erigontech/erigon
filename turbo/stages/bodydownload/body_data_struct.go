package bodydownload

import (
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
)

// DoubleHash is type to be used for the mapping between TxHash and UncleHash to the block header
type DoubleHash [2 * common.HashLength]byte

// BodyDownload represents the state of body downloading process
type BodyDownload struct {
	notDownloaded *roaring64.Bitmap
	bodyMap       map[DoubleHash]*types.Header
}

// NewBodyDownload create a new body download state object
func NewBodyDownload() *BodyDownload {
	return &BodyDownload{
		notDownloaded: roaring64.New(),
		bodyMap:       make(map[DoubleHash]*types.Header),
	}
}
