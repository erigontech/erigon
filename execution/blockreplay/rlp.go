package blockreplay

import (
	"bytes"

	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

func rlpEncode(b *types.Block) ([]byte, error) {
	var buf bytes.Buffer
	if err := b.EncodeRLP(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func rlpDecodeBlock(data []byte) (*types.Block, error) {
	b := &types.Block{}
	if err := rlp.DecodeBytes(data, b); err != nil {
		return nil, err
	}
	return b, nil
}

func rlpEncodeHeader(h *types.Header) ([]byte, error) {
	var buf bytes.Buffer
	if err := h.EncodeRLP(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func rlpDecodeHeader(data []byte) (*types.Header, error) {
	h := &types.Header{}
	if err := rlp.DecodeBytes(data, h); err != nil {
		return nil, err
	}
	return h, nil
}
