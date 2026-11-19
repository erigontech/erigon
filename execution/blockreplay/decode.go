package blockreplay

import (
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

// Block decodes the fixture's block and re-attaches the recovered senders (RLP
// does not carry them).
func (fx *Fixture) Block() (*types.Block, error) {
	b, err := rlpDecodeBlock(fx.BlockRLP)
	if err != nil {
		return nil, err
	}
	if len(fx.Senders) > 0 {
		senders := make([]common.Address, len(fx.Senders))
		for i, s := range fx.Senders {
			senders[i] = common.Address(s)
		}
		b.SendersToTxs(senders)
	}
	return b, nil
}

// ParentHeader decodes the fixture's parent header.
func (fx *Fixture) ParentHeader() (*types.Header, error) {
	return rlpDecodeHeader(fx.ParentHeaderRLP)
}

// SendersList returns the recovered tx senders as common.Address.
func (fx *Fixture) SendersList() []common.Address {
	senders := make([]common.Address, len(fx.Senders))
	for i, s := range fx.Senders {
		senders[i] = common.Address(s)
	}
	return senders
}
