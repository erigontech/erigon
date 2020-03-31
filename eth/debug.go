package eth

import (
	"github.com/ledgerwatch/turbo-geth/p2p"
)

const (
	dbg1 = 1
)

const DebugName = "dbg" // Parity only supports 3 letter capabilities
var DebugVersions = []uint{dbg1}
var DebugLengths = map[uint]uint64{dbg1: 2}

const DebugMaxMsgSize = 10 * 1024 * 1024

// Debug customization for simulator, move it to sub-protocol
const (
	DebugSetGenesisMsg = 0x00
)

type debugPeer struct {
	*p2p.Peer
	rw p2p.MsgReadWriter
}

// SendByteCode sends a BytecodeCode message.
func (p *debugPeer) SendByteCode(id uint64, data [][]byte) error {
	msg := bytecodeMsg{ID: id, Code: data}
	return p2p.Send(p.rw, BytecodeCode, msg)
}
