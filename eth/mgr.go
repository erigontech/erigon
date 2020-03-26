package eth

import (
	"github.com/ledgerwatch/turbo-geth/p2p"
)

// MGR (aka Merry-Go-Round) protocol - providing capabilities of swarm-based-full-sync
// At a high level, MGR operates by enumerating the full state in a predetermined order
// and gossiping this data among the clients which are actively syncing.
// For a client to fully sync it needs to “ride” one full rotation of the merry-go-round.

const (
	mgr1 = 1
)

var MGRName = "mgr" // Parity only supports 3 letter capabilities
var MGRVersions = []uint{mgr1}
var MGRLengths = map[uint]uint64{mgr1: 2}

const MGRMaxMsgSize = 10 * 1024 * 1024

const (
	MGRStatus  = 0x00
	MGRWitness = 0x01
)

type mgrPeer struct {
	*p2p.Peer
	rw p2p.MsgReadWriter
}

// SendByteCode sends a BytecodeCode message.
func (p *mgrPeer) SendByteCode(id uint64, data [][]byte) error {
	msg := bytecodeMsg{ID: id, Code: data}
	return p2p.Send(p.rw, BytecodeCode, msg)
}
