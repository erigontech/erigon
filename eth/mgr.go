package eth

import (
	"io"
	"sync"

	"github.com/ledgerwatch/turbo-geth/eth/mgr"
	"github.com/ledgerwatch/turbo-geth/p2p"
)

// MGR (aka Merry-Go-Round) protocol - providing capabilities of swarm-based-full-sync
// At a high level, MGR operates by enumerating the full state in a predetermined order
// and gossiping this data among the clients which are actively syncing.
// For a client to fully sync it needs to “ride” one full rotation of the merry-go-round.

const (
	mgr1 = 1
)

const MGRName = "mgr" // Parity only supports 3 letter capabilities
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

type nodeState interface {
	GetBlockNr() uint64
}

type mgrBroadcast struct {
	lock     sync.RWMutex
	peers    map[string]*peer
	schedule *mgr.Schedule
	state    nodeState
}

func NewMgr(schedule *mgr.Schedule, nodeState nodeState) *mgrBroadcast {
	return &mgrBroadcast{schedule: schedule, state: nodeState}
}

func (m *mgrBroadcast) Start() {
	for {
		block := m.state.GetBlockNr()
		tick, err := m.schedule.Tick(block)
		if err != nil {
			panic(err)
		}
		//witnessCache := map[string][]byte{}
		for m.state.GetBlockNr() <= tick.ToBlock {
			// Produce and Broadcast witness of slice

			//retain := trie.NewRetainRange(common.CopyBytes(slice.From), common.CopyBytes(slice.To))
			//if tick.IsLastInCycle() {
			//	fmt.Printf("\nretain: %s\n", retain)
			//}
			//witness, err2 := tds.Trie().ExtractWitness(false, retain)
			//if err2 != nil {
			//	panic(err2)
			//}
			//
			//buf.Reset()
			//_, err = witness.WriteTo(&buf)
			//if err != nil {
			//	panic(err)
			//}
		}
	}
}

func (m *mgrBroadcast) AddPeer(p *peer) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.peers[p.id] = p
}

func (m *mgrBroadcast) RemovePeer(id string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	delete(m.peers, id)
}

func (m *mgrBroadcast) Peer(id string) *peer {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.peers[id]
}

func (m *mgrBroadcast) Broadcast(witness io.Reader) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	for _, p := range m.peers {
		if err := p.rw.WriteMsg(p2p.Msg{Code: MGRWitness, Size: 0, Payload: witness}); err != nil {
			p.Log().Debug("MGR message sending failed", "err", err)
		}
	}
}
