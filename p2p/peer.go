// Copyright 2014 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package p2p

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/mclock"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
	"github.com/erigontech/erigon/p2p/event"
)

var (
	ErrShuttingDown = errors.New("shutting down")
)

const (
	baseProtocolVersion    = 5
	baseProtocolLength     = uint64(16)
	baseProtocolMaxMsgSize = 2 * 1024

	snappyProtocolVersion = 5

	pingInterval = 15 * time.Second
)

const (
	// devp2p message codes
	handshakeMsg = 0x00
	discMsg      = 0x01
	pingMsg      = 0x02
	pongMsg      = 0x03
)

// protoHandshake is the RLP structure of the protocol handshake.
type protoHandshake struct {
	Version    uint64
	Name       string
	Caps       []Cap
	ListenPort uint64
	Pubkey     []byte // secp256k1 public key

	// Ignore additional fields (for forward compatibility).
	Rest []rlp.RawValue `rlp:"tail"`
}

// PeerEventType is the type of peer events emitted by a p2p.Server
type PeerEventType string

const (
	// PeerEventTypeAdd is the type of event emitted when a peer is added
	// to a p2p.Server
	PeerEventTypeAdd PeerEventType = "add"

	// PeerEventTypeDrop is the type of event emitted when a peer is
	// dropped from a p2p.Server
	PeerEventTypeDrop PeerEventType = "drop"

	// PeerEventTypeMsgSend is the type of event emitted when a
	// message is successfully sent to a peer
	PeerEventTypeMsgSend PeerEventType = "msgsend"

	// PeerEventTypeMsgRecv is the type of event emitted when a
	// message is received from a peer
	PeerEventTypeMsgRecv PeerEventType = "msgrecv"
)

// PeerEvent is an event emitted when peers are either added or dropped from
// a p2p.Server or when a message is sent or received on a peer connection
type PeerEvent struct {
	Type          PeerEventType `json:"type"`
	Peer          enode.ID      `json:"peer"`
	Error         string        `json:"error,omitempty"`
	Protocol      string        `json:"protocol,omitempty"`
	MsgCode       *uint64       `json:"msg_code,omitempty"`
	MsgSize       *uint32       `json:"msg_size,omitempty"`
	LocalAddress  string        `json:"local,omitempty"`
	RemoteAddress string        `json:"remote,omitempty"`
}

// Peer represents a connected remote node.
type Peer struct {
	rw      *conn
	running map[string]*protoRW
	log     log.Logger
	created mclock.AbsTime

	wg       sync.WaitGroup
	protoErr chan *PeerError
	closed   chan struct{}
	pingRecv chan struct{}
	disc     chan *PeerError

	// events receives message send / receive events if set
	events         *event.Feed
	pubkey         [64]byte
	metricsEnabled bool
}

// NewPeer returns a peer for testing purposes.
func NewPeer(id enode.ID, pubkey [64]byte, name string, caps []Cap, metricsEnabled bool) *Peer {
	pipe, _ := net.Pipe()
	node := enode.SignNull(new(enr.Record), id)
	conn := &conn{fd: pipe, transport: nil, node: node, caps: caps, name: name}
	peer := newPeer(log.Root(), conn, nil, pubkey, metricsEnabled)
	close(peer.closed) // ensures Disconnect doesn't block
	return peer
}

// ID returns the node's unique identifier.
func (p *Peer) ID() enode.ID {
	return p.rw.node.ID()
}

func (p *Peer) Pubkey() [64]byte {
	return p.pubkey
}

// Node returns the peer's node descriptor.
func (p *Peer) Node() *enode.Node {
	return p.rw.node
}

// Name returns an abbreviated form of the name
func (p *Peer) Name() string {
	s := p.rw.name
	if len(s) > 20 {
		return s[:20] + "..."
	}
	return s
}

// Fullname returns the node name that the remote node advertised.
func (p *Peer) Fullname() string {
	return p.rw.name
}

// Caps returns the capabilities (supported subprotocols) of the remote peer.
func (p *Peer) Caps() []Cap {
	// TODO: maybe return copy
	return p.rw.caps
}

// RunningCap returns true if the peer is actively connected using any of the
// enumerated versions of a specific protocol, meaning that at least one of the
// versions is supported by both this node and the peer p.
func (p *Peer) RunningCap(protocol string, versions []uint) bool {
	if proto, ok := p.running[protocol]; ok {
		for _, ver := range versions {
			if proto.Version == ver {
				return true
			}
		}
	}
	return false
}

// RemoteAddr returns the remote address of the network connection.
func (p *Peer) RemoteAddr() net.Addr {
	return p.rw.fd.RemoteAddr()
}

// LocalAddr returns the local address of the network connection.
func (p *Peer) LocalAddr() net.Addr {
	return p.rw.fd.LocalAddr()
}

// Disconnect terminates the peer connection with the given reason.
// It returns immediately and does not wait until the connection is closed.
func (p *Peer) Disconnect(err *PeerError) {
	select {
	case p.disc <- err:
	case <-p.closed:
	}
}

// String implements fmt.Stringer.
func (p *Peer) String() string {
	id := p.ID()
	return fmt.Sprintf("Peer %x %v", id[:8], p.RemoteAddr())
}

// Inbound returns true if the peer is an inbound connection
func (p *Peer) Inbound() bool {
	return p.rw.is(inboundConn)
}

func newPeer(logger log.Logger, conn *conn, protocols []Protocol, pubkey [64]byte, metricsEnabled bool) *Peer {
	log := logger.New("id", conn.node.ID(), "conn", conn.flags)

	protomap := matchProtocols(protocols, conn.caps, conn, log)
	p := &Peer{
		rw:             conn,
		running:        protomap,
		created:        mclock.Now(),
		disc:           make(chan *PeerError),
		protoErr:       make(chan *PeerError, len(protomap)+1), // protocols + pingLoop
		closed:         make(chan struct{}),
		pingRecv:       make(chan struct{}, 16),
		log:            log,
		pubkey:         pubkey,
		metricsEnabled: metricsEnabled,
	}
	return p
}

func (p *Peer) Log() log.Logger {
	return p.log
}

func makeFirstCharCap(input string) string {
	// Convert the entire string to lowercase
	input = strings.ToLower(input)
	// Use strings.Title to capitalize the first letter of each word
	input = strings.ToUpper(input[:1]) + input[1:]
	return input
}

func convertToCamelCase(input string) string {
	parts := strings.Split(input, "_")
	if len(parts) == 1 {
		return input
	}

	var result string

	for _, part := range parts {
		if len(part) > 0 && part != parts[len(parts)-1] {
			result += makeFirstCharCap(part)
		}
	}

	return result
}

func (p *Peer) run() (peerErr *PeerError) {
	var (
		writeStart = make(chan struct{}, 1)
		writeErr   = make(chan error, 1)
		readErr    = make(chan error, 1)
	)

	p.wg.Add(2)
	go p.readLoop(readErr)
	go p.pingLoop()

	// Start all protocol handlers.
	writeStart <- struct{}{}
	p.startProtocols(writeStart, writeErr)

	defer func() {
		close(p.closed)
		p.rw.close(peerErr.Reason)
		p.wg.Wait()
	}()

	// Wait for an error or disconnect.
	for {
		select {
		case err := <-writeErr:
			if err != nil {
				return NewPeerError(PeerErrorDiscReason, DiscNetworkError, err, "Peer.run writeErr")
			}
			// Allow the next write to start if there was no error.
			writeStart <- struct{}{}
		case err := <-readErr:
			if reason, ok := err.(DiscReason); ok {
				return NewPeerError(PeerErrorDiscReasonRemote, reason, nil, "Peer.run got a remote DiscReason")
			} else {
				return NewPeerError(PeerErrorDiscReason, DiscNetworkError, err, "Peer.run readErr")
			}
		case err := <-p.protoErr:
			return err
		case err := <-p.disc:
			return err
		}
	}
}

func (p *Peer) pingLoop() {
	defer dbg.LogPanic()
	ping := time.NewTimer(pingInterval)
	defer p.wg.Done()
	defer ping.Stop()
	for {
		select {
		case <-ping.C:
			if err := SendItems(p.rw, pingMsg); err != nil {
				p.protoErr <- NewPeerError(PeerErrorPingFailure, DiscNetworkError, err, "Failed to send pingMsg")
				return
			}
			ping.Reset(pingInterval)
		case <-p.pingRecv:
			if err := SendItems(p.rw, pongMsg); err != nil {
				p.protoErr <- NewPeerError(PeerErrorPongFailure, DiscNetworkError, err, "Failed to send pongMsg")
				return
			}
		case <-p.closed:
			return
		}
	}
}

func (p *Peer) readLoop(errc chan<- error) {
	defer dbg.LogPanic()
	defer p.wg.Done()
	for {
		msg, err := p.rw.ReadMsg()
		if err != nil {
			errc <- err
			return
		}

		if err = p.handle(msg); err != nil {
			errc <- err
			return
		}
	}
}

func (p *Peer) handle(msg Msg) error {
	switch {
	case msg.Code == pingMsg:
		msg.Discard()
		select {
		case p.pingRecv <- struct{}{}:
		case <-p.closed:
		}
	case msg.Code == discMsg:
		// This is the last message.
		// We don't need to discard because the connection will be closed after it.
		reason, err := DisconnectMessagePayloadDecode(msg.Payload)
		if err != nil {
			p.log.Debug("Peer.handle: failed to rlp.Decode msg.Payload", "err", err)
		}
		return reason
	case msg.Code < baseProtocolLength:
		// ignore other base protocol messages
		msg.Discard()
		return nil
	default:
		// it's a subprotocol message
		proto, err := p.getProto(msg.Code)
		if err != nil {
			return fmt.Errorf("msg code out of range: %v", msg.Code)
		}

		if p.metricsEnabled {
			m := fmt.Sprintf("%s_%s_%d_%#02x", ingressMeterName, proto.Name, proto.Version, msg.Code-proto.offset)
			metrics.GetOrCreateGauge(m).SetUint32(msg.meterSize)
			metrics.GetOrCreateGauge(m + "_packets").Set(1)
		}
		select {
		case proto.in <- msg:
			return nil
		case <-p.closed:
			return io.EOF
		}
	}
	return nil
}

func countMatchingProtocols(protocols []Protocol, caps []Cap) int {
	n := 0
	for _, cap := range caps {
		for _, proto := range protocols {
			if proto.Name == cap.Name && proto.Version == cap.Version {
				n++
			}
		}
	}
	return n
}

// matchProtocols creates structures for matching named subprotocols.
func matchProtocols(protocols []Protocol, caps []Cap, rw MsgReadWriter, logger log.Logger) map[string]*protoRW {
	sort.Sort(capsByNameAndVersion(caps))
	offset := baseProtocolLength
	result := make(map[string]*protoRW)

outer:
	for _, cap := range caps {
		for _, proto := range protocols {
			if proto.Name == cap.Name && proto.Version == cap.Version {
				// If an old protocol version matched, revert it
				if old := result[cap.Name]; old != nil {
					offset -= old.Length
				}
				// Assign the new match
				result[cap.Name] = &protoRW{Protocol: proto, offset: offset, in: make(chan Msg), w: rw, logger: logger}
				offset += proto.Length

				continue outer
			}
		}
	}
	return result
}

func (p *Peer) startProtocols(writeStart <-chan struct{}, writeErr chan<- error) {
	p.wg.Add(len(p.running))
	for _, proto := range p.running {
		proto := proto
		proto.closed = p.closed
		proto.wstart = writeStart
		proto.werr = writeErr
		var rw MsgReadWriter = proto
		if p.events != nil {
			rw = newMsgEventer(rw, p.events, p.ID(), proto.Name, p.RemoteAddr().String(), p.LocalAddr().String())
		}
		p.log.Trace(fmt.Sprintf("Starting protocol %s/%d", proto.Name, proto.Version))
		go func() {
			defer dbg.LogPanic()
			defer p.wg.Done()
			err := proto.Run(p, rw)
			// only unit test protocols can return nil
			if err == nil {
				err = NewPeerError(PeerErrorTest, DiscQuitting, nil, fmt.Sprintf("Protocol %s/%d returned", proto.Name, proto.Version))
			}
			p.protoErr <- err
		}()
	}
}

// getProto finds the protocol responsible for handling
// the given message code.
func (p *Peer) getProto(code uint64) (*protoRW, error) {
	for _, proto := range p.running {
		if code >= proto.offset && code < proto.offset+proto.Length {
			return proto, nil
		}
	}
	return nil, NewPeerError(PeerErrorInvalidMessageCode, DiscProtocolError, nil, fmt.Sprintf("code=%d", code))
}

type protoRW struct {
	Protocol
	in     chan Msg        // receives read messages
	closed <-chan struct{} // receives when peer is shutting down
	wstart <-chan struct{} // receives when write may start
	werr   chan<- error    // for write results
	offset uint64
	w      MsgWriter
	logger log.Logger
}

var traceMsg = false

func (rw *protoRW) WriteMsg(msg Msg) (err error) {
	if msg.Code >= rw.Length {
		return NewPeerError(PeerErrorInvalidMessageCode, DiscProtocolError, nil, fmt.Sprintf("not handled code=%d", msg.Code))
	}

	msg.meterCap = rw.cap()
	msg.meterCode = msg.Code
	msg.Code += rw.offset

	select {
	case <-rw.wstart:
		err = rw.w.WriteMsg(msg)

		if traceMsg {
			if err != nil {
				rw.logger.Trace("Write failed", "cap", rw.cap(), "msg", msg.Code-rw.offset, "size", msg.Size, "err", err)
			} else {
				rw.logger.Trace("Wrote", "cap", rw.cap(), "msg", msg.Code-rw.offset, "size", msg.Size)
			}
		}

		// Report write status back to Peer.run. It will initiate
		// shutdown if the error is non-nil and unblock the next write
		// otherwise. The calling protocol code should exit for errors
		// as well but we don't want to rely on that.
		rw.werr <- err
	case <-rw.closed:
		err = ErrShuttingDown
	}
	return err
}

func (rw *protoRW) ReadMsg() (Msg, error) {

	select {
	case msg := <-rw.in:
		msg.Code -= rw.offset
		if traceMsg {
			rw.logger.Trace("Read", "cap", rw.cap(), "msg", msg.Code, "size", msg.Size)
		}
		return msg, nil
	case <-rw.closed:
		return Msg{}, io.EOF
	}
}

// PeerInfo represents a short summary of the information known about a connected
// peer. Sub-protocol independent fields are contained and initialized here, with
// protocol specifics delegated to all connected sub-protocols.
type PeerInfo struct {
	ENR     string   `json:"enr,omitempty"` // Ethereum Node Record
	Enode   string   `json:"enode"`         // Node URL
	ID      string   `json:"id"`            // Unique node identifier
	Name    string   `json:"name"`          // Name of the node, including client type, version, OS, custom data
	Caps    []string `json:"caps"`          // Protocols advertised by this peer
	Network struct {
		LocalAddress  string `json:"localAddress"`  // Local endpoint of the TCP data connection
		RemoteAddress string `json:"remoteAddress"` // Remote endpoint of the TCP data connection
		Inbound       bool   `json:"inbound"`
		Trusted       bool   `json:"trusted"`
		Static        bool   `json:"static"`
	} `json:"network"`
	Protocols map[string]interface{} `json:"protocols"` // Sub-protocol specific metadata fields
}

// Info gathers and returns a collection of metadata known about a peer.
func (p *Peer) Info() *PeerInfo {
	// Gather the protocol capabilities
	caps := make([]string, 0, len(p.Caps()))
	for _, cap := range p.Caps() {
		caps = append(caps, cap.String())
	}
	// Assemble the generic peer metadata
	info := &PeerInfo{
		Enode:     p.Node().URLv4(),
		ID:        hex.EncodeToString(p.pubkey[:]),
		Name:      p.Fullname(),
		Caps:      caps,
		Protocols: make(map[string]interface{}),
	}
	if p.Node().Seq() > 0 {
		info.ENR = p.Node().String()
	}
	info.Network.LocalAddress = p.LocalAddr().String()
	info.Network.RemoteAddress = p.RemoteAddr().String()
	info.Network.Inbound = p.rw.is(inboundConn)
	info.Network.Trusted = p.rw.is(trustedConn)
	info.Network.Static = p.rw.is(staticDialedConn)

	// Gather all the running protocol infos
	for _, proto := range p.running {
		protoInfo := interface{}("unknown")
		if query := proto.Protocol.PeerInfo; query != nil {
			if metadata := query(p.Pubkey()); metadata != nil {
				protoInfo = metadata
			} else {
				protoInfo = "handshake"
			}
		}
		info.Protocols[proto.Name] = protoInfo
	}
	return info
}
