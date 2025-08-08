package shutter

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/transport"
	ma "github.com/multiformats/go-multiaddr"
)

const PipeProtocolCode = 999 // Custom protocol code for pipe transport

type PipeTransport struct {
	mu        sync.RWMutex
	listeners map[string]*PipeListener
	pipes     map[string]*PipePair
}

type PipePair struct {
	conn1 net.Conn
	conn2 net.Conn
}

type PipeListener struct {
	addr     ma.Multiaddr
	acceptCh chan net.Conn
	closeCh  chan struct{}
	closed   bool
	mu       sync.Mutex
}

type PipeConn struct {
	net.Conn
	transport  *PipeTransport
	localAddr  ma.Multiaddr
	remoteAddr ma.Multiaddr
}

func NewPipeTransport() *PipeTransport {
	return &PipeTransport{
		listeners: make(map[string]*PipeListener),
		pipes:     make(map[string]*PipePair),
	}
}

// Transport interface implementation
func (t *PipeTransport) Dial(ctx context.Context, raddr ma.Multiaddr, p peer.ID) (transport.CapableConn, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Extract the pipe identifier from the multiaddr
	pipeId, err := extractPipeId(raddr)
	if err != nil {
		return nil, fmt.Errorf("invalid pipe address: %w", err)
	}

	// Check if there's a listener for this address
	listener, exists := t.listeners[pipeId]
	if !exists {
		return nil, fmt.Errorf("no listener for pipe %s", pipeId)
	}

	// Create a pipe connection
	conn1, conn2 := net.Pipe()

	// Store the pipe pair
	pairKey := fmt.Sprintf("%s-%d", pipeId, time.Now().UnixNano())
	t.pipes[pairKey] = &PipePair{conn1: conn1, conn2: conn2}

	// Send one end to the listener
	select {
	case listener.acceptCh <- conn2:
	case <-listener.closeCh:
		conn1.Close()
		conn2.Close()
		return nil, fmt.Errorf("listener closed")
	case <-ctx.Done():
		conn1.Close()
		conn2.Close()
		return nil, ctx.Err()
	}

	// Create local address for dialer
	localAddr, _ := ma.NewMultiaddr("/pipe/dialer")

	return &PipeCapableConn{
		PipeConn: &PipeConn{
			Conn:       conn1,
			transport:  t,
			localAddr:  localAddr,
			remoteAddr: raddr,
		},
	}, nil
}

func (t *PipeTransport) CanDial(addr ma.Multiaddr) bool {
	protocols := addr.Protocols()
	for _, p := range protocols {
		if p.Code == PipeProtocolCode {
			return true
		}
	}
	return false
}

func (t *PipeTransport) Listen(laddr ma.Multiaddr) (transport.Listener, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	pipeId, err := extractPipeId(laddr)
	if err != nil {
		return nil, fmt.Errorf("invalid pipe address: %w", err)
	}

	if _, exists := t.listeners[pipeId]; exists {
		return nil, fmt.Errorf("already listening on pipe %s", pipeId)
	}

	listener := &PipeListener{
		addr:     laddr,
		acceptCh: make(chan net.Conn, 10),
		closeCh:  make(chan struct{}),
	}

	t.listeners[pipeId] = listener
	return listener, nil
}

func (t *PipeTransport) Protocols() []int {
	return []int{PipeProtocolCode}
}

func (t *PipeTransport) Proxy() bool {
	return false
}

// PipeListener implementation
func (l *PipeListener) Accept() (transport.CapableConn, error) {
	select {
	case conn := <-l.acceptCh:
		return &PipeCapableConn{
			PipeConn: &PipeConn{
				Conn:       conn,
				localAddr:  l.addr,
				remoteAddr: l.addr, // For pipe, local and remote are conceptually the same
			},
		}, nil
	case <-l.closeCh:
		return nil, fmt.Errorf("listener closed")
	}
}

func (l *PipeListener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}

	l.closed = true
	close(l.closeCh)
	return nil
}

func (l *PipeListener) Addr() net.Addr {
	return &PipeAddr{addr: l.addr}
}

func (l *PipeListener) Multiaddr() ma.Multiaddr {
	return l.addr
}

// PipeCapableConn wraps PipeConn to implement transport.CapableConn
type PipeCapableConn struct {
	*PipeConn
}

func (c *PipeCapableConn) CloseWithError(errCode network.ConnErrorCode) error {
	//TODO implement me
	panic("CloseWithError implement me")
}

func (c *PipeCapableConn) IsClosed() bool {
	//TODO implement me
	panic("IsClosed implement me")
}

func (c *PipeCapableConn) OpenStream(ctx context.Context) (network.MuxedStream, error) {
	//TODO implement me
	panic("OpenStream implement me")
}

func (c *PipeCapableConn) AcceptStream() (network.MuxedStream, error) {
	//TODO implement me
	panic("AcceptStream implement me")
}

func (c *PipeCapableConn) LocalPeer() peer.ID {
	//TODO implement me
	panic("LocalPeer implement me")
}

func (c *PipeCapableConn) RemotePeer() peer.ID {
	//TODO implement me
	panic("RemotePeer implement me")
}

func (c *PipeCapableConn) RemotePublicKey() libp2pcrypto.PubKey {
	//TODO implement me
	panic("RemotePublicKey implement me")
}

func (c *PipeCapableConn) ConnState() network.ConnectionState {
	return network.ConnectionState{
		Transport: "pipe",
	}
}

func (c *PipeCapableConn) Scope() network.ConnScope {
	return &network.NullScope{}
}

// PipeConn methods
func (c *PipeConn) LocalMultiaddr() ma.Multiaddr {
	return c.localAddr
}

func (c *PipeConn) RemoteMultiaddr() ma.Multiaddr {
	return c.remoteAddr
}

func (c *PipeConn) Transport() transport.Transport {
	return c.transport
}

// PipeAddr implements net.Addr for pipe addresses
type PipeAddr struct {
	addr ma.Multiaddr
}

func (a *PipeAddr) Network() string {
	return "pipe"
}

func (a *PipeAddr) String() string {
	return a.addr.String()
}

// Helper function to extract pipe ID from multiaddr
func extractPipeId(addr ma.Multiaddr) (string, error) {
	// Expected format: /pipe/<id>
	components := ma.Split(addr)
	if len(components) < 2 {
		return "", fmt.Errorf("invalid pipe address format")
	}

	for i, comp := range components {
		if comp.Protocols()[0].Code == PipeProtocolCode {
			if i+1 >= len(components) {
				return "", fmt.Errorf("missing pipe ID")
			}
			value, err := components[i+1].ValueForProtocol(components[i+1].Protocols()[0].Code)
			if err != nil {
				return "", err
			}
			return value, nil
		}
	}

	return "", fmt.Errorf("pipe protocol not found in address")
}

// Register the pipe protocol with multiaddr
func init() {
	// Register pipe protocol if not already registered
	err := ma.AddProtocol(ma.Protocol{
		Name:       "pipe",
		Code:       PipeProtocolCode,
		VCode:      ma.CodeToVarint(PipeProtocolCode),
		Size:       ma.LengthPrefixedVarSize,
		Transcoder: ma.NewTranscoderFromFunctions(pipeStB, pipeBtS, nil),
	})
	if err != nil {
		// Protocol might already be registered, ignore error
		_ = err
	}
}

func pipeStB(s string) ([]byte, error) {
	return []byte(s), nil
}

func pipeBtS(b []byte) (string, error) {
	return string(b), nil
}
