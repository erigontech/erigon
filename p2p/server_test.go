// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package p2p

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"errors"
	"io"
	"math/rand"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/p2p/enr"
	"github.com/ledgerwatch/erigon/p2p/rlpx"
	"github.com/ledgerwatch/log/v3"
)

type testTransport struct {
	*rlpxTransport
	rpub     *ecdsa.PublicKey
	closeErr error
}

func newTestTransport(rpub *ecdsa.PublicKey, fd net.Conn, dialDest *ecdsa.PublicKey) transport {
	wrapped := newRLPX(fd, dialDest).(*rlpxTransport)
	wrapped.conn.InitWithSecrets(rlpx.Secrets{
		AES:        make([]byte, 16),
		MAC:        make([]byte, 16),
		EgressMAC:  sha256.New(),
		IngressMAC: sha256.New(),
	})
	return &testTransport{rpub: rpub, rlpxTransport: wrapped}
}

func (c *testTransport) doEncHandshake(prv *ecdsa.PrivateKey) (*ecdsa.PublicKey, error) {
	return c.rpub, nil
}

func (c *testTransport) doProtoHandshake(our *protoHandshake) (*protoHandshake, error) {
	pubkey := crypto.MarshalPubkey(c.rpub)
	return &protoHandshake{Pubkey: pubkey, Name: "test"}, nil
}

func (c *testTransport) close(err error) {
	c.conn.Close()
	c.closeErr = err
}

func startTestServer(t *testing.T, remoteKey *ecdsa.PublicKey, pf func(*Peer), logger log.Logger) *Server {
	config := Config{
		Name:            "test",
		MaxPeers:        10,
		MaxPendingPeers: 10,
		ListenAddr:      "127.0.0.1:0",
		NoDiscovery:     true,
		PrivateKey:      newkey(),
	}
	server := &Server{
		Config:      config,
		newPeerHook: pf,
		newTransport: func(fd net.Conn, dialDest *ecdsa.PublicKey) transport {
			return newTestTransport(remoteKey, fd, dialDest)
		},
	}
	if err := server.TestStart(logger); err != nil {
		t.Fatalf("Could not start server: %v", err)
	}
	return server
}

func (srv *Server) TestStart(logger log.Logger) error {
	return srv.Start(context.Background(), logger)
}

func TestServerListen(t *testing.T) {
	logger := log.New()
	// start the test server
	connected := make(chan *Peer)
	remid := &newkey().PublicKey
	srv := startTestServer(t, remid, func(p *Peer) {
		if p.ID() != enode.PubkeyToIDV4(remid) {
			t.Error("peer func called with wrong node id")
		}
		connected <- p
	}, logger)
	defer close(connected)
	defer srv.Stop()

	// dial the test server
	conn, err := net.DialTimeout("tcp", srv.ListenAddr, 5*time.Second)
	if err != nil {
		t.Fatalf("could not dial: %v", err)
	}
	defer conn.Close()

	select {
	case peer := <-connected:
		if peer.LocalAddr().String() != conn.RemoteAddr().String() {
			t.Errorf("peer started with wrong conn: got %v, want %v",
				peer.LocalAddr(), conn.RemoteAddr())
		}
		peers := srv.Peers()
		if !reflect.DeepEqual(peers, []*Peer{peer}) {
			t.Errorf("GoodPeers mismatch: got %v, want %v", peers, []*Peer{peer})
		}
	case <-time.After(1 * time.Second):
		t.Error("server did not accept within one second")
	}
}

func TestServerDial(t *testing.T) {
	logger := log.New()
	// run a one-shot TCP server to handle the connection.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("could not setup listener: %v", err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		accepted <- conn
	}()

	// start the server
	connected := make(chan *Peer)
	remid := &newkey().PublicKey
	srv := startTestServer(t, remid, func(p *Peer) { connected <- p }, logger)
	defer close(connected)
	defer srv.Stop()

	// tell the server to connect
	tcpAddr := listener.Addr().(*net.TCPAddr)
	node := enode.NewV4(remid, tcpAddr.IP, tcpAddr.Port, 0)
	srv.AddPeer(node)

	select {
	case conn := <-accepted:
		defer conn.Close()

		select {
		case peer := <-connected:
			if peer.ID() != enode.PubkeyToIDV4(remid) {
				t.Errorf("peer has wrong id")
			}
			if peer.Name() != "test" {
				t.Errorf("peer has wrong name")
			}
			if peer.RemoteAddr().String() != conn.LocalAddr().String() {
				t.Errorf("peer started with wrong conn: got %v, want %v",
					peer.RemoteAddr(), conn.LocalAddr())
			}
			peers := srv.Peers()
			if !reflect.DeepEqual(peers, []*Peer{peer}) {
				t.Errorf("GoodPeers mismatch: got %v, want %v", peers, []*Peer{peer})
			}

			// Test AddTrustedPeer/RemoveTrustedPeer and changing Trusted flags
			// Particularly for race conditions on changing the flag state.
			if p := srv.Peers()[0]; p.Info().Network.Trusted {
				t.Errorf("peer is trusted prematurely: %v", p)
			}
			done := make(chan bool)
			go func() {
				srv.AddTrustedPeer(node)
				if p := srv.Peers()[0]; !p.Info().Network.Trusted {
					t.Errorf("peer is not trusted after AddTrustedPeer: %v", p)
				}
				srv.RemoveTrustedPeer(node)
				if p := srv.Peers()[0]; p.Info().Network.Trusted {
					t.Errorf("peer is trusted after RemoveTrustedPeer: %v", p)
				}
				done <- true
			}()
			// Trigger potential race conditions
			peer = srv.Peers()[0]
			_ = peer.Inbound()
			_ = peer.Info()
			<-done
		case <-time.After(1 * time.Second):
			t.Error("server did not launch peer within one second")
		}

	case <-time.After(1 * time.Second):
		t.Error("server did not connect within one second")
	}
}

// This test checks that RemovePeer disconnects the peer if it is connected.
func TestServerRemovePeerDisconnect(t *testing.T) {
	logger := log.New()
	srv1 := &Server{Config: Config{
		PrivateKey:      newkey(),
		MaxPeers:        1,
		MaxPendingPeers: 1,
		NoDiscovery:     true,
	}}
	srv2 := &Server{Config: Config{
		PrivateKey:      newkey(),
		MaxPeers:        1,
		MaxPendingPeers: 1,
		NoDiscovery:     true,
		NoDial:          true,
		ListenAddr:      "127.0.0.1:0",
	}}
	if err := srv1.TestStart(logger); err != nil {
		t.Fatal("cant start srv1")
	}
	defer srv1.Stop()
	if err := srv2.TestStart(logger); err != nil {
		t.Fatal("cant start srv2")
	}
	defer srv2.Stop()

	if !syncAddPeer(srv1, srv2.Self()) {
		t.Fatal("peer not connected")
	}
	srv1.RemovePeer(srv2.Self())
	if srv1.PeerCount() > 0 {
		t.Fatal("removed peer still connected")
	}
}

// This test checks that connections are disconnected just after the encryption handshake
// when the server is at capacity. Trusted connections should still be accepted.
func TestServerAtCap(t *testing.T) {
	logger := log.New()
	trustedNode := newkey()
	trustedID := enode.PubkeyToIDV4(&trustedNode.PublicKey)
	srv := &Server{
		Config: Config{
			PrivateKey:      newkey(),
			MaxPeers:        10,
			MaxPendingPeers: 10,
			NoDial:          true,
			NoDiscovery:     true,
			TrustedNodes:    []*enode.Node{newNode(trustedID, "")},
		},
	}
	if err := srv.TestStart(logger); err != nil {
		t.Fatalf("could not start: %v", err)
	}
	defer srv.Stop()

	newconn := func(id enode.ID) *conn {
		fd, _ := net.Pipe()
		tx := newTestTransport(&trustedNode.PublicKey, fd, nil)
		node := enode.SignNull(new(enr.Record), id)
		return &conn{fd: fd, transport: tx, flags: inboundConn, node: node, cont: make(chan error)}
	}

	// Inject a few connections to fill up the peer set.
	for i := 0; i < srv.Config.MaxPeers; i++ {
		c := newconn(randomID())
		if err := srv.checkpoint(c, srv.checkpointAddPeer); err != nil {
			t.Fatalf("could not add conn %d: %v", i, err)
		}
	}

	// Try inserting a non-trusted connection.
	anotherID := randomID()
	c := newconn(anotherID)
	if err := srv.checkpoint(c, srv.checkpointPostHandshake); err != nil {
		t.Error("unexpected error @ checkpointPostHandshake:", err)
	}
	if err := srv.checkpoint(c, srv.checkpointAddPeer); err != DiscTooManyPeers {
		t.Error("wrong error for insert:", err)
	}

	// Try inserting a trusted connection.
	c = newconn(trustedID)
	if err := srv.checkpoint(c, srv.checkpointPostHandshake); err != nil {
		t.Error("unexpected error @ checkpointPostHandshake:", err)
	}
	if err := srv.checkpoint(c, srv.checkpointAddPeer); err != nil {
		t.Error("unexpected error for trusted conn:", err)
	}
	if !c.is(trustedConn) {
		t.Error("Server did not set trusted flag")
	}

	// Remove from trusted set and try again
	srv.RemoveTrustedPeer(newNode(trustedID, ""))
	c = newconn(trustedID)
	if err := srv.checkpoint(c, srv.checkpointPostHandshake); err != nil {
		t.Error("unexpected error @ checkpointPostHandshake:", err)
	}
	if err := srv.checkpoint(c, srv.checkpointAddPeer); err != DiscTooManyPeers {
		t.Error("wrong error for insert:", err)
	}

	// Add anotherID to trusted set and try again
	srv.AddTrustedPeer(newNode(anotherID, ""))
	c = newconn(anotherID)
	if err := srv.checkpoint(c, srv.checkpointPostHandshake); err != nil {
		t.Error("unexpected error @ checkpointPostHandshake:", err)
	}
	if err := srv.checkpoint(c, srv.checkpointAddPeer); err != nil {
		t.Error("unexpected error for trusted conn:", err)
	}
	if !c.is(trustedConn) {
		t.Error("Server did not set trusted flag")
	}
}

func TestServerPeerLimits(t *testing.T) {
	logger := log.New()
	srvkey := newkey()
	clientkey := newkey()
	clientnode := enode.NewV4(&clientkey.PublicKey, nil, 0, 0)

	var tp = &setupTransport{
		pubkey: &clientkey.PublicKey,
		phs: protoHandshake{
			Pubkey: crypto.MarshalPubkey(&clientkey.PublicKey),
			Caps:   []Cap{discard.cap()},
		},
		lock: sync.Mutex{},
	}

	srv := &Server{
		Config: Config{
			PrivateKey:      srvkey,
			MaxPeers:        0,
			MaxPendingPeers: 50,
			NoDial:          true,
			NoDiscovery:     true,
			Protocols:       []Protocol{discard},
		},
		newTransport: func(fd net.Conn, dialDest *ecdsa.PublicKey) transport { return tp },
	}
	if err := srv.TestStart(logger); err != nil {
		t.Fatalf("couldn't start server: %v", err)
	}
	defer srv.Stop()

	// Check that server is full (MaxPeers=0)
	flags := dynDialedConn
	dialDest := clientnode
	conn, _ := net.Pipe()
	err := srv.SetupConn(conn, flags, dialDest)
	_ = conn.Close()
	if !errors.Is(err, DiscTooManyPeers) {
		t.Fatalf("expected DiscTooManyPeers, but got error: %q", err)
	}

	srv.AddTrustedPeer(clientnode)

	// Check that server allows a trusted peer despite being full.
	conn, _ = net.Pipe()
	err = srv.SetupConn(conn, flags, dialDest)
	_ = conn.Close()
	if err != nil {
		t.Fatalf("failed to bypass MaxPeers with trusted node: %q", err)
	}

	srv.RemoveTrustedPeer(clientnode)

	// Check that server is full again.
	conn, _ = net.Pipe()
	err = srv.SetupConn(conn, flags, dialDest)
	_ = conn.Close()
	if !errors.Is(err, DiscTooManyPeers) {
		t.Fatalf("expected DiscTooManyPeers, but got error: %q", err)
	}
}

func TestServerSetupConn(t *testing.T) {
	logger := log.New()
	var (
		clientkey, srvkey = newkey(), newkey()
		clientpub         = &clientkey.PublicKey
		srvpub            = &srvkey.PublicKey
	)
	tests := []struct {
		dontstart bool
		tt        *setupTransport
		flags     connFlag
		dialDest  *enode.Node

		wantCloseErr error
		wantCalls    string
	}{
		{
			dontstart:    true,
			tt:           &setupTransport{pubkey: clientpub},
			wantCalls:    "close,",
			wantCloseErr: errServerStopped,
		},
		{
			tt:           &setupTransport{pubkey: clientpub, encHandshakeErr: errors.New("read error")},
			flags:        inboundConn,
			wantCalls:    "doEncHandshake,close,",
			wantCloseErr: errors.New("read error"),
		},
		{
			tt:           &setupTransport{pubkey: clientpub, phs: protoHandshake{Pubkey: randomID().Bytes()}},
			dialDest:     enode.NewV4(clientpub, nil, 0, 0),
			flags:        dynDialedConn,
			wantCalls:    "doEncHandshake,doProtoHandshake,close,",
			wantCloseErr: DiscUnexpectedIdentity,
		},
		{
			tt:           &setupTransport{pubkey: clientpub, protoHandshakeErr: errors.New("foo")},
			dialDest:     enode.NewV4(clientpub, nil, 0, 0),
			flags:        dynDialedConn,
			wantCalls:    "doEncHandshake,doProtoHandshake,close,",
			wantCloseErr: errors.New("foo"),
		},
		{
			tt:           &setupTransport{pubkey: srvpub, phs: protoHandshake{Pubkey: crypto.MarshalPubkey(srvpub)}},
			flags:        inboundConn,
			wantCalls:    "doEncHandshake,doProtoHandshake,close,",
			wantCloseErr: DiscSelf,
		},
		{
			tt:           &setupTransport{pubkey: clientpub, phs: protoHandshake{Pubkey: crypto.MarshalPubkey(clientpub)}},
			flags:        inboundConn,
			wantCalls:    "doEncHandshake,doProtoHandshake,close,",
			wantCloseErr: DiscUselessPeer,
		},
	}

	for i, test := range tests {
		t.Run(test.wantCalls, func(t *testing.T) {
			cfg := Config{
				PrivateKey:      srvkey,
				MaxPeers:        10,
				MaxPendingPeers: 10,
				NoDial:          true,
				NoDiscovery:     true,
				Protocols:       []Protocol{discard},
			}
			srv := &Server{
				Config:       cfg,
				newTransport: func(fd net.Conn, dialDest *ecdsa.PublicKey) transport { return test.tt }, //nolint:scopelint
			}
			if !test.dontstart {
				if err := srv.TestStart(logger); err != nil {
					t.Fatalf("couldn't start server: %v", err)
				}
				defer srv.Stop()
			}
			p1, _ := net.Pipe()
			srv.SetupConn(p1, test.flags, test.dialDest)
			if !reflect.DeepEqual(test.tt.closeErr, test.wantCloseErr) {
				t.Errorf("test %d: close error mismatch: got %q, want %q", i, test.tt.closeErr, test.wantCloseErr)
			}
			if test.tt.calls != test.wantCalls {
				t.Errorf("test %d: calls mismatch: got %q, want %q", i, test.tt.calls, test.wantCalls)
			}
		})
	}
}

type setupTransport struct {
	pubkey            *ecdsa.PublicKey
	encHandshakeErr   error
	phs               protoHandshake
	protoHandshakeErr error

	calls    string
	closeErr error
	lock     sync.Mutex
}

func (c *setupTransport) doEncHandshake(prv *ecdsa.PrivateKey) (*ecdsa.PublicKey, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.calls += "doEncHandshake,"
	return c.pubkey, c.encHandshakeErr
}

func (c *setupTransport) doProtoHandshake(our *protoHandshake) (*protoHandshake, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.calls += "doProtoHandshake,"
	if c.protoHandshakeErr != nil {
		return nil, c.protoHandshakeErr
	}
	return &c.phs, nil
}

func (c *setupTransport) close(err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.calls += "close,"
	c.closeErr = err
}

// setupConn shouldn't write to/read from the connection.
func (c *setupTransport) WriteMsg(Msg) error {
	return errors.New("WriteMsg called on setupTransport")
}

func (c *setupTransport) ReadMsg() (Msg, error) {
	return Msg{}, errors.New("ReadMsg called on setupTransport")
}

func newkey() *ecdsa.PrivateKey {
	key, err := crypto.GenerateKey()
	if err != nil {
		panic("couldn't generate key: " + err.Error())
	}
	return key
}

func randomID() (id enode.ID) {
	for i := range id {
		id[i] = byte(rand.Intn(255))
	}
	return id
}

// This test checks that inbound connections are throttled by IP.
func TestServerInboundThrottle(t *testing.T) {
	logger := log.New()
	const timeout = 5 * time.Second
	newTransportCalled := make(chan struct{})
	srv := &Server{
		Config: Config{
			PrivateKey:      newkey(),
			ListenAddr:      "127.0.0.1:0",
			MaxPeers:        10,
			MaxPendingPeers: 10,
			NoDial:          true,
			NoDiscovery:     true,
			Protocols:       []Protocol{discard},
		},
		newTransport: func(fd net.Conn, dialDest *ecdsa.PublicKey) transport {
			newTransportCalled <- struct{}{}
			return newRLPX(fd, dialDest)
		},
		listenFunc: func(network, laddr string) (net.Listener, error) {
			fakeAddr := &net.TCPAddr{IP: net.IP{95, 33, 21, 2}, Port: 4444}
			return listenFakeAddr(network, laddr, fakeAddr)
		},
	}
	if err := srv.TestStart(logger); err != nil {
		t.Fatal("can't start: ", err)
	}
	defer srv.Stop()

	// Dial the test server.
	conn, err := net.DialTimeout("tcp", srv.ListenAddr, timeout)
	if err != nil {
		t.Fatalf("could not dial: %v", err)
	}
	select {
	case <-newTransportCalled:
		// OK
	case <-time.After(timeout):
		t.Error("newTransport not called")
	}
	conn.Close()

	// Dial again. This time the server should close the connection immediately.
	connClosed := make(chan struct{}, 1)
	conn, err = net.DialTimeout("tcp", srv.ListenAddr, timeout)
	if err != nil {
		t.Fatalf("could not dial: %v", err)
	}
	defer conn.Close()
	go func() {
		conn.SetDeadline(time.Now().Add(timeout))
		buf := make([]byte, 10)
		if n, err := conn.Read(buf); err != io.EOF || n != 0 {
			t.Errorf("expected io.EOF and n == 0, got error %q and n == %d", err, n)
		}
		connClosed <- struct{}{}
	}()
	select {
	case <-connClosed:
		// OK
	case <-newTransportCalled:
		t.Error("newTransport called for second attempt")
	case <-time.After(timeout):
		t.Error("connection not closed within timeout")
	}
}

func listenFakeAddr(network, laddr string, remoteAddr net.Addr) (net.Listener, error) {
	l, err := net.Listen(network, laddr)
	if err == nil {
		l = &fakeAddrListener{l, remoteAddr}
	}
	return l, err
}

// fakeAddrListener is a listener that creates connections with a mocked remote address.
type fakeAddrListener struct {
	net.Listener
	remoteAddr net.Addr
}

type fakeAddrConn struct {
	net.Conn
	remoteAddr net.Addr
}

func (l *fakeAddrListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &fakeAddrConn{c, l.remoteAddr}, nil
}

func (c *fakeAddrConn) RemoteAddr() net.Addr {
	return c.remoteAddr
}

func syncAddPeer(srv *Server, node *enode.Node) bool {
	var (
		ch      = make(chan *PeerEvent)
		sub     = srv.SubscribeEvents(ch)
		timeout = time.After(2 * time.Second)
	)
	defer sub.Unsubscribe()
	srv.AddPeer(node)
	for {
		select {
		case ev := <-ch:
			if ev.Type == PeerEventTypeAdd && ev.Peer == node.ID() {
				return true
			}
		case <-timeout:
			return false
		}
	}
}
