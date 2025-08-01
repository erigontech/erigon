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

// Package p2p implements the Ethereum p2p network protocols.
package p2p

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/debug"
	"github.com/erigontech/erigon-lib/common/mclock"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/p2p/discover"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
	"github.com/erigontech/erigon/p2p/event"
	"github.com/erigontech/erigon/p2p/nat"
	"github.com/erigontech/erigon/p2p/netutil"
)

const (
	defaultDialTimeout = 15 * time.Second

	// This is the fairness knob for the discovery mixer. When looking for peers, we'll
	// wait this long for a single source of candidates before moving on and trying other
	// sources.
	discmixTimeout = 5 * time.Second

	// Connectivity defaults.
	defaultDialRatio = 3

	// This time limits inbound connection attempts per source IP.
	inboundThrottleTime = 30 * time.Second

	// Maximum time allowed for reading a complete message.
	// This is effectively the amount of time a connection can be idle.
	frameReadTimeout = 30 * time.Second

	// Maximum amount of time allowed for writing a complete message.
	frameWriteTimeout = 20 * time.Second

	serverStatsLogInterval = 60 * time.Second
)

var errServerStopped = errors.New("server stopped")

// Config holds Server options.
type Config struct {
	// This field must be set to a valid secp256k1 private key.
	PrivateKey *ecdsa.PrivateKey `toml:"-"`

	// MaxPeers is the maximum number of peers that can be
	// connected. It must be greater than zero.
	MaxPeers int

	// MaxPendingPeers is the maximum number of peers that can be pending in the
	// handshake phase, counted separately for inbound and outbound connections.
	// It must be greater than zero.
	MaxPendingPeers int `toml:",omitempty"`

	// DialRatio controls the ratio of inbound to dialed connections.
	// Example: a DialRatio of 2 allows 1/2 of connections to be dialed.
	// Setting DialRatio to zero defaults it to 3.
	DialRatio int `toml:",omitempty"`

	// NoDiscovery can be used to disable the peer discovery mechanism.
	// Disabling is useful for protocol debugging (manual topology).
	NoDiscovery bool

	// DiscoveryV5 specifies whether the new topic-discovery based V5 discovery
	// protocol should be started or not.
	DiscoveryV5 bool `toml:",omitempty"`

	// Name sets the node name of this server.
	// Use common.MakeName to create a name that follows existing conventions.
	Name string `toml:"-"`

	// BootstrapNodes are used to establish connectivity
	// with the rest of the network.
	BootstrapNodes []*enode.Node

	// BootstrapNodesV5 are used to establish connectivity
	// with the rest of the network using the V5 discovery
	// protocol.
	BootstrapNodesV5 []*enode.Node `toml:",omitempty"`

	// Static nodes are used as pre-configured connections which are always
	// maintained and re-connected on disconnects.
	StaticNodes []*enode.Node

	// Trusted nodes are used as pre-configured connections which are always
	// allowed to connect, even above the peer limit.
	TrustedNodes []*enode.Node

	// Connectivity can be restricted to certain IP networks.
	// If this option is set to a non-nil value, only hosts which match one of the
	// IP networks contained in the list are considered.
	NetRestrict *netutil.Netlist `toml:",omitempty"`

	// NodeDatabase is the path to the database containing the previously seen
	// live nodes in the network.
	NodeDatabase string `toml:",omitempty"`

	// Protocols should contain the protocols supported
	// by the server. Matching protocols are launched for
	// each peer.
	Protocols []Protocol `toml:"-"`

	// If ListenAddr is set to a non-nil address, the server
	// will listen for incoming connections.
	//
	// If the port is zero, the operating system will pick a port. The
	// ListenAddr field will be updated with the actual address when
	// the server is started.
	ListenAddr string

	// AllowedPorts is list of ports allowed to pick to create Listener on it (see ListenAddr)
	// for different protocol versions
	AllowedPorts []uint

	// eth/66, eth/67, etc
	ProtocolVersion []uint

	SentryAddr []string

	// If set to a non-nil value, the given NAT port mapper
	// is used to make the listening port available to the
	// Internet.
	NAT nat.Interface `toml:",omitempty"`

	// NAT interface description (see NAT.Parse()).
	NATSpec string

	// If Dialer is set to a non-nil value, the given Dialer
	// is used to dial outbound peer connections.
	Dialer NodeDialer `toml:"-"`

	// If NoDial is true, the server will not dial any peers.
	NoDial bool `toml:",omitempty"`

	// If EnableMsgEvents is set then the server will emit PeerEvents
	// whenever a message is sent to or received from a peer
	EnableMsgEvents bool

	// it is actually used but a linter got confused
	clock mclock.Clock //nolint:structcheck

	TmpDir string

	MetricsEnabled bool

	DiscoveryDNS []string
}

func (config *Config) ListenPort() int {
	_, portStr, err := net.SplitHostPort(config.ListenAddr)
	if err != nil {
		return 0
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 0
	}
	return port
}

// Server manages all peer connections.
type Server struct {
	// Config fields may not be modified while the server is running.
	Config

	// Hooks for testing. These are useful because we can inhibit
	// the whole protocol stack.
	newTransport func(net.Conn, *ecdsa.PublicKey) transport
	newPeerHook  func(*Peer)
	listenFunc   func(network, addr string) (net.Listener, error)

	lock    sync.Mutex // protects whole bunch of stuff
	running atomic.Bool

	listener     net.Listener
	ourHandshake *protoHandshake
	loopWG       sync.WaitGroup // loop, listenLoop
	peerFeed     event.Feed
	logger       log.Logger

	nodedb             *enode.DB
	localnode          *enode.LocalNode
	localnodeAddrCache atomic.Pointer[string]
	ntab               *discover.UDPv4
	DiscV5             *discover.UDPv5
	discmix            *enode.FairMix
	dialsched          *dialScheduler

	// Channels into the run loop.
	quitCtx                 context.Context
	quitFunc                context.CancelFunc
	quit                    <-chan struct{}
	addtrusted              chan *enode.Node
	removetrusted           chan *enode.Node
	peerOp                  chan peerOpFunc
	peerOpDone              chan struct{}
	delpeer                 chan peerDrop
	checkpointPostHandshake chan *conn
	checkpointAddPeer       chan *conn

	// State of run loop and listenLoop.
	inboundHistory expHeap

	errorsMu sync.Mutex
	errors   map[string]uint
}

type peerOpFunc func(map[enode.ID]*Peer)

type peerDrop struct {
	*Peer
	err *PeerError
}

type connFlag int32

const (
	dynDialedConn connFlag = 1 << iota
	staticDialedConn
	inboundConn
	trustedConn
)

// conn wraps a network connection with information gathered
// during the two handshakes.
type conn struct {
	fd net.Conn
	transport
	node   *enode.Node
	flags  connFlag
	cont   chan error // The run loop uses cont to signal errors to SetupConn.
	caps   []Cap      // valid after the protocol handshake
	name   string     // valid after the protocol handshake
	pubkey [64]byte
}

type transport interface {
	// The two handshakes.
	doEncHandshake(prv *ecdsa.PrivateKey) (*ecdsa.PublicKey, error)
	doProtoHandshake(our *protoHandshake) (*protoHandshake, error)
	// The MsgReadWriter can only be used after the encryption
	// handshake has completed. The code uses conn.id to track this
	// by setting it to a non-nil value after the encryption handshake.
	MsgReadWriter
	// transports must provide Close because we use MsgPipe in some
	// tests. Closing the actual network connection doesn't do
	// anything in those tests because MsgPipe doesn't use it.
	close(err error)
}

func (c *conn) String() string {
	s := c.flags.String()
	if (c.node.ID() != enode.ID{}) {
		s += " " + c.node.ID().String()
	}
	s += " " + c.fd.RemoteAddr().String()
	return s
}

func (f connFlag) String() string {
	s := ""
	if f&trustedConn != 0 {
		s += "-trusted"
	}
	if f&dynDialedConn != 0 {
		s += "-dyndial"
	}
	if f&staticDialedConn != 0 {
		s += "-staticdial"
	}
	if f&inboundConn != 0 {
		s += "-inbound"
	}
	if s != "" {
		s = s[1:]
	}
	return s
}

func (c *conn) is(f connFlag) bool {
	flags := connFlag(atomic.LoadInt32((*int32)(&c.flags)))
	return flags&f != 0
}

func (c *conn) set(f connFlag, val bool) {
	for {
		oldFlags := connFlag(atomic.LoadInt32((*int32)(&c.flags)))
		flags := oldFlags
		if val {
			flags |= f
		} else {
			flags &= ^f
		}
		if atomic.CompareAndSwapInt32((*int32)(&c.flags), int32(oldFlags), int32(flags)) {
			return
		}
	}
}

// LocalNode returns the local node record.
func (srv *Server) LocalNode() *enode.LocalNode {
	return srv.localnode
}

// Peers returns all connected peers.
func (srv *Server) Peers() []*Peer {
	var ps []*Peer
	srv.doPeerOp(func(peers map[enode.ID]*Peer) {
		for _, p := range peers {
			ps = append(ps, p)
		}
	})
	return ps
}

func (srv *Server) SetP2PListenFunc(listenFunc func(network, addr string) (net.Listener, error)) {
	srv.listenFunc = listenFunc
}

// PeerCount returns the number of connected peers.
func (srv *Server) PeerCount() int {
	var count int
	srv.doPeerOp(func(ps map[enode.ID]*Peer) {
		count = len(ps)
	})
	return count
}

// AddPeer adds the given node to the static node set. When there is room in the peer set,
// the server will connect to the node. If the connection fails for any reason, the server
// will attempt to reconnect the peer.
func (srv *Server) AddPeer(node *enode.Node) {
	srv.dialsched.addStatic(node)
}

// RemovePeer removes a node from the static node set. It also disconnects from the given
// node if it is currently connected as a peer.
//
// This method blocks until all protocols have exited and the peer is removed. Do not use
// RemovePeer in protocol implementations, call Disconnect on the Peer instead.
func (srv *Server) RemovePeer(node *enode.Node) {
	var (
		ch  chan *PeerEvent
		sub event.Subscription
	)
	// Disconnect the peer on the main loop.
	srv.doPeerOp(func(peers map[enode.ID]*Peer) {
		srv.dialsched.removeStatic(node)
		if peer := peers[node.ID()]; peer != nil {
			ch = make(chan *PeerEvent, 1)
			sub = srv.peerFeed.Subscribe(ch)
			peer.Disconnect(NewPeerError(PeerErrorDiscReason, DiscRequested, nil, "Server.RemovePeer Disconnect"))
		}
	})
	// Wait for the peer connection to end.
	if ch != nil {
		defer sub.Unsubscribe()
		for ev := range ch {
			if ev.Peer == node.ID() && ev.Type == PeerEventTypeDrop {
				return
			}
		}
	}
}

// AddTrustedPeer adds the given node to a reserved whitelist which allows the
// node to always connect, even if the slot are full.
func (srv *Server) AddTrustedPeer(node *enode.Node) {
	select {
	case srv.addtrusted <- node:
	case <-srv.quit:
	}
}

// RemoveTrustedPeer removes the given node from the trusted peer set.
func (srv *Server) RemoveTrustedPeer(node *enode.Node) {
	select {
	case srv.removetrusted <- node:
	case <-srv.quit:
	}
}

// SubscribeEvents subscribes the given channel to peer events.
func (srv *Server) SubscribeEvents(ch chan *PeerEvent) event.Subscription {
	return srv.peerFeed.Subscribe(ch)
}

// Self returns the local node's endpoint information.
func (srv *Server) Self() (ln *enode.Node) {
	if srv.localnode != nil {
		return srv.localnode.Node()
	}
	return enode.NewV4(&srv.PrivateKey.PublicKey, net.ParseIP("0.0.0.0"), 0, 0)
}

// Stop terminates the server and all active peer connections.
// It blocks until all active connections have been closed.
func (srv *Server) Stop() {
	srv.lock.Lock()
	if !srv.running.Load() {
		if srv.nodedb != nil {
			srv.nodedb.Close()
		}
		srv.lock.Unlock()
		return
	}
	srv.running.Store(false)
	srv.quitFunc()
	if srv.listener != nil {
		// this unblocks listener Accept
		_ = srv.listener.Close()
	}
	if srv.nodedb != nil {
		srv.nodedb.Close()
	}
	srv.lock.Unlock()
	srv.loopWG.Wait()
}

// sharedUDPConn implements a shared connection. Write sends messages to the underlying connection while read returns
// messages that were found unprocessable and sent to the unhandled channel by the primary listener.
type sharedUDPConn struct {
	*net.UDPConn
	unhandled chan discover.ReadPacket
}

// ReadFromUDP implements discover.UDPConn
func (s *sharedUDPConn) ReadFromUDP(b []byte) (n int, addr *net.UDPAddr, err error) {
	packet, ok := <-s.unhandled
	if !ok {
		return 0, nil, errors.New("connection was closed")
	}
	l := len(packet.Data)
	if l > len(b) {
		l = len(b)
	}
	copy(b[:l], packet.Data[:l])
	return l, packet.Addr, nil
}

// Close implements discover.UDPConn
func (s *sharedUDPConn) Close() error {
	return nil
}

// Start starts running the server.
// Servers can not be re-used after stopping.
func (srv *Server) Start(ctx context.Context, logger log.Logger) error {
	if srv.running.Load() {
		return errors.New("server already running")
	}
	srv.lock.Lock()
	defer srv.lock.Unlock()

	srv.logger = logger
	if srv.clock == nil {
		srv.clock = mclock.System{}
	}
	if srv.NoDial && srv.ListenAddr == "" {
		srv.logger.Warn("P2P server will be useless, neither dialing nor listening")
	}

	// static fields
	if srv.PrivateKey == nil {
		return errors.New("Server.PrivateKey must be set to a non-nil key")
	}
	if srv.MaxPendingPeers <= 0 {
		return errors.New("MaxPendingPeers must be greater than zero")
	}
	if srv.newTransport == nil {
		srv.newTransport = newRLPX
	}
	if srv.listenFunc == nil {
		srv.listenFunc = net.Listen
	}
	srv.quitCtx, srv.quitFunc = context.WithCancel(ctx)
	srv.quit = srv.quitCtx.Done()
	srv.delpeer = make(chan peerDrop)
	srv.checkpointPostHandshake = make(chan *conn)
	srv.checkpointAddPeer = make(chan *conn)
	srv.addtrusted = make(chan *enode.Node)
	srv.removetrusted = make(chan *enode.Node)
	srv.peerOp = make(chan peerOpFunc)
	srv.peerOpDone = make(chan struct{})

	if err := srv.setupLocalNode(); err != nil {
		return err
	}
	if srv.ListenAddr != "" {
		if err := srv.setupListening(srv.quitCtx); err != nil {
			return err
		}
	}
	if err := srv.setupDiscovery(srv.quitCtx); err != nil {
		return err
	}
	srv.setupDialScheduler()

	srv.running.Store(true)
	srv.loopWG.Add(1)
	go srv.run()
	return nil
}

func (srv *Server) updateLocalNodeStaticAddrCache() {
	localNodeAddr := srv.localnode.Node().URLv4()
	srv.localnodeAddrCache.Store(&localNodeAddr)

}
func (srv *Server) setupLocalNode() error {
	// Create the devp2p handshake.
	pubkey := crypto.MarshalPubkey(&srv.PrivateKey.PublicKey)
	srv.ourHandshake = &protoHandshake{Version: baseProtocolVersion, Name: srv.Name, Pubkey: pubkey}
	for _, p := range srv.Protocols {
		srv.ourHandshake.Caps = append(srv.ourHandshake.Caps, p.cap())
	}
	sort.Sort(capsByNameAndVersion(srv.ourHandshake.Caps))
	// Create the local node
	db, err := enode.OpenDB(srv.quitCtx, srv.Config.NodeDatabase, srv.Config.TmpDir, srv.logger)
	if err != nil {
		return err
	}
	srv.nodedb = db

	srv.localnode = enode.NewLocalNode(db, srv.PrivateKey, srv.logger)
	srv.localnode.SetFallbackIP(net.IP{127, 0, 0, 1})

	// TODO: check conflicts
	for _, p := range srv.Protocols {
		for _, e := range p.Attributes {
			srv.localnode.Set(e)
		}
	}

	srv.updateLocalNodeStaticAddrCache()

	switch srv.NAT.(type) {
	case nil:
		// No NAT interface, do nothing.
	case nat.ExtIP:
		// ExtIP doesn't block, set the IP right away.
		ip, _ := srv.NAT.ExternalIP()
		srv.localnode.SetStaticIP(ip)
		srv.updateLocalNodeStaticAddrCache()
	default:
		// Ask the router about the IP. This takes a while and blocks startup,
		// do it in the background.
		srv.loopWG.Add(1)
		go func() {
			defer debug.LogPanic()
			defer srv.loopWG.Done()
			if ip, err := srv.NAT.ExternalIP(); err == nil {
				srv.logger.Info("NAT ExternalIP resolved", "ip", ip)
				srv.localnode.SetStaticIP(ip)
				srv.updateLocalNodeStaticAddrCache()
			} else {
				srv.logger.Warn("NAT ExternalIP resolution has failed, try to pass a different --nat option", "err", err)
			}
		}()
	}
	return nil
}

func (srv *Server) setupDiscovery(ctx context.Context) error {
	srv.discmix = enode.NewFairMix(discmixTimeout)

	// Add protocol-specific discovery sources.
	added := make(map[string]bool)
	for _, proto := range srv.Protocols {
		if proto.DialCandidates != nil && !added[proto.Name] {
			srv.discmix.AddSource(proto.DialCandidates)
			added[proto.Name] = true
		}
	}

	// Don't listen on UDP endpoint if DHT is disabled.
	if srv.NoDiscovery && !srv.DiscoveryV5 {
		return nil
	}

	addr, err := net.ResolveUDPAddr("udp", srv.ListenAddr)
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}
	realaddr := conn.LocalAddr().(*net.UDPAddr)
	srv.logger.Trace("UDP listener up", "addr", realaddr)
	if srv.NAT != nil {
		if !realaddr.IP.IsLoopback() && srv.NAT.SupportsMapping() {
			srv.loopWG.Add(1)
			go func() {
				defer debug.LogPanic()
				defer srv.loopWG.Done()
				nat.Map(srv.NAT, srv.quit, "udp", realaddr.Port, realaddr.Port, "ethereum discovery", srv.logger)
			}()
		}
	}
	srv.localnode.SetFallbackUDP(realaddr.Port)
	srv.updateLocalNodeStaticAddrCache()

	// Discovery V4
	var unhandled chan discover.ReadPacket
	var sconn *sharedUDPConn
	if !srv.NoDiscovery {
		if srv.DiscoveryV5 {
			unhandled = make(chan discover.ReadPacket, 100)
			sconn = &sharedUDPConn{conn, unhandled}
		}
		cfg := discover.Config{
			PrivateKey:  srv.PrivateKey,
			NetRestrict: srv.NetRestrict,
			Bootnodes:   srv.BootstrapNodes,
			Unhandled:   unhandled,
			Log:         srv.logger,
		}
		ntab, err := discover.ListenV4(ctx, strconv.FormatUint(uint64(srv.Config.Protocols[0].Version), 10), conn, srv.localnode, cfg)
		if err != nil {
			return err
		}
		srv.ntab = ntab
		srv.discmix.AddSource(ntab.RandomNodes())
	}

	// Discovery V5
	if srv.DiscoveryV5 {
		cfg := discover.Config{
			PrivateKey:  srv.PrivateKey,
			NetRestrict: srv.NetRestrict,
			Bootnodes:   srv.BootstrapNodesV5,
			Log:         srv.logger,
		}
		version := uint64(srv.Config.Protocols[0].Version)
		var err error
		if sconn != nil {
			srv.DiscV5, err = discover.ListenV5(ctx, strconv.FormatUint(version, 10), sconn, srv.localnode, cfg)
		} else {
			srv.DiscV5, err = discover.ListenV5(ctx, strconv.FormatUint(version, 10), conn, srv.localnode, cfg)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (srv *Server) setupDialScheduler() {
	config := dialConfig{
		self:           srv.localnode.ID(),
		maxDialPeers:   srv.maxDialedConns(),
		maxActiveDials: srv.MaxPendingPeers,
		log:            srv.logger,
		netRestrict:    srv.NetRestrict,
		dialer:         srv.Dialer,
		clock:          srv.clock,
	}
	if srv.ntab != nil {
		config.resolver = srv.ntab
	}
	if config.dialer == nil {
		config.dialer = tcpDialer{&net.Dialer{Timeout: defaultDialTimeout}}
	}
	var subProtocolVersion uint
	if len(srv.Protocols) > 0 {
		subProtocolVersion = srv.Protocols[0].Version
	}
	srv.dialsched = newDialScheduler(config, srv.discmix, srv.SetupConn, subProtocolVersion)
	for _, n := range srv.StaticNodes {
		srv.dialsched.addStatic(n)
	}
}

func (srv *Server) maxInboundConns() int {
	return srv.MaxPeers - srv.maxDialedConns()
}

func (srv *Server) maxDialedConns() (limit int) {
	if srv.NoDial || srv.MaxPeers == 0 {
		return 0
	}
	if srv.DialRatio == 0 {
		limit = srv.MaxPeers / defaultDialRatio
	} else {
		limit = srv.MaxPeers / srv.DialRatio
	}
	if limit == 0 {
		limit = 1
	}
	return limit
}

func (srv *Server) setupListening(ctx context.Context) error {
	// Launch the listener.
	listener, err := srv.listenFunc("tcp", srv.ListenAddr)
	if err != nil {
		return err
	}
	srv.listener = listener
	srv.ListenAddr = listener.Addr().String()

	// Update the local node record and map the TCP listening port if NAT is configured.
	if tcp, ok := listener.Addr().(*net.TCPAddr); ok {
		srv.localnode.Set(enr.TCP(tcp.Port))
		srv.updateLocalNodeStaticAddrCache()

		if !tcp.IP.IsLoopback() && (srv.NAT != nil) && srv.NAT.SupportsMapping() {
			srv.loopWG.Add(1)
			go func() {
				defer debug.LogPanic()
				defer srv.loopWG.Done()
				nat.Map(srv.NAT, srv.quit, "tcp", tcp.Port, tcp.Port, "ethereum p2p", srv.logger)
			}()
		}
	}

	srv.loopWG.Add(1)
	go func() {
		defer debug.LogPanic()
		defer srv.loopWG.Done()
		srv.listenLoop(ctx)
	}()
	return nil
}

// doPeerOp runs fn on the main loop.
func (srv *Server) doPeerOp(fn peerOpFunc) {
	select {
	case srv.peerOp <- fn:
		<-srv.peerOpDone
	case <-srv.quit:
	}
}

// run is the main loop of the server.
func (srv *Server) run() {
	defer debug.LogPanic()
	if len(srv.Config.Protocols) > 0 {
		srv.logger.Info("Started P2P networking", "version", srv.Config.Protocols[0].Version, "self", *srv.localnodeAddrCache.Load(), "name", srv.Name)
	}
	defer srv.loopWG.Done()
	defer srv.nodedb.Close()
	defer srv.discmix.Close()
	defer srv.dialsched.stop()

	var (
		peers        = make(map[enode.ID]*Peer)
		inboundCount = 0
		trusted      = make(map[enode.ID]bool, len(srv.TrustedNodes))
	)
	// Put trusted nodes into a map to speed up checks.
	// Trusted peers are loaded on startup or added via AddTrustedPeer RPC.
	for _, n := range srv.TrustedNodes {
		trusted[n.ID()] = true
	}

	logTimer := time.NewTicker(serverStatsLogInterval)
	defer logTimer.Stop()

running:
	for {
		select {
		case <-srv.quit:
			// The server was stopped. Run the cleanup logic.
			break running
		case n := <-srv.addtrusted:
			// This channel is used by AddTrustedPeer to add a node
			// to the trusted node set.
			srv.logger.Trace("Adding trusted node", "node", n)
			trusted[n.ID()] = true
			if p, ok := peers[n.ID()]; ok {
				p.rw.set(trustedConn, true)
			}

		case n := <-srv.removetrusted:
			// This channel is used by RemoveTrustedPeer to remove a node
			// from the trusted node set.
			srv.logger.Trace("Removing trusted node", "node", n)
			delete(trusted, n.ID())
			if p, ok := peers[n.ID()]; ok {
				p.rw.set(trustedConn, false)
			}

		case op := <-srv.peerOp:
			// This channel is used by GoodPeers and PeerCount.
			op(peers)
			srv.peerOpDone <- struct{}{}

		case c := <-srv.checkpointPostHandshake:
			// A connection has passed the encryption handshake so
			// the remote identity is known (but hasn't been verified yet).
			if trusted[c.node.ID()] {
				// Ensure that the trusted flag is set before checking against MaxPeers.
				c.flags |= trustedConn
			}
			c.cont <- nil

		case c := <-srv.checkpointAddPeer:
			// At this point the connection is past the protocol handshake.
			// Its capabilities are known and the remote identity is verified.
			err := srv.postHandshakeChecks(peers, inboundCount, c)
			if err == nil {
				// The handshakes are done and it passed all checks.
				p := srv.launchPeer(c, c.pubkey)
				peers[c.node.ID()] = p
				srv.logger.Trace("Adding p2p peer", "peercount", len(peers), "url", p.Node(), "conn", c.flags, "name", p.Fullname())
				srv.dialsched.peerAdded(c)
				if p.Inbound() {
					inboundCount++
				}
			}
			c.cont <- err

		case pd := <-srv.delpeer:
			// A peer disconnected.
			d := common.PrettyDuration(mclock.Now() - pd.created)
			delete(peers, pd.ID())
			srv.logger.Trace("Removing p2p peer", "peercount", len(peers), "url", pd.Node(), "duration", d, "err", pd.err)
			srv.dialsched.peerRemoved(pd.rw)
			if pd.Inbound() {
				inboundCount--
			}
		case <-logTimer.C:
			vals := []interface{}{"protocol", srv.Config.Protocols[0].Version, "peers", len(peers), "trusted", len(trusted), "inbound", inboundCount}
			vals = append(vals, srv.listErrors()...)

			srv.logger.Debug("[p2p] Server", vals...)
		}
	}

	srv.logger.Trace("P2P networking is spinning down")

	// Terminate discovery. If there is a running lookup it will terminate soon.
	if srv.ntab != nil {
		srv.ntab.Close()
	}
	if srv.DiscV5 != nil {
		srv.DiscV5.Close()
	}
	// Disconnect all peers.
	for _, p := range peers {
		p.Disconnect(NewPeerError(PeerErrorDiscReason, DiscQuitting, nil, "Server.run() spindown"))
	}
	// Wait for peers to shut down. Pending connections and tasks are
	// not handled here and will terminate soon-ish because srv.quit
	// is closed.
	for len(peers) > 0 {
		p := <-srv.delpeer
		srv.logger.Trace("<-delpeer (spindown)")
		delete(peers, p.ID())
	}
}

func (srv *Server) postHandshakeChecks(peers map[enode.ID]*Peer, inboundCount int, c *conn) error {
	switch {
	case !c.is(trustedConn) && len(peers) >= srv.MaxPeers:
		return DiscTooManyPeers
	case !c.is(trustedConn) && c.is(inboundConn) && inboundCount >= srv.maxInboundConns():
		return DiscTooManyPeers
	case peers[c.node.ID()] != nil:
		return DiscAlreadyConnected
	case c.node.ID() == srv.localnode.ID():
		return DiscSelf
	case (len(srv.Protocols) > 0) && (countMatchingProtocols(srv.Protocols, c.caps) == 0):
		return DiscUselessPeer
	default:
		return nil
	}
}

// listenLoop runs in its own goroutine and accepts
// inbound connections.
func (srv *Server) listenLoop(ctx context.Context) {
	srv.logger.Trace("TCP listener up", "addr", srv.listener.Addr())

	srv.resetErrors()

	// The slots limit accepts of new connections.
	slots := semaphore.NewWeighted(int64(srv.MaxPendingPeers))

	// Wait for slots to be returned on exit. This ensures all connection goroutines
	// are down before listenLoop returns.
	defer func() {
		_ = slots.Acquire(ctx, int64(srv.MaxPendingPeers))
	}()

	for {
		// Wait for a free slot before accepting.
		if slotErr := slots.Acquire(ctx, 1); slotErr != nil {
			if !errors.Is(slotErr, context.Canceled) {
				srv.logger.Error("Failed to get a peer connection slot", "err", slotErr)
			}
			return
		}

		var (
			fd      net.Conn
			err     error
			lastLog time.Time
		)
		for {
			fd, err = srv.listener.Accept()
			if netutil.IsTemporaryError(err) {
				if time.Since(lastLog) > 1*time.Second {
					srv.logger.Trace("Temporary read error", "err", err)
					lastLog = time.Now()
				}
				time.Sleep(time.Millisecond * 200)
				continue
			} else if err != nil {
				// Log the error unless the server is shutting down.
				select {
				case <-srv.quit:
				default:
					srv.logger.Error("Server listener failed to accept a connection", "err", err)
				}
				slots.Release(1)
				return
			}
			break
		}

		remoteIP := netutil.AddrIP(fd.RemoteAddr())
		if err := srv.checkInboundConn(fd, remoteIP); err != nil {
			srv.logger.Trace("Rejected inbound connection", "addr", fd.RemoteAddr(), "err", err)
			_ = fd.Close()
			slots.Release(1)
			continue
		}
		if remoteIP != nil {
			var addr *net.TCPAddr
			if tcp, ok := fd.RemoteAddr().(*net.TCPAddr); ok {
				addr = tcp
			}
			fd = newMeteredConn(fd, true, addr)
			srv.logger.Trace("Accepted connection", "addr", fd.RemoteAddr())
		}
		go func() {
			defer debug.LogPanic()
			defer slots.Release(1)
			// The error is logged in Server.setupConn().
			_ = srv.SetupConn(fd, inboundConn, nil)
		}()
	}
}

func (srv *Server) checkInboundConn(fd net.Conn, remoteIP net.IP) error {
	if remoteIP == nil {
		return nil
	}
	// Reject connections that do not match NetRestrict.
	if srv.NetRestrict != nil && !srv.NetRestrict.Contains(remoteIP) {
		return errors.New("not whitelisted in NetRestrict")
	}
	// Reject Internet peers that try too often.
	now := srv.clock.Now()
	srv.inboundHistory.expire(now, nil)
	if !netutil.IsLAN(remoteIP) && srv.inboundHistory.contains(remoteIP.String()) {
		return errors.New("too many attempts")
	}
	srv.inboundHistory.add(remoteIP.String(), now.Add(inboundThrottleTime))
	return nil
}

// SetupConn runs the handshakes and attempts to add the connection
// as a peer. It returns when the connection has been added as a peer
// or the handshakes have failed.
func (srv *Server) SetupConn(fd net.Conn, flags connFlag, dialDest *enode.Node) error {
	c := &conn{fd: fd, flags: flags, cont: make(chan error)}
	if dialDest == nil {
		c.transport = srv.newTransport(fd, nil)
	} else {
		c.transport = srv.newTransport(fd, dialDest.Pubkey())
	}

	err := srv.setupConn(c, flags, dialDest)
	if err != nil {
		c.close(err)
	}
	return err
}

func (srv *Server) setupConn(c *conn, flags connFlag, dialDest *enode.Node) error {
	// Prevent leftover pending conns from entering the handshake.
	if !srv.running.Load() {
		return errServerStopped
	}

	// If dialing, figure out the remote public key.
	var dialPubkey *ecdsa.PublicKey
	if dialDest != nil {
		dialPubkey = new(ecdsa.PublicKey)
		if err := dialDest.Load((*enode.Secp256k1)(dialPubkey)); err != nil {
			err = errors.New("dial destination doesn't have a secp256k1 public key")
			srv.logger.Trace("Setting up connection failed", "addr", c.fd.RemoteAddr(), "conn", c.flags, "err", err)
			return err
		}
	}

	// Run the RLPx handshake.
	remotePubkey, err := c.doEncHandshake(srv.PrivateKey)
	if err != nil {
		srv.addError(err)
		srv.logger.Trace("Failed RLPx handshake", "addr", c.fd.RemoteAddr(), "conn", c.flags, "err", err)
		return err
	}
	copy(c.pubkey[:], crypto.MarshalPubkey(remotePubkey))
	if dialDest != nil {
		c.node = dialDest
	} else {
		c.node = nodeFromConn(remotePubkey, c.fd)
	}
	clog := srv.logger.New("id", c.node.ID(), "addr", c.fd.RemoteAddr(), "conn", c.flags)
	err = srv.checkpoint(c, srv.checkpointPostHandshake)
	if err != nil {
		clog.Trace("Rejected peer", "err", err)
		return err
	}

	// Run the capability negotiation handshake.
	phs, err := c.doProtoHandshake(srv.ourHandshake)
	if err != nil {
		srv.addError(err)
		clog.Trace("Failed p2p handshake", "err", err)
		return err
	}
	if id := c.node.ID(); !bytes.Equal(crypto.Keccak256(phs.Pubkey), id[:]) {
		clog.Trace("Wrong devp2p handshake identity", "phsid", hex.EncodeToString(phs.Pubkey))
		return DiscUnexpectedIdentity
	}
	c.caps, c.name = phs.Caps, phs.Name
	err = srv.checkpoint(c, srv.checkpointAddPeer)
	if err != nil {
		clog.Trace("Rejected peer", "err", err)
		return err
	}

	return nil
}

func nodeFromConn(pubkey *ecdsa.PublicKey, conn net.Conn) *enode.Node {
	var ip net.IP
	var port int
	if tcp, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		ip = tcp.IP
		port = tcp.Port
	}
	return enode.NewV4(pubkey, ip, port, port)
}

// checkpoint sends the conn to run, which performs the
// post-handshake checks for the stage (posthandshake, addpeer).
func (srv *Server) checkpoint(c *conn, stage chan<- *conn) error {
	select {
	case stage <- c:
	case <-srv.quit:
		return errServerStopped
	}
	return <-c.cont
}

func (srv *Server) launchPeer(c *conn, pubkey [64]byte) *Peer {
	p := newPeer(srv.logger, c, srv.Protocols, pubkey, srv.MetricsEnabled)
	if srv.EnableMsgEvents {
		// If message events are enabled, pass the peerFeed
		// to the peer.
		p.events = &srv.peerFeed
	}
	go srv.runPeer(p)
	return p
}

// runPeer runs in its own goroutine for each peer.
func (srv *Server) runPeer(p *Peer) {
	defer debug.LogPanic()
	if srv.newPeerHook != nil {
		srv.newPeerHook(p)
	}
	srv.peerFeed.Send(&PeerEvent{
		Type:          PeerEventTypeAdd,
		Peer:          p.ID(),
		RemoteAddress: p.RemoteAddr().String(),
		LocalAddress:  p.LocalAddr().String(),
	})

	// Run the per-peer main loop.
	err := p.run()

	// Announce disconnect on the main loop to update the peer set.
	// The main loop waits for existing peers to be sent on srv.delpeer
	// before returning, so this send should not select on srv.quit.
	srv.delpeer <- peerDrop{p, err}

	// Broadcast peer drop to external subscribers. This needs to be
	// after the send to delpeer so subscribers have a consistent view of
	// the peer set (i.e. Server.GoodPeers() doesn't include the peer when the
	// event is received.
	srv.peerFeed.Send(&PeerEvent{
		Type:          PeerEventTypeDrop,
		Peer:          p.ID(),
		Error:         err.Error(),
		RemoteAddress: p.RemoteAddr().String(),
		LocalAddress:  p.LocalAddr().String(),
	})
}

// NodeInfo represents a short summary of the information known about the host.
type NodeInfo struct {
	ID    string `json:"id"`    // Unique node identifier
	Name  string `json:"name"`  // Name of the node, including client type, version, OS, custom data
	Enode string `json:"enode"` // Enode URL for adding this peer from remote peers
	ENR   string `json:"enr"`   // Ethereum Node Record
	IP    string `json:"ip"`    // IP address of the node
	Ports struct {
		Discovery int `json:"discovery"` // UDP listening port for discovery protocol
		Listener  int `json:"listener"`  // TCP listening port for RLPx
	} `json:"ports"`
	ListenAddr string                 `json:"listenAddr"`
	Protocols  map[string]interface{} `json:"protocols"`
}

// NodeInfo gathers and returns a collection of metadata known about the host.
func (srv *Server) NodeInfo() *NodeInfo {
	// Gather and assemble the generic node infos
	node := srv.Self()
	info := &NodeInfo{
		Name:       srv.Name,
		Enode:      node.URLv4(),
		ID:         node.ID().String(),
		IP:         node.IP().String(),
		ListenAddr: srv.ListenAddr,
		Protocols:  make(map[string]interface{}),
	}
	info.Ports.Discovery = node.UDP()
	info.Ports.Listener = node.TCP()
	info.ENR = node.String()

	// Gather all the running protocol infos (only once per protocol type)
	for _, proto := range srv.Protocols {
		if _, ok := info.Protocols[proto.Name]; !ok {
			nodeInfo := interface{}("unknown")
			if query := proto.NodeInfo; query != nil {
				nodeInfo = proto.NodeInfo()
			}
			info.Protocols[proto.Name] = nodeInfo
		}
	}
	return info
}

// PeersInfo returns an array of metadata objects describing connected peers.
func (srv *Server) PeersInfo() []*PeerInfo {
	// Gather all the generic and sub-protocol specific infos
	infos := make([]*PeerInfo, 0, srv.PeerCount())
	for _, peer := range srv.Peers() {
		if peer != nil {
			infos = append(infos, peer.Info())
		}
	}
	// Sort the result array alphabetically by node identifier
	for i := 0; i < len(infos); i++ {
		for j := i + 1; j < len(infos); j++ {
			if infos[i].ID > infos[j].ID {
				infos[i], infos[j] = infos[j], infos[i]
			}
		}
	}
	return infos
}

func (srv *Server) addError(err error) {
	if err == nil {
		return
	}
	srv.errorsMu.Lock()
	defer srv.errorsMu.Unlock()
	if srv.errors == nil {
		srv.errors = make(map[string]uint)
	}
	srv.errors[cleanError(err.Error())]++
}

func (srv *Server) resetErrors() {
	srv.errorsMu.Lock()
	srv.errors = map[string]uint{}
	srv.errorsMu.Unlock()
}

func (srv *Server) listErrors() []interface{} {
	srv.errorsMu.Lock()
	defer srv.errorsMu.Unlock()

	list := make([]interface{}, 0, len(srv.errors)*2)
	for err, count := range srv.errors {
		list = append(list, err, count)
	}
	return list
}

func cleanError(err string) string {
	switch {
	case strings.HasSuffix(err, "i/o timeout"):
		return "i/o timeout"
	case strings.HasSuffix(err, "closed by the remote host."):
		return "closed by remote"
	case strings.HasSuffix(err, "connection reset by peer"):
		return "closed by remote"
	default:
		return err
	}
}
