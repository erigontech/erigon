// Copyright 2015 The go-ethereum Authors
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
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	mrand "math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/mclock"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/netutil"
)

const (
	// This is the amount of time spent waiting in between redialing a certain node. The
	// limit is a bit higher than inboundThrottleTime to prevent failing dials in small
	// private networks.
	dialHistoryExpiration = inboundThrottleTime + 5*time.Second

	// Config for the "Looking for peers" message.
	dialStatsLogInterval = 60 * time.Second // printed at most this often

	// Endpoint resolution is throttled with bounded backoff.
	initialResolveDelay = 60 * time.Second
	maxResolveDelay     = time.Hour
)

// NodeDialer is used to connect to nodes in the network, typically by using
// an underlying net.Dialer but also using net.Pipe in tests.
type NodeDialer interface {
	Dial(context.Context, *enode.Node) (net.Conn, error)
}

type nodeResolver interface {
	Resolve(*enode.Node) *enode.Node
}

// tcpDialer implements NodeDialer using real TCP connections.
type tcpDialer struct {
	d *net.Dialer
}

func (t tcpDialer) Dial(ctx context.Context, dest *enode.Node) (net.Conn, error) {
	return t.d.DialContext(ctx, "tcp", nodeAddr(dest).String())
}

func nodeAddr(n *enode.Node) net.Addr {
	return &net.TCPAddr{IP: n.IP(), Port: n.TCP()}
}

// checkDial errors:
var (
	errSelf             = errors.New("is self")
	errAlreadyDialing   = errors.New("already dialing")
	errAlreadyConnected = errors.New("already connected")
	errRecentlyDialed   = errors.New("recently dialed")
	errNotWhitelisted   = errors.New("not contained in netrestrict whitelist")
	errNoPort           = errors.New("node does not provide TCP port")
)

// dialer creates outbound connections and submits them into Server.
// Two types of peer connections can be created:
//
//   - static dials are pre-configured connections. The dialer attempts
//     keep these nodes connected at all times.
//
//   - dynamic dials are created from node discovery results. The dialer
//     continuously reads candidate nodes from its input iterator and attempts
//     to create peer connections to nodes arriving through the iterator.
type dialScheduler struct {
	dialConfig
	mutex       sync.Mutex
	setupFunc   dialSetupFunc
	wg          sync.WaitGroup
	cancel      context.CancelFunc
	ctx         context.Context
	nodesIn     chan *enode.Node
	doneCh      chan *dialTask
	addStaticCh chan *enode.Node
	remStaticCh chan *enode.Node
	addPeerCh   chan *conn
	remPeerCh   chan *conn

	subProtocolVersion uint

	// Everything below here belongs to loop and
	// should only be accessed by code on the loop goroutine.
	dialing   map[enode.ID]*dialTask // active tasks
	peers     map[enode.ID]connFlag  // all connected peers
	dialPeers int                    // current number of dialed peers

	// The static map tracks all static dial tasks. The subset of usable static dial tasks
	// (i.e. those passing checkDial) is kept in staticPool. The scheduler prefers
	// launching random static tasks from the pool over launching dynamic dials from the
	// iterator.
	static     map[enode.ID]*dialTask
	staticPool []*dialTask

	// The dial history keeps recently dialed nodes. Members of history are not dialed.
	history          expHeap
	historyTimer     mclock.Timer
	historyTimerTime mclock.AbsTime

	// for logStats
	dialed int
	errors map[string]uint
}

type dialSetupFunc func(net.Conn, connFlag, *enode.Node) error

type dialConfig struct {
	self           enode.ID         // our own ID
	maxDialPeers   int              // maximum number of dialed peers
	maxActiveDials int              // maximum number of active dials
	netRestrict    *netutil.Netlist // IP whitelist, disabled if nil
	resolver       nodeResolver
	dialer         NodeDialer
	log            log.Logger
	clock          mclock.Clock
	rand           *mrand.Rand
}

func (cfg dialConfig) withDefaults() dialConfig {
	if cfg.log == nil {
		cfg.log = log.Root()
	}
	if cfg.clock == nil {
		cfg.clock = mclock.System{}
	}
	if cfg.rand == nil {
		seedb := make([]byte, 8)
		if _, err := crand.Read(seedb); err != nil {
			panic(err)
		}
		seed := int64(binary.BigEndian.Uint64(seedb))
		cfg.rand = mrand.New(mrand.NewSource(seed)) // nolint: gosec
	}
	return cfg
}

func newDialScheduler(config dialConfig, it enode.Iterator, setupFunc dialSetupFunc, subProtocolVersion uint) *dialScheduler {
	d := &dialScheduler{
		dialConfig:  config.withDefaults(),
		setupFunc:   setupFunc,
		dialing:     make(map[enode.ID]*dialTask),
		static:      make(map[enode.ID]*dialTask),
		peers:       make(map[enode.ID]connFlag),
		doneCh:      make(chan *dialTask),
		nodesIn:     make(chan *enode.Node),
		addStaticCh: make(chan *enode.Node),
		remStaticCh: make(chan *enode.Node),
		addPeerCh:   make(chan *conn),
		remPeerCh:   make(chan *conn),

		subProtocolVersion: subProtocolVersion,
		errors:             map[string]uint{},
	}

	d.ctx, d.cancel = context.WithCancel(context.Background())
	d.wg.Add(2)
	go d.readNodes(it)
	go d.loop(it)
	return d
}

// stop shuts down the dialer, canceling all current dial tasks.
func (d *dialScheduler) stop() {
	d.cancel()
	d.wg.Wait()
}

// addStatic adds a static dial candidate.
func (d *dialScheduler) addStatic(n *enode.Node) {
	select {
	case d.addStaticCh <- n:
	case <-d.ctx.Done():
	}
}

// removeStatic removes a static dial candidate.
func (d *dialScheduler) removeStatic(n *enode.Node) {
	select {
	case d.remStaticCh <- n:
	case <-d.ctx.Done():
	}
}

// peerAdded updates the peer set.
func (d *dialScheduler) peerAdded(c *conn) {
	select {
	case d.addPeerCh <- c:
	case <-d.ctx.Done():
	}
}

// peerRemoved updates the peer set.
func (d *dialScheduler) peerRemoved(c *conn) {
	select {
	case d.remPeerCh <- c:
	case <-d.ctx.Done():
	}
}

// loop is the main loop of the dialer.
func (d *dialScheduler) loop(it enode.Iterator) {
	defer dbg.LogPanic()
	var (
		nodesCh    chan *enode.Node
		historyExp = make(chan struct{}, 1)
	)

	logTimer := time.NewTicker(dialStatsLogInterval)
	defer logTimer.Stop()

loop:
	for {
		// Launch new dials if slots are available.
		slots := d.freeDialSlots()
		d.startStaticDials()
		if slots > 0 {
			nodesCh = d.nodesIn
		} else {
			nodesCh = nil
		}
		d.rearmHistoryTimer(historyExp)

		select {
		case <-d.ctx.Done():
			it.Close()
			break loop

		case <-logTimer.C:
			d.logStats()

		case node := <-nodesCh:
			if err := d.checkDial(node); err != nil {
				d.log.Trace("Discarding dial candidate", "id", node.ID(), "ip", node.IP(), "reason", err)
			} else {
				d.startDial(newDialTask(node, dynDialedConn))
			}

		case task := <-d.doneCh:
			id := task.dest().ID()
			delete(d.dialing, id)
			d.updateStaticPool(id)
			d.dialed++

		case c := <-d.addPeerCh:
			if c.is(dynDialedConn) || c.is(staticDialedConn) {
				d.dialPeers++
			}
			id := c.node.ID()
			d.peers[id] = connFlag(atomic.LoadInt32((*int32)(&c.flags)))
			// Remove from static pool because the node is now connected.
			task := d.static[id]
			if task != nil && task.staticPoolIndex >= 0 {
				d.removeFromStaticPool(task.staticPoolIndex)
			}
			// TODO: cancel dials to connected peers

		case c := <-d.remPeerCh:
			if c.is(dynDialedConn) || c.is(staticDialedConn) {
				d.dialPeers--
			}
			delete(d.peers, c.node.ID())
			d.updateStaticPool(c.node.ID())

		case node := <-d.addStaticCh:
			id := node.ID()
			_, exists := d.static[id]
			d.log.Trace("Adding static node", "id", id, "ip", node.IP(), "added", !exists)
			if exists {
				continue loop
			}
			task := newDialTask(node, staticDialedConn)
			d.static[id] = task
			if d.checkDial(node) == nil {
				d.addToStaticPool(task)
			}

		case node := <-d.remStaticCh:
			id := node.ID()
			task := d.static[id]
			d.log.Trace("Removing static node", "id", id, "ok", task != nil)
			if task != nil {
				delete(d.static, id)
				if task.staticPoolIndex >= 0 {
					d.removeFromStaticPool(task.staticPoolIndex)
				}
			}

		case <-historyExp:
			d.expireHistory()

		}
	}

	d.stopHistoryTimer(historyExp)
	for range d.dialing {
		<-d.doneCh
	}
	d.wg.Done()
}

// readNodes runs in its own goroutine and delivers nodes from
// the input iterator to the nodesIn channel.
func (d *dialScheduler) readNodes(it enode.Iterator) {
	defer dbg.LogPanic()
	defer d.wg.Done()

	for it.Next() {
		select {
		case d.nodesIn <- it.Node():
		case <-d.ctx.Done():
		}
	}
}

// or comes back online.
// nolint
func (d *dialScheduler) logStats() {
	vals := []interface{}{"protocol", d.subProtocolVersion,
		"peers", fmt.Sprintf("%d/%d", len(d.peers), d.maxDialPeers), "tried", d.dialed, "static", len(d.static)}

	d.mutex.Lock()
	for err, count := range d.errors {
		vals = append(vals, err, count)
	}
	d.mutex.Unlock()

	d.log.Debug("[p2p] Dial scheduler", vals...)
}

// rearmHistoryTimer configures d.historyTimer to fire when the
// next item in d.history expires.
func (d *dialScheduler) rearmHistoryTimer(ch chan struct{}) {
	if len(d.history) == 0 || d.historyTimerTime == d.history.nextExpiry() {
		return
	}
	d.stopHistoryTimer(ch)
	d.historyTimerTime = d.history.nextExpiry()
	timeout := time.Duration(d.historyTimerTime - d.clock.Now())
	d.historyTimer = d.clock.AfterFunc(timeout, func() { ch <- struct{}{} })
}

// stopHistoryTimer stops the timer and drains the channel it sends on.
func (d *dialScheduler) stopHistoryTimer(ch chan struct{}) {
	if d.historyTimer != nil && !d.historyTimer.Stop() {
		<-ch
	}
}

// expireHistory removes expired items from d.history.
func (d *dialScheduler) expireHistory() {
	d.historyTimer.Stop()
	d.historyTimer = nil
	d.historyTimerTime = 0
	d.history.expire(d.clock.Now(), func(hkey string) {
		var id enode.ID
		copy(id[:], hkey)
		d.updateStaticPool(id)
	})
}

// freeDialSlots returns the number of free dial slots. The result can be negative
// when peers are connected while their task is still running.
func (d *dialScheduler) freeDialSlots() int {
	slots := (d.maxDialPeers - d.dialPeers) * 2
	if slots > d.maxActiveDials {
		slots = d.maxActiveDials
	}
	free := slots - len(d.dialing)
	return free
}

// checkDial returns an error if node n should not be dialed.
func (d *dialScheduler) checkDial(n *enode.Node) error {
	if n.ID() == d.self {
		return errSelf
	}
	if n.IP() != nil && n.TCP() == 0 {
		// This check can trigger if a non-TCP node is found
		// by discovery. If there is no IP, the node is a static
		// node and the actual endpoint will be resolved later in dialTask.
		return errNoPort
	}
	if _, ok := d.dialing[n.ID()]; ok {
		return errAlreadyDialing
	}
	if _, ok := d.peers[n.ID()]; ok {
		return errAlreadyConnected
	}
	if d.netRestrict != nil && !d.netRestrict.Contains(n.IP()) {
		return errNotWhitelisted
	}
	if d.history.contains(string(n.ID().Bytes())) {
		return errRecentlyDialed
	}
	return nil
}

// startStaticDials starts n static dial tasks.
func (d *dialScheduler) startStaticDials() {
	for len(d.staticPool) > 0 {
		idx := d.rand.Intn(len(d.staticPool))
		task := d.staticPool[idx]
		d.startDial(task)
		d.removeFromStaticPool(idx)
	}
}

// updateStaticPool attempts to move the given static dial back into staticPool.
func (d *dialScheduler) updateStaticPool(id enode.ID) {
	task, ok := d.static[id]
	if ok && task.staticPoolIndex < 0 && d.checkDial(task.dest()) == nil {
		d.addToStaticPool(task)
	}
}

func (d *dialScheduler) addToStaticPool(task *dialTask) {
	if task.staticPoolIndex >= 0 {
		panic("attempt to add task to staticPool twice")
	}
	d.staticPool = append(d.staticPool, task)
	task.staticPoolIndex = len(d.staticPool) - 1
}

// removeFromStaticPool removes the task at idx from staticPool. It does that by moving the
// current last element of the pool to idx and then shortening the pool by one.
func (d *dialScheduler) removeFromStaticPool(idx int) {
	task := d.staticPool[idx]
	end := len(d.staticPool) - 1
	d.staticPool[idx] = d.staticPool[end]
	d.staticPool[idx].staticPoolIndex = idx
	d.staticPool[end] = nil
	d.staticPool = d.staticPool[:end]
	task.staticPoolIndex = -1
}

// startDial runs the given dial task in a separate goroutine.
func (d *dialScheduler) startDial(task *dialTask) {
	node := task.dest()
	d.log.Trace("Starting p2p dial", "id", node.ID(), "ip", node.IP(), "flag", task.flags)
	hkey := string(node.ID().Bytes())
	d.history.add(hkey, d.clock.Now().Add(dialHistoryExpiration))
	d.dialing[node.ID()] = task
	go func() {
		defer dbg.LogPanic()
		task.run(d)
		d.doneCh <- task
	}()
}

// A dialTask generated for each node that is dialed.
type dialTask struct {
	staticPoolIndex int
	flags           connFlag
	// These fields are private to the task and should not be
	// accessed by dialScheduler while the task is running.
	destPtr      atomic.Pointer[enode.Node]
	lastResolved mclock.AbsTime
	resolveDelay time.Duration
}

func newDialTask(dest *enode.Node, flags connFlag) *dialTask {
	t := &dialTask{flags: flags, staticPoolIndex: -1}
	t.destPtr.Store(dest)
	return t
}

func (t *dialTask) dest() *enode.Node {
	return t.destPtr.Load()
}

type dialError struct {
	error
}

func (t *dialTask) run(d *dialScheduler) {
	if t.needResolve() && !t.resolve(d) {
		return
	}

	err := t.dial(d, t.dest())
	if err != nil {
		// For static nodes, resolve one more time if dialing fails.
		if _, ok := err.(*dialError); ok && t.flags&staticDialedConn != 0 {
			if t.resolve(d) {
				t.dial(d, t.dest()) //nolint:errcheck
			}
		}
	}
}

func (t *dialTask) needResolve() bool {
	return t.flags&staticDialedConn != 0 && t.dest().IP() == nil
}

// resolve attempts to find the current endpoint for the destination
// using discovery.
//
// Resolve operations are throttled with backoff to avoid flooding the
// discovery network with useless queries for nodes that don't exist.
// The backoff delay resets when the node is found.
func (t *dialTask) resolve(d *dialScheduler) bool {
	if d.resolver == nil {
		return false
	}
	if t.resolveDelay == 0 {
		t.resolveDelay = initialResolveDelay
	}
	if t.lastResolved > 0 && time.Duration(d.clock.Now()-t.lastResolved) < t.resolveDelay {
		return false
	}
	resolved := d.resolver.Resolve(t.dest())
	t.lastResolved = d.clock.Now()
	if resolved == nil {
		t.resolveDelay *= 2
		if t.resolveDelay > maxResolveDelay {
			t.resolveDelay = maxResolveDelay
		}
		d.log.Warn("Resolving node failed", "id", t.dest().ID(), "newdelay", t.resolveDelay)
		return false
	}
	// The node was found.
	t.resolveDelay = initialResolveDelay
	t.destPtr.Store(resolved)
	d.log.Trace("Resolved node", "id", resolved.ID(), "addr", &net.TCPAddr{IP: resolved.IP(), Port: resolved.TCP()})
	return true
}

// dial performs the actual connection attempt.
func (t *dialTask) dial(d *dialScheduler, dest *enode.Node) error {
	fd, err := d.dialer.Dial(d.ctx, dest)
	if err != nil {
		cleanErr := cleanupDialErr(err)
		d.log.Trace("Dial error", "id", dest.ID(), "addr", nodeAddr(dest), "conn", t.flags, "err", cleanErr)

		d.mutex.Lock()
		d.errors[cleanErr.Error()] = d.errors[cleanErr.Error()] + 1
		d.mutex.Unlock()
		return &dialError{err}
	}
	mfd := newMeteredConn(fd, false, &net.TCPAddr{IP: dest.IP(), Port: dest.TCP()})
	return d.setupFunc(mfd, t.flags, dest)
}

func (t *dialTask) String() string {
	node := t.dest()
	id := node.ID()
	return fmt.Sprintf("%v %x %v:%d", t.flags, id[:8], node.IP(), node.TCP())
}

func cleanupDialErr(err error) error {
	if netErr, ok := err.(*net.OpError); ok && netErr.Op == "dial" {
		return netErr.Err
	}
	return err
}
