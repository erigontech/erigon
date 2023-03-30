// Copyright 2016 The go-ethereum Authors
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

// Package ethstats implements the network stats reporting service.
package ethstats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cmd/sentry/sentry"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/node"
)

const (
	// historyUpdateRange is the number of blocks a node should report upon login or
	// history request.
	historyUpdateRange = 50
)

// Service implements an Ethereum netstats reporting daemon that pushes local
// chain statistics up to a monitoring server.
type Service struct {
	servers   []*sentry.GrpcServer // Peer-to-peer server to retrieve networking infos
	chaindb   kv.RoDB
	networkid uint64
	engine    consensus.Engine // Consensus engine to retrieve variadic block fields

	node string // Name of the node to display on the monitoring page
	pass string // Password to authorize access to the monitoring page
	host string // Remote address of the monitoring service

	quitCh <-chan struct{}
	headCh <-chan [][]byte

	pongCh chan struct{} // Pong notifications are fed into this channel
	histCh chan []uint64 // History request block numbers are fed into this channel

}

// connWrapper is a wrapper to prevent concurrent-write or concurrent-read on the
// websocket.
//
// From Gorilla websocket docs:
//
//	Connections support one concurrent reader and one concurrent writer.
//	Applications are responsible for ensuring that no more than one goroutine calls the write methods
//	  - NextWriter, SetWriteDeadline, WriteMessage, WriteJSON, EnableWriteCompression, SetCompressionLevel
//	concurrently and that no more than one goroutine calls the read methods
//	  - NextReader, SetReadDeadline, ReadMessage, ReadJSON, SetPongHandler, SetPingHandler
//	concurrently.
//	The Close and WriteControl methods can be called concurrently with all other methods.
type connWrapper struct {
	conn *websocket.Conn

	rlock sync.Mutex
	wlock sync.Mutex
}

func newConnectionWrapper(conn *websocket.Conn) *connWrapper {
	return &connWrapper{conn: conn}
}

// WriteJSON wraps corresponding method on the websocket but is safe for concurrent calling
func (w *connWrapper) WriteJSON(v interface{}) error {
	if w.conn == nil {
		return nil
	}
	w.wlock.Lock()
	defer w.wlock.Unlock()

	return w.conn.WriteJSON(v)
}

// ReadJSON wraps corresponding method on the websocket but is safe for concurrent calling
func (w *connWrapper) ReadJSON(v interface{}) error {
	w.rlock.Lock()
	defer w.rlock.Unlock()

	return w.conn.ReadJSON(v)
}

// Close wraps corresponding method on the websocket but is safe for concurrent calling
func (w *connWrapper) Close() error {
	// The Close and WriteControl methods can be called concurrently with all other methods,
	// so the mutex is not used here
	return w.conn.Close()
}

// New returns a monitoring service ready for stats reporting.
func New(node *node.Node, servers []*sentry.GrpcServer, chainDB kv.RoDB, engine consensus.Engine, url string, networkid uint64, quitCh <-chan struct{}, headCh chan [][]byte) error {
	// Parse the netstats connection url
	re := regexp.MustCompile("([^:@]*)(:([^@]*))?@(.+)")
	parts := re.FindStringSubmatch(url)
	if len(parts) != 5 {
		return fmt.Errorf("invalid netstats url: \"%s\", should be nodename:secret@host:port", url)
	}
	ethstats := &Service{
		engine:    engine,
		servers:   servers,
		node:      parts[1],
		pass:      parts[3],
		host:      parts[4],
		pongCh:    make(chan struct{}),
		histCh:    make(chan []uint64, 1),
		networkid: networkid,
		chaindb:   chainDB,
		headCh:    headCh,
		quitCh:    quitCh,
	}

	node.RegisterLifecycle(ethstats)
	return nil
}

// Start implements node.Lifecycle, starting up the monitoring and reporting daemon.
func (s *Service) Start() error {
	go s.loop()

	log.Info("Stats daemon started")
	return nil
}

// Stop implements node.Lifecycle, terminating the monitoring and reporting daemon.
func (s *Service) Stop() error {
	log.Info("Stats daemon stopped")
	return nil
}

// loop keeps trying to connect to the netstats server, reporting chain events
// until termination.
func (s *Service) loop() {
	// Resolve the URL, defaulting to TLS, but falling back to none too
	path := fmt.Sprintf("%s/api", s.host)
	urls := []string{path}

	// url.Parse and url.IsAbs is unsuitable (https://github.com/golang/go/issues/19779)
	if !strings.Contains(path, "://") {
		urls = []string{"wss://" + path, "ws://" + path}
	}

	errTimer := time.NewTimer(0)
	defer errTimer.Stop()
	// Loop reporting until termination
	for {
		select {
		case <-s.quitCh:
			return
		case <-errTimer.C:
			// Establish a websocket connection to the server on any supported URL
			var (
				conn *connWrapper
				err  error
			)
			dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
			header := make(http.Header)
			header.Set("origin", "http://localhost")
			for _, url := range urls {
				//nolint
				c, _, err := dialer.Dial(url, header)
				if err == nil {
					conn = newConnectionWrapper(c)
					break
				}
			}
			if err != nil || conn == nil {
				log.Warn("Stats server unreachable")
				errTimer.Reset(10 * time.Second)
				continue
			}
			// Authenticate the client with the server
			if err = s.login(conn); err != nil {
				log.Warn("Stats login failed", "err", err)
				conn.Close()
				errTimer.Reset(10 * time.Second)
				continue
			}
			go s.readLoop(conn)

			// Keep sending status updates until the connection breaks
			fullReport := time.NewTicker(15 * time.Second)

			for err == nil {
				select {
				case <-s.quitCh:
					fullReport.Stop()
					// Make sure the connection is closed
					conn.Close()
					return

				case <-fullReport.C:
					if err = s.report(conn); err != nil {
						log.Warn("Full stats report failed", "err", err)
					}
				case list := <-s.histCh:
					if err = s.reportHistory(conn, list); err != nil {
						log.Warn("Requested history report failed", "err", err)
					}
				case <-s.headCh:
					if err = s.reportBlock(conn); err != nil {
						log.Warn("Block stats report failed", "err", err)
					}

				}
			}
			fullReport.Stop()

			// Close the current connection and establish a new one
			conn.Close()
			errTimer.Reset(0)
		}
	}
}

// readLoop loops as long as the connection is alive and retrieves data packets
// from the network socket. If any of them match an active request, it forwards
// it, if they themselves are requests it initiates a reply, and lastly it drops
// unknown packets.
func (s *Service) readLoop(conn *connWrapper) {
	// If the read loop exists, close the connection
	defer func(conn *connWrapper) {
		closeErr := conn.Close()
		if closeErr != nil {
			log.Warn("Failed to close connection", "err", closeErr)
		}
	}(conn)

	for {
		// Retrieve the next generic network packet and bail out on error
		var blob json.RawMessage
		if err := conn.ReadJSON(&blob); err != nil {
			log.Warn("Failed to retrieve stats server message", "err", err)
			return
		}
		// If the network packet is a system ping, respond to it directly
		var ping string
		if err := json.Unmarshal(blob, &ping); err == nil && strings.HasPrefix(ping, "primus::ping::") {
			if err := conn.WriteJSON(strings.ReplaceAll(ping, "ping", "pong")); err != nil {
				log.Warn("Failed to respond to system ping message", "err", err)
				return
			}
			continue
		}
		// Not a system ping, try to decode an actual state message
		var msg map[string][]interface{}
		if err := json.Unmarshal(blob, &msg); err != nil {
			log.Warn("Failed to decode stats server message", "err", err)
			return
		}
		log.Trace("Received message from stats server", "msg", msg)
		if len(msg["emit"]) == 0 {
			log.Warn("Stats server sent non-broadcast", "msg", msg)
			return
		}
		command, ok := msg["emit"][0].(string)
		if !ok {
			log.Warn("Invalid stats server message type", "type", msg["emit"][0])
			return
		}
		// If the message is a ping reply, deliver (someone must be listening!)
		if len(msg["emit"]) == 2 && command == "node-pong" {
			select {
			case s.pongCh <- struct{}{}:
				// Pong delivered, continue listening
				continue
			default:
				// Ping routine dead, abort
				log.Warn("Stats server pinger seems to have died")
				return
			}
		}
		// If the message is a history request, forward to the event processor
		if len(msg["emit"]) == 2 && command == "history" {
			// Make sure the request is valid and doesn't crash us
			request, ok := msg["emit"][1].(map[string]interface{})
			if !ok {
				log.Warn("Invalid stats history request", "msg", msg["emit"][1])
				select {
				case s.histCh <- nil: // Treat it as an no indexes request
				default:
				}
				continue
			}
			list, ok := request["list"].([]interface{})
			if !ok {
				log.Warn("Invalid stats history block list", "list", request["list"])
				return
			}
			// Convert the block number list to an integer list
			numbers := make([]uint64, len(list))
			for i, num := range list {
				n, ok := num.(float64)
				if !ok {
					log.Warn("Invalid stats history block number", "number", num)
					return
				}
				numbers[i] = uint64(n)
			}
			select {
			case s.histCh <- numbers:
				continue
			default:
			}
		}
		// Report anything else and continue
		log.Info("Unknown stats message", "msg", msg)
	}
}

// nodeInfo is the collection of meta information about a node that is displayed
// on the monitoring page.
type nodeInfo struct {
	Name     string `json:"name"`
	Node     string `json:"node"`
	Port     int    `json:"port"`
	Network  string `json:"net"`
	Protocol string `json:"protocol"`
	API      string `json:"api"`
	Os       string `json:"os"`
	OsVer    string `json:"os_v"`
	Client   string `json:"client"`
	History  bool   `json:"canUpdateHistory"`
}

// authMsg is the authentication infos needed to login to a monitoring server.
type authMsg struct {
	ID     string   `json:"id"`
	Info   nodeInfo `json:"info"`
	Secret string   `json:"secret"`
}

// login tries to authorize the client at the remote server.
func (s *Service) login(conn *connWrapper) error {
	// Construct and send the login authentication
	// infos := s.server.NodeInfo()

	protocols := make([]string, 0, len(s.servers))
	for _, srv := range s.servers {
		for _, p := range srv.Protocols {
			protocols = append(protocols, fmt.Sprintf("%s/%d", p.Name, p.Version))
		}
	}
	nodeName := "Erigon"
	if len(s.servers) > 0 {
		nodeInfo, err := s.servers[0].NodeInfo(context.TODO(), nil)
		if err != nil {
			return err
		}
		nodeName = nodeInfo.Name
	}

	auth := &authMsg{
		ID: s.node,
		Info: nodeInfo{
			Name:     s.node,
			Node:     nodeName,
			Port:     0,
			Network:  fmt.Sprintf("%d", s.networkid),
			Protocol: strings.Join(protocols, ", "),
			API:      "No",
			Os:       runtime.GOOS,
			OsVer:    runtime.GOARCH,
			Client:   "0.1.1",
			History:  true,
		},
		Secret: s.pass,
	}
	login := map[string][]interface{}{
		"emit": {"hello", auth},
	}
	if err := conn.WriteJSON(login); err != nil {
		return err
	}
	// Retrieve the remote ack or connection termination
	var ack map[string][]string
	if err := conn.ReadJSON(&ack); err != nil || len(ack["emit"]) != 1 || ack["emit"][0] != "ready" {
		return errors.New("unauthorized")
	}
	return nil
}

// report collects all possible data to report and send it to the stats server.
// This should only be used on reconnects or rarely to avoid overloading the
// server. Use the individual methods for reporting subscribed events.
func (s *Service) report(conn *connWrapper) error {
	if err := s.reportLatency(conn); err != nil {
		return err
	}
	if err := s.reportBlock(conn); err != nil {
		return err
	}
	if err := s.reportPending(conn); err != nil {
		return err
	}
	if err := s.reportStats(conn); err != nil {
		return err
	}
	return nil
}

// reportLatency sends a ping request to the server, measures the RTT time and
// finally sends a latency update.
func (s *Service) reportLatency(conn *connWrapper) error {
	// Send the current time to the ethstats server
	start := time.Now()

	ping := map[string][]interface{}{
		"emit": {"node-ping", map[string]string{
			"id":         s.node,
			"clientTime": start.String(),
		}},
	}
	if err := conn.WriteJSON(ping); err != nil {
		return err
	}
	// Wait for the pong request to arrive back
	select {
	case <-s.pongCh:
		// Pong delivered, report the latency
	case <-time.After(5 * time.Second):
		// Ping timeout, abort
		return errors.New("ping timed out")
	}
	latency := strconv.Itoa(int((time.Since(start) / time.Duration(2)).Nanoseconds() / 1000000))

	// Send back the measured latency
	log.Trace("Sending measured latency to ethstats", "latency", latency)

	stats := map[string][]interface{}{
		"emit": {"latency", map[string]string{
			"id":      s.node,
			"latency": latency,
		}},
	}
	return conn.WriteJSON(stats)
}

// blockStats is the information to report about individual blocks.
type blockStats struct {
	Number     *big.Int          `json:"number"`
	Hash       libcommon.Hash    `json:"hash"`
	ParentHash libcommon.Hash    `json:"parentHash"`
	Timestamp  *big.Int          `json:"timestamp"`
	Miner      libcommon.Address `json:"miner"`
	GasUsed    uint64            `json:"gasUsed"`
	GasLimit   uint64            `json:"gasLimit"`
	Diff       string            `json:"difficulty"`
	TotalDiff  string            `json:"totalDifficulty"`
	Txs        []txStats         `json:"transactions"`
	TxHash     libcommon.Hash    `json:"transactionsRoot"`
	Root       libcommon.Hash    `json:"stateRoot"`
	Uncles     uncleStats        `json:"uncles"`
}

// txStats is the information to report about individual transactions.
type txStats struct {
	Hash libcommon.Hash `json:"hash"`
}

// uncleStats is a custom wrapper around an uncle array to force serializing
// empty arrays instead of returning null for them.
type uncleStats []*types.Header

func (s uncleStats) MarshalJSON() ([]byte, error) {
	if uncles := ([]*types.Header)(s); len(uncles) > 0 {
		return json.Marshal(uncles)
	}
	return []byte("[]"), nil
}

// reportBlock retrieves the current chain head and reports it to the stats server.
func (s *Service) reportBlock(conn *connWrapper) error {
	roTx, err := s.chaindb.BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer roTx.Rollback()

	block := rawdb.ReadCurrentBlock(roTx)
	if block == nil {
		return nil
	}

	td, err := rawdb.ReadTd(roTx, block.Hash(), block.NumberU64())
	if err != nil {
		return err
	}

	// Gather the block details from the header or block chain
	details := s.assembleBlockStats(block, td)

	// Assemble the block report and send it to the server
	log.Trace("Sending new block to ethstats", "number", details.Number, "hash", details.Hash)

	stats := map[string]interface{}{
		"id":    s.node,
		"block": details,
	}
	report := map[string][]interface{}{
		"emit": {"block", stats},
	}
	return conn.WriteJSON(report)
}

// assembleBlockStats retrieves any required metadata to report a single block
// and assembles the block stats. If block is nil, the current head is processed.
func (s *Service) assembleBlockStats(block *types.Block, td *big.Int) *blockStats {
	if td == nil {
		td = libcommon.Big0
	}
	// Gather the block infos from the local blockchain
	txs := make([]txStats, 0, len(block.Transactions()))
	for _, tx := range block.Transactions() {
		txs = append(txs, txStats{tx.Hash()})
	}

	return &blockStats{
		Number:     block.Header().Number,
		Hash:       block.Hash(),
		ParentHash: block.Header().ParentHash,
		Timestamp:  new(big.Int).SetUint64(block.Header().Time),
		Miner:      block.Header().Coinbase,
		GasUsed:    block.Header().GasUsed,
		GasLimit:   block.Header().GasLimit,
		Diff:       block.Header().Difficulty.String(),
		TotalDiff:  td.String(),
		Txs:        txs,
		TxHash:     block.Header().TxHash,
		Root:       block.Header().Root,
		Uncles:     block.Uncles(),
	}
}

// reportHistory retrieves the most recent batch of blocks and reports it to the
// stats server.
func (s *Service) reportHistory(conn *connWrapper, list []uint64) error {
	roTx, err := s.chaindb.BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer roTx.Rollback()

	// Figure out the indexes that need reporting
	indexes := make([]uint64, 0, historyUpdateRange)
	if len(list) > 0 {
		// Specific indexes requested, send them back in particular
		indexes = append(indexes, list...)
	} else {
		// No indexes requested, send back the top ones
		headHash := rawdb.ReadHeadBlockHash(roTx)
		headNumber := rawdb.ReadHeaderNumber(roTx, headHash)
		if headNumber == nil {
			return nil
		}
		start := int(*headNumber - historyUpdateRange + 1)
		if start < 0 {
			start = 0
		}
		for i := uint64(start); i <= *headNumber; i++ {
			indexes = append(indexes, i)
		}
	}
	// Gather the batch of blocks to report
	history := make([]*blockStats, len(indexes))
	for i, number := range indexes {
		// Retrieve the next block if it's known to us
		block, err := rawdb.ReadBlockByNumber(roTx, number)
		if err != nil {
			return err
		}
		td, err := rawdb.ReadTd(roTx, block.Hash(), number)
		if err != nil {
			return err
		}
		// If we do have the block, add to the history and continue
		if block != nil {
			history[len(history)-1-i] = s.assembleBlockStats(block, td)
			continue
		}
		// Ran out of blocks, cut the report short and send
		history = history[len(history)-i:]
		break
	}
	// Assemble the history report and send it to the server
	if len(history) > 0 {
		log.Trace("Sending historical blocks to ethstats", "first", history[0].Number, "last", history[len(history)-1].Number)
	} else {
		log.Trace("No history to send to stats server")
	}
	stats := map[string]interface{}{
		"id":      s.node,
		"history": history,
	}
	report := map[string][]interface{}{
		"emit": {"history", stats},
	}
	return conn.WriteJSON(report)
}

// reportPending retrieves the current number of pending transactions and reports
// it to the stats server.
func (s *Service) reportPending(conn *connWrapper) error {
	/*	// Retrieve the pending count from the local blockchain
		pending, _ := s.backend.Stats()
		// Assemble the transaction stats and send it to the server
		log.Trace("Sending pending transactions to ethstats", "count", pending)

		stats := map[string]interface{}{
			"id": s.node,
			"stats": &pendStats{
				Pending: pending,
			},
		}
		report := map[string][]interface{}{
			"emit": {"pending", stats},
		}
		return conn.WriteJSON(report)*/
	return nil
}

// nodeStats is the information to report about the local node.
type nodeStats struct {
	Active    bool `json:"active"`
	Syncing   bool `json:"syncing"`
	Mining    bool `json:"mining"`
	Hashrate  int  `json:"hashrate"`
	GoodPeers int  `json:"peers"`
	GasPrice  int  `json:"gasPrice"`
	Uptime    int  `json:"uptime"`
}

// reportStats retrieves various stats about the node at the networking and
// mining layer and reports it to the stats server.
func (s *Service) reportStats(conn *connWrapper) error {
	roTx, err := s.chaindb.BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer roTx.Rollback()
	sync, err := stages.GetStageProgress(roTx, stages.Execution)
	if err != nil {
		return err
	}
	finishSync, err := stages.GetStageProgress(roTx, stages.Finish)
	if err != nil {
		return err
	}
	// TODO(Giulio2002): peer tracking
	peerCount := 0
	for _, srv := range s.servers {
		counts := srv.SimplePeerCount()
		for _, count := range counts {
			peerCount += count
		}
	}
	stats := map[string]interface{}{
		"id": s.node,
		"stats": &nodeStats{
			Active:    true,
			Mining:    false,
			Hashrate:  0,
			GoodPeers: peerCount,
			GasPrice:  0,
			Syncing:   sync != finishSync,
			Uptime:    100,
		},
	}
	report := map[string][]interface{}{
		"emit": {"stats", stats},
	}
	return conn.WriteJSON(report)
}
