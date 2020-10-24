// Copyright 2018 The go-ethereum Authors
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

package enode

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"github.com/c2h5oh/datasize"
	"net"
	"os"
	"sync"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

// Keys in the node database.

const (
	dbVersionKey   = "version" // Version of the database to flush if changes
	dbNodePrefix   = "n:"      // Identifier to prefix node entries with
	dbLocalPrefix  = "local:"
	dbDiscoverRoot = "v4"
	dbDiscv5Root   = "v5"

	// These fields are stored per ID and IP, the full key is "n:<ID>:v4:<IP>:findfail".
	// Use nodeItemKey to create those keys.
	dbNodeFindFails = "findfail"
	dbNodePing      = "lastping"
	dbNodePong      = "lastpong"
	dbNodeSeq       = "seq"

	// Local information is keyed by ID only, the full key is "local:<ID>:seq".
	// Use localItemKey to create those keys.
	dbLocalSeq = "seq"
)

const (
	dbNodeExpiration = 24 * time.Hour // Time after which an unseen node should be dropped.
	dbCleanupCycle   = time.Hour      // Time period for running the expiration task.
	dbVersion        = 9
)

var zeroIP = make(net.IP, 16)

// DB is the node database, storing previously seen nodes and any collected metadata about
// them for QoS purposes.
type DB struct {
	kv     ethdb.KV      // Interface to the database itself
	runner sync.Once     // Ensures we can start at most one expirer
	quit   chan struct{} // Channel to signal the expiring thread to stop
}

// OpenDB opens a node database for storing and retrieving infos about known peers in the
// network. If no path is given an in-memory, temporary database is constructed.
func OpenDB(path string) (*DB, error) {
	if path == "" {
		return newMemoryDB()
	}
	return newPersistentDB(path)
}

var bucketsConfig = func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
	return dbutils.BucketsCfg{
		dbutils.InodesBucket: {},
	}
}

// newMemoryNodeDB creates a new in-memory node database without a persistent backend.
func newMemoryDB() (*DB, error) {
	db, err := ethdb.NewLMDB().InMem().WithBucketsConfig(bucketsConfig).Open()
	if err != nil {
		return nil, err
	}
	return &DB{kv: db, quit: make(chan struct{})}, nil
}

// newPersistentNodeDB creates/opens a persistent node database,
// also flushing its contents in case of a version mismatch.
func newPersistentDB(path string) (*DB, error) {
	kv, err := ethdb.NewLMDB().Path(path).MapSize(64 * datasize.MB).WithBucketsConfig(bucketsConfig).Open()
	if err != nil {
		return nil, err
	}
	// The nodes contained in the cache correspond to a certain protocol version.
	// Flush all nodes if the version doesn't match.
	currentVer := make([]byte, binary.MaxVarintLen64)
	currentVer = currentVer[:binary.PutVarint(currentVer, int64(dbVersion))]

	var blob []byte
	if err := kv.Update(context.Background(), func(tx ethdb.Tx) error {
		c := tx.Cursor(dbutils.InodesBucket)
		v, errGet := c.SeekExact([]byte(dbVersionKey))
		if errGet != nil {
			return errGet
		}
		if v != nil {
			// v only lives during transaction tx
			blob = make([]byte, len(v))
			copy(blob, v)
			return nil
		}
		return c.Put([]byte(dbVersionKey), currentVer)
	}); err != nil {
		return nil, err
	}
	if blob != nil && !bytes.Equal(blob, currentVer) {
		kv.Close()
		if err := os.Remove(path); err != nil {
			return nil, err
		}
		return newPersistentDB(path)
	}
	return &DB{kv: kv, quit: make(chan struct{})}, nil
}

// nodeKey returns the database key for a node record.
func nodeKey(id ID) []byte {
	key := append([]byte(dbNodePrefix), id[:]...)
	key = append(key, ':')
	key = append(key, dbDiscoverRoot...)
	return key
}

// splitNodeKey returns the node ID of a key created by nodeKey.
func splitNodeKey(key []byte) (id ID, rest []byte) {
	if !bytes.HasPrefix(key, []byte(dbNodePrefix)) {
		return ID{}, nil
	}
	item := key[len(dbNodePrefix):]
	copy(id[:], item[:len(id)])
	return id, item[len(id)+1:]
}

// nodeItemKey returns the database key for a node metadata field.
func nodeItemKey(id ID, ip net.IP, field string) []byte {
	ip16 := ip.To16()
	if ip16 == nil {
		panic(fmt.Errorf("invalid IP (length %d)", len(ip)))
	}
	return bytes.Join([][]byte{nodeKey(id), ip16, []byte(field)}, []byte{':'})
}

// splitNodeItemKey returns the components of a key created by nodeItemKey.
func splitNodeItemKey(key []byte) (id ID, ip net.IP, field string) {
	id, key = splitNodeKey(key)
	// Skip discover root.
	if string(key) == dbDiscoverRoot {
		return id, nil, ""
	}
	key = key[len(dbDiscoverRoot)+1:]
	// Split out the IP.
	ip = net.IP(key[:16])
	if ip4 := ip.To4(); ip4 != nil {
		ip = ip4
	}
	key = key[16+1:]
	// Field is the remainder of key.
	field = string(key)
	return id, ip, field
}

func v5Key(id ID, ip net.IP, field string) []byte {
	return bytes.Join([][]byte{
		[]byte(dbNodePrefix),
		id[:],
		[]byte(dbDiscv5Root),
		ip.To16(),
		[]byte(field),
	}, []byte{':'})
}

// localItemKey returns the key of a local node item.
func localItemKey(id ID, field string) []byte {
	key := append([]byte(dbLocalPrefix), id[:]...)
	key = append(key, ':')
	key = append(key, field...)
	return key
}

// fetchInt64 retrieves an integer associated with a particular key.
func (db *DB) fetchInt64(key []byte) int64 {
	var val int64
	if err := db.kv.View(context.Background(), func(tx ethdb.Tx) error {
		blob, errGet := tx.GetOne(dbutils.InodesBucket, key)
		if errGet != nil {
			return errGet
		}
		if blob != nil {
			if v, read := binary.Varint(blob); read > 0 {
				val = v
			}
		}
		return nil
	}); err != nil {
		return 0
	}

	return val
}

// storeInt64 stores an integer in the given key.
func (db *DB) storeInt64(key []byte, n int64) error {
	blob := make([]byte, binary.MaxVarintLen64)
	blob = blob[:binary.PutVarint(blob, n)]
	return db.kv.Update(context.Background(), func(tx ethdb.Tx) error {
		return tx.Cursor(dbutils.InodesBucket).Put(common.CopyBytes(key), blob)
	})
}

// fetchUint64 retrieves an integer associated with a particular key.
func (db *DB) fetchUint64(key []byte) uint64 {
	var val uint64
	if err := db.kv.View(context.Background(), func(tx ethdb.Tx) error {
		blob, errGet := tx.GetOne(dbutils.InodesBucket, key)
		if errGet != nil {
			return errGet
		}
		if blob != nil {
			val, _ = binary.Uvarint(blob)
		}
		return nil
	}); err != nil {
		return 0
	}
	return val
}

// storeUint64 stores an integer in the given key.
func (db *DB) storeUint64(key []byte, n uint64) error {
	blob := make([]byte, binary.MaxVarintLen64)
	blob = blob[:binary.PutUvarint(blob, n)]
	return db.kv.Update(context.Background(), func(tx ethdb.Tx) error {
		return tx.Cursor(dbutils.InodesBucket).Put(common.CopyBytes(key), blob)
	})
}

// Node retrieves a node with a given id from the database.
func (db *DB) Node(id ID) *Node {
	var blob []byte
	if err := db.kv.View(context.Background(), func(tx ethdb.Tx) error {
		v, errGet := tx.GetOne(dbutils.InodesBucket, nodeKey(id))
		if errGet != nil {
			return errGet
		}
		if v != nil {
			blob = make([]byte, len(v))
			copy(blob, v)
		}
		return nil
	}); err != nil {
		return nil
	}
	if blob == nil {
		return nil
	}
	return mustDecodeNode(id[:], blob)
}

func mustDecodeNode(id, data []byte) *Node {
	node := new(Node)
	if err := rlp.DecodeBytes(data, &node.r); err != nil {
		panic(fmt.Errorf("p2p/enode: can't decode node %x in DB: %v", id, err))
	}
	// Restore node id cache.
	copy(node.id[:], id)
	return node
}

// UpdateNode inserts - potentially overwriting - a node into the peer database.
func (db *DB) UpdateNode(node *Node) error {
	if node.Seq() < db.NodeSeq(node.ID()) {
		return nil
	}
	blob, err := rlp.EncodeToBytes(&node.r)
	if err != nil {
		return err
	}
	if err := db.kv.Update(context.Background(), func(tx ethdb.Tx) error {
		return tx.Cursor(dbutils.InodesBucket).Put(nodeKey(node.ID()), blob)
	}); err != nil {
		return err
	}
	return db.storeUint64(nodeItemKey(node.ID(), zeroIP, dbNodeSeq), node.Seq())
}

// NodeSeq returns the stored record sequence number of the given node.
func (db *DB) NodeSeq(id ID) uint64 {
	return db.fetchUint64(nodeItemKey(id, zeroIP, dbNodeSeq))
}

// Resolve returns the stored record of the node if it has a larger sequence
// number than n.
func (db *DB) Resolve(n *Node) *Node {
	if n.Seq() > db.NodeSeq(n.ID()) {
		return n
	}
	return db.Node(n.ID())
}

// DeleteNode deletes all information associated with a node.
func (db *DB) DeleteNode(id ID) {
	deleteRange(db.kv, nodeKey(id))
}

func deleteRange(db ethdb.KV, prefix []byte) {
	if err := db.Update(context.Background(), func(tx ethdb.Tx) error {
		c := tx.Cursor(dbutils.InodesBucket)
		for k, _, err := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, _, err = c.Next() {
			if err != nil {
				return err
			}
			if err := c.Delete(k); err != nil {
				return nil
			}
		}
		return nil
	}); err != nil {
		log.Warn("nodeDB.deleteRange failed", "err", err)
	}
}

// ensureExpirer is a small helper method ensuring that the data expiration
// mechanism is running. If the expiration goroutine is already running, this
// method simply returns.
//
// The goal is to start the data evacuation only after the network successfully
// bootstrapped itself (to prevent dumping potentially useful seed nodes). Since
// it would require significant overhead to exactly trace the first successful
// convergence, it's simpler to "ensure" the correct state when an appropriate
// condition occurs (i.e. a successful bonding), and discard further events.
func (db *DB) ensureExpirer() {
	db.runner.Do(func() { go db.expirer() })
}

// expirer should be started in a go routine, and is responsible for looping ad
// infinitum and dropping stale data from the database.
func (db *DB) expirer() {
	tick := time.NewTicker(dbCleanupCycle)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			db.expireNodes()
		case <-db.quit:
			return
		}
	}
}

// expireNodes iterates over the database and deletes all nodes that have not
// been seen (i.e. received a pong from) for some time.
func (db *DB) expireNodes() {
	var (
		threshold    = time.Now().Add(-dbNodeExpiration).Unix()
		youngestPong int64
	)
	var toDelete [][]byte
	if err := db.kv.View(context.Background(), func(tx ethdb.Tx) error {
		c := tx.Cursor(dbutils.InodesBucket)
		p := []byte(dbNodePrefix)
		var prevId ID
		var empty = true
		for k, v, err := c.Seek(p); bytes.HasPrefix(k, p); k, v, err = c.Next() {
			if err != nil {
				return err
			}
			id, ip, field := splitNodeItemKey(k)
			if field == dbNodePong {
				time, _ := binary.Varint(v)
				if time > youngestPong {
					youngestPong = time
				}
				if time < threshold {
					// Last pong from this IP older than threshold, remove fields belonging to it.
					toDelete = append(toDelete, nodeItemKey(id, ip, ""))
				}
			}
			if id != prevId {
				if youngestPong > 0 && youngestPong < threshold {
					toDelete = append(toDelete, nodeKey(prevId))
				}
				youngestPong = 0
			}
			prevId = id
			empty = false
		}
		if !empty {
			if youngestPong > 0 && youngestPong < threshold {
				toDelete = append(toDelete, nodeKey(prevId))
			}
			youngestPong = 0
		}
		return nil
	}); err != nil {
		log.Warn("nodeDB.expireNodes failed", "err", err)
	}
	for _, td := range toDelete {
		deleteRange(db.kv, td)
	}
}

// LastPingReceived retrieves the time of the last ping packet received from
// a remote node.
func (db *DB) LastPingReceived(id ID, ip net.IP) time.Time {
	return time.Unix(db.fetchInt64(nodeItemKey(id, ip, dbNodePing)), 0)
}

// UpdateLastPingReceived updates the last time we tried contacting a remote node.
func (db *DB) UpdateLastPingReceived(id ID, ip net.IP, instance time.Time) error {
	return db.storeInt64(nodeItemKey(id, ip, dbNodePing), instance.Unix())
}

// LastPongReceived retrieves the time of the last successful pong from remote node.
func (db *DB) LastPongReceived(id ID, ip net.IP) time.Time {
	// Launch expirer
	db.ensureExpirer()
	return time.Unix(db.fetchInt64(nodeItemKey(id, ip, dbNodePong)), 0)
}

// UpdateLastPongReceived updates the last pong time of a node.
func (db *DB) UpdateLastPongReceived(id ID, ip net.IP, instance time.Time) error {
	return db.storeInt64(nodeItemKey(id, ip, dbNodePong), instance.Unix())
}

// FindFails retrieves the number of findnode failures since bonding.
func (db *DB) FindFails(id ID, ip net.IP) int {
	return int(db.fetchInt64(nodeItemKey(id, ip, dbNodeFindFails)))
}

// UpdateFindFails updates the number of findnode failures since bonding.
func (db *DB) UpdateFindFails(id ID, ip net.IP, fails int) error {
	return db.storeInt64(nodeItemKey(id, ip, dbNodeFindFails), int64(fails))
}

// FindFailsV5 retrieves the discv5 findnode failure counter.
func (db *DB) FindFailsV5(id ID, ip net.IP) int {
	return int(db.fetchInt64(v5Key(id, ip, dbNodeFindFails)))
}

// UpdateFindFailsV5 stores the discv5 findnode failure counter.
func (db *DB) UpdateFindFailsV5(id ID, ip net.IP, fails int) error {
	return db.storeInt64(v5Key(id, ip, dbNodeFindFails), int64(fails))
}

// LocalSeq retrieves the local record sequence counter.
func (db *DB) localSeq(id ID) uint64 {
	return db.fetchUint64(localItemKey(id, dbLocalSeq))
}

// storeLocalSeq stores the local record sequence counter.
func (db *DB) storeLocalSeq(id ID, n uint64) {
	db.storeUint64(localItemKey(id, dbLocalSeq), n)
}

// QuerySeeds retrieves random nodes to be used as potential seed nodes
// for bootstrapping.
func (db *DB) QuerySeeds(n int, maxAge time.Duration) []*Node {
	var (
		now   = time.Now()
		nodes = make([]*Node, 0, n)
		id    ID
	)

	if err := db.kv.View(context.Background(), func(tx ethdb.Tx) error {
		c := tx.Cursor(dbutils.InodesBucket)
	seek:
		for seeks := 0; len(nodes) < n && seeks < n*5; seeks++ {
			// Seek to a random entry. The first byte is incremented by a
			// random amount each time in order to increase the likelihood
			// of hitting all existing nodes in very small databases.
			ctr := id[0]
			rand.Read(id[:])
			id[0] = ctr + id[0]%16
			var n *Node
			for k, v, err := c.Seek(nodeKey(id)); k != nil && n == nil; k, v, err = c.Next() {
				if err != nil {
					return err
				}
				id, rest := splitNodeKey(k)
				if string(rest) == dbDiscoverRoot {
					n = mustDecodeNode(id[:], v)
				}
			}
			if n == nil {
				id[0] = 0
				continue // iterator exhausted
			}
			db.ensureExpirer()
			pongKey := nodeItemKey(n.ID(), n.IP(), dbNodePong)
			var lastPongReceived int64
			blob, errGet := tx.GetOne(dbutils.InodesBucket, pongKey)
			if errGet != nil {
				return errGet
			}
			if blob != nil {
				if v, read := binary.Varint(blob); read > 0 {
					lastPongReceived = v
				}
			}
			if now.Sub(time.Unix(lastPongReceived, 0)) > maxAge {
				continue
			}
			for i := range nodes {
				if nodes[i].ID() == n.ID() {
					continue seek // duplicate
				}
			}
			nodes = append(nodes, n)
		}
		return nil
	}); err != nil {
		log.Warn("nodeDB.QuerySeeds failed", "err", err)
	}
	return nodes
}

// close flushes and closes the database files.
func (db *DB) Close() {
	close(db.quit)
	db.kv.Close()
}
