// Copyright 2015 The go-ethereum Authors
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

// Contains the node database, storing previously seen nodes and any collected
// metadata about them for QoS purposes.

package discv5

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

var (
	nodeDBNilNodeID      = NodeID{}       // Special node ID to use as a nil element.
	nodeDBNodeExpiration = 24 * time.Hour // Time after which an unseen node should be dropped.
	nodeDBCleanupCycle   = time.Hour      // Time period for running the expiration task.
)

// nodeDB stores all nodes we know about.
type nodeDB struct {
	kv     ethdb.KV      // Interface to the database itself
	self   NodeID        // Own node id to prevent adding it into the database
	runner sync.Once     // Ensures we can start at most one expirer
	quit   chan struct{} // Channel to signal the expiring thread to stop
}

// Schema layout for the node database
const dbbucket = "discv5"

var (
	nodeDBVersionKey = []byte("version") // Version of the database to flush if changes
	nodeDBItemPrefix = []byte("n:")      // Identifier to prefix node entries with

	nodeDBDiscoverRoot      = ":discover"
	nodeDBDiscoverPing      = nodeDBDiscoverRoot + ":lastping"
	nodeDBDiscoverPong      = nodeDBDiscoverRoot + ":lastpong"
	nodeDBDiscoverFindFails = nodeDBDiscoverRoot + ":findfail"
	nodeDBTopicRegTickets   = ":tickets"
)

// newNodeDB creates a new node database for storing and retrieving infos about
// known peers in the network. If no path is given, an in-memory, temporary
// database is constructed.
func newNodeDB(path string, version int, self NodeID) (*nodeDB, error) {
	if path == "" {
		return newMemoryNodeDB(self)
	}
	return newPersistentNodeDB(path, version, self)
}

// newMemoryNodeDB creates a new in-memory node database without a persistent
// backend.
func newMemoryNodeDB(self NodeID) (*nodeDB, error) {
	kv := ethdb.NewLMDB().InMem().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbbucket: {},
		}
	}).MustOpen()
	return &nodeDB{
		kv:   kv,
		self: self,
		quit: make(chan struct{}),
	}, nil
}

// newPersistentNodeDB creates/opens a persistent node database,
// also flushing its contents in case of a version mismatch.
func newPersistentNodeDB(path string, version int, self NodeID) (*nodeDB, error) {
	db := ethdb.NewLMDB().Path(path).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbbucket: {},
		}
	}).MustOpen()
	// The nodes contained in the cache correspond to a certain protocol version.
	// Flush all nodes if the version doesn't match.
	currentVer := make([]byte, binary.MaxVarintLen64)
	currentVer = currentVer[:binary.PutVarint(currentVer, int64(version))]

	var blob []byte
	if err := db.Update(context.Background(), func(tx ethdb.Tx) error {
		c := tx.Cursor(dbbucket)
		if v, _ := c.SeekExact(nodeDBVersionKey); v != nil {
			// v only lives during transaction tx
			blob = make([]byte, len(v))
			copy(blob, v)
			return nil
		}

		return c.Put(nodeDBVersionKey, currentVer)
	}); err != nil {
		return nil, err
	}
	if blob != nil && !bytes.Equal(blob, currentVer) {
		db.Close()
		if err := os.RemoveAll(path); err != nil {
			return nil, err
		}
		return newPersistentNodeDB(path, version, self)
	}
	return &nodeDB{
		kv:   db,
		self: self,
		quit: make(chan struct{}),
	}, nil
}

// makeKey generates the key-blob from a node id and its particular
// field of interest.
func makeKey(id NodeID, field string) []byte {
	if bytes.Equal(id[:], nodeDBNilNodeID[:]) {
		return []byte(field)
	}
	return append(nodeDBItemPrefix, append(id[:], field...)...)
}

// splitKey tries to split a database key into a node id and a field part.
func splitKey(key []byte) (id NodeID, field string) {
	// If the key is not of a node, return it plainly
	if !bytes.HasPrefix(key, nodeDBItemPrefix) {
		return NodeID{}, string(key)
	}
	// Otherwise split the id and field
	item := key[len(nodeDBItemPrefix):]
	copy(id[:], item[:len(id)])
	field = string(item[len(id):])

	return id, field
}

// fetchInt64 retrieves an integer instance associated with a particular
// database key.
func (db *nodeDB) fetchInt64(key []byte) int64 {
	var val int64
	if err := db.kv.View(context.Background(), func(tx ethdb.Tx) error {
		c := tx.Cursor(dbbucket)
		blob, _ := c.SeekExact(key)
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

// storeInt64 update a specific database entry to the current time instance as a
// unix timestamp.
func (db *nodeDB) storeInt64(key []byte, n int64) error {
	blob := make([]byte, binary.MaxVarintLen64)
	blob = blob[:binary.PutVarint(blob, n)]
	return db.kv.Update(context.Background(), func(tx ethdb.Tx) error {
		return tx.Cursor(dbbucket).Put(key, blob)
	})
}

func (db *nodeDB) storeRLP(key []byte, val interface{}) error {
	blob, err := rlp.EncodeToBytes(val)
	if err != nil {
		return err
	}
	return db.kv.Update(context.Background(), func(tx ethdb.Tx) error {
		return tx.Cursor(dbbucket).Put(key, blob)
	})
}

func (db *nodeDB) fetchRLP(key []byte, val interface{}) error {
	var blob []byte
	if err := db.kv.View(context.Background(), func(tx ethdb.Tx) error {
		c := tx.Cursor(dbbucket)
		v, err := c.SeekExact(key)
		if err != nil {
			return err
		}
		if v != nil {
			blob = make([]byte, len(v))
			copy(blob, v)
		}
		return nil
	}); err != nil {
		return err
	}
	err := rlp.DecodeBytes(blob, val)
	if err != nil {
		log.Warn(fmt.Sprintf("key %x (%T) %v", key, val, err))
	}
	return err
}

// node retrieves a node with a given id from the database.
func (db *nodeDB) node(id NodeID) *Node {
	var node Node
	if err := db.fetchRLP(makeKey(id, nodeDBDiscoverRoot), &node); err != nil {
		return nil
	}
	node.sha = crypto.Keccak256Hash(node.ID[:])
	return &node
}

// updateNode inserts - potentially overwriting - a node into the peer database.
func (db *nodeDB) updateNode(node *Node) error {
	return db.storeRLP(makeKey(node.ID, nodeDBDiscoverRoot), node)
}

// deleteNode deletes all information/keys associated with a node.
func (db *nodeDB) deleteNode(id NodeID) error {
	return db.kv.Update(context.Background(), func(tx ethdb.Tx) error {
		c := tx.Cursor(dbbucket)
		p := makeKey(id, "")
		for k, _, err := c.Seek(p); bytes.HasPrefix(k, p); k, _, err = c.Next() {
			if err != nil {
				return err
			}
			if err := c.DeleteCurrent(); err != nil {
				return err
			}
		}
		return nil
	})
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
func (db *nodeDB) ensureExpirer() {
	db.runner.Do(func() { go db.expirer() })
}

// expirer should be started in a go routine, and is responsible for looping ad
// infinitum and dropping stale data from the database.
func (db *nodeDB) expirer() {
	tick := time.NewTicker(nodeDBCleanupCycle)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			if err := db.expireNodes(); err != nil {
				log.Error(fmt.Sprintf("Failed to expire nodedb items: %v", err))
			}
		case <-db.quit:
			return
		}
	}
}

// expireNodes iterates over the database and deletes all nodes that have not
// been seen (i.e. received a pong from) for some allotted time.
func (db *nodeDB) expireNodes() error {
	threshold := time.Now().Add(-nodeDBNodeExpiration)

	var toDelete []NodeID
	// Find discovered nodes that are older than the allowance
	if err := db.kv.View(context.Background(), func(tx ethdb.Tx) error {
		c := tx.Cursor(dbbucket)
		for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
			if err != nil {
				return err
			}
			// Skip the item if not a discovery node
			id, field := splitKey(k)
			if field != nodeDBDiscoverRoot {
				continue
			}
			// Skip the node if not expired yet (and not self)
			if !bytes.Equal(id[:], db.self[:]) {
				if seen := db.lastPong(id); seen.After(threshold) {
					continue
				}
			}
			// Otherwise delete all associated information
			toDelete = append(toDelete, id)
		}
		return nil
	}); err != nil {
		return err
	}
	for _, id := range toDelete {
		if err := db.deleteNode(id); err != nil {
			return err
		}
	}
	return nil
}

// lastPing retrieves the time of the last ping packet send to a remote node,
// requesting binding.
func (db *nodeDB) lastPing(id NodeID) time.Time {
	return time.Unix(db.fetchInt64(makeKey(id, nodeDBDiscoverPing)), 0)
}

// updateLastPing updates the last time we tried contacting a remote node.
func (db *nodeDB) updateLastPing(id NodeID, instance time.Time) error {
	return db.storeInt64(makeKey(id, nodeDBDiscoverPing), instance.Unix())
}

// lastPong retrieves the time of the last successful contact from remote node.
func (db *nodeDB) lastPong(id NodeID) time.Time {
	return time.Unix(db.fetchInt64(makeKey(id, nodeDBDiscoverPong)), 0)
}

// updateLastPong updates the last time a remote node successfully contacted.
func (db *nodeDB) updateLastPong(id NodeID, instance time.Time) error {
	return db.storeInt64(makeKey(id, nodeDBDiscoverPong), instance.Unix())
}

// findFails retrieves the number of findnode failures since bonding.
func (db *nodeDB) findFails(id NodeID) int {
	return int(db.fetchInt64(makeKey(id, nodeDBDiscoverFindFails)))
}

// updateFindFails updates the number of findnode failures since bonding.
func (db *nodeDB) updateFindFails(id NodeID, fails int) error {
	return db.storeInt64(makeKey(id, nodeDBDiscoverFindFails), int64(fails))
}

// querySeeds retrieves random nodes to be used as potential seed nodes
// for bootstrapping.
func (db *nodeDB) querySeeds(n int, maxAge time.Duration) []*Node {
	var (
		now   = time.Now()
		nodes = make([]*Node, 0, n)
		id    NodeID
	)
	if err := db.kv.View(context.Background(), func(tx ethdb.Tx) error {
		c := tx.Cursor(dbbucket)
		c2 := tx.Cursor(dbbucket)
	seek:
		for seeks := 0; len(nodes) < n && seeks < n*5; seeks++ {
			// Seek to a random entry. The first byte is incremented by a
			// random amount each time in order to increase the likelihood
			// of hitting all existing nodes in very small databases.
			ctr := id[0]
			rand.Read(id[:])
			id[0] = ctr + id[0]%16
			var n *Node
			for k, v, err := c.Seek(makeKey(id, nodeDBDiscoverRoot)); k != nil && n == nil; k, v, err = c.Next() {
				if err != nil {
					return err
				}
				id, field := splitKey(k)
				if field != nodeDBDiscoverRoot {
					continue
				}
				var nd Node
				if err := rlp.DecodeBytes(v, &nd); err != nil {
					log.Warn(fmt.Sprintf("invalid node %x: %v", id, err))
				}
				n = &nd
			}
			if n == nil {
				id[0] = 0
				continue // iterator exhausted
			}
			if n.ID == db.self {
				continue
			}
			pongKey := makeKey(n.ID, nodeDBDiscoverPong)
			var lastPong int64
			if blob, _ := c2.SeekExact(pongKey); blob != nil {
				if v, read := binary.Varint(blob); read > 0 {
					lastPong = v
				}
			}
			if now.Sub(time.Unix(lastPong, 0)) > maxAge {
				continue
			}
			for i := range nodes {
				if nodes[i].ID == n.ID {
					continue seek // duplicate
				}
			}
			nodes = append(nodes, n)
		}
		return nil
	}); err != nil {
		panic(err)
	}
	return nodes
}

func (db *nodeDB) fetchTopicRegTickets(id NodeID) (issued, used uint32) {
	key := makeKey(id, nodeDBTopicRegTickets)
	var blob []byte
	if err := db.kv.View(context.Background(), func(tx ethdb.Tx) error {
		c := tx.Cursor(dbbucket)
		v, err := c.SeekExact(key)
		if err != nil {
			return err
		}
		if v != nil {
			blob = make([]byte, len(v))
			copy(blob, v)
		}
		return nil
	}); err != nil {
		panic(err)
	}
	if len(blob) != 8 {
		return 0, 0
	}
	issued = binary.BigEndian.Uint32(blob[0:4])
	used = binary.BigEndian.Uint32(blob[4:8])
	return
}

func (db *nodeDB) updateTopicRegTickets(id NodeID, issued, used uint32) error {
	key := makeKey(id, nodeDBTopicRegTickets)
	blob := make([]byte, 8)
	binary.BigEndian.PutUint32(blob[0:4], issued)
	binary.BigEndian.PutUint32(blob[4:8], used)
	return db.kv.Update(context.Background(), func(tx ethdb.Tx) error {
		return tx.Cursor(dbbucket).Put(key, blob)
	})
}

// close flushes and closes the database files.
func (db *nodeDB) close() {
	close(db.quit)
	db.kv.Close()
}
