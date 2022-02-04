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
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/google/btree"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"
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

var (
	errInvalidIP = errors.New("invalid IP")
)

var zeroIP = make(net.IP, 16)

// DB is the node database, storing previously seen nodes and any collected metadata about
// them for QoS purposes.
type DB struct {
	kvCache     *btree.BTree
	kvCacheLock sync.RWMutex
	path        string        // Remember path for log messages
	kv          kv.RwDB       // Interface to the database itself
	runner      sync.Once     // Ensures we can start at most one expirer
	quit        chan struct{} // Channel to signal the expiring thread to stop
}

// DbItem is type of items stored in the kvCache's btrees
type DbItem struct {
	key []byte
	val []byte
}

func (di *DbItem) Less(than btree.Item) bool {
	i := than.(*DbItem)
	return bytes.Compare(di.key, i.key) < 0
}

// OpenDB opens a node database for storing and retrieving infos about known peers in the
// network. If no path is given an in-memory, temporary database is constructed.
func OpenDB(path string) (*DB, error) {
	logger := log.New() //TODO: move higher
	if path == "" {
		return newMemoryDB(logger)
	}
	return newPersistentDB(logger, path)
}

var bucketsConfig = func(defaultBuckets kv.TableCfg) kv.TableCfg {
	return kv.TableCfg{
		kv.Inodes: {},
	}
}

// newMemoryNodeDB creates a new in-memory node database without a persistent backend.
func newMemoryDB(logger log.Logger) (*DB, error) {
	db := &DB{quit: make(chan struct{})}
	var err error
	db.kv, err = mdbx.NewMDBX(logger).InMem().Label(kv.SentryDB).WithTablessCfg(bucketsConfig).Open()
	if err != nil {
		return nil, err
	}
	db.kvCache = btree.New(32)
	return db, nil
}

// newPersistentNodeDB creates/opens a persistent node database,
// also flushing its contents in case of a version mismatch.
func newPersistentDB(logger log.Logger, path string) (*DB, error) {
	var db kv.RwDB
	var err error
	db, err = mdbx.NewMDBX(logger).Path(path).Label(kv.SentryDB).MapSize(1024 * datasize.MB).WithTablessCfg(bucketsConfig).Open()
	if err != nil {
		return nil, err
	}
	// The nodes contained in the cache correspond to a certain protocol version.
	// Flush all nodes if the version doesn't match.
	currentVer := make([]byte, binary.MaxVarintLen64)
	currentVer = currentVer[:binary.PutVarint(currentVer, int64(dbVersion))]

	var blob []byte
	if err := db.Update(context.Background(), func(tx kv.RwTx) error {
		c, err := tx.RwCursor(kv.Inodes)
		if err != nil {
			return err
		}
		_, v, errGet := c.SeekExact([]byte(dbVersionKey))
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
		db.Close()
		if err := os.Remove(path); err != nil {
			return nil, err
		}
		return newPersistentDB(logger, path)
	}
	return &DB{path: path, kvCache: btree.New(32), kv: db, quit: make(chan struct{})}, nil
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
	ip = key[:16]
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

func (db *DB) getFromCache(key []byte) []byte {
	db.kvCacheLock.RLock()
	defer db.kvCacheLock.RUnlock()
	i := db.kvCache.Get(&DbItem{key: key})
	if i != nil {
		di := i.(*DbItem)
		return di.val
	}
	return nil
}

func (db *DB) setToCache(key, val []byte) {
	db.kvCacheLock.Lock()
	defer db.kvCacheLock.Unlock()
	db.kvCache.ReplaceOrInsert(&DbItem{key: key, val: val})
	if db.kvCache.Len() > 16*1024 {
		db.commitCache(false /* logit */)
	}
}

func (db *DB) searchCache(key []byte) (foundKey, foundVal, nextKey []byte) {
	if key == nil {
		return nil, nil, nil
	}
	db.kvCacheLock.RLock()
	defer db.kvCacheLock.RUnlock()
	db.kvCache.AscendGreaterOrEqual(&DbItem{key: key}, func(i btree.Item) bool {
		di := i.(*DbItem)
		if foundKey == nil {
			foundKey = di.key
			foundVal = di.val
			return true
		}
		nextKey = di.key
		return false
	})
	return
}

// fetchInt64 retrieves an integer associated with a particular key.
func (db *DB) fetchInt64(key []byte) int64 {
	var val int64
	if blob := db.getFromCache(key); blob != nil {
		if v, read := binary.Varint(blob); read > 0 {
			return v
		}
		return 0
	}
	if err := db.kv.View(context.Background(), func(tx kv.Tx) error {
		blob, errGet := tx.GetOne(kv.Inodes, key)
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
	db.setToCache(common.CopyBytes(key), blob)
	return nil
}

// fetchUint64 retrieves an integer associated with a particular key.
func (db *DB) fetchUint64(key []byte) uint64 {
	var val uint64
	if blob := db.getFromCache(key); blob != nil {
		val, _ = binary.Uvarint(blob)
		return val
	}
	if err := db.kv.View(context.Background(), func(tx kv.Tx) error {
		blob, errGet := tx.GetOne(kv.Inodes, key)
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
	db.setToCache(common.CopyBytes(key), blob)
	return nil
}

// Node retrieves a node with a given id from the database.
func (db *DB) Node(id ID) *Node {
	var blob []byte
	blob = db.getFromCache(nodeKey(id))
	if blob == nil {
		if err := db.kv.View(context.Background(), func(tx kv.Tx) error {
			v, errGet := tx.GetOne(kv.Inodes, nodeKey(id))
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
	}
	if blob == nil {
		return nil
	}
	return mustDecodeNode(id[:], blob)
}

func mustDecodeNode(id, data []byte) *Node {
	node := new(Node)
	if err := rlp.DecodeBytes(data, &node.r); err != nil {
		panic(fmt.Errorf("p2p/enode: can't decode node %x in DB: %w", id, err))
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
	db.setToCache(nodeKey(node.ID()), blob)
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
	deleteRange(db, nodeKey(id))
}

func deleteRange(db *DB, prefix []byte) {
	db.kvCacheLock.Lock()
	defer db.kvCacheLock.Unlock()
	// First delete relevant entries from the cache
	db.kvCache.AscendGreaterOrEqual(&DbItem{key: prefix}, func(i btree.Item) bool {
		di := i.(*DbItem)
		if !bytes.HasPrefix(di.key, prefix) {
			return false
		}
		di.val = nil // Mark for deletion
		return true
	})
	// Now mark all other entries for deletion
	if err := db.kv.View(context.Background(), func(tx kv.Tx) error {
		c, err := tx.Cursor(kv.Inodes)
		if err != nil {
			return err
		}
		for k, _, err := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, _, err = c.Next() {
			if err != nil {
				return err
			}
			if f := db.kvCache.Get(&DbItem{key: k}); f == nil {
				// Only copy key if item is missing in the cache
				db.kvCache.ReplaceOrInsert(&DbItem{key: common.CopyBytes(k), val: nil})
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
	if err := db.kv.View(context.Background(), func(tx kv.Tx) error {
		c, err := tx.Cursor(kv.Inodes)
		if err != nil {
			return err
		}
		p := []byte(dbNodePrefix)
		var prevId ID
		var empty = true
		ci := cachedIter{c: c, db: db}
		for k, v, err := ci.Seek(p); bytes.HasPrefix(k, p); k, v, err = ci.Next() {
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
		deleteRange(db, td)
	}
}

// LastPingReceived retrieves the time of the last ping packet received from
// a remote node.
func (db *DB) LastPingReceived(id ID, ip net.IP) time.Time {
	if ip = ip.To16(); ip == nil {
		return time.Time{}
	}
	return time.Unix(db.fetchInt64(nodeItemKey(id, ip, dbNodePing)), 0)
}

// UpdateLastPingReceived updates the last time we tried contacting a remote node.
func (db *DB) UpdateLastPingReceived(id ID, ip net.IP, instance time.Time) error {
	if ip = ip.To16(); ip == nil {
		return errInvalidIP
	}
	return db.storeInt64(nodeItemKey(id, ip, dbNodePing), instance.Unix())
}

// LastPongReceived retrieves the time of the last successful pong from remote node.
func (db *DB) LastPongReceived(id ID, ip net.IP) time.Time {
	if ip = ip.To16(); ip == nil {
		return time.Time{}
	}
	// Launch expirer
	db.ensureExpirer()
	return time.Unix(db.fetchInt64(nodeItemKey(id, ip, dbNodePong)), 0)
}

// UpdateLastPongReceived updates the last pong time of a node.
func (db *DB) UpdateLastPongReceived(id ID, ip net.IP, instance time.Time) error {
	if ip = ip.To16(); ip == nil {
		return errInvalidIP
	}
	return db.storeInt64(nodeItemKey(id, ip, dbNodePong), instance.Unix())
}

// FindFails retrieves the number of findnode failures since bonding.
func (db *DB) FindFails(id ID, ip net.IP) int {
	if ip = ip.To16(); ip == nil {
		return 0
	}
	return int(db.fetchInt64(nodeItemKey(id, ip, dbNodeFindFails)))
}

// UpdateFindFails updates the number of findnode failures since bonding.
func (db *DB) UpdateFindFails(id ID, ip net.IP, fails int) error {
	if ip = ip.To16(); ip == nil {
		return errInvalidIP
	}
	return db.storeInt64(nodeItemKey(id, ip, dbNodeFindFails), int64(fails))
}

// FindFailsV5 retrieves the discv5 findnode failure counter.
func (db *DB) FindFailsV5(id ID, ip net.IP) int {
	if ip = ip.To16(); ip == nil {
		return 0
	}
	return int(db.fetchInt64(v5Key(id, ip, dbNodeFindFails)))
}

// UpdateFindFailsV5 stores the discv5 findnode failure counter.
func (db *DB) UpdateFindFailsV5(id ID, ip net.IP, fails int) error {
	if ip = ip.To16(); ip == nil {
		return errInvalidIP
	}
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

type cachedIter struct {
	c                                kv.Cursor
	db                               *DB
	cKey, cVal                       []byte
	cacheKey, cacheVal, cacheNextKey []byte
}

func (ci *cachedIter) Seek(searchKey []byte) (k, v []byte, err error) {
	ci.cKey, ci.cVal, err = ci.c.Seek(searchKey)
	if err != nil {
		return nil, nil, err
	}
	ci.cacheKey, ci.cacheVal, ci.cacheNextKey = ci.db.searchCache(searchKey)
	return ci.Next()
}

func (ci *cachedIter) Next() (k, v []byte, err error) {
	for {
		if ci.cKey == nil && ci.cacheKey == nil {
			k = nil
			v = nil
			return
		}
		if ci.cKey == nil {
			k = ci.cacheKey
			v = ci.cacheVal
			ci.cacheKey, ci.cacheVal, ci.cacheNextKey = ci.db.searchCache(ci.cacheNextKey)
			if v != nil {
				// if v == nil, it is deleted entry and we try the next record
				return
			}
			continue
		}
		if ci.cacheKey == nil {
			k = ci.cKey
			v = ci.cVal
			ci.cKey, ci.cVal, err = ci.c.Next()
			return
		}
		switch bytes.Compare(ci.cKey, ci.cacheKey) {
		case -1:
			k = ci.cKey
			v = ci.cVal
			ci.cKey, ci.cVal, err = ci.c.Next()
			return
		case 0:
			k = ci.cacheKey
			v = ci.cacheVal
			ci.cacheKey, ci.cacheVal, ci.cacheNextKey = ci.db.searchCache(ci.cacheNextKey)
			ci.cKey, ci.cVal, err = ci.c.Next()
			if v != nil {
				// if v == nil, it is deleted entry and we try the next record
				return
			}
		case 1:
			k = ci.cacheKey
			v = ci.cacheVal
			ci.cacheKey, ci.cacheVal, ci.cacheNextKey = ci.db.searchCache(ci.cacheNextKey)
			if v != nil {
				// if v == nil, it is deleted entry and we try the next record
				return
			}
		}
	}
}

// QuerySeeds retrieves random nodes to be used as potential seed nodes
// for bootstrapping.
func (db *DB) QuerySeeds(n int, maxAge time.Duration) []*Node {
	var (
		now   = time.Now()
		nodes = make([]*Node, 0, n)
		id    ID
	)

	if err := db.kv.View(context.Background(), func(tx kv.Tx) error {
		c, err := tx.Cursor(kv.Inodes)
		if err != nil {
			return err
		}
		ci := &cachedIter{db: db, c: c}
	seek:
		for seeks := 0; len(nodes) < n && seeks < n*5; seeks++ {
			// Seek to a random entry. The first byte is incremented by a
			// random amount each time in order to increase the likelihood
			// of hitting all existing nodes in very small databases.
			ctr := id[0]
			rand.Read(id[:])
			id[0] = ctr + id[0]%16
			var n *Node
			for k, v, err := ci.Seek(nodeKey(id)); k != nil && n == nil; k, v, err = ci.Next() {
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
			blob := db.getFromCache(pongKey)
			if blob == nil {
				var errGet error
				if blob, errGet = tx.GetOne(kv.Inodes, pongKey); errGet != nil {
					return errGet
				}
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

func (db *DB) commitCache(logit bool) {
	entriesUpdated := 0
	entriesDeleted := 0
	if err := db.kv.Update(context.Background(), func(tx kv.RwTx) error {
		var err error
		db.kvCache.Ascend(func(i btree.Item) bool {
			di := i.(*DbItem)
			if di.val == nil {
				if err = tx.Delete(kv.Inodes, di.key, nil); err != nil {
					return false
				}
				entriesUpdated++
			} else {
				if err = tx.Put(kv.Inodes, di.key, di.val); err != nil {
					return false
				}
				entriesDeleted++
			}
			return true
		})
		return err
	}); err != nil {
		log.Warn("p2p node database update failed", "path", db.path, "err", err)
	} else {
		if logit {
			log.Info("Successfully update p2p node database", "path", db.path, "updated", entriesUpdated, "deleted", entriesDeleted)
		}
		db.kvCache.Clear(true)
	}
}

// close flushes and closes the database files.
func (db *DB) Close() {
	select {
	case <-db.quit:
		return // means closed already
	default:
	}
	if db.quit == nil {
		return
	}
	close(db.quit)
	db.kvCacheLock.Lock()
	defer db.kvCacheLock.Unlock()
	db.commitCache(true /* logit */)
	db.kv.Close()
}
