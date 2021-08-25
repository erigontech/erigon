/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package txpool

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/hashicorp/golang-lru/simplelru"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"
	"go.uber.org/atomic"
)

// Pool is interface for the transaction pool
// This interface exists for the convinience of testing, and not yet because
// there are multiple implementations
type Pool interface {
	// IdHashKnown check whether transaction with given Id hash is known to the pool
	IdHashKnown(tx kv.Tx, hash []byte) (bool, error)
	Started() bool
	GetRlp(tx kv.Tx, hash []byte) ([]byte, error)
	Add(db kv.RoDB, newTxs TxSlots, senders *SendersCache) error
	OnNewBlock(stateChanges map[string]senderInfo, unwindTxs, minedTxs TxSlots, protocolBaseFee, pendingBaseFee, blockHeight uint64, senders *SendersCache) error

	AddNewGoodPeer(peerID PeerID)
}

var _ Pool = (*TxPool)(nil) // compile-time interface check

// SubPoolMarker ordered bitset responsible to sort transactions by sub-pools. Bits meaning:
// 1. Minimum fee requirement. Set to 1 if feeCap of the transaction is no less than in-protocol parameter of minimal base fee. Set to 0 if feeCap is less than minimum base fee, which means this transaction will never be included into this particular chain.
// 2. Absence of nonce gaps. Set to 1 for transactions whose nonce is N, state nonce for the sender is M, and there are transactions for all nonces between M and N from the same sender. Set to 0 is the transaction's nonce is divided from the state nonce by one or more nonce gaps.
// 3. Sufficient balance for gas. Set to 1 if the balance of sender's account in the state is B, nonce of the sender in the state is M, nonce of the transaction is N, and the sum of feeCap x gasLimit + transferred_value of all transactions from this sender with nonces N+1 ... M is no more than B. Set to 0 otherwise. In other words, this bit is set if there is currently a guarantee that the transaction and all its required prior transactions will be able to pay for gas.
// 4. Dynamic fee requirement. Set to 1 if feeCap of the transaction is no less than baseFee of the currently pending block. Set to 0 otherwise.
// 5. Local transaction. Set to 1 if transaction is local.
type SubPoolMarker uint8

const (
	EnoughFeeCapProtocol = 0b10000
	NoNonceGaps          = 0b01000
	EnoughBalance        = 0b00100
	EnoughFeeCapBlock    = 0b00010
	IsLocal              = 0b00001
)

// metaTx holds transaction and some metadata
type metaTx struct {
	subPool        SubPoolMarker
	effectiveTip   uint64 // max(minTip, minFeeCap - baseFee)
	Tx             *TxSlot
	bestIndex      int
	worstIndex     int
	currentSubPool SubPoolType
}

func newMetaTx(slot *TxSlot, isLocal bool) *metaTx {
	mt := &metaTx{Tx: slot, worstIndex: -1, bestIndex: -1}
	if isLocal {
		mt.subPool = IsLocal
	}
	return mt
}

type SubPoolType uint8

const PendingSubPool SubPoolType = 1
const BaseFeeSubPool SubPoolType = 2
const QueuedSubPool SubPoolType = 3

const PendingSubPoolLimit = 10 * 1024
const BaseFeeSubPoolLimit = 10 * 1024
const QueuedSubPoolLimit = 10 * 1024

const MaxSendersInfoCache = 2 * (PendingSubPoolLimit + BaseFeeSubPoolLimit + QueuedSubPoolLimit)

type nonce2Tx struct{ *btree.BTree }

type senderInfo struct {
	balance    uint256.Int
	nonce      uint64
	txNonce2Tx *nonce2Tx // sorted map of nonce => *metaTx
}

//nolint
func newSenderInfo(nonce uint64, balance uint256.Int) *senderInfo {
	return &senderInfo{nonce: nonce, balance: balance, txNonce2Tx: &nonce2Tx{btree.New(32)}}
}

type byNonce struct{ *metaTx }

func (i *byNonce) Less(than btree.Item) bool {
	return i.metaTx.Tx.nonce < than.(*byNonce).metaTx.Tx.nonce
}

type SendersCache struct {
	lock        sync.RWMutex
	blockHeight atomic.Uint64
	senderID    uint64
	senderIDs   map[string]uint64
	senderInfo  map[uint64]*senderInfo
}

func NewSendersCache() *SendersCache {
	return &SendersCache{
		senderIDs:  map[string]uint64{},
		senderInfo: map[uint64]*senderInfo{},
	}
}

func (sc *SendersCache) Id(addr string, tx kv.Tx) (uint64, bool, error) {
	sc.lock.RLock() //TODO: it may load mapping from db,maybe just use thread-safe maps to don't worry much
	defer sc.lock.RUnlock()
	return sc.id(addr, tx)
}
func (sc *SendersCache) id(addr string, tx kv.Tx) (uint64, bool, error) {
	id, ok := sc.senderIDs[addr]
	if !ok {
		v, err := tx.GetOne(kv.PooledSenderID, []byte(addr))
		if err != nil {
			return 0, false, err
		}
		if len(v) == 0 {
			return 0, false, nil
		}
		id = binary.BigEndian.Uint64(v)
	}
	return id, true, nil
}
func (sc *SendersCache) Info(id uint64, tx kv.Tx) (*senderInfo, error) {
	sc.lock.RLock()
	defer sc.lock.RUnlock()
	return sc.info(id, tx)
}
func (sc *SendersCache) info(id uint64, tx kv.Tx) (*senderInfo, error) {
	info, ok := sc.senderInfo[id]
	if !ok {
		encID := make([]byte, 8)
		binary.BigEndian.PutUint64(encID, id)
		v, err := tx.GetOne(kv.PooledSender, encID)
		if err != nil {
			return nil, err
		}
		if len(v) == 0 {
			panic("what to do in this case?")
			info = newSenderInfo(0, *uint256.NewInt(0))
			sc.senderInfo[id] = info
			return info, nil
		}
		balance := uint256.NewInt(0)
		balance.SetBytes(v[8:])
		info = newSenderInfo(binary.BigEndian.Uint64(v), *balance)
		sc.senderInfo[id] = info
	}
	return info, nil
}

//nolint
func (sc *SendersCache) printDebug(prefix string) {
	fmt.Printf("%s.SendersCache.senderInfo\n", prefix)
	for i, j := range sc.senderInfo {
		fmt.Printf("\tid=%d,nonce=%d,balance=%d,txs.len=%d\n", i, j.nonce, j.balance.Uint64(), j.txNonce2Tx.Len())
	}
}

func (sc *SendersCache) len() int {
	sc.lock.RLock()
	defer sc.lock.RUnlock()
	return len(sc.senderInfo)
}

/*
func (sc *SendersCache) evict() int {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	if len(sc.senderIDs) < MaxSendersInfoCache {
		return 0
	}

	count := 0
	for addr, id := range sc.senderIDs {
		if sc.senderInfo[id].txNonce2Tx.Len() > 0 {
			continue
		}
		delete(sc.senderInfo, id)
		delete(sc.senderIDs, addr)
		count++
	}
	return count
}
*/

func (sc *SendersCache) onNewTxs(coreDBTx kv.RoDB, tx kv.Tx, newTxs TxSlots) error {
	if err := sc.ensureSenderIDOnNewTxs(tx, newTxs); err != nil {
		return err
	}
	toLoad, err := sc.setTxSenderID(tx, newTxs)
	if err != nil {
		return err
	}
	if len(toLoad) == 0 {
		return nil
	}
	diff, err := loadSenders(coreDBTx, toLoad)
	if err != nil {
		return err
	}
	sc.set(diff)
	return nil
}

func (sc *SendersCache) onNewBlock(tx kv.Tx, stateChanges map[string]senderInfo, unwindTxs, minedTxs TxSlots, blockHeight uint64) error {
	//TODO: if see non-continuous block heigh - load gap from changesets
	sc.blockHeight.Store(blockHeight)

	//`loadSenders` goes by network to core - and it must be outside of SendersCache lock. But other methods must be locked
	if err := sc.mergeStateChanges(tx, stateChanges, unwindTxs, minedTxs); err != nil {
		return err
	}
	if _, err := sc.setTxSenderID(tx, unwindTxs); err != nil {
		return err
	}
	if _, err := sc.setTxSenderID(tx, minedTxs); err != nil {
		return err
	}
	return nil
}
func (sc *SendersCache) set(diff map[uint64]senderInfo) {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	for id := range diff { // merge state changes
		a := diff[id]
		sc.senderInfo[id] = &a
	}
}
func (sc *SendersCache) mergeStateChanges(tx kv.Tx, stateChanges map[string]senderInfo, unwindedTxs, minedTxs TxSlots) error {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	return sc.mergeStateChangesLocked(tx, stateChanges, unwindedTxs, minedTxs)
}
func (sc *SendersCache) mergeStateChangesLocked(tx kv.Tx, stateChanges map[string]senderInfo, unwindedTxs, minedTxs TxSlots) error {
	for addr, v := range stateChanges { // merge state changes
		id, ok, err := sc.id(addr, tx)
		if err != nil {
			return err
		}
		if !ok {
			sc.senderID++
			id = sc.senderID
			sc.senderIDs[addr] = id
		}
		old, ok := sc.senderInfo[id]
		if ok {
			old.nonce = v.nonce
			old.balance = v.balance
			v.txNonce2Tx = old.txNonce2Tx
		} else {
			sc.senderInfo[id] = newSenderInfo(v.nonce, v.balance)
		}
	}

	/*
		for addr, id := range sc.senderIDs { // merge state changes
			if v, ok := stateChanges[addr]; ok {
				sc.senderInfo[id] = newSenderInfo(v.nonce, v.balance)
			}
		}
	*/

	for i := 0; i < unwindedTxs.senders.Len(); i++ {
		id, ok, err := sc.id(string(unwindedTxs.senders.At(i)), tx)
		if err != nil {
			return err
		}
		if !ok {
			sc.senderID++
			id = sc.senderID
			sc.senderIDs[string(unwindedTxs.senders.At(i))] = id
		}
		if _, ok := sc.senderInfo[id]; !ok {
			if _, ok := stateChanges[string(unwindedTxs.senders.At(i))]; !ok {
				sc.senderInfo[id] = newSenderInfo(0, *uint256.NewInt(0))
			}
		}
	}

	for i := 0; i < len(minedTxs.txs); i++ {
		id, ok, err := sc.id(string(minedTxs.senders.At(i)), tx)
		if err != nil {
			return err
		}
		if !ok {
			sc.senderID++
			id = sc.senderID
			sc.senderIDs[string(minedTxs.senders.At(i))] = id
		}
		if _, ok := sc.senderInfo[id]; !ok {
			if _, ok := stateChanges[string(minedTxs.senders.At(i))]; !ok {
				sc.senderInfo[id] = newSenderInfo(0, *uint256.NewInt(0))
			}
		}

		//if v, ok := stateChanges[string(minedTxs.senders.At(i))]; ok {
		//	sc.senderInfo[id] = newSenderInfo(v.nonce, v.balance)
		//}
	}
	return nil
}

func (sc *SendersCache) ensureSenderIDOnNewTxs(tx kv.Tx, newTxs TxSlots) error {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	for i := 0; i < len(newTxs.txs); i++ {
		_, ok, err := sc.id(string(newTxs.senders.At(i)), tx)
		if err != nil {
			return err
		}
		if ok {
			continue
		}
		sc.senderID++
		sc.senderIDs[string(newTxs.senders.At(i))] = sc.senderID
	}
	return nil
}

func (sc *SendersCache) setTxSenderID(tx kv.Tx, txs TxSlots) (map[uint64]string, error) {
	sc.lock.RLock()
	defer sc.lock.RUnlock()
	toLoad := map[uint64]string{}
	for i := range txs.txs {
		addr := string(txs.senders.At(i))

		// assign ID to each new sender
		id, ok, err := sc.id(addr, tx)
		if err != nil {
			return nil, err
		}
		if !ok {
			panic("not supported yet")
		}
		txs.txs[i].senderID = id

		// load data from db if need
		_, ok = sc.senderInfo[txs.txs[i].senderID]
		if ok {
			continue
		}
		_, ok = toLoad[txs.txs[i].senderID]
		if ok {
			continue
		}
		toLoad[txs.txs[i].senderID] = addr
	}
	return toLoad, nil
}
func (sc *SendersCache) fromDB(ctx context.Context, tx, coreTx kv.Tx) error {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	//c, err := tx.Cursor(kv.PooledSenderID)
	//if err != nil {
	//	return err
	//}
	//defer c.Close()
	//for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
	//	if err != nil {
	//		return err
	//	}
	//	id := binary.BigEndian.Uint64(v)
	//	sc.senderIDs[string(k)] = id
	//}

	if err := tx.ForEach(kv.PooledSender, nil, func(k, v []byte) error {
		id := binary.BigEndian.Uint64(k)
		balance := uint256.NewInt(0)
		balance.SetBytes(v[8:])
		sc.senderInfo[id] = newSenderInfo(binary.BigEndian.Uint64(v), *balance)
		return nil
	}); err != nil {
		return err
	}

	{
		v, err := tx.GetOne(kv.PoolInfo, SenderCacheHeightKey)
		if err != nil {
			return err
		}
		if len(v) > 0 {
			sc.blockHeight.Store(binary.BigEndian.Uint64(v))
		}
	}
	{
		v, err := tx.GetOne(kv.PoolInfo, SenderCacheIDKey)
		if err != nil {
			return err
		}
		if len(v) > 0 {
			sc.senderID = binary.BigEndian.Uint64(v)
		}
	}

	if coreTx == nil {
		return nil
	}
	/*TODO: tx.ForEach must be implemented as buffered server-side stream
	encNum := make([]byte, 8)
	diff := map[string]senderInfo{}
	binary.BigEndian.PutUint64(encNum, sc.blockHeight.Load())
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	if err := coreTx.ForEach(kv.AccountChangeSet, encNum, func(k, v []byte) error {
		info, err := loadSender(coreTx, v[:20])
		if err != nil {
			return err
		}
		diff[string(v[:20])] = *info
		select {
		case <-logEvery.C:
			log.Info("loading changesets", "block", binary.BigEndian.Uint64(k))
		case <-ctx.Done():
			return nil
		default:
		}
		return nil
	}); err != nil {
		return err
	}
	if err := sc.mergeStateChangesLocked(tx, diff, TxSlots{}, TxSlots{}); err != nil {
		return err
	}
	*/
	return nil
}

var SenderCacheIDKey = []byte("sender_cache_id")
var SenderCacheHeightKey = []byte("sender_cache_heght")
var PoolPendingBaseFeeKey = []byte("pending_base_fee")
var PoolProtocolBaseFeeKey = []byte("protocol_base_fee")

func (sc *SendersCache) flush(tx kv.RwTx) error {
	sc.lock.RLock()
	defer sc.lock.RUnlock()
	encID := make([]byte, 8)
	for addr, id := range sc.senderIDs {
		binary.BigEndian.PutUint64(encID, id)
		currentV, err := tx.GetOne(kv.PooledSenderID, []byte(addr))
		if err != nil {
			return err
		}
		if currentV != nil && bytes.Equal(currentV, encID) {
			continue
		}
		if err := tx.Put(kv.PooledSenderID, []byte(addr), encID); err != nil {
			return err
		}
	}
	v := make([]byte, 8, 8+32)
	for id, info := range sc.senderInfo {
		binary.BigEndian.PutUint64(encID, id)
		binary.BigEndian.PutUint64(v, info.nonce)
		v = append(v[:8], info.balance.Bytes()...)
		currentV, err := tx.GetOne(kv.PooledSender, encID)
		if err != nil {
			return err
		}
		if currentV != nil && bytes.Equal(currentV, v) {
			continue
		}
		if err := tx.Put(kv.PooledSender, encID, v); err != nil {
			return err
		}
	}

	binary.BigEndian.PutUint64(encID, sc.blockHeight.Load())
	if err := tx.Put(kv.PoolInfo, SenderCacheHeightKey, encID); err != nil {
		return err
	}
	binary.BigEndian.PutUint64(encID, sc.senderID)
	if err := tx.Put(kv.PoolInfo, SenderCacheIDKey, encID); err != nil {
		return err
	}

	//TODO: flush and evict

	return nil
}

func loadSenders(coreDB kv.RoDB, toLoad map[uint64]string) (map[uint64]senderInfo, error) {
	diff := make(map[uint64]senderInfo, len(toLoad))
	if err := coreDB.View(context.Background(), func(tx kv.Tx) error {
		for id := range toLoad {
			info, err := loadSender(tx, []byte(toLoad[id]))
			if err != nil {
				return err
			}
			diff[id] = *info
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return diff, nil
}

func loadSender(coreTx kv.Tx, addr []byte) (*senderInfo, error) {
	encoded, err := coreTx.GetOne(kv.PlainState, addr)
	if err != nil {
		return nil, err
	}
	if len(encoded) == 0 {
		return newSenderInfo(0, *uint256.NewInt(0)), nil
	}
	nonce, balance, err := DecodeSender(encoded)
	if err != nil {
		return nil, err
	}
	return newSenderInfo(nonce, balance), nil
}

// TxPool - holds all pool-related data structures and lock-based tiny methods
// most of logic implemented by pure tests-friendly functions
type TxPool struct {
	lock *sync.RWMutex

	protocolBaseFee atomic.Uint64
	pendingBaseFee  atomic.Uint64

	senderID                 uint64
	byHash                   map[string]*metaTx // tx_hash => tx
	pending, baseFee, queued *SubPool

	// track isLocal flag of already mined transactions. used at unwind.
	localsHistory *simplelru.LRU
	db            kv.RwDB

	// fields for transaction propagation
	recentlyConnectedPeers *recentlyConnectedPeers
	newTxs                 chan Hashes
	deletedTxs             []*metaTx
	senders                *SendersCache
}

func New(newTxs chan Hashes, senders *SendersCache, db kv.RwDB) (*TxPool, error) {
	localsHistory, err := simplelru.NewLRU(1024, nil)
	if err != nil {
		return nil, err
	}
	if err = restoreIsLocalHistory(db, localsHistory); err != nil {
		return nil, err
	}
	return &TxPool{
		lock:                   &sync.RWMutex{},
		byHash:                 map[string]*metaTx{},
		localsHistory:          localsHistory,
		recentlyConnectedPeers: &recentlyConnectedPeers{},
		pending:                NewSubPool(PendingSubPool),
		baseFee:                NewSubPool(BaseFeeSubPool),
		queued:                 NewSubPool(QueuedSubPool),
		newTxs:                 newTxs,
		senders:                senders,
		db:                     db,
		senderID:               1,
	}, nil
}

//nolint
func (p *TxPool) printDebug(prefix string) {
	fmt.Printf("%s.pool.byHash\n", prefix)
	for _, j := range p.byHash {
		fmt.Printf("\tsenderID=%d, nonce=%d, tip=%d\n", j.Tx.senderID, j.Tx.nonce, j.Tx.tip)
	}
	fmt.Printf("%s.pool.queues.len: %d,%d,%d\n", prefix, p.pending.Len(), p.baseFee.Len(), p.queued.Len())
	for i := range *p.pending.best {
		(*p.pending.best)[i].Tx.printDebug(fmt.Sprintf("%s.pending: %b", prefix, (*p.pending.best)[i].subPool))
	}
	for i := range *p.queued.best {
		(*p.queued.best)[i].Tx.printDebug(fmt.Sprintf("%s.queued : %b", prefix, (*p.queued.best)[i].subPool))
	}
}
func (p *TxPool) logStats(sc *SendersCache) {
	protocolBaseFee, pendingBaseFee := p.protocolBaseFee.Load(), p.pendingBaseFee.Load()

	p.lock.RLock()
	defer p.lock.RUnlock()
	log.Info(fmt.Sprintf("baseFee: %dm->%dm; queuesSize: pending=%d/%d, baseFee=%d/%d, queued=%d/%d; sendersCache: id=%d,info=%d",
		protocolBaseFee/1_000_000, pendingBaseFee/1_000_000,
		p.pending.Len(), PendingSubPoolLimit, p.baseFee.Len(), BaseFeeSubPoolLimit, p.queued.Len(), QueuedSubPoolLimit,
		len(sc.senderIDs), len(sc.senderInfo),
	))
}
func (p *TxPool) GetRlp(tx kv.Tx, hash []byte) ([]byte, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	txn, ok := p.byHash[string(hash)]
	if !ok || txn.Tx.rlp == nil {
		return tx.GetOne(kv.PooledTransaction, hash)
	}
	return txn.Tx.rlp, nil
}
func (p *TxPool) AppendLocalHashes(buf []byte) []byte {
	p.lock.RLock()
	defer p.lock.RUnlock()
	for hash, txn := range p.byHash {
		if txn.subPool&IsLocal == 0 {
			continue
		}
		buf = append(buf, hash...)
	}
	return buf
}
func (p *TxPool) AppendRemoteHashes(buf []byte) []byte {
	p.lock.RLock()
	defer p.lock.RUnlock()

	for hash, txn := range p.byHash {
		if txn.subPool&IsLocal != 0 {
			continue
		}
		buf = append(buf, hash...)
	}
	return buf
}
func (p *TxPool) AppendAllHashes(buf []byte) []byte {
	buf = p.AppendLocalHashes(buf)
	buf = p.AppendRemoteHashes(buf)
	return buf
}
func (p *TxPool) IdHashKnown(tx kv.Tx, hash []byte) (bool, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	_, ok := p.byHash[string(hash)]
	if !ok {
		return tx.Has(kv.PooledTransaction, hash)
	}
	return true, nil
}
func (p *TxPool) IdHashIsLocal(hash []byte) bool {
	p.lock.RLock()
	defer p.lock.RUnlock()

	txn, ok := p.byHash[string(hash)]
	if !ok {
		return false
	}
	return txn.subPool&IsLocal != 0
}
func (p *TxPool) AddNewGoodPeer(peerID PeerID) { p.recentlyConnectedPeers.AddPeer(peerID) }

func (p *TxPool) Started() bool {
	return p.protocolBaseFee.Load() > 0
}

func (p *TxPool) Add(coreDB kv.RoDB, newTxs TxSlots, senders *SendersCache) error {
	t := time.Now()
	tx, err := p.db.BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := senders.onNewTxs(coreDB, tx, newTxs); err != nil {
		return err
	}
	if err := newTxs.Valid(); err != nil {
		return err
	}

	protocolBaseFee, pendingBaseFee := p.protocolBaseFee.Load(), p.pendingBaseFee.Load()
	if protocolBaseFee == 0 || pendingBaseFee == 0 {
		return fmt.Errorf("non-zero base fee: %d,%d", protocolBaseFee, pendingBaseFee)
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	if err := onNewTxs(tx, senders, newTxs, protocolBaseFee, pendingBaseFee, p.pending, p.baseFee, p.queued, p.byHash, p.localsHistory, p.discardLocked); err != nil {
		return err
	}
	notifyNewTxs := make(Hashes, 0, 32*len(newTxs.txs))
	for i := range newTxs.txs {
		_, ok := p.byHash[string(newTxs.txs[i].idHash[:])]
		if !ok {
			continue
		}
		notifyNewTxs = append(notifyNewTxs, newTxs.txs[i].idHash[:]...)
	}
	if len(notifyNewTxs) > 0 {
		select {
		case p.newTxs <- notifyNewTxs:
		default:
		}
	}

	log.Info("on new txs", "in", time.Since(t))
	return nil
}
func onNewTxs(tx kv.Tx, senders *SendersCache, newTxs TxSlots, protocolBaseFee, pendingBaseFee uint64, pending, baseFee, queued *SubPool, byHash map[string]*metaTx, localsHistory *simplelru.LRU, discard func(*metaTx, kv.Tx)) error {
	for i := range newTxs.txs {
		if newTxs.txs[i].senderID == 0 {
			return fmt.Errorf("senderID can't be zero")
		}
	}

	changedSenders := map[uint64]*senderInfo{}
	if err := unsafeAddToPool(tx, senders, newTxs, pending, byHash, func(txn *metaTx) {
		switch txn.currentSubPool {
		case PendingSubPool:
			pending.UnsafeRemove(txn)
		case BaseFeeSubPool:
			baseFee.UnsafeRemove(txn)
		case QueuedSubPool:
			queued.UnsafeRemove(txn)
		default:
			//already removed
		}
	}, func(i *metaTx, sender *senderInfo) {
		changedSenders[i.Tx.senderID] = sender
		if _, ok := localsHistory.Get(i.Tx.idHash); ok {
			i.subPool |= IsLocal
		}
	}); err != nil {
		return err
	}

	for _, sender := range changedSenders {
		onSenderChange(sender, protocolBaseFee, pendingBaseFee)
	}

	pending.EnforceInvariants()
	baseFee.EnforceInvariants()
	queued.EnforceInvariants()

	promote(pending, baseFee, queued, func(i *metaTx) {
		discard(i, tx)
	})

	return nil
}

func (p *TxPool) setBaseFee(protocolBaseFee, pendingBaseFee uint64) (uint64, uint64) {
	p.protocolBaseFee.Store(protocolBaseFee)
	hasNewVal := pendingBaseFee > 0
	if pendingBaseFee < protocolBaseFee {
		pendingBaseFee = protocolBaseFee
		hasNewVal = true
	}
	if hasNewVal {
		p.pendingBaseFee.Store(pendingBaseFee)
	}
	return protocolBaseFee, p.pendingBaseFee.Load()
}

func (p *TxPool) OnNewBlock(stateChanges map[string]senderInfo, unwindTxs, minedTxs TxSlots, protocolBaseFee, pendingBaseFee, blockHeight uint64, senders *SendersCache) error {
	t := time.Now()
	tx, err := p.db.BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	protocolBaseFee, pendingBaseFee = p.setBaseFee(protocolBaseFee, pendingBaseFee)
	if err := senders.onNewBlock(tx, stateChanges, unwindTxs, minedTxs, blockHeight); err != nil {
		return err
	}
	//log.Debug("[txpool] new block", "unwinded", len(unwindTxs.txs), "mined", len(minedTxs.txs), "protocolBaseFee", protocolBaseFee, "blockHeight", blockHeight)
	if err := unwindTxs.Valid(); err != nil {
		return err
	}
	if err := minedTxs.Valid(); err != nil {
		return err
	}

	p.lock.Lock()
	defer p.lock.Unlock()
	if err := onNewBlock(tx, senders, unwindTxs, minedTxs.txs, protocolBaseFee, pendingBaseFee, p.pending, p.baseFee, p.queued, p.byHash, p.localsHistory, p.discardLocked); err != nil {
		return err
	}

	notifyNewTxs := make(Hashes, 0, 32*len(unwindTxs.txs))
	for i := range unwindTxs.txs {
		_, ok := p.byHash[string(unwindTxs.txs[i].idHash[:])]
		if !ok {
			continue
		}
		notifyNewTxs = append(notifyNewTxs, unwindTxs.txs[i].idHash[:]...)
	}
	if len(notifyNewTxs) > 0 {
		select {
		case p.newTxs <- notifyNewTxs:
		default:
		}
	}
	//count := senders.evict()
	//if count > 0 {
	//	log.Debug("evicted senders", "amount", count)
	//}

	log.Info("on new block", "in", time.Since(t))
	return nil
}
func (p *TxPool) flush(tx kv.RwTx, senders *SendersCache) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	for i := 0; i < len(p.deletedTxs); i++ {
		if err := tx.Delete(kv.PooledTransaction, p.deletedTxs[i].Tx.idHash[:], nil); err != nil {
			return err
		}
	}
	p.deletedTxs = p.deletedTxs[:0]

	txHashes := p.localsHistory.Keys()
	encID := make([]byte, 8)
	if err := tx.ClearBucket(kv.RecentLocalTransaction); err != nil {
		return err
	}
	for i := range txHashes {
		binary.BigEndian.PutUint64(encID, uint64(i))
		if err := tx.Append(kv.RecentLocalTransaction, encID, txHashes[i].([]byte)); err != nil {
			return err
		}
	}

	v := make([]byte, 0, 1024)
	for txHash, metaTx := range p.byHash {
		if metaTx.Tx.rlp == nil {
			continue
		}
		v = ensureEnoughSize(v, 8+8+len(metaTx.Tx.rlp))
		binary.BigEndian.PutUint64(v, metaTx.Tx.senderID)
		binary.BigEndian.PutUint64(v[8:], 0) // block num - timestamp
		copy(v[8+8:], metaTx.Tx.rlp)
		currentV, err := tx.GetOne(kv.PooledTransaction, []byte(txHash))
		if err != nil {
			return err
		}
		if currentV != nil && bytes.Equal(currentV, v) {
			continue
		}
		if err := tx.Put(kv.PooledTransaction, []byte(txHash), v); err != nil {
			return err
		}
		metaTx.Tx.rlp = nil
	}

	binary.BigEndian.PutUint64(encID, p.protocolBaseFee.Load())
	if err := tx.Put(kv.PoolInfo, PoolProtocolBaseFeeKey, encID); err != nil {
		return err
	}
	binary.BigEndian.PutUint64(encID, p.pendingBaseFee.Load())
	if err := tx.Put(kv.PoolInfo, PoolPendingBaseFeeKey, encID); err != nil {
		return err
	}

	if err := senders.flush(tx); err != nil {
		return err
	}

	return nil
}
func (p *TxPool) discardLocked(mt *metaTx, tx kv.Tx) {
	delete(p.byHash, string(mt.Tx.idHash[:]))
	p.deletedTxs = append(p.deletedTxs, mt)
	sender, err := p.senders.Info(mt.Tx.senderID, tx)
	if err != nil {
		log.Warn("get sender info", "err", err)
	} else {
		sender.txNonce2Tx.Delete(&byNonce{mt})
	}
	if mt.subPool&IsLocal != 0 {
		//TODO: only add to history if sender is not in list of local-senders
		p.localsHistory.Add(mt.Tx.idHash, struct{}{})
	}
}

func (p *TxPool) fromDB(ctx context.Context, tx, coreTx kv.Tx, senders *SendersCache) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if err := senders.fromDB(ctx, tx, coreTx); err != nil {
		return err
	}

	c, err := tx.Cursor(kv.RecentLocalTransaction)
	if err != nil {
		return err
	}
	defer c.Close()
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		hashID := [32]byte{}
		copy(hashID[:], v)
		p.localsHistory.Add(hashID, struct{}{})
	}

	c, err = tx.Cursor(kv.PooledTransaction)
	if err != nil {
		return err
	}
	defer c.Close()
	txs := TxSlots{}
	total, err := c.Count()
	if err != nil {
		return err
	}
	txs.Growth(int(total))

	parseCtx := NewTxParseContext()
	parseCtx.WithSender(false)
	i := 0
	hashID := [32]byte{}
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		txs.txs[i] = &TxSlot{}

		_, err := parseCtx.ParseTransaction(v[8+8:], 0, txs.txs[i], nil)
		if err != nil {
			return err
		}
		txs.txs[i].senderID = binary.BigEndian.Uint64(v)
		//bkock num = binary.BigEndian.Uint64(v[8:])
		copy(hashID[:], k)
		_, isLocalTx := p.localsHistory.Get(hashID)
		txs.isLocal[i] = isLocalTx
		i++
	}

	var protocolBaseFee, pendingBaseFee uint64
	{
		v, err := tx.GetOne(kv.PoolInfo, PoolProtocolBaseFeeKey)
		if err != nil {
			return err
		}
		if len(v) > 0 {
			protocolBaseFee = binary.BigEndian.Uint64(v)
		}
	}
	{
		v, err := tx.GetOne(kv.PoolInfo, PoolPendingBaseFeeKey)
		if err != nil {
			return err
		}
		if len(v) > 0 {
			pendingBaseFee = binary.BigEndian.Uint64(v)
		}
	}
	if err := onNewTxs(tx, senders, txs, protocolBaseFee, pendingBaseFee, p.pending, p.baseFee, p.queued, p.byHash, p.localsHistory, p.discardLocked); err != nil {
		return err
	}
	p.pendingBaseFee.Store(pendingBaseFee)
	p.protocolBaseFee.Store(protocolBaseFee)

	return nil
}

func onNewBlock(tx kv.Tx, senders *SendersCache, unwindTxs TxSlots, minedTxs []*TxSlot, protocolBaseFee, pendingBaseFee uint64, pending, baseFee, queued *SubPool, byHash map[string]*metaTx, localsHistory *simplelru.LRU, discard func(*metaTx, kv.Tx)) error {
	for i := range unwindTxs.txs {
		if unwindTxs.txs[i].senderID == 0 {
			return fmt.Errorf("onNewBlock.unwindTxs: senderID can't be zero")
		}
	}
	for i := range minedTxs {
		if minedTxs[i].senderID == 0 {
			return fmt.Errorf("onNewBlock.minedTxs: senderID can't be zero")
		}
	}

	if err := removeMined(tx, senders, minedTxs, pending, baseFee, queued, func(i *metaTx) {
		discard(i, tx)
	}); err != nil {
		return err
	}

	changedSenders := make(map[uint64]*senderInfo, len(unwindTxs.txs)/4)

	// This can be thought of a reverse operation from the one described before.
	// When a block that was deemed "the best" of its height, is no longer deemed "the best", the
	// transactions contained in it, are now viable for inclusion in other blocks, and therefore should
	// be returned into the transaction pool.
	// An interesting note here is that if the block contained any transactions local to the node,
	// by being first removed from the pool (from the "local" part of it), and then re-injected,
	// they effective lose their priority over the "remote" transactions. In order to prevent that,
	// somehow the fact that certain transactions were local, needs to be remembered for some
	// time (up to some "immutability threshold").
	if err := unsafeAddToPool(tx, senders, unwindTxs, pending, byHash, func(txn *metaTx) {
		switch txn.currentSubPool {
		case PendingSubPool:
			pending.UnsafeRemove(txn)
		case BaseFeeSubPool:
			baseFee.UnsafeRemove(txn)
		case QueuedSubPool:
			queued.UnsafeRemove(txn)
		default:
			//already removed
		}
	}, func(i *metaTx, sender *senderInfo) {
		changedSenders[i.Tx.senderID] = sender
		if _, ok := localsHistory.Get(i.Tx.idHash); ok {
			//TODO: also check if sender is in list of local-senders
			i.subPool |= IsLocal
		}
	}); err != nil {
		return err
	}

	for _, sender := range changedSenders {
		onSenderChange(sender, protocolBaseFee, pendingBaseFee)
	}

	pending.EnforceInvariants()
	baseFee.EnforceInvariants()
	queued.EnforceInvariants()

	promote(pending, baseFee, queued, func(i *metaTx) {
		discard(i, tx)
	})

	return nil
}

// removeMined - apply new highest block (or batch of blocks)
//
// 1. New best block arrives, which potentially changes the balance and the nonce of some senders.
// We use senderIds data structure to find relevant senderId values, and then use senders data structure to
// modify state_balance and state_nonce, potentially remove some elements (if transaction with some nonce is
// included into a block), and finally, walk over the transaction records and update SubPool fields depending on
// the actual presence of nonce gaps and what the balance is.
func removeMined(tx kv.Tx, senders *SendersCache, minedTxs []*TxSlot, pending, baseFee, queued *SubPool, discard func(tx *metaTx)) error {
	noncesToRemove := map[uint64]uint64{}
	for _, tx := range minedTxs {
		nonce, ok := noncesToRemove[tx.senderID]
		if !ok || tx.nonce > nonce {
			noncesToRemove[tx.senderID] = tx.nonce
		}
	}

	var toDel []*metaTx // can't delete items while iterate them
	for senderID, nonce := range noncesToRemove {
		sender, err := senders.Info(senderID, tx)
		if err != nil {
			return err
		}
		//if sender.txNonce2Tx.Len() > 0 {
		//log.Debug("[txpool] removing mined", "senderID", tx.senderID, "sender.txNonce2Tx.len()", sender.txNonce2Tx.Len())
		//}
		// delete mined transactions from everywhere
		sender.txNonce2Tx.Ascend(func(i btree.Item) bool {
			it := i.(*byNonce)
			//log.Debug("[txpool] removing mined, cmp nonces", "tx.nonce", it.metaTx.Tx.nonce, "sender.nonce", sender.nonce)
			if it.metaTx.Tx.nonce > nonce {
				return false
			}
			toDel = append(toDel, it.metaTx)
			// del from sub-pool
			switch it.metaTx.currentSubPool {
			case PendingSubPool:
				pending.UnsafeRemove(it.metaTx)
			case BaseFeeSubPool:
				baseFee.UnsafeRemove(it.metaTx)
			case QueuedSubPool:
				queued.UnsafeRemove(it.metaTx)
			default:
				//already removed
			}
			return true
		})

		for i := range toDel {
			discard(toDel[i])
		}
		toDel = toDel[:0]
	}
	return nil
}

// unwind
func unsafeAddToPool(tx kv.Tx, senders *SendersCache, newTxs TxSlots, to *SubPool, byHash map[string]*metaTx, discard func(tx *metaTx), beforeAdd func(tx *metaTx, sender *senderInfo)) error {
	for i, txn := range newTxs.txs {
		if _, ok := byHash[string(txn.idHash[:])]; ok {
			continue
		}
		mt := newMetaTx(txn, newTxs.isLocal[i])

		sender, err := senders.Info(txn.senderID, tx)
		if err != nil {
			return err
		}
		// Insert to pending pool, if pool doesn't have txn with same Nonce and bigger Tip
		if found := sender.txNonce2Tx.Get(&byNonce{mt}); found != nil {
			if txn.tip <= found.(*byNonce).Tx.tip {
				continue
			}
		}

		byHash[string(mt.Tx.idHash[:])] = mt
		replaced := sender.txNonce2Tx.ReplaceOrInsert(&byNonce{mt})
		if replaced != nil {
			replacedMT := replaced.(*byNonce).metaTx
			if replacedMT.Tx.idHash != mt.Tx.idHash {
				delete(byHash, string(replacedMT.Tx.idHash[:]))
			}
			discard(replacedMT)
		}
		beforeAdd(mt, sender)
		to.UnsafeAdd(mt)
	}
	return nil
}

func onSenderChange(sender *senderInfo, protocolBaseFee, pendingBaseFee uint64) {
	noGapsNonce := sender.nonce + 1
	cumulativeRequiredBalance := uint256.NewInt(0)
	minFeeCap := uint64(math.MaxUint64)
	minTip := uint64(math.MaxUint64)
	sender.txNonce2Tx.Ascend(func(i btree.Item) bool {
		it := i.(*byNonce)

		// Sender has enough balance for: gasLimit x feeCap + transferred_value
		needBalance := uint256.NewInt(it.metaTx.Tx.gas)
		needBalance.Mul(needBalance, uint256.NewInt(it.metaTx.Tx.feeCap))
		needBalance.Add(needBalance, &it.metaTx.Tx.value)
		minFeeCap = min(minFeeCap, it.metaTx.Tx.feeCap)
		minTip = min(minTip, it.metaTx.Tx.tip)
		if pendingBaseFee >= minFeeCap {
			it.metaTx.effectiveTip = minTip
		} else {
			it.metaTx.effectiveTip = minFeeCap - pendingBaseFee
		}
		// 1. Minimum fee requirement. Set to 1 if feeCap of the transaction is no less than in-protocol
		// parameter of minimal base fee. Set to 0 if feeCap is less than minimum base fee, which means
		// this transaction will never be included into this particular chain.
		it.metaTx.subPool &^= EnoughFeeCapProtocol
		if it.metaTx.Tx.feeCap >= protocolBaseFee {
			//fmt.Printf("alex1: %d,%d,%d,%d\n", it.metaTx.NeedBalance.Uint64(), it.metaTx.Tx.gas, it.metaTx.Tx.feeCap, it.metaTx.Tx.value.Uint64())
			//fmt.Printf("alex2: %d,%t\n", sender.balance.Uint64(), it.metaTx.SenderHasEnoughBalance)
			it.metaTx.subPool |= EnoughFeeCapProtocol
		} else {
			it.metaTx.subPool = 0 // TODO: we immediately drop all transactions if they have no first bit - then maybe we don't need this bit at all? And don't add such transactions to queue?
			return true
		}

		// 2. Absence of nonce gaps. Set to 1 for transactions whose nonce is N, state nonce for
		// the sender is M, and there are transactions for all nonces between M and N from the same
		// sender. Set to 0 is the transaction's nonce is divided from the state nonce by one or more nonce gaps.
		it.metaTx.subPool &^= NoNonceGaps
		if noGapsNonce == it.metaTx.Tx.nonce {
			it.metaTx.subPool |= NoNonceGaps
			noGapsNonce++
		}

		// 3. Sufficient balance for gas. Set to 1 if the balance of sender's account in the
		// state is B, nonce of the sender in the state is M, nonce of the transaction is N, and the
		// sum of feeCap x gasLimit + transferred_value of all transactions from this sender with
		// nonces N+1 ... M is no more than B. Set to 0 otherwise. In other words, this bit is
		// set if there is currently a guarantee that the transaction and all its required prior
		// transactions will be able to pay for gas.
		it.metaTx.subPool &^= EnoughBalance
		if it.metaTx.Tx.nonce > sender.nonce {
			cumulativeRequiredBalance = cumulativeRequiredBalance.Add(cumulativeRequiredBalance, needBalance) // already deleted all transactions with nonce <= sender.nonce
			if sender.balance.Gt(cumulativeRequiredBalance) || sender.balance.Eq(cumulativeRequiredBalance) {
				it.metaTx.subPool |= EnoughBalance
			}
		}

		// 4. Dynamic fee requirement. Set to 1 if feeCap of the transaction is no less than
		// baseFee of the currently pending block. Set to 0 otherwise.
		it.metaTx.subPool &^= EnoughFeeCapBlock
		if it.metaTx.Tx.feeCap >= pendingBaseFee {
			it.metaTx.subPool |= EnoughFeeCapBlock
		}

		// 5. Local transaction. Set to 1 if transaction is local.
		// can't change

		return true
	})
}

func promote(pending, baseFee, queued *SubPool, discard func(tx *metaTx)) {
	//1. If top element in the worst green queue has subPool != 0b1111 (binary), it needs to be removed from the green pool.
	//   If subPool < 0b1000 (not satisfying minimum fee), discard.
	//   If subPool == 0b1110, demote to the yellow pool, otherwise demote to the red pool.
	for worst := pending.Worst(); pending.Len() > 0; worst = pending.Worst() {
		if worst.subPool >= 0b11110 {
			break
		}
		if worst.subPool >= 0b11100 {
			baseFee.Add(pending.PopWorst())
			continue
		}
		if worst.subPool >= 0b10000 {
			queued.Add(pending.PopWorst())
			continue
		}
		discard(pending.PopWorst())
	}

	//2. If top element in the worst green queue has subPool == 0b1111, but there is not enough room in the pool, discard.
	for worst := pending.Worst(); pending.Len() > PendingSubPoolLimit; worst = pending.Worst() {
		if worst.subPool >= 0b11111 { // TODO: here must 'subPool == 0b1111' or 'subPool <= 0b1111' ?
			break
		}
		discard(pending.PopWorst())
	}

	//3. If the top element in the best yellow queue has subPool == 0b1111, promote to the green pool.
	for best := baseFee.Best(); baseFee.Len() > 0; best = baseFee.Best() {
		if best.subPool < 0b11110 {
			break
		}
		pending.Add(baseFee.PopBest())
	}

	//4. If the top element in the worst yellow queue has subPool != 0x1110, it needs to be removed from the yellow pool.
	//   If subPool < 0b1000 (not satisfying minimum fee), discard. Otherwise, demote to the red pool.
	for worst := baseFee.Worst(); baseFee.Len() > 0; worst = baseFee.Worst() {
		if worst.subPool >= 0b11100 {
			break
		}
		if worst.subPool >= 0b10000 {
			queued.Add(baseFee.PopWorst())
			continue
		}
		discard(baseFee.PopWorst())
	}

	//5. If the top element in the worst yellow queue has subPool == 0x1110, but there is not enough room in the pool, discard.
	for worst := baseFee.Worst(); baseFee.Len() > BaseFeeSubPoolLimit; worst = baseFee.Worst() {
		if worst.subPool >= 0b11110 {
			break
		}
		discard(baseFee.PopWorst())
	}

	//6. If the top element in the best red queue has subPool == 0x1110, promote to the yellow pool. If subPool == 0x1111, promote to the green pool.
	for best := queued.Best(); queued.Len() > 0; best = queued.Best() {
		if best.subPool < 0b11100 {
			break
		}
		if best.subPool < 0b11110 {
			baseFee.Add(queued.PopBest())
			continue
		}

		pending.Add(queued.PopBest())
	}

	//7. If the top element in the worst red queue has subPool < 0b1000 (not satisfying minimum fee), discard.
	for worst := queued.Worst(); queued.Len() > 0; worst = queued.Worst() {
		if worst.subPool >= 0b10000 {
			break
		}

		discard(queued.PopWorst())
	}

	//8. If the top element in the worst red queue has subPool >= 0b100, but there is not enough room in the pool, discard.
	for _ = queued.Worst(); queued.Len() > QueuedSubPoolLimit; _ = queued.Worst() {
		discard(queued.PopWorst())
	}
}

type SubPool struct {
	t     SubPoolType
	best  *BestQueue
	worst *WorstQueue
}

func NewSubPool(t SubPoolType) *SubPool {
	return &SubPool{t: t, best: &BestQueue{}, worst: &WorstQueue{}}
}

func (p *SubPool) EnforceInvariants() {
	heap.Init(p.worst)
	heap.Init(p.best)
}
func (p *SubPool) Best() *metaTx {
	if len(*p.best) == 0 {
		return nil
	}
	return (*p.best)[0]
}
func (p *SubPool) Worst() *metaTx {
	if len(*p.worst) == 0 {
		return nil
	}
	return (*p.worst)[0]
}
func (p *SubPool) PopBest() *metaTx {
	i := heap.Pop(p.best).(*metaTx)
	heap.Remove(p.worst, i.worstIndex)
	return i
}
func (p *SubPool) PopWorst() *metaTx {
	i := heap.Pop(p.worst).(*metaTx)
	heap.Remove(p.best, i.bestIndex)
	return i
}
func (p *SubPool) Len() int { return p.best.Len() }
func (p *SubPool) Add(i *metaTx) {
	i.currentSubPool = p.t
	heap.Push(p.best, i)
	heap.Push(p.worst, i)
}

// UnsafeRemove - does break Heap invariants, but it has O(1) instead of O(log(n)) complexity.
// Must manually call heap.Init after such changes.
// Make sense to batch unsafe changes
func (p *SubPool) UnsafeRemove(i *metaTx) {
	if p.Len() == 0 {
		return
	}
	if p.Len() == 1 && i.bestIndex == 0 {
		p.worst.Pop()
		p.best.Pop()
		return
	}
	// manually call funcs instead of heap.Pop
	p.worst.Swap(i.worstIndex, p.worst.Len()-1)
	p.worst.Pop()
	p.best.Swap(i.bestIndex, p.best.Len()-1)
	p.best.Pop()
}
func (p *SubPool) UnsafeAdd(i *metaTx) {
	i.currentSubPool = p.t
	p.worst.Push(i)
	p.best.Push(i)
}
func (p *SubPool) DebugPrint(prefix string) {
	for i, it := range *p.best {
		fmt.Printf("%s.best: %d, %d, %d\n", prefix, i, it.subPool, it.bestIndex)
	}
	for i, it := range *p.worst {
		fmt.Printf("%s.worst: %d, %d, %d\n", prefix, i, it.subPool, it.worstIndex)
	}
}

type BestQueue []*metaTx

func (mt *metaTx) Less(than *metaTx) bool {
	if mt.subPool != than.subPool {
		return mt.subPool < than.subPool
	}

	if mt.effectiveTip != than.effectiveTip {
		return mt.effectiveTip < than.effectiveTip
	}

	if mt.Tx.nonce != than.Tx.nonce {
		return mt.Tx.nonce < than.Tx.nonce
	}
	return false
}

func (p BestQueue) Len() int           { return len(p) }
func (p BestQueue) Less(i, j int) bool { return !p[i].Less(p[j]) } // We want Pop to give us the highest, not lowest, priority so we use !less here.
func (p BestQueue) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
	p[i].bestIndex = i
	p[j].bestIndex = j
}
func (p *BestQueue) Push(x interface{}) {
	n := len(*p)
	item := x.(*metaTx)
	item.bestIndex = n
	*p = append(*p, item)
}

func (p *BestQueue) Pop() interface{} {
	old := *p
	n := len(old)
	item := old[n-1]
	old[n-1] = nil          // avoid memory leak
	item.bestIndex = -1     // for safety
	item.currentSubPool = 0 // for safety
	*p = old[0 : n-1]
	return item
}

type WorstQueue []*metaTx

func (p WorstQueue) Len() int           { return len(p) }
func (p WorstQueue) Less(i, j int) bool { return p[i].Less(p[j]) }
func (p WorstQueue) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
	p[i].worstIndex = i
	p[j].worstIndex = j
}
func (p *WorstQueue) Push(x interface{}) {
	n := len(*p)
	item := x.(*metaTx)
	item.worstIndex = n
	*p = append(*p, x.(*metaTx))
}
func (p *WorstQueue) Pop() interface{} {
	old := *p
	n := len(old)
	item := old[n-1]
	old[n-1] = nil          // avoid memory leak
	item.worstIndex = -1    // for safety
	item.currentSubPool = 0 // for safety
	*p = old[0 : n-1]
	return item
}

// BroadcastLoop - does:
// send pending byHash to p2p:
//      - new byHash
//      - all pooled byHash to recently connected peers
//      - all local pooled byHash to random peers periodically
// promote/demote transactions
// reorgs
func BroadcastLoop(ctx context.Context, db kv.RwDB, coreDB kv.RoDB, p *TxPool, senders *SendersCache, newTxs chan Hashes, send *Send, timings Timings) {
	db.Update(ctx, func(tx kv.RwTx) error { return tx.ClearBucket(kv.PooledSender) })
	if err := db.View(ctx, func(tx kv.Tx) error {
		return coreDB.View(ctx, func(coreTx kv.Tx) error {
			return p.fromDB(ctx, tx, coreTx, senders)
		})
	}); err != nil {
		log.Error("restore from db", "err", err)
	}
	p.logStats(senders)

	logEvery := time.NewTicker(timings.logEvery)
	defer logEvery.Stop()
	commitEvery := time.NewTicker(15 * time.Second)
	defer commitEvery.Stop()

	syncToNewPeersEvery := time.NewTicker(timings.syncToNewPeersEvery)
	defer syncToNewPeersEvery.Stop()

	localTxHashes := make([]byte, 0, 128)
	remoteTxHashes := make([]byte, 0, 128)

	for {
		select {
		case <-ctx.Done():
			return
		case <-logEvery.C:
			p.logStats(senders)
			log.Info("cache", "size", senders.len())
		case <-commitEvery.C:
			if db != nil {
				t := time.Now()
				if err := db.Update(ctx, func(tx kv.RwTx) error {
					if err := p.flush(tx, senders); err != nil {
						return err
					}
					sd, _, err := tx.(*mdbx.MdbxTx).SpaceDirty()
					if err != nil {
						return err
					}
					log.Info("flush", "in", time.Since(t), "dirty_kb", sd/1024)
					t = time.Now()
					return nil
				}); err != nil {
					log.Error("flush is local history", "err", err)
				}
				log.Info("commit", "in", time.Since(t))
			}
		case h := <-newTxs:
			// first broadcast all local txs to all peers, then non-local to random sqrt(peersAmount) peers
			localTxHashes = localTxHashes[:0]
			remoteTxHashes = remoteTxHashes[:0]

			for i := 0; i < h.Len(); i++ {
				if p.IdHashIsLocal(h.At(i)) {
					localTxHashes = append(localTxHashes, h.At(i)...)
				} else {
					remoteTxHashes = append(localTxHashes, h.At(i)...)
				}
			}

			send.BroadcastLocalPooledTxs(localTxHashes)
			send.BroadcastRemotePooledTxs(remoteTxHashes)
		case <-syncToNewPeersEvery.C: // new peer
			newPeers := p.recentlyConnectedPeers.GetAndClean()
			if len(newPeers) == 0 {
				continue
			}
			remoteTxHashes = p.AppendAllHashes(remoteTxHashes[:0])
			send.PropagatePooledTxsToPeersList(newPeers, remoteTxHashes)
		}
	}
}

func restoreIsLocalHistory(db kv.RwDB, localsHistory *simplelru.LRU) error {
	if db == nil {
		return nil
	}
	return db.View(context.Background(), func(tx kv.Tx) error {
		return tx.ForPrefix(kv.RecentLocalTransaction, nil, func(k, v []byte) error {
			localsHistory.Add(copyBytes(v), struct{}{})
			return nil
		})
	})
}

func copyBytes(b []byte) (copiedBytes []byte) {
	copiedBytes = make([]byte, len(b))
	copy(copiedBytes, b)
	return
}

// recentlyConnectedPeers does buffer IDs of recently connected good peers
// then sync of pooled Transaction can happen to all of then at once
// DoS protection and performance saving
// it doesn't track if peer disconnected, it's fine
type recentlyConnectedPeers struct {
	lock  sync.RWMutex
	peers []PeerID
}

func (l *recentlyConnectedPeers) AddPeer(p PeerID) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.peers = append(l.peers, p)
}

func (l *recentlyConnectedPeers) GetAndClean() []PeerID {
	l.lock.Lock()
	defer l.lock.Unlock()
	peers := l.peers
	l.peers = nil
	return peers
}

func min(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}
