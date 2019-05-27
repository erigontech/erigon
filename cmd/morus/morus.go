package main

import (
	"bytes"
	"flag"
	"fmt"
	"hash"
	"io"
	"math/big"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"syscall"
	"time"

	"github.com/boltdb/bolt"
	lru "github.com/hashicorp/golang-lru"

	"github.com/ethereum/go-ethereum/avl"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"

	"golang.org/x/crypto/sha3"
)

var (
	cpuprofile = flag.String("cpu-profile", "", "write cpu profile `file`")
	blockchain = flag.String("blockchain", "data/blockchain", "file containing blocks to load")
	hashlen    = flag.Int("hashlen", 32, "size of the hashes for inter-page references")
	datadir    = flag.String("datadir", ".", "directory for data files")
	load       = flag.Bool("load", false, "load blocks into pages")
	analysis   = flag.Bool("analysis", false, "perform analysis")
)

var emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")
var emptyCodeHash = crypto.Keccak256(nil)

// ChainContext implements Ethereum's core.ChainContext and consensus.Engine
// interfaces. It is needed in order to apply and process Ethereum
// transactions. There should only be a single implementation in Ethermint. For
// the purposes of Ethermint, it should be support retrieving headers and
// consensus parameters from  the current blockchain to be used during
// transaction processing.
//
// NOTE: Ethermint will distribute the fees out to validators, so the structure
// and functionality of this is a WIP and subject to change.
type ChainContext struct {
	Coinbase        common.Address
	headersByNumber map[uint64]*types.Header
}

func NewChainContext() *ChainContext {
	return &ChainContext{
		headersByNumber: make(map[uint64]*types.Header),
	}
}

// Engine implements Ethereum's core.ChainContext interface. As a ChainContext
// implements the consensus.Engine interface, it is simply returned.
func (cc *ChainContext) Engine() consensus.Engine {
	return cc
}

// SetHeader implements Ethereum's core.ChainContext interface. It sets the
// header for the given block number.
func (cc *ChainContext) SetHeader(number uint64, header *types.Header) {
	cc.headersByNumber[number] = header
}

// GetHeader implements Ethereum's core.ChainContext interface.
//
// TODO: The Cosmos SDK supports retreiving such information in contexts and
// multi-store, so this will be need to be integrated.
func (cc *ChainContext) GetHeader(_ common.Hash, number uint64) *types.Header {
	if header, ok := cc.headersByNumber[number]; ok {
		return header
	}

	return nil
}

// Author implements Ethereum's consensus.Engine interface. It is responsible
// for returned the address of the validtor to receive any fees. This function
// is only invoked if the given author in the ApplyTransaction call is nil.
//
// NOTE: Ethermint will distribute the fees out to validators, so the structure
// and functionality of this is a WIP and subject to change.
func (cc *ChainContext) Author(_ *types.Header) (common.Address, error) {
	return cc.Coinbase, nil
}

// APIs implements Ethereum's consensus.Engine interface. It currently performs
// a no-op.
//
// TODO: Do we need to support such RPC APIs? This will tie into a bigger
// discussion on if we want to support web3.
func (cc *ChainContext) APIs(_ consensus.ChainReader) []rpc.API {
	return nil
}

// CalcDifficulty implements Ethereum's consensus.Engine interface. It currently
// performs a no-op.
func (cc *ChainContext) CalcDifficulty(_ consensus.ChainReader, _ uint64, _ *types.Header) *big.Int {
	return nil
}

// Finalize implements Ethereum's consensus.Engine interface. It currently
// performs a no-op.
//
// TODO: Figure out if this needs to be hooked up to any part of the ABCI?
func (cc *ChainContext) Finalize(
	_ consensus.ChainReader, _ *types.Header, _ *state.StateDB,
	_ []*types.Transaction, _ []*types.Header, _ []*types.Receipt,
) (*types.Block, error) {
	return nil, nil
}

// Prepare implements Ethereum's consensus.Engine interface. It currently
// performs a no-op.
//
// TODO: Figure out if this needs to be hooked up to any part of the ABCI?
func (cc *ChainContext) Prepare(_ consensus.ChainReader, _ *types.Header) error {
	return nil
}

// Seal implements Ethereum's consensus.Engine interface. It currently
// performs a no-op.
//
// TODO: Figure out if this needs to be hooked up to any part of the ABCI?
func (cc *ChainContext) Seal(_ consensus.ChainReader, _ *types.Block, _ chan<- *types.Block, _ <-chan struct{}) error {
	return nil
}

// SealHash implements Ethereum's consensus.Engine interface. It returns the
// hash of a block prior to it being sealed.
func (cc *ChainContext) SealHash(header *types.Header) common.Hash {
	return common.Hash{}
}

// VerifyHeader implements Ethereum's consensus.Engine interface. It currently
// performs a no-op.
//
// TODO: Figure out if this needs to be hooked up to any part of the Cosmos SDK
// handlers?
func (cc *ChainContext) VerifyHeader(_ consensus.ChainReader, _ *types.Header, _ bool) error {
	return nil
}

// VerifyHeaders implements Ethereum's consensus.Engine interface. It
// currently performs a no-op.
//
// TODO: Figure out if this needs to be hooked up to any part of the Cosmos SDK
// handlers?
func (cc *ChainContext) VerifyHeaders(_ consensus.ChainReader, _ []*types.Header, _ []bool) (chan<- struct{}, <-chan error) {
	return nil, nil
}

// VerifySeal implements Ethereum's consensus.Engine interface. It currently
// performs a no-op.
//
// TODO: Figure out if this needs to be hooked up to any part of the Cosmos SDK
// handlers?
func (cc *ChainContext) VerifySeal(_ consensus.ChainReader, _ *types.Header) error {
	return nil
}

// VerifyUncles implements Ethereum's consensus.Engine interface. It currently
// performs a no-op.
func (cc *ChainContext) VerifyUncles(_ consensus.ChainReader, _ *types.Block) error {
	return nil
}

// Close implements Ethereum's consensus.Engine interface. It terminates any
// background threads maintained by the consensus engine. It currently performs
// a no-op.
func (cc *ChainContext) Close() error {
	return nil
}

type MorusDb struct {
	db             *avl.Avl3
	codeDb         *bolt.DB
	preDb          *bolt.DB
	addrDb         *bolt.DB
	codeCache      *lru.Cache
	codeSizeCache  *lru.Cache
	preimageCache  *lru.Cache
	a2iCache       *lru.Cache
	i2aCache       *lru.Cache
	currentStateDb *state.StateDB
}

func NewMorusDb(datadir string, hashlen int) *MorusDb {
	db := avl.NewAvl3()
	db.SetHashLength(uint32(hashlen))
	pagefile := filepath.Join(datadir, "pages")
	valuefile := filepath.Join(datadir, "values")
	verfile := filepath.Join(datadir, "versions")
	codefile := filepath.Join(datadir, "codes")
	prefile := filepath.Join(datadir, "preimages")
	addrfile := filepath.Join(datadir, "addrs")
	db.UseFiles(pagefile, valuefile, verfile, false)
	codeDb, err := bolt.Open(codefile, 0600, &bolt.Options{})
	if err != nil {
		panic(err)
	}
	preDb, err := bolt.Open(prefile, 0600, &bolt.Options{})
	if err != nil {
		panic(err)
	}
	addrDb, err := bolt.Open(addrfile, 0600, &bolt.Options{})
	if err != nil {
		panic(err)
	}
	csc, err := lru.New(100000)
	if err != nil {
		panic(err)
	}
	cc, err := lru.New(10000)
	if err != nil {
		panic(err)
	}
	pic, err := lru.New(1000000)
	if err != nil {
		panic(err)
	}
	a2i, err := lru.New(1000000)
	if err != nil {
		panic(err)
	}
	i2a, err := lru.New(1000000)
	if err != nil {
		panic(err)
	}
	morus := &MorusDb{
		db:            db,
		codeDb:        codeDb,
		preDb:         preDb,
		addrDb:        addrDb,
		codeCache:     cc,
		codeSizeCache: csc,
		preimageCache: pic,
		a2iCache:      a2i,
		i2aCache:      i2a,
	}
	//db.SetCompare(morus.Compare)
	return morus
}

var preimageBucket = []byte("P")

func encodeUint64(x uint64) []byte {
	var e []byte
	var limit uint64
	limit = 32
	for bytecount := 1; bytecount <= 8; bytecount++ {
		if x < limit {
			e = make([]byte, bytecount)
			b := x
			for i := bytecount - 1; i > 0; i-- {
				e[i] = byte(b & 0xff)
				b >>= 8
			}
			e[0] = byte(b) | (byte(bytecount) << 5) // 3 most significant bits of the first byte are bytecount
			break
		}
		limit <<= 8
	}
	return e
}

func decodeUint64(e []byte) uint64 {
	bytecount := int(e[0] >> 5)
	x := uint64(e[0] & 0x1f)
	for i := 1; i < bytecount; i++ {
		if i >= len(e) {
			fmt.Printf("e: %x\n", e)
		}
		x = (x << 8) | uint64(e[i])
	}
	return x
}

var id2addrBucket = []byte("I2A")
var addr2idBucket = []byte("A2I")

func (md *MorusDb) AddrToId(addr common.Address, create bool) []byte {
	var id []byte
	if cached, ok := md.a2iCache.Get(addr); ok {
		id = cached.([]byte)
		if id != nil || !create {
			return id
		}
	}
	var seq uint64
	if err := md.addrDb.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(addr2idBucket)
		if err != nil {
			return err
		}
		rBucket, err := tx.CreateBucketIfNotExists(id2addrBucket)
		if err != nil {
			return err
		}
		id = bucket.Get(addr[:])
		if id != nil {
			id = common.CopyBytes(id)
		} else if create {
			var err error
			seq, err = bucket.NextSequence()
			if err != nil {
				return err
			}
			id = encodeUint64(seq)
			if err = bucket.Put(addr[:], id); err != nil {
				return err
			}
			if err = rBucket.Put(id, addr[:]); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		panic(err)
	}
	md.a2iCache.Add(addr, id)
	if id != nil {
		md.i2aCache.Add(seq, addr)
	}
	return id
}

func (md *MorusDb) IdToAddr(id []byte) common.Address {
	seq := decodeUint64(id)
	if cached, ok := md.i2aCache.Get(seq); ok {
		return cached.(common.Address)
	}
	var addr common.Address
	var found bool
	if err := md.addrDb.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(id2addrBucket)
		if bucket == nil {
			return nil
		}
		v := bucket.Get(id)
		if v != nil {
			copy(addr[:], v)
			found = true
		}
		return nil
	}); err != nil {
		panic(err)
	}
	if found {
		md.i2aCache.Add(seq, addr)
	}
	return addr
}

func (md *MorusDb) CommitPreimages(statedb *state.StateDB) {
	if err := md.preDb.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(preimageBucket)
		if err != nil {
			return err
		}
		pi := statedb.Preimages()
		for hash, preimage := range pi {
			md.preimageCache.Add(hash, preimage)
			if err := bucket.Put(hash[:], preimage); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		panic(err)
	}
}

func (md *MorusDb) LatestVersion() int64 {
	return int64(md.db.CurrentVersion())
}

func (md *MorusDb) Commit() uint64 {
	return md.db.Commit()
}

func (md *MorusDb) Close() {
	md.db.Close()
	md.codeDb.Close()
	md.preDb.Close()
	md.addrDb.Close()
}

func (md *MorusDb) PrintStats() {
	md.db.PrintStats()
}

func accountToEncoding(account *state.Account) ([]byte, error) {
	var data []byte
	var err error
	if (account.CodeHash == nil || bytes.Equal(account.CodeHash, emptyCodeHash)) && (account.Root == emptyRoot || account.Root == common.Hash{}) {
		if (account.Balance == nil || account.Balance.Sign() == 0) && account.Nonce == 0 {
			data = []byte{byte(192)}
		} else {
			var extAccount state.ExtAccount
			extAccount.Nonce = account.Nonce
			extAccount.Balance = account.Balance
			if extAccount.Balance == nil {
				extAccount.Balance = new(big.Int)
			}
			data, err = rlp.EncodeToBytes(extAccount)
			if err != nil {
				return nil, err
			}
		}
	} else {
		a := *account
		if a.Balance == nil {
			a.Balance = new(big.Int)
		}
		if a.CodeHash == nil {
			a.CodeHash = emptyCodeHash
		}
		if a.Root == (common.Hash{}) {
			a.Root = emptyRoot
		}
		data, err = rlp.EncodeToBytes(a)
		if err != nil {
			return nil, err
		}
	}
	return data, err
}

func encodingToAccount(enc []byte) (*state.Account, error) {
	if enc == nil || len(enc) == 0 {
		return nil, nil
	}
	var data state.Account
	// Kind of hacky
	if len(enc) == 1 {
		data.Balance = new(big.Int)
		data.CodeHash = emptyCodeHash
		data.Root = emptyRoot
	} else if len(enc) < 60 {
		var extData state.ExtAccount
		if err := rlp.DecodeBytes(enc, &extData); err != nil {
			return nil, err
		}
		data.Nonce = extData.Nonce
		data.Balance = extData.Balance
		data.CodeHash = emptyCodeHash
		data.Root = emptyRoot
	} else {
		if err := rlp.DecodeBytes(enc, &data); err != nil {
			return nil, err
		}
	}
	return &data, nil
}

var codeBucket = []byte("C")

const (
	PREFIX_ACCOUNT   = byte(0)
	PREFIX_STORAGE   = byte(1)
	PREFIX_ONE_WORD  = byte(2)
	PREFIX_TWO_WORDS = byte(3)
)

func (md *MorusDb) createAccountKey(address *common.Address, create bool) []byte {
	/*
		id := md.AddrToId(*address, create)
		if id == nil {
			return nil
		}
		k := make([]byte, 1 + len(id))
		k[0] = PREFIX_ACCOUNT
		copy(k[1:], id[:])
		return k
	*/
	k := *address
	return k[:]
}

func (md *MorusDb) createStorageKey(address *common.Address, key *common.Hash) (full, comp []byte, compressed bool) {
	full = make([]byte, 52)
	copy(full[0:], (*address)[:])
	copy(full[20:], (*key)[:])
	/*
		full = make([]byte, 53)
		full[0] = PREFIX_STORAGE
		copy(full[1:], (*address)[:])
		copy(full[21:], (*key)[:])
		var preimage []byte
		if cached, ok := md.preimageCache.Get(*key); ok {
			preimage = cached.([]byte)
		} else {
			if err := md.preDb.View(func(tx *bolt.Tx) error {
				bucket := tx.Bucket(preimageBucket)
				if bucket == nil {
					return nil
				}
				v := bucket.Get((*key)[:])
				if len(v) > 0 {
					preimage = common.CopyBytes(v)
				}
				return nil
			}); err != nil {
				panic(err)
			}
			md.preimageCache.Add(*key, preimage) // even if it is nil
		}
		if preimage != nil {
			if len(preimage) == 32 {
				v := bytes.TrimLeft(preimage, "\x00")
				k := make([]byte, 21 + len(v))
				k[0] = PREFIX_ONE_WORD
				copy(k[1:], (*address)[:])
				copy(k[21:], v)
				return full, k, true
			} else if len(preimage) == 64 {
				v1 := bytes.TrimLeft(preimage[:32], "\x00")
				v2 := bytes.TrimLeft(preimage[32:], "\x00")
				k := make([]byte, 22 + len(v1) + len(v2))
				k[0] = PREFIX_TWO_WORDS
				copy(k[1:], (*address)[:])
				k[21] = byte(len(v1))
				copy(k[22:], v1)
				copy(k[22+len(v1):], v2)
				return full, k, true
			}
		}
	*/
	return full, full, false
}

// keccakState wraps sha3.state. In addition to the usual hash methods, it also supports
// Read to get a variable amount of data from the hash state. Read is faster than Sum
// because it doesn't copy the internal state, but also modifies the internal state.
type keccakState interface {
	hash.Hash
	Read([]byte) (int, error)
}

type hasher struct {
	sha keccakState
}

var hasherPool = make(chan *hasher, 128)

func newHasher() *hasher {
	var h *hasher
	select {
	case h = <-hasherPool:
	default:
		h = &hasher{sha: sha3.NewLegacyKeccak256().(keccakState)}
	}
	return h
}

func returnHasherToPool(h *hasher) {
	select {
	case hasherPool <- h:
	default:
		fmt.Printf("Allowing hasher to be garbage collected, pool is full\n")
	}
}

func (md *MorusDb) restoreKey(x []byte) []byte {
	switch x[0] {
	case PREFIX_ACCOUNT:
		addr := md.IdToAddr(x[1:])
		return addr[:]
	case PREFIX_STORAGE:
		return x[1:]
	case PREFIX_ONE_WORD:
		var k [52]byte
		copy(k[:], x[1:21])
		h := newHasher()
		defer returnHasherToPool(h)
		h.sha.Reset()
		h.sha.Write(x[21:])
		h.sha.Read(k[20:])
		return k[:]
	case PREFIX_TWO_WORDS:
		var k [52]byte
		l1 := int(x[21])
		h := newHasher()
		defer returnHasherToPool(h)
		h.sha.Reset()
		h.sha.Write(k[:32-l1]) // zeros
		h.sha.Write(x[22 : 22+l1])
		l2 := len(x) - 22 - l1
		h.sha.Write(k[:32-l2])
		h.sha.Write(x[22+l1:])
		copy(k[:], x[1:21])
		h.sha.Read(k[20:])
		return k[:]
	}
	panic("")
}

func (md *MorusDb) Compare(x, y []byte) int {
	return bytes.Compare(md.restoreKey(x), md.restoreKey(y))
}

func (md *MorusDb) ReadAccountData(address common.Address) (*state.Account, error) {
	key := md.createAccountKey(&address, false)
	if key == nil {
		return nil, nil
	}
	enc, found := md.db.Get(key)
	if !found || enc == nil || len(enc) == 0 {
		return nil, nil
	}
	return encodingToAccount(enc)
}

func (md *MorusDb) ReadAccountStorage(address common.Address, key *common.Hash) ([]byte, error) {
	full, comp, compressed := md.createStorageKey(&address, key)
	var enc []byte
	var found bool
	if compressed {
		enc, found = md.db.Get(full)
	}
	if found {
		md.db.Delete(full)
		md.db.Insert(comp, enc)
	} else {
		enc, found = md.db.Get(comp)
	}
	enc, found = md.db.Get(full)
	if !found || enc == nil || len(enc) == 0 {
		return nil, nil
	}
	return enc, nil
}

func (md *MorusDb) ReadAccountCode(codeHash common.Hash) ([]byte, error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}
	if cached, ok := md.codeCache.Get(codeHash); ok {
		return cached.([]byte), nil
	}
	var code []byte
	if err := md.codeDb.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(codeBucket)
		if bucket == nil {
			return nil
		}
		v := bucket.Get(codeHash[:])
		if len(v) > 0 {
			code = common.CopyBytes(v)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if code != nil {
		md.codeSizeCache.Add(codeHash, len(code))
		md.codeCache.Add(codeHash, code)
	}
	return code, nil
}

func (md *MorusDb) ReadAccountCodeSize(codeHash common.Hash) (int, error) {
	if cached, ok := md.codeSizeCache.Get(codeHash); ok {
		return cached.(int), nil
	}
	code, err := md.ReadAccountCode(codeHash)
	if err != nil {
		return 0, err
	}
	return len(code), nil
}

func (md *MorusDb) UpdateAccountData(address common.Address, original, account *state.Account) error {
	data, err := accountToEncoding(account)
	if err != nil {
		return err
	}
	md.db.Insert(md.createAccountKey(&address, true), data)
	return nil
}

func (md *MorusDb) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	if err := md.codeDb.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(codeBucket)
		if err != nil {
			return err
		}
		return bucket.Put(codeHash[:], code)
	}); err != nil {
		return err
	}
	return nil
}

func (md *MorusDb) DeleteAccount(address common.Address, original *state.Account) error {
	key := md.createAccountKey(&address, false)
	if key == nil {
		return nil
	}
	md.db.Delete(key)
	return nil
}

func (md *MorusDb) WriteAccountStorage(address common.Address, key, original, value *common.Hash) error {
	v := bytes.TrimLeft(value[:], "\x00")
	vv := make([]byte, len(v))
	copy(vv, v)
	_, comp, _ := md.createStorageKey(&address, key)
	if len(vv) > 0 {
		md.db.Insert(comp, vv)
	} else {
		md.db.Delete(comp)
	}
	return nil
}

// Some weird constants to avoid constant memory allocs for them.
var (
	big8  = big.NewInt(8)
	big32 = big.NewInt(32)
)

// accumulateRewards credits the coinbase of the given block with the mining
// reward. The total reward consists of the static block reward and rewards for
// included uncles. The coinbase of each uncle block is also rewarded.
func accumulateRewards(config *params.ChainConfig, state *state.StateDB, header *types.Header, uncles []*types.Header) {
	// select the correct block reward based on chain progression
	blockReward := ethash.FrontierBlockReward
	if config.IsByzantium(header.Number) {
		blockReward = ethash.ByzantiumBlockReward
	}

	// accumulate the rewards for the miner and any included uncles
	reward := new(big.Int).Set(blockReward)
	r := new(big.Int)

	for _, uncle := range uncles {
		r.Add(uncle.Number, big8)
		r.Sub(r, header.Number)
		r.Mul(r, blockReward)
		r.Div(r, big8)
		state.AddBalance(uncle.Coinbase, r)
		r.Div(blockReward, big32)
		reward.Add(reward, r)
	}

	state.AddBalance(header.Coinbase, reward)
}

func loadAll() {
	noopWriter := state.NewNoopWriter()
	morus := NewMorusDb(*datadir, *hashlen)
	if morus.LatestVersion() == 0 {
		statedb := state.New(morus)
		genBlock := core.DefaultGenesisBlock()
		for addr, account := range genBlock.Alloc {
			statedb.AddBalance(addr, account.Balance)
			statedb.SetCode(addr, account.Code)
			statedb.SetNonce(addr, account.Nonce)

			for key, value := range account.Storage {
				statedb.SetState(addr, key, value)
			}
		}
		if err := statedb.Commit(false, morus); err != nil {
			panic(err)
		}
		morus.CommitPreimages(statedb)
		cp := morus.Commit()

		fmt.Printf("Committed pages for genesis state: %d\n", cp)
	}
	// file with blockchain data exported from geth by using "geth exportdb"
	// command.
	input, err := os.Open(*blockchain)
	if err != nil {
		panic(err)
	}
	defer input.Close()

	// ethereum mainnet config
	chainConfig := params.MainnetChainConfig

	// create RLP stream for exported blocks
	stream := rlp.NewStream(input, 0)

	var block types.Block

	chainContext := NewChainContext()
	vmConfig := vm.Config{EnablePreimageRecording: true}

	startTime := time.Now()
	interrupt := false

	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()
	var lastSkipped uint64
	var cpRun uint64
	for !interrupt {
		if err = stream.Decode(&block); err == io.EOF {
			err = nil
			break
		} else if err != nil {
			panic(fmt.Errorf("failed to decode at block %d: %s", block.NumberU64(), err))
		}

		// don't import blocks already imported
		if block.NumberU64() < uint64(morus.LatestVersion()) {
			lastSkipped = block.NumberU64()
			continue
		}

		if lastSkipped > 0 {
			fmt.Printf("skipped blocks up to %d\n", lastSkipped)
			lastSkipped = 0
		}

		header := block.Header()
		chainContext.Coinbase = header.Coinbase
		chainContext.SetHeader(block.NumberU64(), header)

		statedb := state.New(morus)

		var (
			receipts types.Receipts
			usedGas  = new(uint64)
			allLogs  []*types.Log
			gp       = new(core.GasPool).AddGas(block.GasLimit())
		)

		if chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0 {
			misc.ApplyDAOHardFork(statedb)
		}

		for i, tx := range block.Transactions() {
			statedb.Prepare(tx.Hash(), block.Hash(), i)

			receipt, _, err := core.ApplyTransaction(chainConfig, chainContext, nil, gp, statedb, noopWriter, header, tx, usedGas, vmConfig)

			if err != nil {
				panic(fmt.Errorf("at block %d, tx %x: %v", block.NumberU64(), tx.Hash(), err))
			}

			receipts = append(receipts, receipt)
			allLogs = append(allLogs, receipt.Logs...)
		}

		// apply mining rewards to the geth stateDB

		accumulateRewards(chainConfig, statedb, header, block.Uncles())

		// commit block in geth
		err = statedb.Commit(chainConfig.IsEIP158(block.Number()), morus)
		if err != nil {
			panic(fmt.Errorf("at block %d: %v", block.NumberU64(), err))
		}

		// commit block in Ethermint
		morus.CommitPreimages(statedb)
		cp := morus.Commit()
		cpRun += cp

		if (block.NumberU64() % 10000) == 0 {
			fmt.Printf("processed %d blocks, time so far: %v\n", block.NumberU64(), time.Since(startTime))
			fmt.Printf("committed pages: %d, Mb %.3f\n", cpRun, float64(cpRun)*float64(avl.PageSize)/1024.0/1024.0)
			morus.PrintStats()
			cpRun = 0
		}

		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			fmt.Println("interrupted, please wait for cleanup...")
		default:
		}
	}
	morus.Close()

	fmt.Printf("processed %d blocks\n", block.NumberU64())
}

func avltest() {
	f, err := os.Create("avl1.dot")
	if err != nil {
		panic(err)
	}
	tr := avl.NewAvl3()
	tr.SetTracing(true)
	tr.Insert([]byte("aC"), []byte("Cv"))
	tr.Insert([]byte("aB"), []byte("Bv"))
	tr.Insert([]byte("aF"), []byte("Fv"))
	tr.Insert([]byte("aA"), []byte("Av"))
	tr.Insert([]byte("aE"), []byte("Ev"))
	tr.Insert([]byte("aG"), []byte("Gv"))
	tr.Insert([]byte("aD"), []byte("Dv"))
	tr.Insert([]byte("aH"), []byte("Hv"))
	tr.Insert([]byte("aI"), []byte("Iv"))
	tr.Insert([]byte("aJ"), []byte("Jv"))
	tr.Insert([]byte("aF"), []byte("Fv1"))
	tr.Insert([]byte("aC"), []byte("Cv1"))
	tr.Insert([]byte("aE"), []byte("Ev1"))
	tr.Insert([]byte("aD"), []byte("Dv1"))
	tr.Insert([]byte("aX"), []byte("xv"))
	tr.Insert([]byte("aW"), []byte("wv"))
	tr.Insert([]byte("aV"), []byte("vv"))
	tr.Insert([]byte("aU"), []byte("uv"))
	tr.Insert([]byte("aG"), []byte("1v"))
	tr.Insert([]byte("aP"), []byte("2v"))
	tr.Insert([]byte("aS"), []byte("3v"))
	tr.Insert([]byte("aO"), []byte("4v"))
	tr.Insert([]byte("aL"), []byte("5v"))
	tr.Commit()
	tr.Insert([]byte("aK"), []byte("6v"))
	tr.Insert([]byte("aM"), []byte("8v"))
	tr.Insert([]byte("aN"), []byte("7v"))
	tr.Insert([]byte("aW"), []byte("wv1"))
	tr.Insert([]byte("aJ"), []byte("6v"))
	tr.Commit()
	tr.Insert([]byte("aR"), []byte("8v"))
	tr.Insert([]byte("aT"), []byte("7v"))
	tr.Insert([]byte("aH"), []byte("Hv"))
	tr.Delete([]byte("aX"))
	tr.Delete([]byte("aF"))
	tr.Delete([]byte("aC"))
	tr.Delete([]byte("aE"))
	tr.Commit()
	err = avl.DotGraph("1", tr, f)
	if err != nil {
		panic(err)
	}
	err = f.Close()
	if err != nil {
		panic(err)
	}
	cmd := exec.Command("dot", "-Tpng", "-O", "avl1.dot")
	err = cmd.Run()
	if err != nil {
		panic(err)
	}
}

func doAnalysis() {
	morus := NewMorusDb(*datadir, *hashlen)
	defer morus.Close()
	morus.db.Analyse()
}

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			fmt.Printf("could not create CPU profile: %v\n", err)
			return
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Printf("could not start CPU profile: %v\n", err)
			return
		}
		defer pprof.StopCPUProfile()
	}
	if *analysis {
		doAnalysis()
	} else {
		loadAll()
	}
	//avltest()
}
