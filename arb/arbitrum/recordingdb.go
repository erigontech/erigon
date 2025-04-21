package arbitrum

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/erigontech/erigon-lib/chain"
	state2 "github.com/erigontech/erigon-lib/state"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon/arb/ethdb"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/execution/consensus"

	"github.com/erigontech/erigon-lib/rlp"
)

var (
	recordingDbSize       = metrics.GetOrCreateGauge("arb_validator_recordingdb_size") // note: only updating when adding state, not when removing - but should be good enough
	recordingDbReferences = metrics.GetOrCreateGauge("arb_validator_recordingdb_references")
)

type RecordingKV struct {
	diskDb        kv.Tx
	state         state.StateReader
	bucket        string
	readDbEntries map[common.Hash][]byte
	enableBypass  bool
}

func (db *RecordingKV) ReadAccountCode(address common.Address, incarnation uint64) ([]byte, error) {
	enc, err := db.state.ReadAccountCode(address, incarnation)
	if err != nil {
		return nil, err
	}
	hash := crypto.Keccak256Hash(enc)
	db.readDbEntries[hash] = enc
	// if crypto.Keccak256Hash(enc) != hash {
	// 	return nil, fmt.Errorf("recording KV attempted to access non-hash key %v", hash)
	// }
	//else if len(key) == len(rawdb.CodePrefix)+32 && bytes.HasPrefix(key, rawdb.CodePrefix) {
	// copy(hash[:], key[len(rawdb.CodePrefix):])
	return enc, nil
}

func NewRecordingKV(s state.StateReader) *RecordingKV {
	return &RecordingKV{state: s, readDbEntries: make(map[common.Hash][]byte), enableBypass: false}
}

func newRecordingKV(tx kv.Tx, bucket string) *RecordingKV {
	return &RecordingKV{diskDb: tx, bucket: bucket, readDbEntries: make(map[common.Hash][]byte), enableBypass: false}
}

//func newRecordingKV(inner *triedb.Database, diskDb ethdb.KeyValueStore) *RecordingKV {
//	return &RecordingKV{inner, diskDb, make(map[common.Hash][]byte), false}
//}

func (db *RecordingKV) Has(key []byte) (bool, error) {
	return false, errors.New("recording KV doesn't support Has")
}

func (db *RecordingKV) Get(key []byte) ([]byte, error) {
	var hash common.Hash
	var res []byte
	var err error
	if len(key) == 32 {
		copy(hash[:], key)
		res, err = db.diskDb.GetOne(db.bucket, key)
		// res, err = ibs.inner.Node(hash)
	} else if len(key) == len(rawdb.CodePrefix)+32 && bytes.HasPrefix(key, rawdb.CodePrefix) {
		// Retrieving code
		copy(hash[:], key[len(rawdb.CodePrefix):])
		res, err = db.diskDb.GetOne(db.bucket, key)
	} else {
		err = fmt.Errorf("recording KV attempted to access non-hash key %v", hex.EncodeToString(key))
	}
	if err != nil {
		return nil, err
	}
	if db.enableBypass {
		return res, nil
	}
	if crypto.Keccak256Hash(res) != hash {
		return nil, fmt.Errorf("recording KV attempted to access non-hash key %v", hash)
	}
	db.readDbEntries[hash] = res
	return res, nil
}

func (db *RecordingKV) Put(key []byte, value []byte) error {
	return errors.New("recording KV doesn't support Put")
}

func (db *RecordingKV) Delete(key []byte) error {
	return errors.New("recording KV doesn't support Delete")
}

func (db *RecordingKV) NewBatch() ethdb.Batch {
	// if ibs.enableBypass {
	// 	return ibs.diskDb.NewBatch()
	// }
	log.Error("recording KV: attempted to create batch when bypass not enabled")
	return nil
}

func (db *RecordingKV) NewBatchWithSize(size int) ethdb.Batch {
	// if ibs.enableBypass {
	// 	return ibs.diskDb.NewBatchWithSize(size)
	// }
	log.Error("recording KV: attempted to create batch when bypass not enabled")
	return nil
}

func (db *RecordingKV) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	// if ibs.enableBypass {
	// 	return ibs.diskDb.NewIterator(prefix, start)
	// }
	log.Error("recording KV: attempted to create iterator when bypass not enabled")
	return nil
}

func (db *RecordingKV) NewSnapshot() (ethdb.Snapshot, error) {
	// This is fine as RecordingKV doesn't support mutation
	return db, nil
}

func (db *RecordingKV) Stat(property string) (string, error) {
	return "", errors.New("recording KV doesn't support Stat")
}

func (db *RecordingKV) Compact(start []byte, limit []byte) error {
	return nil
}

func (db *RecordingKV) Close() error {
	return nil
}

func (db *RecordingKV) Release() {}

func (db *RecordingKV) GetRecordedEntries() map[common.Hash][]byte {
	return db.readDbEntries
}
func (db *RecordingKV) EnableBypass() {
	db.enableBypass = true
}

// // ChainContextBackend provides methods required to implement ChainContext.
// type ChainContextBackend interface {
// 	Engine() consensus.Engine
// 	HeaderByNumber(context.Context, rpc.BlockNumber) (*types.Header, error)
// }

// // ChainContext is an implementation of core.ChainContext. It's main use-case
// // is instantiating a vm.BlockContext without having access to the BlockChain object.
// type ChainContext struct {
// 	b   ChainContextBackend
// 	ctx context.Context
// }

type RecordingChainContext struct {
	// ChainHeaderReader                     consensus.Engine
	consensus.ChainHeaderReader // stagedsync.ChainReader
	minBlockNumberAccessed      uint64
	initialBlockNumber          uint64
}

func newRecordingChainContext(inner consensus.ChainHeaderReader, blocknumber uint64) *RecordingChainContext {
	return &RecordingChainContext{
		ChainHeaderReader:      inner,
		minBlockNumberAccessed: blocknumber,
		initialBlockNumber:     blocknumber,
	}
}

// func (r *RecordingChainContext) Engine() consensus.Engine {
// 	return r.ChainHeaderReader
// }
// func (r *RecordingChainContext) Config() *chain.Config {
// 	return r.bc.Config()
// }

func (r *RecordingChainContext) GetHeader(hash common.Hash, num uint64) *types.Header {
	if num < r.minBlockNumberAccessed {
		r.minBlockNumberAccessed = num
	}
	return r.ChainHeaderReader.GetHeader(hash, num)
}

func (r *RecordingChainContext) GetMinBlockNumberAccessed() uint64 {
	return r.minBlockNumberAccessed
}

type RecordingDatabaseConfig struct {
	TrieDirtyCache int
	TrieCleanCache int
}

type RecordingDatabase struct {
	config     *RecordingDatabaseConfig
	tdb        kv.TemporalRwDB
	tx         kv.TemporalRwTx
	sd         *state2.SharedDomains
	ibs        *state.IntraBlockState
	bc         core.BlockChain
	mutex      sync.Mutex // protects StateFor and Dereference
	references int64
}

func NewRecordDB(config *RecordingDatabaseConfig, ethdb kv.TemporalRwDB, ethTx kv.TemporalRwTx, bc core.BlockChain, sd *state2.SharedDomains) *RecordingDatabase {
	return &RecordingDatabase{
		config: config,
		sd:     sd,
		tx:     ethTx,
		tdb:    ethdb,
		bc:     bc,
	}

}

func NewRecordingDatabase(config *RecordingDatabaseConfig, ethdb kv.TemporalRwDB, blockchain core.BlockChain) *RecordingDatabase {
	//hashConfig := *hashdb.Defaults
	//hashConfig.CleanCacheSize = config.TrieCleanCache
	//trieConfig := triedb.Config{
	//	Preimages: false,
	//	HashDB:    &hashConfig,
	//}
	//
	tx, err := ethdb.BeginTemporalRw(context.Background())
	if err != nil {
		log.Error("Failed to create temporal tx", "err", err)
		return nil
	}

	sd, err := state2.NewSharedDomains(tx, log.New())
	if err != nil {
		log.Error("Failed to open shaerd domains", "err", err)
		return nil
	}
	return &RecordingDatabase{
		config: config,
		sd:     sd,
		tx:     tx,
		//ibs:     state.NewDatabaseWithConfig(ethdb, &trieConfig),
		ibs: state.New(state.NewReaderV3(sd)),
		bc:  blockchain,
	}
}

// Normal geth state.New + Reference is not atomic vs Dereference. This one is.
// This function does not recreate a state
func (r *RecordingDatabase) StateFor(header *types.Header) (*state.IntraBlockState, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// rawdbv3.TxNums.Max(r., blockNum uint64)

	rd := state.NewReaderV3(r.sd)
	sdb := state.New(rd)
	//sdb, err := state.NewDeterministic(header.Root, r.ibs)
	//if err == nil {
	//	r.referenceRootLockHeld(header.Root)
	//}
	return sdb, nil
}

func (r *RecordingDatabase) Dereference(header *types.Header) {
	if header != nil {
		r.dereferenceRoot(header.Root)
	}
}

func (r *RecordingDatabase) WriteStateToDatabase(header *types.Header) error {
	if _, err := r.sd.ComputeCommitment(context.Background(), true, header.Number.Uint64(), ""); err != nil {
		return err
	}
	// if header != nil {
	// 	return r.ibs.TrieDB().Commit(header.Root, true)
	// }
	return nil
}

// lock must be held when calling that
func (r *RecordingDatabase) referenceRootLockHeld(root common.Hash) {
	r.references++
	recordingDbReferences.SetUint64(uint64(r.references))

	// TODO looks like we dont need that at all
	// Reference adds a new reference from a parent node to a child node. This function
	// is used to add reference between internal trie node and external node(e.g. storage
	// trie root), all internal trie nodes are referenced together by database itself.
	// r.ibs.TrieDB().Reference(root, common.Hash{})
}

func (r *RecordingDatabase) dereferenceRoot(root common.Hash) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.references--
	recordingDbReferences.SetUint64(uint64(r.references))

	// todo we do not need at all
	// Dereference removes an existing reference from a root node. It's only
	// supported by hash-based database and will return an error for others.
	// r.ibs.TrieDB().Dereference(root)
}

func (r *RecordingDatabase) addStateVerify(statedb state.IntraBlockStateArbitrum, expected common.Hash, blockNumber uint64) (state.IntraBlockStateArbitrum, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	result, err := r.sd.ComputeCommitment(context.Background(), true, blockNumber, "")
	// result, err := statedb.Commit(blockNumber, true)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(result, expected[:]) {
		return nil, fmt.Errorf("bad root hash expected: %v got: %v", expected, result)
	}
	r.referenceRootLockHeld(common.Hash(result))

	// _, size, _ := r.ibs.TrieDB().Size()
	// limit := common.StorageSize(r.config.TrieDirtyCache) * 1024 * 1024
	// recordingDbSize.SetUint64(uint64(size))
	// if size > limit {
	// 	log.Info("Recording DB: flushing to disk", "size", size, "limit", limit)
	// 	r.ibs.TrieDB().Cap(limit - ethdb.IdealBatchSize)
	// 	_, size, _ = r.ibs.TrieDB().Size()
	// 	recordingDbSize.SetUint64(uint64(size))
	// }
	return statedb, nil
	//return state.New(result, statedb.Database(), nil)
}

func (r *RecordingDatabase) PrepareRecording(ctx context.Context, lastBlockHeader *types.Header, logFunc StateBuildingLogFunction) (state.IntraBlockStateArbitrum, consensus.ChainHeaderReader, *RecordingKV, error) {
	_, err := r.GetOrRecreateState(ctx, lastBlockHeader, logFunc)
	if err != nil {
		return nil, nil, nil, err
	}
	finalDereference := lastBlockHeader // dereference in case of error
	defer func() { r.Dereference(finalDereference) }()
	//recordingKeyValue := newRecordingKV(r.ibs.TrieDB(), r.ibs.DiskDB())
	recordingKeyValue := NewRecordingKV(state.NewReaderV3(r.sd))
	// state.WrapDatabaseWithWasm(wasm kv.RwDB, 0, targets []state.WasmTarget)

	// recordingStateDatabase := state.NewDatabase(rawdb.WrapDatabaseWithWasm(rawdb.NewDatabase(recordingKeyValue), r.ibs.WasmStore(), 0, r.ibs.WasmTargets()))
	// var prevRoot common.Hash
	// if lastBlockHeader != nil {
	// 	prevRoot = lastBlockHeader.Root
	// }
	// recordingStateDb, err := state.New(prevRoot, recordingStateDatabase)
	// if err != nil {
	// 	return nil, nil, nil, fmt.Errorf("failed to create recordingStateDb: %w", err)
	// }
	recordingStateDb := state.NewArbitrum(state.New(recordingKeyValue.state))
	recordingStateDb.StartRecording()
	var recordingChainContext *RecordingChainContext
	if lastBlockHeader != nil {
		if !lastBlockHeader.Number.IsUint64() {
			return nil, nil, nil, errors.New("block number not uint64")
		}
		recordingChainContext = newRecordingChainContext(r.bc.ChainReader(), lastBlockHeader.Number.Uint64())
	}
	finalDereference = nil
	return recordingStateDb, recordingChainContext, recordingKeyValue, nil
}

func (r *RecordingDatabase) PreimagesFromRecording(chainContextIf consensus.ChainHeaderReader, recordingDb *RecordingKV) (map[common.Hash][]byte, error) {
	entries := recordingDb.GetRecordedEntries()
	recordingChainContext, ok := chainContextIf.(*RecordingChainContext)
	if (recordingChainContext == nil) || (!ok) {
		return nil, errors.New("recordingChainContext invalid")
	}

	for i := recordingChainContext.GetMinBlockNumberAccessed(); i <= recordingChainContext.initialBlockNumber; i++ {
		header, err := r.bc.HeaderByNumber(context.Background(), r.tx, i)
		if err != nil {
			return nil, fmt.Errorf("Error reading header %d: %v\n", i, err)
		}
		hash := header.Hash()
		bytes, err := rlp.EncodeToBytes(header)
		if err != nil {
			return nil, fmt.Errorf("Error RLP encoding header: %v\n", err)
		}
		entries[hash] = bytes
	}
	return entries, nil
}

func (r *RecordingDatabase) GetOrRecreateState(ctx context.Context, header *types.Header, logFunc StateBuildingLogFunction) (state.IntraBlockStateArbitrum, error) {
	stateFor := func(header *types.Header) (state.IntraBlockStateArbitrum, StateReleaseFunc, error) {
		s, err := r.StateFor(header)
		// we don't use the release functor pattern here yet
		return state.NewArbitrum(s), NoopStateRelease, err
	}
	state, currentHeader, _, err := FindLastAvailableState(ctx, r.bc, stateFor, header, logFunc, -1)
	if err != nil {
		return nil, err
	}
	if currentHeader == header {
		return state, nil
	}
	lastRoot := currentHeader.Root
	defer func() {
		if (lastRoot != common.Hash{}) {
			r.dereferenceRoot(lastRoot)
		}
	}()
	blockToRecreate := currentHeader.Number.Uint64() + 1
	prevHash := currentHeader.Hash()
	returnedBlockNumber := header.Number.Uint64()
	for ctx.Err() == nil {
		var block *types.Block
		state, block, err = AdvanceStateByBlock(ctx, r.bc, state, header, blockToRecreate, prevHash, logFunc)
		if err != nil {
			return nil, err
		}
		prevHash = block.Hash()
		state, err = r.addStateVerify(state, block.Root(), block.NumberU64())
		if err != nil {
			return nil, fmt.Errorf("failed committing state for block %d : %w", blockToRecreate, err)
		}
		r.dereferenceRoot(lastRoot)
		lastRoot = block.Root()
		if blockToRecreate >= returnedBlockNumber {
			if block.Hash() != header.Hash() {
				return nil, fmt.Errorf("blockHash doesn't match when recreating number: %d expected: %v got: %v", blockToRecreate, header.Hash(), block.Hash())
			}
			// don't dereference this one
			lastRoot = common.Hash{}
			return state, nil
		}
		blockToRecreate++
	}
	return nil, ctx.Err()
}

func (r *RecordingDatabase) ReferenceCount() int64 {
	return r.references
}

func (r *RecordingDatabase) Config() *chain.Config {
	return r.bc.Config()
}
