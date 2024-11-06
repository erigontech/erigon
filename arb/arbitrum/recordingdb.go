package arbitrum

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon/arb/ethdb"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"

	"github.com/erigontech/erigon/rlp"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/hashdb"
)

var (
	recordingDbSize       = metrics.GetOrCreateGauge("arb/validator/recordingdb/size") // note: only updating when adding state, not when removing - but should be good enough
	recordingDbReferences = metrics.GetOrCreateGauge("arb/validator/recordingdb/references")
)

type RecordingKV struct {
	inner         *triedb.Database
	diskDb        ethdb.KeyValueStore
	readDbEntries map[common.Hash][]byte
	enableBypass  bool
}

func newRecordingKV(inner *triedb.Database, diskDb ethdb.KeyValueStore) *RecordingKV {
	return &RecordingKV{inner, diskDb, make(map[common.Hash][]byte), false}
}

func (db *RecordingKV) Has(key []byte) (bool, error) {
	return false, errors.New("recording KV doesn't support Has")
}

func (db *RecordingKV) Get(key []byte) ([]byte, error) {
	var hash common.Hash
	var res []byte
	var err error
	if len(key) == 32 {
		copy(hash[:], key)
		res, err = db.inner.Node(hash)
	} else if len(key) == len(rawdb.CodePrefix)+32 && bytes.HasPrefix(key, rawdb.CodePrefix) {
		// Retrieving code
		copy(hash[:], key[len(rawdb.CodePrefix):])
		res, err = db.diskDb.Get(key)
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
	if db.enableBypass {
		return db.diskDb.NewBatch()
	}
	log.Error("recording KV: attempted to create batch when bypass not enabled")
	return nil
}

func (db *RecordingKV) NewBatchWithSize(size int) ethdb.Batch {
	if db.enableBypass {
		return db.diskDb.NewBatchWithSize(size)
	}
	log.Error("recording KV: attempted to create batch when bypass not enabled")
	return nil
}

func (db *RecordingKV) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	if db.enableBypass {
		return db.diskDb.NewIterator(prefix, start)
	}
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

type RecordingChainContext struct {
	bc                     core.ChainContext
	minBlockNumberAccessed uint64
	initialBlockNumber     uint64
}

func newRecordingChainContext(inner core.ChainContext, blocknumber uint64) *RecordingChainContext {
	return &RecordingChainContext{
		bc:                     inner,
		minBlockNumberAccessed: blocknumber,
		initialBlockNumber:     blocknumber,
	}
}

func (r *RecordingChainContext) Engine() consensus.Engine {
	return r.bc.Engine()
}

func (r *RecordingChainContext) GetHeader(hash common.Hash, num uint64) *types.Header {
	if num < r.minBlockNumberAccessed {
		r.minBlockNumberAccessed = num
	}
	return r.bc.GetHeader(hash, num)
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
	db         state.Database
	bc         *core.BlockChain
	mutex      sync.Mutex // protects StateFor and Dereference
	references int64
}

func NewRecordingDatabase(config *RecordingDatabaseConfig, ethdb ethdb.Database, blockchain *core.BlockChain) *RecordingDatabase {
	hashConfig := *hashdb.Defaults
	hashConfig.CleanCacheSize = config.TrieCleanCache
	trieConfig := triedb.Config{
		Preimages: false,
		HashDB:    &hashConfig,
	}
	return &RecordingDatabase{
		config: config,
		db:     state.NewDatabaseWithConfig(ethdb, &trieConfig),
		bc:     blockchain,
	}
}

// Normal geth state.New + Reference is not atomic vs Dereference. This one is.
// This function does not recreate a state
func (r *RecordingDatabase) StateFor(header *types.Header) (*state.StateDB, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	sdb, err := state.NewDeterministic(header.Root, r.db)
	if err == nil {
		r.referenceRootLockHeld(header.Root)
	}
	return sdb, err
}

func (r *RecordingDatabase) Dereference(header *types.Header) {
	if header != nil {
		r.dereferenceRoot(header.Root)
	}
}

func (r *RecordingDatabase) WriteStateToDatabase(header *types.Header) error {
	if header != nil {
		return r.db.TrieDB().Commit(header.Root, true)
	}
	return nil
}

// lock must be held when calling that
func (r *RecordingDatabase) referenceRootLockHeld(root common.Hash) {
	r.references++
	recordingDbReferences.Update(r.references)
	r.db.TrieDB().Reference(root, common.Hash{})
}

func (r *RecordingDatabase) dereferenceRoot(root common.Hash) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.references--
	recordingDbReferences.Update(r.references)
	r.db.TrieDB().Dereference(root)
}

func (r *RecordingDatabase) addStateVerify(statedb *state.StateDB, expected common.Hash, blockNumber uint64) (*state.StateDB, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	result, err := statedb.Commit(blockNumber, true)
	if err != nil {
		return nil, err
	}
	if result != expected {
		return nil, fmt.Errorf("bad root hash expected: %v got: %v", expected, result)
	}
	r.referenceRootLockHeld(result)

	_, size, _ := r.db.TrieDB().Size()
	limit := common.StorageSize(r.config.TrieDirtyCache) * 1024 * 1024
	recordingDbSize.Update(int64(size))
	if size > limit {
		log.Info("Recording DB: flushing to disk", "size", size, "limit", limit)
		r.db.TrieDB().Cap(limit - ethdb.IdealBatchSize)
		_, size, _ = r.db.TrieDB().Size()
		recordingDbSize.Update(int64(size))
	}
	return state.New(result, statedb.Database(), nil)
}

func (r *RecordingDatabase) PrepareRecording(ctx context.Context, lastBlockHeader *types.Header, logFunc StateBuildingLogFunction) (*state.StateDB, core.ChainContext, *RecordingKV, error) {
	_, err := r.GetOrRecreateState(ctx, lastBlockHeader, logFunc)
	if err != nil {
		return nil, nil, nil, err
	}
	finalDereference := lastBlockHeader // dereference in case of error
	defer func() { r.Dereference(finalDereference) }()
	recordingKeyValue := newRecordingKV(r.db.TrieDB(), r.db.DiskDB())

	recordingStateDatabase := state.NewDatabase(rawdb.WrapDatabaseWithWasm(rawdb.NewDatabase(recordingKeyValue), r.db.WasmStore(), 0, r.db.WasmTargets()))
	var prevRoot common.Hash
	if lastBlockHeader != nil {
		prevRoot = lastBlockHeader.Root
	}
	recordingStateDb, err := state.NewDeterministic(prevRoot, recordingStateDatabase)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create recordingStateDb: %w", err)
	}
	recordingStateDb.StartRecording()
	var recordingChainContext *RecordingChainContext
	if lastBlockHeader != nil {
		if !lastBlockHeader.Number.IsUint64() {
			return nil, nil, nil, errors.New("block number not uint64")
		}
		recordingChainContext = newRecordingChainContext(r.bc, lastBlockHeader.Number.Uint64())
	}
	finalDereference = nil
	return recordingStateDb, recordingChainContext, recordingKeyValue, nil
}

func (r *RecordingDatabase) PreimagesFromRecording(chainContextIf core.ChainContext, recordingDb *RecordingKV) (map[common.Hash][]byte, error) {
	entries := recordingDb.GetRecordedEntries()
	recordingChainContext, ok := chainContextIf.(*RecordingChainContext)
	if (recordingChainContext == nil) || (!ok) {
		return nil, errors.New("recordingChainContext invalid")
	}

	for i := recordingChainContext.GetMinBlockNumberAccessed(); i <= recordingChainContext.initialBlockNumber; i++ {
		header := r.bc.GetHeaderByNumber(i)
		hash := header.Hash()
		bytes, err := rlp.EncodeToBytes(header)
		if err != nil {
			return nil, fmt.Errorf("Error RLP encoding header: %v\n", err)
		}
		entries[hash] = bytes
	}
	return entries, nil
}

func (r *RecordingDatabase) GetOrRecreateState(ctx context.Context, header *types.Header, logFunc StateBuildingLogFunction) (*state.StateDB, error) {
	stateFor := func(header *types.Header) (*state.StateDB, StateReleaseFunc, error) {
		state, err := r.StateFor(header)
		// we don't use the release functor pattern here yet
		return state, NoopStateRelease, err
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
