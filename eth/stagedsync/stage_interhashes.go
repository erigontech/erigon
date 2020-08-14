package stagedsync

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/trie"
)

func SpawnIntermediateHashesStage(s *StageState, db ethdb.Database, datadir string, quit <-chan struct{}) error {
	to, err := s.ExecutionAt(db)
	if err != nil {
		return err
	}

	if s.BlockNumber == to {
		// we already did hash check for this block
		// we don't do the obvious `if s.BlockNumber > to` to support reorgs more naturally
		s.Done()
		return nil
	}

	hash := rawdb.ReadCanonicalHash(db, to)
	syncHeadHeader := rawdb.ReadHeader(db, hash, to)
	expectedRootHash := syncHeadHeader.Root

	log.Info("Generating intermediate hashes", "from", s.BlockNumber, "to", to)
	if s.BlockNumber == 0 {
		if err := regenerateIntermediateHashes(db, datadir, expectedRootHash, quit); err != nil {
			return err
		}
	} else {
		if err := incrementIntermediateHashes(s, db, to, datadir, expectedRootHash, quit); err != nil {
			return err
		}
	}
	return s.DoneAndUpdate(db, to)
}

func regenerateIntermediateHashes(db ethdb.Database, datadir string, expectedRootHash common.Hash, quit <-chan struct{}) error {
	log.Info("Regeneration intermediate hashes started")
	collector := etl.NewCollector(datadir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	hashCollector := func(keyHex []byte, hash []byte) error {
		if len(keyHex)%2 != 0 || len(keyHex) == 0 {
			return nil
		}
		k := make([]byte, len(keyHex)/2)
		trie.CompressNibbles(keyHex, &k)
		return collector.Collect(k, common.CopyBytes(hash))
	}
	loader := trie.NewFlatDbSubTrieLoader()
	if err := loader.Reset(db, trie.NewRetainList(0), trie.NewRetainList(0), hashCollector /* HashCollector */, [][]byte{nil}, []int{0}, false); err != nil {
		return err
	}
	t := time.Now()
	if subTries, err := loader.LoadSubTries(); err == nil {
		generationIHTook := time.Since(t)
		if subTries.Hashes[0] != expectedRootHash {
			return fmt.Errorf("wrong trie root: %x, expected (from header): %x", subTries.Hashes[0], expectedRootHash)
		}
		log.Debug("Collection finished",
			"root hash", subTries.Hashes[0].Hex(),
			"gen IH", generationIHTook,
		)
	} else {
		return err
	}
	if err := collector.Load(db, dbutils.IntermediateTrieHashBucket, etl.IdentityLoadFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return fmt.Errorf("gen ih stage: fail load data to bucket: %d", err)
	}
	log.Info("Regeneration ended")
	return nil
}

type Receiver struct {
	defaultReceiver       *trie.DefaultReceiver
	accountMap            map[string]*accounts.Account
	storageMap            map[string][]byte
	removingAccount       []byte
	currentAccountWithInc []byte
	unfurlList            []string
	currentIdx            int
	quitCh                <-chan struct{}
}

func NewReceiver(quitCh <-chan struct{}) *Receiver {
	return &Receiver{
		defaultReceiver: trie.NewDefaultReceiver(),
		accountMap:      make(map[string]*accounts.Account),
		storageMap:      make(map[string][]byte),
		quitCh:          quitCh,
	}
}

func (r *Receiver) Receive(
	itemType trie.StreamItem,
	accountKey []byte,
	storageKey []byte,
	accountValue *accounts.Account,
	storageValue []byte,
	hash []byte,
	cutoff int,
) error {
	if err := common.Stopped(r.quitCh); err != nil {
		return err
	}

	storage := itemType == trie.StorageStreamItem || itemType == trie.SHashStreamItem
	for r.currentIdx < len(r.unfurlList) {
		ks := r.unfurlList[r.currentIdx]
		k := []byte(ks)
		var c int
		switch itemType {
		case trie.StorageStreamItem, trie.SHashStreamItem:
			c = bytes.Compare(k, storageKey)
		case trie.AccountStreamItem, trie.AHashStreamItem:
			c = bytes.Compare(k, accountKey)
		case trie.CutoffStreamItem:
			c = -1
		}
		if c > 0 {
			if storage {
				if r.removingAccount != nil && bytes.HasPrefix(storageKey, r.removingAccount) {
					return nil
				}
				if r.currentAccountWithInc == nil {
					return nil
				}
				if !bytes.HasPrefix(storageKey, r.currentAccountWithInc) {
					return nil
				}
			}
			if itemType == trie.AccountStreamItem {
				r.currentAccountWithInc = dbutils.GenerateStoragePrefix(accountKey, accountValue.Incarnation)
			}
			return r.defaultReceiver.Receive(itemType, accountKey, storageKey, accountValue, storageValue, hash, cutoff)
		}
		r.currentIdx++
		if len(k) > common.HashLength {
			if r.removingAccount != nil && bytes.HasPrefix(k, r.removingAccount) {
				continue
			}
			if r.currentAccountWithInc == nil {
				continue
			}
			if !bytes.HasPrefix(k, r.currentAccountWithInc) {
				continue
			}
			v := r.storageMap[ks]
			if len(v) > 0 {
				if err := r.defaultReceiver.Receive(trie.StorageStreamItem, nil, k, nil, v, nil, 0); err != nil {
					return err
				}
			}
		} else {
			v := r.accountMap[ks]
			if v != nil {
				if err := r.defaultReceiver.Receive(trie.AccountStreamItem, k, nil, v, nil, nil, 0); err != nil {
					return err
				}
				r.removingAccount = nil
				r.currentAccountWithInc = dbutils.GenerateStoragePrefix(k, v.Incarnation)
			} else {
				r.removingAccount = k
				r.currentAccountWithInc = nil
			}
		}
		if c == 0 {
			return nil
		}
	}
	// We ran out of modifications, simply pass through
	if storage {
		if r.removingAccount != nil && bytes.HasPrefix(storageKey, r.removingAccount) {
			return nil
		}
		if r.currentAccountWithInc == nil {
			return nil
		}
		if !bytes.HasPrefix(storageKey, r.currentAccountWithInc) {
			return nil
		}
	}
	r.removingAccount = nil
	if itemType == trie.AccountStreamItem {
		r.currentAccountWithInc = dbutils.GenerateStoragePrefix(accountKey, accountValue.Incarnation)
	}
	return r.defaultReceiver.Receive(itemType, accountKey, storageKey, accountValue, storageValue, hash, cutoff)
}

func (r *Receiver) Result() trie.SubTries {
	return r.defaultReceiver.Result()
}

func (r *Receiver) accountLoad(k []byte, value []byte, _ etl.State, _ etl.LoadNextFunc) error {
	newK, err := transformPlainStateKey(k)
	if err != nil {
		return err
	}
	newKStr := string(newK)
	if len(value) > 0 {
		var a accounts.Account
		if err = a.DecodeForStorage(value); err != nil {
			return err
		}
		r.accountMap[newKStr] = &a
	} else {
		delete(r.accountMap, newKStr)
	}
	r.unfurlList = append(r.unfurlList, newKStr)
	return nil
}

func (r *Receiver) storageLoad(k []byte, value []byte, _ etl.State, _ etl.LoadNextFunc) error {
	newK, err := transformPlainStateKey(k)
	if err != nil {
		return err
	}
	newKStr := string(newK)
	if len(value) > 0 {
		r.storageMap[newKStr] = common.CopyBytes(value)
	} else {
		delete(r.storageMap, newKStr)
	}
	r.unfurlList = append(r.unfurlList, newKStr)
	return nil
}

type HashPromoter struct {
	db               ethdb.Database
	ChangeSetBufSize uint64
	TempDir          string
	quitCh           <-chan struct{}
}

func NewHashPromoter(db ethdb.Database, quitCh <-chan struct{}) *HashPromoter {
	return &HashPromoter{
		db:               db,
		ChangeSetBufSize: 256 * 1024 * 1024,
		TempDir:          os.TempDir(),
		quitCh:           quitCh,
	}
}

func (p *HashPromoter) Promote(s *StageState, from, to uint64, storage bool, r *Receiver) error {
	var changeSetBucket string
	if storage {
		changeSetBucket = dbutils.PlainStorageChangeSetBucket
	} else {
		changeSetBucket = dbutils.PlainAccountChangeSetBucket
	}
	log.Debug("Incremental state promotion of intermediate hashes", "from", from, "to", to, "csbucket", string(changeSetBucket))

	// Can't skip stage even if it was done before interruptoin because need to fill non-persistent data structure: Receiver

	startkey := dbutils.EncodeTimestamp(from + 1)

	var l OldestAppearedLoad
	if storage {
		l.innerLoadFunc = r.storageLoad
	} else {
		l.innerLoadFunc = r.accountLoad
	}
	if err := etl.Transform(
		p.db,
		changeSetBucket,
		"",
		p.TempDir,
		getExtractFunc2(changeSetBucket),
		// here we avoid getting the state from changesets,
		// we just care about the accounts that did change,
		// so we can directly read from the PlainTextBuffer
		getFromPlainStateAndLoad2(p.db, l.LoadFunc),
		etl.TransformArgs{
			BufferType:      etl.SortableOldestAppearedBuffer,
			ExtractStartKey: startkey,
			Quit:            p.quitCh,
		},
	); err != nil {
		return err
	}
	return nil
}

func (p *HashPromoter) Unwind(s *StageState, u *UnwindState, storage bool, r *Receiver) error {
	to := u.UnwindPoint
	var changeSetBucket string
	if storage {
		changeSetBucket = dbutils.PlainStorageChangeSetBucket
	} else {
		changeSetBucket = dbutils.PlainAccountChangeSetBucket
	}
	log.Info("Unwinding of intermediate hashes", "from", s.BlockNumber, "to", to, "csbucket", changeSetBucket)

	startkey := dbutils.EncodeTimestamp(to + 1)

	// Can't skip stage even if it was done before interruptoin because need to fill non-persistent data structure: Receiver

	var l OldestAppearedLoad
	if storage {
		l.innerLoadFunc = r.storageLoad
	} else {
		l.innerLoadFunc = r.accountLoad
	}
	if err := etl.Transform(
		p.db,
		changeSetBucket,
		"",
		p.TempDir,
		getUnwindExtractFunc(changeSetBucket),
		l.LoadFunc,
		etl.TransformArgs{
			BufferType:      etl.SortableOldestAppearedBuffer,
			ExtractStartKey: startkey,
			Quit:            p.quitCh,
		},
	); err != nil {
		return err
	}
	return nil
}

func incrementIntermediateHashes(s *StageState, db ethdb.Database, to uint64, datadir string, expectedRootHash common.Hash, quit <-chan struct{}) error {
	p := NewHashPromoter(db, quit)
	p.TempDir = datadir
	r := NewReceiver(quit)
	if err := p.Promote(s, s.BlockNumber, to, false /* storage */, r); err != nil {
		return err
	}
	if err := p.Promote(s, s.BlockNumber, to, true /* storage */, r); err != nil {
		return err
	}
	for ks, acc := range r.accountMap {
		// Fill the code hashes
		if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
			if codeHash, err := db.Get(dbutils.ContractCodeBucket, dbutils.GenerateStoragePrefix([]byte(ks), acc.Incarnation)); err == nil {
				copy(acc.CodeHash[:], codeHash)
			} else if !errors.Is(err, ethdb.ErrKeyNotFound) {
				return fmt.Errorf("adjusting codeHash for ks %x, inc %d: %w", ks, acc.Incarnation, err)
			}
		}
	}
	unfurl := trie.NewRetainList(0)
	sort.Strings(r.unfurlList)
	for _, ks := range r.unfurlList {
		unfurl.AddKey([]byte(ks))
	}
	collector := etl.NewCollector(datadir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	hashCollector := func(keyHex []byte, hash []byte) error {
		if len(keyHex)%2 != 0 || len(keyHex) == 0 {
			return nil
		}
		k := make([]byte, len(keyHex)/2)
		trie.CompressNibbles(keyHex, &k)
		return collector.Collect(k, common.CopyBytes(hash))
	}
	loader := trie.NewFlatDbSubTrieLoader()
	// hashCollector in the line below will collect deletes
	if err := loader.Reset(db, unfurl, trie.NewRetainList(0), hashCollector, [][]byte{nil}, []int{0}, false); err != nil {
		return err
	}
	// hashCollector in the line below will collect creations of new intermediate hashes
	r.defaultReceiver.Reset(trie.NewRetainList(0), hashCollector, false)
	//loader.SetStreamReceiver(r)
	t := time.Now()
	subTries, err := loader.LoadSubTries()
	if err != nil {
		return err
	}
	generationIHTook := time.Since(t)
	if subTries.Hashes[0] != expectedRootHash {
		return fmt.Errorf("wrong trie root: %x, expected (from header): %x", subTries.Hashes[0], expectedRootHash)
	}
	log.Info("Collection finished",
		"root hash", subTries.Hashes[0].Hex(),
		"gen IH", generationIHTook,
	)

	if err := collector.Load(db, dbutils.IntermediateTrieHashBucket, etl.IdentityLoadFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return err
	}
	return nil
}

func UnwindIntermediateHashesStage(u *UnwindState, s *StageState, db ethdb.Database, datadir string, quit <-chan struct{}) error {
	hash := rawdb.ReadCanonicalHash(db, u.UnwindPoint)
	syncHeadHeader := rawdb.ReadHeader(db, hash, u.UnwindPoint)
	expectedRootHash := syncHeadHeader.Root

	if err := unwindIntermediateHashesStageImpl(u, s, db, datadir, expectedRootHash, quit); err != nil {
		return err
	}
	if err := u.Done(db); err != nil {
		return fmt.Errorf("unwind IntermediateHashes: reset: %w", err)
	}
	return nil
}

func unwindIntermediateHashesStageImpl(u *UnwindState, s *StageState, db ethdb.Database, datadir string, expectedRootHash common.Hash, quit <-chan struct{}) error {
	p := NewHashPromoter(db, quit)
	p.TempDir = datadir
	r := NewReceiver(quit)
	if err := p.Unwind(s, u, false /* storage */, r); err != nil {
		return err
	}
	if err := p.Unwind(s, u, true /* storage */, r); err != nil {
		return err
	}
	for ks, acc := range r.accountMap {
		// Fill the code hashes
		if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
			if codeHash, err := db.Get(dbutils.ContractCodeBucket, dbutils.GenerateStoragePrefix([]byte(ks), acc.Incarnation)); err == nil {
				copy(acc.CodeHash[:], codeHash)
			} else if errors.Is(err, ethdb.ErrKeyNotFound) {
				copy(acc.CodeHash[:], trie.EmptyCodeHash[:])
			} else {
				return fmt.Errorf("adjusting codeHash for ks %x, inc %d: %w", ks, acc.Incarnation, err)
			}
		}
	}
	sort.Strings(r.unfurlList)
	unfurl := trie.NewRetainList(0)
	for _, ks := range r.unfurlList {
		unfurl.AddKey([]byte(ks))
	}
	collector := etl.NewCollector(datadir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	hashCollector := func(keyHex []byte, hash []byte) error {
		if len(keyHex)%2 != 0 || len(keyHex) == 0 {
			return nil
		}
		k := make([]byte, len(keyHex)/2)
		trie.CompressNibbles(keyHex, &k)
		return collector.Collect(k, common.CopyBytes(hash))
	}
	loader := trie.NewFlatDbSubTrieLoader()
	// hashCollector in the line below will collect deletes
	if err := loader.Reset(db, unfurl, trie.NewRetainList(0), hashCollector, [][]byte{nil}, []int{0}, false); err != nil {
		return err
	}
	// hashCollector in the line below will collect creations of new intermediate hashes
	r.defaultReceiver.Reset(trie.NewRetainList(0), hashCollector, false)
	loader.SetStreamReceiver(r)
	t := time.Now()
	subTries, err := loader.LoadSubTries()
	if err != nil {
		return err
	}
	generationIHTook := time.Since(t)
	if subTries.Hashes[0] != expectedRootHash {
		return fmt.Errorf("wrong trie root: %x, expected (from header): %x", subTries.Hashes[0], expectedRootHash)
	}
	log.Info("Collection finished",
		"root hash", subTries.Hashes[0].Hex(),
		"gen IH", generationIHTook,
	)
	if err := collector.Load(db, dbutils.IntermediateTrieHashBucket, etl.IdentityLoadFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return err
	}
	return nil
}

func ResetHashState(db ethdb.Database) error {
	if err := db.(ethdb.NonTransactional).ClearBuckets(
		dbutils.CurrentStateBucket,
		dbutils.ContractCodeBucket,
		dbutils.IntermediateTrieHashBucket,
	); err != nil {
		return err
	}
	batch := db.NewBatch()
	if err := stages.SaveStageProgress(batch, stages.IntermediateHashes, 0, nil); err != nil {
		return err
	}
	if err := stages.SaveStageUnwind(batch, stages.IntermediateHashes, 0, nil); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(batch, stages.HashState, 0, nil); err != nil {
		return err
	}
	if err := stages.SaveStageUnwind(batch, stages.HashState, 0, nil); err != nil {
		return err
	}
	if _, err := batch.Commit(); err != nil {
		return err
	}

	return nil
}
