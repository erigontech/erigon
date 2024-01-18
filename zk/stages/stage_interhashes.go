package stages

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/state"
	state2 "github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	db2 "github.com/ledgerwatch/erigon/smt/pkg/db"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"github.com/ledgerwatch/log/v3"

	"strings"

	"context"
	"math/big"
	"time"

	"bytes"
	"encoding/json"
	"io"
	"net/http"

	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/ledgerwatch/erigon/zk"
	"github.com/status-im/keycard-go/hexutils"
)

type ZkInterHashesCfg struct {
	db                kv.RwDB
	checkRoot         bool
	badBlockHalt      bool
	tmpDir            string
	saveNewHashesToDB bool // no reason to save changes when calculating root for mining
	blockReader       services.FullBlockReader
	hd                *headerdownload.HeaderDownload

	historyV3 bool
	agg       *state.AggregatorV3
	zk        *ethconfig.Zk
}

func StageZkInterHashesCfg(db kv.RwDB, checkRoot, saveNewHashesToDB, badBlockHalt bool, tmpDir string, blockReader services.FullBlockReader, hd *headerdownload.HeaderDownload, historyV3 bool, agg *state.AggregatorV3, zk *ethconfig.Zk) ZkInterHashesCfg {
	return ZkInterHashesCfg{
		db:                db,
		checkRoot:         checkRoot,
		tmpDir:            tmpDir,
		saveNewHashesToDB: saveNewHashesToDB,
		badBlockHalt:      badBlockHalt,
		blockReader:       blockReader,
		hd:                hd,

		historyV3: historyV3,
		agg:       agg,
		zk:        zk,
	}
}

func SpawnZkIntermediateHashesStage(s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, cfg ZkInterHashesCfg, ctx context.Context, quiet bool) (libcommon.Hash, error) {
	logPrefix := s.LogPrefix()

	quit := ctx.Done()
	_ = quit

	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return trie.EmptyRoot, err
		}
		defer tx.Rollback()
	}

	to, err := s.ExecutionAt(tx)
	if err != nil {
		return trie.EmptyRoot, err
	}

	if s.BlockNumber == to {
		// we already did hash check for this block
		// we don't do the obvious `if s.BlockNumber > to` to support reorgs more naturally
		return trie.EmptyRoot, nil
	}

	var expectedRootHash libcommon.Hash
	var headerHash libcommon.Hash
	var syncHeadHeader *types.Header
	if cfg.checkRoot {
		syncHeadHeader, err = cfg.blockReader.HeaderByNumber(ctx, tx, to)
		if err != nil {
			return trie.EmptyRoot, err
		}
		if syncHeadHeader == nil {
			return trie.EmptyRoot, fmt.Errorf("no header found with number %d", to)
		}
		expectedRootHash = syncHeadHeader.Root
		headerHash = syncHeadHeader.Hash()
	}
	if !quiet && to > s.BlockNumber+16 {
		log.Info(fmt.Sprintf("[%s] Generating intermediate hashes", logPrefix), "from", s.BlockNumber, "to", to)
	}

	var root libcommon.Hash

	shouldRegenerate := to > s.BlockNumber && to-s.BlockNumber > cfg.zk.RebuildTreeAfter

	eridb := db2.NewEriDb(tx)
	smt := smt.NewSMT(eridb)

	if s.BlockNumber == 0 || shouldRegenerate {
		if root, err = regenerateIntermediateHashes(logPrefix, tx, eridb, smt, cfg, &expectedRootHash, ctx, quit); err != nil {
			return trie.EmptyRoot, err
		}
	} else {
		// increment to latest executed block
		incrementTo, err := stages.GetStageProgress(tx, stages.Execution)
		if err != nil {
			return trie.EmptyRoot, err
		}

		syncHeadHeader, err = cfg.blockReader.HeaderByNumber(ctx, tx, incrementTo)
		if err != nil {
			return trie.EmptyRoot, err
		}
		if syncHeadHeader == nil {
			return trie.EmptyRoot, fmt.Errorf("no header found with number %d", to)
		}
		expectedRootHash = syncHeadHeader.Root
		headerHash = syncHeadHeader.Hash()
		if root, err = zkIncrementIntermediateHashes(logPrefix, s, tx, eridb, smt, incrementTo, cfg, &expectedRootHash, quit); err != nil {
			return trie.EmptyRoot, err
		}
	}

	log.Info(fmt.Sprintf("[%s] Trie root", logPrefix), "hash", root.Hex())

	hashErr := verifyStateRoot(smt, &expectedRootHash, &cfg, logPrefix, to, tx)
	if hashErr != nil {
		panic(fmt.Errorf("state root mismatch (checking state and RPC): %w, %s", hashErr, root.Hex()))
	}

	if cfg.checkRoot && root != expectedRootHash {
		log.Error(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", logPrefix, to, root, expectedRootHash, headerHash))
		if cfg.badBlockHalt {
			return trie.EmptyRoot, fmt.Errorf("wrong trie root")
		}
		if cfg.hd != nil {
			cfg.hd.ReportBadHeaderPoS(headerHash, syncHeadHeader.ParentHash)
		}
		if to > s.BlockNumber {
			//unwindTo := (to + s.BlockNumber) / 2 // Binary search for the correct block, biased to the lower numbers
			//log.Warn("Unwinding due to incorrect root hash", "to", unwindTo)
			//u.UnwindTo(unwindTo, headerHash)
		}
	} else if err = s.Update(tx, to); err != nil {
		return trie.EmptyRoot, err
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return trie.EmptyRoot, err
		}
	}

	return root, err
}

func UnwindZkIntermediateHashesStage(u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, cfg ZkInterHashesCfg, ctx context.Context) (err error) {
	quit := ctx.Done()
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	syncHeadHeader, err := cfg.blockReader.HeaderByNumber(ctx, tx, u.UnwindPoint)
	if err != nil {
		return err
	}
	if syncHeadHeader == nil {
		return fmt.Errorf("header not found for block number %d", u.UnwindPoint)
	}
	expectedRootHash := syncHeadHeader.Root

	root, err := unwindZkSMT(s.LogPrefix(), s.BlockNumber, u.UnwindPoint, tx, cfg, &expectedRootHash, quit)
	if err != nil {
		return err
	}
	_ = root

	if err := u.Done(tx); err != nil {
		return err
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func regenerateIntermediateHashes(logPrefix string, db kv.RwTx, eridb *db2.EriDb, smtIn *smt.SMT, cfg ZkInterHashesCfg, expectedRootHash *libcommon.Hash, ctx context.Context, quitCh <-chan struct{}) (libcommon.Hash, error) {
	log.Info(fmt.Sprintf("[%s] Regeneration trie hashes started", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Regeneration ended", logPrefix))

	if err := stages.SaveStageProgress(db, stages.IntermediateHashes, 0); err != nil {
		log.Warn(fmt.Sprint("regenerate SaveStageProgress to zero error: ", err))
	}

	var a *accounts.Account
	var addr libcommon.Address
	var as map[string]string
	var inc uint64

	psr := state2.NewPlainStateReader(db)

	log.Info(fmt.Sprintf("[%s] Collecting account data...", logPrefix))
	dataCollectStartTime := time.Now()
	keys := []utils.NodeKey{}

	// get total accounts count for progress printer
	total := uint64(0)
	if err := psr.ForEach(kv.PlainState, nil, func(k, acc []byte) error {
		total++
		return nil
	}); err != nil {
		return trie.EmptyRoot, err
	}

	progressChan, stopProgressPrinter := zk.ProgressPrinterWithoutValues(fmt.Sprintf("[%s] SMT regenerate progress", logPrefix), total*2)

	progCt := uint64(0)
	err := psr.ForEach(kv.PlainState, nil, func(k, acc []byte) error {
		progCt++
		progressChan <- progCt
		var err error
		if len(k) == 20 {
			if a != nil { // don't run process on first loop for first account (or it will miss collecting storage)
				keys, err = processAccount(eridb, a, as, inc, psr, addr, keys)
				if err != nil {
					return err
				}
			}

			a = &accounts.Account{}

			if err := a.DecodeForStorage(acc); err != nil {
				// TODO: not an account?
				as = make(map[string]string)
				return nil
			}
			addr = libcommon.BytesToAddress(k)
			inc = a.Incarnation
			// empty storage of previous account
			as = make(map[string]string)
		} else { // otherwise we're reading storage
			_, incarnation, key := dbutils.PlainParseCompositeStorageKey(k)
			if incarnation != inc {
				return nil
			}

			sk := fmt.Sprintf("0x%032x", key)
			v := fmt.Sprintf("0x%032x", acc)

			as[sk] = fmt.Sprint(TrimHexString(v))
		}
		return nil
	})

	stopProgressPrinter()

	if err != nil {
		return trie.EmptyRoot, err
	}

	// process the final account
	keys, err = processAccount(eridb, a, as, inc, psr, addr, keys)
	if err != nil {
		return trie.EmptyRoot, err
	}

	dataCollectTime := time.Since(dataCollectStartTime)
	log.Info(fmt.Sprintf("[%s] Collecting account data finished in %v", logPrefix, dataCollectTime))

	// generate tree
	if _, err := smtIn.GenerateFromKVBulk(logPrefix, keys); err != nil {
		return trie.EmptyRoot, err
	}

	err2 := db.ClearBucket("HermezSmtAccountValues")
	if err2 != nil {
		log.Warn(fmt.Sprint("regenerate SaveStageProgress to zero error: ", err2))
	}

	root := smtIn.LastRoot()
	err = eridb.CommitBatch()
	if err != nil {
		return trie.EmptyRoot, err
	}

	return libcommon.BigToHash(root), nil
}

func zkIncrementIntermediateHashes(logPrefix string, s *stagedsync.StageState, db kv.RwTx, eridb *db2.EriDb, dbSmt *smt.SMT, to uint64, cfg ZkInterHashesCfg, expectedRootHash *libcommon.Hash, quit <-chan struct{}) (libcommon.Hash, error) {
	log.Info(fmt.Sprintf("[%s] Increment trie hashes started", logPrefix), "previousRootHeight", s.BlockNumber, "calculatingRootHeight", to)
	defer log.Info(fmt.Sprintf("[%s] Increment ended", logPrefix))

	eridb.OpenBatch(quit)

	ac, err := db.CursorDupSort(kv.AccountChangeSet)
	if err != nil {
		return trie.EmptyRoot, err
	}
	defer ac.Close()

	sc, err := db.CursorDupSort(kv.StorageChangeSet)
	if err != nil {
		return trie.EmptyRoot, err
	}
	defer sc.Close()

	// progress printer
	accChanges := make(map[libcommon.Address]*accounts.Account)
	codeChanges := make(map[libcommon.Address]string)
	storageChanges := make(map[libcommon.Address]map[string]string)

	// NB: changeset tables are zero indexed
	// changeset tables contain historical value at N-1, so we look up values from plainstate
	for i := s.BlockNumber + 1; i <= to; i++ {
		dupSortKey := dbutils.EncodeBlockNumber(i)

		// i+1 to get state at the beginning of the next batch
		psr := state2.NewPlainState(db, i+1, systemcontracts.SystemContractCodeLookup["Hermez"])

		// collect changes to accounts and code
		for _, v, err := ac.SeekExact(dupSortKey); err == nil && v != nil; _, v, err = ac.NextDup() {
			addr := libcommon.BytesToAddress(v[:length.Addr])

			currAcc, err := psr.ReadAccountData(addr)
			if err != nil {
				return trie.EmptyRoot, err
			}

			// store the account
			accChanges[addr] = currAcc

			cc, err := psr.ReadAccountCode(addr, currAcc.Incarnation, currAcc.CodeHash)
			if err != nil {
				return trie.EmptyRoot, err
			}

			ach := hexutils.BytesToHex(cc)
			if len(ach) > 0 {
				hexcc := fmt.Sprintf("0x%s", ach)
				codeChanges[addr] = hexcc
				if err != nil {
					return trie.EmptyRoot, err
				}
			}
		}

		err = db.ForPrefix(kv.StorageChangeSet, dupSortKey, func(sk, sv []byte) error {
			changesetKey := sk[length.BlockNum:]
			address, incarnation := dbutils.PlainParseStoragePrefix(changesetKey)

			sstorageKey := sv[:length.Hash]
			stk := libcommon.BytesToHash(sstorageKey)

			value, err := psr.ReadAccountStorage(address, incarnation, &stk)
			if err != nil {
				return err
			}

			stkk := fmt.Sprintf("0x%032x", stk)
			v := fmt.Sprintf("0x%032x", libcommon.BytesToHash(value))

			m := make(map[string]string)
			m[stkk] = v

			if storageChanges[address] == nil {
				storageChanges[address] = make(map[string]string)
			}
			storageChanges[address][stkk] = v
			return nil
		})
		if err != nil {
			return trie.EmptyRoot, err
		}
	}

	total := len(accChanges) + len(codeChanges) + len(storageChanges)

	progressChan, stopProgressPrinter := zk.ProgressPrinter(fmt.Sprintf("[%s] Progress inserting values", logPrefix), uint64(total))

	// update the tree
	for addr, acc := range accChanges {
		if err := updateAccInTree(dbSmt, addr, acc); err != nil {
			stopProgressPrinter()
			return trie.EmptyRoot, err
		}
		progressChan <- 1
	}

	for addr, code := range codeChanges {
		if err := dbSmt.SetContractBytecode(addr.String(), code); err != nil {
			stopProgressPrinter()
			return trie.EmptyRoot, err
		}
		progressChan <- 1
	}

	for addr, storage := range storageChanges {
		if _, err := dbSmt.SetContractStorage(addr.String(), storage); err != nil {
			stopProgressPrinter()
			return trie.EmptyRoot, err
		}
		progressChan <- 1
	}
	stopProgressPrinter()

	log.Info(fmt.Sprintf("[%s] Regeneration trie hashes finished. Commiting batch", logPrefix))

	if err := verifyLastHash(dbSmt, expectedRootHash, &cfg, logPrefix); err != nil {
		eridb.RollbackBatch()
		log.Error("failed to verify hash")
		return trie.EmptyRoot, err
	}

	if err := eridb.CommitBatch(); err != nil {
		return trie.EmptyRoot, err
	}

	lr := dbSmt.LastRoot()

	hash := libcommon.BigToHash(lr)
	return hash, nil
}

func unwindZkSMT(logPrefix string, from, to uint64, db kv.RwTx, cfg ZkInterHashesCfg, expectedRootHash *libcommon.Hash, quit <-chan struct{}) (libcommon.Hash, error) {
	log.Info(fmt.Sprintf("[%s] Unwind trie hashes started", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Unwind ended", logPrefix))

	eridb := db2.NewEriDb(db)
	dbSmt := smt.NewSMT(eridb)

	log.Info(fmt.Sprintf("[%s]", logPrefix), "last root", libcommon.BigToHash(dbSmt.LastRoot()))

	eridb.OpenBatch(quit)

	ac, err := db.CursorDupSort(kv.AccountChangeSet)
	if err != nil {
		return trie.EmptyRoot, err
	}
	defer ac.Close()

	sc, err := db.CursorDupSort(kv.StorageChangeSet)
	if err != nil {
		return trie.EmptyRoot, err
	}
	defer sc.Close()

	currentPsr := state2.NewPlainStateReader(db)

	total := from - to + 1
	progressChan, stopPrinter := zk.ProgressPrinter(fmt.Sprintf("[%s] Progress unwinding", logPrefix), total)
	defer stopPrinter()

	// walk backwards through the blocks, applying state changes, and deletes
	// PlainState contains data AT the block
	// History tables contain data BEFORE the block - so need a +1 offset
	accDeletes := make([]libcommon.Address, 0)

	for i := from; i >= to+1; i-- {

		accChanges := make(map[libcommon.Address]*accounts.Account)
		codeChanges := make(map[libcommon.Address]string)
		storageChanges := make(map[libcommon.Address]map[string]string)
		dupSortKey := dbutils.EncodeBlockNumber(i)

		// collect changes to accounts and code
		for _, v, err2 := ac.SeekExact(dupSortKey); err2 == nil && v != nil; _, v, err2 = ac.NextDup() {
			addr := libcommon.BytesToAddress(v[:length.Addr])

			// if the account was created in this changeset we should delete it
			if len(v[length.Addr:]) == 0 {
				codeChanges[addr] = ""
				accDeletes = append(accDeletes, addr)
				continue
			}

			// decode the old acc from the changeset
			oldAcc := new(accounts.Account)
			if err := oldAcc.DecodeForStorage(v[length.Addr:]); err != nil {
				return trie.EmptyRoot, err
			}

			// currAcc at block we're unwinding from
			currAcc, err := currentPsr.ReadAccountData(addr)
			if err != nil {
				return trie.EmptyRoot, err
			}

			if oldAcc.Incarnation > 0 {
				if len(v) == 0 { // self-destructed
					accDeletes = append(accDeletes, addr)
				} else {
					if currAcc.Incarnation > oldAcc.Incarnation {
						accDeletes = append(accDeletes, addr)
					}
				}
			}

			// store the account
			accChanges[addr] = oldAcc

			if oldAcc.CodeHash != currAcc.CodeHash {
				cc, err := currentPsr.ReadAccountCode(addr, oldAcc.Incarnation, oldAcc.CodeHash)
				if err != nil {
					return trie.EmptyRoot, err
				}

				ach := hexutils.BytesToHex(cc)
				if len(ach) > 0 {
					hexcc := fmt.Sprintf("0x%s", ach)
					codeChanges[addr] = hexcc
				}
			}
		}

		err = db.ForPrefix(kv.StorageChangeSet, dupSortKey, func(sk, sv []byte) error {
			changesetKey := sk[length.BlockNum:]
			address, _ := dbutils.PlainParseStoragePrefix(changesetKey)

			sstorageKey := sv[:length.Hash]
			stk := libcommon.BytesToHash(sstorageKey)

			value := []byte{0}
			if len(sv[length.Hash:]) != 0 {
				value = sv[length.Hash:]
			}

			stkk := fmt.Sprintf("0x%032x", stk)
			v := fmt.Sprintf("0x%032x", libcommon.BytesToHash(value))

			m := make(map[string]string)
			m[stkk] = v

			if storageChanges[address] == nil {
				storageChanges[address] = make(map[string]string)
			}
			storageChanges[address][stkk] = v
			return nil
		})
		if err != nil {
			return trie.EmptyRoot, err
		}

		// update the tree
		for addr, acc := range accChanges {
			if err := updateAccInTree(dbSmt, addr, acc); err != nil {
				return trie.EmptyRoot, err
			}
		}
		for addr, code := range codeChanges {
			if err := dbSmt.SetContractBytecode(addr.String(), code); err != nil {
				return trie.EmptyRoot, err
			}
		}

		for addr, storage := range storageChanges {
			if _, err := dbSmt.SetContractStorage(addr.String(), storage); err != nil {
				return trie.EmptyRoot, err
			}
		}

		progressChan <- total - i
	}

	for _, k := range accDeletes {
		if err := updateAccInTree(dbSmt, k, nil); err != nil {
			return trie.EmptyRoot, err
		}
	}

	if err := verifyLastHash(dbSmt, expectedRootHash, &cfg, logPrefix); err != nil {
		log.Error("failed to verify hash")
		eridb.RollbackBatch()
		return trie.EmptyRoot, err
	}

	if err := eridb.CommitBatch(); err != nil {
		return trie.EmptyRoot, err
	}

	lr := dbSmt.LastRoot()

	hash := libcommon.BigToHash(lr)
	return hash, nil
}

func updateAccInTree(smt *smt.SMT, addr libcommon.Address, acc *accounts.Account) error {
	if acc != nil {
		n := new(big.Int).SetUint64(acc.Nonce)
		_, err := smt.SetAccountState(addr.String(), acc.Balance.ToBig(), n)
		return err
	}

	_, err := smt.SetAccountState(addr.String(), big.NewInt(0), big.NewInt(0))
	return err
}

func verifyLastHash(dbSmt *smt.SMT, expectedRootHash *libcommon.Hash, cfg *ZkInterHashesCfg, logPrefix string) error {
	hash := libcommon.BigToHash(dbSmt.LastRoot())

	if cfg.checkRoot && hash != *expectedRootHash {
		log.Warn(fmt.Sprintf("[%s] Wrong trie root: %x, expected (from header): %x", logPrefix, hash, expectedRootHash))
		return nil
	}
	log.Info(fmt.Sprintf("[%s] Trie root matches", logPrefix), "hash", hash.Hex())
	return nil
}

func processAccount(db smt.DB, a *accounts.Account, as map[string]string, inc uint64, psr *state2.PlainStateReader, addr libcommon.Address, keys []utils.NodeKey) ([]utils.NodeKey, error) {
	// get the account balance and nonce
	keys, err := insertAccountStateToKV(db, keys, addr.String(), a.Balance.ToBig(), new(big.Int).SetUint64(a.Nonce))
	if err != nil {
		return []utils.NodeKey{}, err
	}

	// store the contract bytecode
	cc, err := psr.ReadAccountCode(addr, inc, a.CodeHash)
	if err != nil {
		return []utils.NodeKey{}, err
	}

	ach := hexutils.BytesToHex(cc)
	if len(ach) > 0 {
		hexcc := fmt.Sprintf("0x%s", ach)
		keys, err = insertContractBytecodeToKV(db, keys, addr.String(), hexcc)
		if err != nil {
			return []utils.NodeKey{}, err
		}
	}

	if len(as) > 0 {
		// store the account storage
		keys, err = insertContractStorageToKV(db, keys, addr.String(), as)
		if err != nil {
			return []utils.NodeKey{}, err
		}
	}

	return keys, nil
}

func insertContractBytecodeToKV(db smt.DB, keys []utils.NodeKey, ethAddr string, bytecode string) ([]utils.NodeKey, error) {
	keyContractCode, err := utils.KeyContractCode(ethAddr)
	if err != nil {
		return []utils.NodeKey{}, err
	}

	keyContractLength, err := utils.KeyContractLength(ethAddr)
	if err != nil {
		return []utils.NodeKey{}, err
	}

	hashedBytecode, err := utils.HashContractBytecode(bytecode)
	if err != nil {
		return []utils.NodeKey{}, err
	}

	parsedBytecode := strings.TrimPrefix(bytecode, "0x")
	if len(parsedBytecode)%2 != 0 {
		parsedBytecode = "0" + parsedBytecode
	}

	bi := utils.ConvertHexToBigInt(hashedBytecode)
	bytecodeLength := len(parsedBytecode) / 2

	x := utils.ScalarToArrayBig(bi)
	valueContractCode, err := utils.NodeValue8FromBigIntArray(x)
	if err != nil {
		return []utils.NodeKey{}, err
	}

	x = utils.ScalarToArrayBig(big.NewInt(int64(bytecodeLength)))
	valueContractLength, err := utils.NodeValue8FromBigIntArray(x)
	if err != nil {
		return []utils.NodeKey{}, err
	}
	if !valueContractCode.IsZero() {
		keys = append(keys, keyContractCode)
		db.InsertAccountValue(keyContractCode, *valueContractCode)

		ks := utils.EncodeKeySource(utils.SC_CODE, utils.ConvertHexToAddress(ethAddr), common.Hash{})
		db.InsertKeySource(keyContractCode, ks)
	}

	if !valueContractLength.IsZero() {
		keys = append(keys, keyContractLength)
		db.InsertAccountValue(keyContractLength, *valueContractLength)

		ks := utils.EncodeKeySource(utils.SC_LENGTH, utils.ConvertHexToAddress(ethAddr), common.Hash{})
		db.InsertKeySource(keyContractLength, ks)
	}

	return keys, nil
}

func insertContractStorageToKV(db smt.DB, keys []utils.NodeKey, ethAddr string, storage map[string]string) ([]utils.NodeKey, error) {
	a := utils.ConvertHexToBigInt(ethAddr)
	add := utils.ScalarToArrayBig(a)

	for k, v := range storage {
		if v == "" {
			continue
		}

		keyStoragePosition, err := utils.KeyContractStorage(add, k)
		if err != nil {
			return []utils.NodeKey{}, err
		}

		base := 10
		if strings.HasPrefix(v, "0x") {
			v = v[2:]
			base = 16
		}

		val, _ := new(big.Int).SetString(v, base)

		x := utils.ScalarToArrayBig(val)
		parsedValue, err := utils.NodeValue8FromBigIntArray(x)
		if err != nil {
			return []utils.NodeKey{}, err
		}
		if !parsedValue.IsZero() {
			keys = append(keys, keyStoragePosition)
			db.InsertAccountValue(keyStoragePosition, *parsedValue)

			sp, _ := utils.StrValToBigInt(k)

			ks := utils.EncodeKeySource(utils.SC_STORAGE, utils.ConvertHexToAddress(ethAddr), common.BigToHash(sp))
			db.InsertKeySource(keyStoragePosition, ks)
		}
	}

	return keys, nil
}

func insertAccountStateToKV(db smt.DB, keys []utils.NodeKey, ethAddr string, balance, nonce *big.Int) ([]utils.NodeKey, error) {
	keyBalance, err := utils.KeyEthAddrBalance(ethAddr)
	if err != nil {
		return []utils.NodeKey{}, err
	}
	keyNonce, err := utils.KeyEthAddrNonce(ethAddr)
	if err != nil {
		return []utils.NodeKey{}, err
	}

	x := utils.ScalarToArrayBig(balance)
	valueBalance, err := utils.NodeValue8FromBigIntArray(x)
	if err != nil {
		return []utils.NodeKey{}, err
	}

	x = utils.ScalarToArrayBig(nonce)
	valueNonce, err := utils.NodeValue8FromBigIntArray(x)
	if err != nil {
		return []utils.NodeKey{}, err
	}

	if !valueBalance.IsZero() {
		keys = append(keys, keyBalance)
		db.InsertAccountValue(keyBalance, *valueBalance)

		ks := utils.EncodeKeySource(utils.KEY_BALANCE, utils.ConvertHexToAddress(ethAddr), common.Hash{})
		db.InsertKeySource(keyBalance, ks)
	}
	if !valueNonce.IsZero() {
		keys = append(keys, keyNonce)
		db.InsertAccountValue(keyNonce, *valueNonce)

		ks := utils.EncodeKeySource(utils.KEY_NONCE, utils.ConvertHexToAddress(ethAddr), common.Hash{})
		db.InsertKeySource(keyNonce, ks)
	}
	return keys, nil
}

// RPC Debug
func verifyStateRoot(dbSmt *smt.SMT, expectedRootHash *libcommon.Hash, cfg *ZkInterHashesCfg, logPrefix string, blockNo uint64, tx kv.RwTx) error {
	hash := libcommon.BigToHash(dbSmt.LastRoot())
	//psr := state2.NewPlainStateReader(tx)

	fmt.Println("[zkevm] interhashes - expected root: ", expectedRootHash.Hex())
	fmt.Println("[zkevm] interhashes - actual root: ", hash.Hex())

	if cfg.checkRoot && hash != *expectedRootHash {
		// [zkevm] - check against the rpc get block by number
		// get block number
		//ss := libcommon.HexToAddress("0x000000000000000000000000000000005ca1ab1e")
		//key := libcommon.HexToHash("0x0")
		//
		//txno, err := psr.ReadAccountStorage(ss, 1, &key)
		//if err != nil {
		//	return err
		//}
		// convert txno to big int
		bigTxNo := big.NewInt(0)
		bigTxNo.SetUint64(blockNo)

		fmt.Println("[zkevm] interhashes - txno: ", bigTxNo)

		sr, err := stateRootByTxNo(bigTxNo, cfg.zk.L2RpcUrl)
		if err != nil {
			return err
		}

		if hash != *sr {
			log.Warn(fmt.Sprintf("[%s] Wrong trie root: %x, expected (from header): %x, from rpc: %x", logPrefix, hash, expectedRootHash, *sr))
			return fmt.Errorf("wrong trie root at %d: %x, expected (from header): %x, from rpc: %x", blockNo, hash, expectedRootHash, *sr)
		}

		log.Info("[zkevm] interhashes - trie root matches rpc get block by number")
		*expectedRootHash = *sr
	}
	return nil
}

func stateRootByTxNo(txNo *big.Int, l2RpcUrl string) (*libcommon.Hash, error) {
	requestBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_getBlockByNumber",
		"params":  []interface{}{txNo.Uint64(), true},
		"id":      1,
	}

	requestBytes, err := json.Marshal(requestBody)
	if err != nil {
		return nil, err
	}

	response, err := http.Post(l2RpcUrl, "application/json", bytes.NewBuffer(requestBytes))
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()

	responseBytes, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	responseMap := make(map[string]interface{})
	if err := json.Unmarshal(responseBytes, &responseMap); err != nil {
		return nil, err
	}

	result, ok := responseMap["result"].(map[string]interface{})
	if !ok {
		return nil, err
	}

	stateRoot, ok := result["stateRoot"].(string)
	if !ok {
		return nil, err
	}
	h := libcommon.HexToHash(stateRoot)

	return &h, nil
}
