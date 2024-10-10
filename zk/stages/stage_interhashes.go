package stages

import (
	"fmt"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/common/length"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/gateway-fm/cdk-erigon-lib/state"
	"github.com/holiman/uint256"
	state2 "github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	db2 "github.com/ledgerwatch/erigon/smt/pkg/db"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/log/v3"

	"strings"

	"context"
	"math/big"
	"time"

	"os"

	"math"

	"github.com/gateway-fm/cdk-erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
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

func StageZkInterHashesCfg(
	db kv.RwDB,
	checkRoot, saveNewHashesToDB, badBlockHalt bool,
	tmpDir string,
	blockReader services.FullBlockReader,
	hd *headerdownload.HeaderDownload,
	historyV3 bool,
	agg *state.AggregatorV3,
	zk *ethconfig.Zk,
) ZkInterHashesCfg {
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

func SpawnZkIntermediateHashesStage(s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, cfg ZkInterHashesCfg, ctx context.Context, quiet bool) (root common.Hash, err error) {
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

	///// DEBUG BISECT /////
	defer func() {
		if cfg.zk.DebugLimit > 0 {
			if err != nil {
				log.Error("Hashing Failed", "block", to, "err", err)
				os.Exit(1)
			} else if to >= cfg.zk.DebugLimit {
				tx.Commit()
				os.Exit(0)
			}
		}
	}()
	///////////////////////

	if s.BlockNumber == to {
		// we already did hash check for this block
		// we don't do the obvious `if s.BlockNumber > to` to support reorgs more naturally
		return trie.EmptyRoot, nil
	}

	if !quiet && to > s.BlockNumber+16 {
		log.Info(fmt.Sprintf("[%s] Generating intermediate hashes", logPrefix), "from", s.BlockNumber, "to", to)
	}

	shouldRegenerate := to > s.BlockNumber && to-s.BlockNumber > cfg.zk.RebuildTreeAfter
	shouldIncrementBecauseOfAFlag := cfg.zk.IncrementTreeAlways
	shouldIncrementBecauseOfExecutionConditions := s.BlockNumber > 0 && !shouldRegenerate
	shouldIncrement := shouldIncrementBecauseOfAFlag || shouldIncrementBecauseOfExecutionConditions

	eridb := db2.NewEriDb(tx)
	smt := smt.NewSMT(eridb, false)

	if cfg.zk.SmtRegenerateInMemory {
		log.Info(fmt.Sprintf("[%s] SMT using mapmutation", logPrefix))
		eridb.OpenBatch(quit)
	} else {
		log.Info(fmt.Sprintf("[%s] SMT not using mapmutation", logPrefix))
	}

	if shouldIncrement {
		if shouldIncrementBecauseOfAFlag {
			log.Debug(fmt.Sprintf("[%s] IncrementTreeAlways true - incrementing tree", logPrefix), "previousRootHeight", s.BlockNumber, "calculatingRootHeight", to)
		}
		if root, err = zkIncrementIntermediateHashes(ctx, logPrefix, s, tx, eridb, smt, s.BlockNumber, to); err != nil {
			return trie.EmptyRoot, err
		}
	} else {
		if root, err = regenerateIntermediateHashes(ctx, logPrefix, tx, eridb, smt, to); err != nil {
			return trie.EmptyRoot, err
		}
	}

	log.Info(fmt.Sprintf("[%s] Trie root", logPrefix), "hash", root.Hex())

	if cfg.checkRoot {
		var syncHeadHeader *types.Header
		if syncHeadHeader, err = cfg.blockReader.HeaderByNumber(ctx, tx, to); err != nil {
			return trie.EmptyRoot, err
		}
		if syncHeadHeader == nil {
			return trie.EmptyRoot, fmt.Errorf("no header found with number %d", to)
		}

		expectedRootHash := syncHeadHeader.Root
		headerHash := syncHeadHeader.Hash()
		if root != expectedRootHash {
			if cfg.zk.SmtRegenerateInMemory {
				eridb.RollbackBatch()
			}
			panic(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", logPrefix, to, root, expectedRootHash, headerHash))
		}

		log.Info(fmt.Sprintf("[%s] State root matches", logPrefix))
	}

	if cfg.zk.SmtRegenerateInMemory {
		if err := eridb.CommitBatch(); err != nil {
			return trie.EmptyRoot, err
		}
	}

	if err = s.Update(tx, to); err != nil {
		return trie.EmptyRoot, err
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return trie.EmptyRoot, err
		}
	}

	return root, err
}

func UnwindZkIntermediateHashesStage(u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, cfg ZkInterHashesCfg, ctx context.Context, silent bool) (err error) {
	quit := ctx.Done()
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	if !silent {
		log.Debug(fmt.Sprintf("[%s] Unwinding intermediate hashes", s.LogPrefix()), "from", s.BlockNumber, "to", u.UnwindPoint)
	}

	var expectedRootHash common.Hash
	syncHeadHeader := rawdb.ReadHeaderByNumber(tx, u.UnwindPoint)
	if err != nil {
		return err
	}
	if syncHeadHeader == nil {
		log.Warn("header not found for block number", "block", u.UnwindPoint)
	} else {
		expectedRootHash = syncHeadHeader.Root
	}

	root, err := unwindZkSMT(ctx, s.LogPrefix(), s.BlockNumber, u.UnwindPoint, tx, cfg.checkRoot, &expectedRootHash, silent, quit)
	if err != nil {
		return err
	}
	_ = root

	hermezDb := hermez_db.NewHermezDb(tx)
	if err := hermezDb.TruncateSmtDepths(u.UnwindPoint); err != nil {
		return err
	}

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

func regenerateIntermediateHashes(ctx context.Context, logPrefix string, db kv.RwTx, eridb *db2.EriDb, smtIn *smt.SMT, toBlock uint64) (common.Hash, error) {
	log.Info(fmt.Sprintf("[%s] Regeneration trie hashes started", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Regeneration ended", logPrefix))

	if err := stages.SaveStageProgress(db, stages.IntermediateHashes, 0); err != nil {
		log.Warn(fmt.Sprint("regenerate SaveStageProgress to zero error: ", err))
	}

	var a *accounts.Account
	var addr common.Address
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
			addr = common.BytesToAddress(k)
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

			as[sk] = TrimHexString(v)
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
	if _, err := smtIn.GenerateFromKVBulk(ctx, logPrefix, keys); err != nil {
		return trie.EmptyRoot, err
	}

	err2 := db.ClearBucket("HermezSmtAccountValues")
	if err2 != nil {
		log.Warn(fmt.Sprint("regenerate SaveStageProgress to zero error: ", err2))
	}

	root := smtIn.LastRoot()

	// save it here so we don't
	hermezDb := hermez_db.NewHermezDb(db)
	if err := hermezDb.WriteSmtDepth(toBlock, uint64(smtIn.GetDepth())); err != nil {
		return trie.EmptyRoot, err
	}

	return common.BigToHash(root), nil
}

func zkIncrementIntermediateHashes(ctx context.Context, logPrefix string, s *stagedsync.StageState, db kv.RwTx, eridb *db2.EriDb, dbSmt *smt.SMT, from, to uint64) (common.Hash, error) {
	log.Info(fmt.Sprintf("[%s] Increment trie hashes started", logPrefix), "previousRootHeight", s.BlockNumber, "calculatingRootHeight", to)
	defer log.Info(fmt.Sprintf("[%s] Increment ended", logPrefix))

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
	accChanges := make(map[common.Address]*accounts.Account)
	codeChanges := make(map[common.Address]string)
	storageChanges := make(map[common.Address]map[string]string)

	// case when we are incrementing from block 1
	// we chould include the 0 block which is the genesis data
	if from != 0 {
		from += 1
	}

	// NB: changeset tables are zero indexed
	// changeset tables contain historical value at N-1, so we look up values from plainstate
	// i+1 to get state at the beginning of the next batch
	psr := state2.NewPlainState(db, from+1, systemcontracts.SystemContractCodeLookup["Hermez"])
	defer psr.Close()

	for i := from; i <= to; i++ {
		dupSortKey := dbutils.EncodeBlockNumber(i)
		psr.SetBlockNr(i + 1)

		// collect changes to accounts and code
		for _, v, err := ac.SeekExact(dupSortKey); err == nil && v != nil; _, v, err = ac.NextDup() {
			addr := common.BytesToAddress(v[:length.Addr])

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
				hexcc := "0x" + ach
				codeChanges[addr] = hexcc
			}
		}

		err = db.ForPrefix(kv.StorageChangeSet, dupSortKey, func(sk, sv []byte) error {
			changesetKey := sk[length.BlockNum:]
			address, incarnation := dbutils.PlainParseStoragePrefix(changesetKey)

			sstorageKey := sv[:length.Hash]
			stk := common.BytesToHash(sstorageKey)

			value, err := psr.ReadAccountStorage(address, incarnation, &stk)
			if err != nil {
				return err
			}

			stkk := fmt.Sprintf("0x%032x", stk)
			v := fmt.Sprintf("0x%032x", common.BytesToHash(value))

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

	if _, _, err := dbSmt.SetStorage(ctx, logPrefix, accChanges, codeChanges, storageChanges); err != nil {
		return trie.EmptyRoot, err
	}

	log.Info(fmt.Sprintf("[%s] Regeneration trie hashes finished. Commiting batch", logPrefix))

	lr := dbSmt.LastRoot()

	hash := common.BigToHash(lr)

	// do not put this outside, because sequencer uses this function to calculate root for each block
	hermezDb := hermez_db.NewHermezDb(db)
	if err := hermezDb.WriteSmtDepth(to, uint64(dbSmt.GetDepth())); err != nil {
		return trie.EmptyRoot, err
	}

	return hash, nil
}

func unwindZkSMT(ctx context.Context, logPrefix string, from, to uint64, db kv.RwTx, checkRoot bool, expectedRootHash *common.Hash, quiet bool, quit <-chan struct{}) (common.Hash, error) {
	if !quiet {
		log.Info(fmt.Sprintf("[%s] Unwind trie hashes started", logPrefix))
		defer log.Info(fmt.Sprintf("[%s] Unwind ended", logPrefix))
	}

	eridb := db2.NewEriDb(db)
	dbSmt := smt.NewSMT(eridb, false)

	if !quiet {
		log.Info(fmt.Sprintf("[%s]", logPrefix), "last root", common.BigToHash(dbSmt.LastRoot()))
	}

	if quit == nil {
		log.Warn("quit channel is nil, creating a new one")
		quit = make(chan struct{})
	}

	// only open the batch if tx is not already one
	if _, ok := db.(*memdb.MemoryMutation); !ok {
		eridb.OpenBatch(quit)
	}

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

	total := uint64(math.Abs(float64(from) - float64(to) + 1))
	printerStopped := false
	progressChan, stopPrinter := zk.ProgressPrinter(fmt.Sprintf("[%s] Progress unwinding", logPrefix), total, quiet)
	defer func() {
		if !printerStopped {
			stopPrinter()
		}
	}()

	// walk backwards through the blocks, applying state changes, and deletes
	// PlainState contains data AT the block
	// History tables contain data BEFORE the block - so need a +1 offset
	accChanges := make(map[common.Address]*accounts.Account)
	codeChanges := make(map[common.Address]string)
	storageChanges := make(map[common.Address]map[string]string)

	addDeletedAcc := func(addr common.Address) {
		deletedAcc := new(accounts.Account)
		deletedAcc.Balance = *uint256.NewInt(0)
		deletedAcc.Nonce = 0
		accChanges[addr] = deletedAcc
	}

	psr := state2.NewPlainState(db, from, systemcontracts.SystemContractCodeLookup["Hermez"])
	defer psr.Close()

	for i := from; i >= to+1; i-- {
		select {
		case <-ctx.Done():
			return trie.EmptyRoot, fmt.Errorf("[%s] Context done", logPrefix)
		default:
		}

		psr.SetBlockNr(i)

		dupSortKey := dbutils.EncodeBlockNumber(i)

		// collect changes to accounts and code
		for _, v, err2 := ac.SeekExact(dupSortKey); err2 == nil && v != nil; _, v, err2 = ac.NextDup() {

			addr := common.BytesToAddress(v[:length.Addr])

			// if the account was created in this changeset we should delete it
			if len(v[length.Addr:]) == 0 {
				codeChanges[addr] = ""
				addDeletedAcc(addr)
				continue
			}

			oldAcc, err := psr.ReadAccountData(addr)
			if err != nil {
				return trie.EmptyRoot, err
			}

			// currAcc at block we're unwinding from
			currAcc, err := currentPsr.ReadAccountData(addr)
			if err != nil {
				return trie.EmptyRoot, err
			}

			if oldAcc.Incarnation > 0 {
				if len(v) == 0 { // self-destructed
					addDeletedAcc(addr)
				} else {
					if currAcc.Incarnation > oldAcc.Incarnation {
						addDeletedAcc(addr)
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
				hexcc := ""
				if len(ach) > 0 {
					hexcc = "0x" + ach
				}
				codeChanges[addr] = hexcc
			}
		}

		err = db.ForPrefix(kv.StorageChangeSet, dupSortKey, func(sk, sv []byte) error {
			changesetKey := sk[length.BlockNum:]
			address, _ := dbutils.PlainParseStoragePrefix(changesetKey)

			sstorageKey := sv[:length.Hash]
			stk := common.BytesToHash(sstorageKey)

			value := []byte{0}
			if len(sv[length.Hash:]) != 0 {
				value = sv[length.Hash:]
			}

			stkk := fmt.Sprintf("0x%032x", stk)
			v := fmt.Sprintf("0x%032x", common.BytesToHash(value))

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

		progressChan <- 1
	}

	stopPrinter()
	printerStopped = true

	if _, _, err := dbSmt.SetStorage(ctx, logPrefix, accChanges, codeChanges, storageChanges); err != nil {
		return trie.EmptyRoot, err
	}

	if err := verifyLastHash(dbSmt, expectedRootHash, checkRoot, logPrefix, quiet); err != nil {
		log.Error("failed to verify hash")
		eridb.RollbackBatch()
		return trie.EmptyRoot, err
	}

	if err := eridb.CommitBatch(); err != nil {
		return trie.EmptyRoot, err
	}

	lr := dbSmt.LastRoot()

	hash := common.BigToHash(lr)
	return hash, nil
}

func verifyLastHash(dbSmt *smt.SMT, expectedRootHash *common.Hash, checkRoot bool, logPrefix string, quiet bool) error {
	hash := common.BigToHash(dbSmt.LastRoot())

	if checkRoot && hash != *expectedRootHash {
		panic(fmt.Sprintf("[%s] Wrong trie root: %x, expected (from header): %x", logPrefix, hash, expectedRootHash))
	}
	if !quiet {
		log.Info(fmt.Sprintf("[%s] Trie root matches", logPrefix), "hash", hash.Hex())
	}
	return nil
}

func processAccount(db smt.DB, a *accounts.Account, as map[string]string, inc uint64, psr *state2.PlainStateReader, addr common.Address, keys []utils.NodeKey) ([]utils.NodeKey, error) {
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
		hexcc := "0x" + ach
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
	keyContractCode := utils.KeyContractCode(ethAddr)
	keyContractLength := utils.KeyContractLength(ethAddr)
	bi := utils.HashContractBytecodeBigInt(bytecode)

	parsedBytecode := strings.TrimPrefix(bytecode, "0x")
	if len(parsedBytecode)%2 != 0 {
		parsedBytecode = "0" + parsedBytecode
	}

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

		keyStoragePosition := utils.KeyContractStorage(add, k)

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
	keyBalance := utils.KeyEthAddrBalance(ethAddr)
	keyNonce := utils.KeyEthAddrNonce(ethAddr)

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
