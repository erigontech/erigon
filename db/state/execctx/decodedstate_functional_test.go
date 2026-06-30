package execctx_test

import (
	"context"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dir"
	log "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/datastruct/btindex"
	"github.com/erigontech/erigon/db/datastruct/existence"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/decodedstate"
)

var decodedStorageKVRangePattern = regexp.MustCompile(`decodedstorage\.(\d+)-(\d+)\.kv$`)

func decodedStateFixtureEntry() decodedstate.DecodedEntry {
	return decodedstate.DecodedEntry{
		Contract:    common.HexToAddress("0x00000000000000000000000000000000000000AA"),
		MappingSlot: common.BigToHash(big.NewInt(7)),
		Keys:        []common.Hash{common.BigToHash(big.NewInt(42))},
		Value:       common.BigToHash(big.NewInt(111)),
		EntryType:   decodedstate.MappingEntry,
	}
}

func writeDecodedEntriesViaLiveCommitPath(t *testing.T, db kv.TemporalRwDB, blockNum uint64, entries []decodedstate.DecodedEntry) {
	t.Helper()

	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, decodedstate.WriteEntriesToTx(tx, blockNum, entries))
	require.NoError(t, tx.Commit())
}

func decodedStorageSnapshotFiles(t *testing.T, dirs datadir.Dirs) []string {
	t.Helper()

	scanDirs := []string{dirs.SnapDomain, dirs.SnapHistory, dirs.SnapIdx, dirs.SnapAccessors}
	files := make([]string, 0, 8)
	for _, dir := range scanDirs {
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		for _, entry := range entries {
			if entry.IsDir() || !strings.Contains(entry.Name(), "decodedstorage.") {
				continue
			}
			files = append(files, filepath.Join(dir, entry.Name()))
		}
	}
	sort.Strings(files)
	return files
}

func newDecodedStateExecCtxDB(t *testing.T, stepSize uint64) (kv.TemporalRwDB, datadir.Dirs) {
	t.Helper()

	db := newTestDb(t, stepSize)
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	dirs := tx.Debug().Dirs()
	tx.Rollback()
	return db, dirs
}

func newDecodedStateExecCtxDBWithReorgDepth(t *testing.T, stepSize, reorgDepth uint64) (kv.TemporalRwDB, datadir.Dirs) {
	t.Helper()

	logger := log.New()
	dirs := datadir.New(t.TempDir())
	rawDB := mdbx.New(dbcfg.ChainDB, logger).InMem(t, dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	t.Cleanup(rawDB.Close)

	agg := state.NewTest(dirs).StepSize(stepSize).ReorgBlockDepth(reorgDepth).Logger(logger).MustOpen(t.Context(), rawDB)
	t.Cleanup(agg.Close)
	require.NoError(t, agg.OpenFolder())

	db, err := temporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	return db, dirs
}

func appendCanonicalTxNumsThrough(t *testing.T, db kv.TemporalRwDB, lastBlock uint64) {
	t.Helper()

	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	for blockNum := uint64(0); blockNum <= lastBlock; blockNum++ {
		require.NoError(t, rawdbv3.TxNums.Append(tx, blockNum, blockNum))
	}
	require.NoError(t, tx.Commit())
}

func decodedStorageStepsInFiles(t *testing.T, db kv.TemporalRwDB) kv.Step {
	t.Helper()

	tx, err := db.BeginTemporalRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	return tx.StepsInFiles(kv.DecodedStorageDomain)
}

type decodedFileRange struct {
	fromStep uint64
	toStep   uint64
}

func decodedStorageDomainFileRanges(t *testing.T, dirs datadir.Dirs) []decodedFileRange {
	t.Helper()

	entries, err := os.ReadDir(dirs.SnapDomain)
	require.NoError(t, err)

	ranges := make([]decodedFileRange, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		match := decodedStorageKVRangePattern.FindStringSubmatch(entry.Name())
		if match == nil {
			continue
		}
		fromStep, err := strconv.ParseUint(match[1], 10, 64)
		require.NoError(t, err)
		toStep, err := strconv.ParseUint(match[2], 10, 64)
		require.NoError(t, err)
		ranges = append(ranges, decodedFileRange{fromStep: fromStep, toStep: toStep})
	}
	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].fromStep == ranges[j].fromStep {
			return ranges[i].toStep < ranges[j].toStep
		}
		return ranges[i].fromStep < ranges[j].fromStep
	})
	return ranges
}

func maxContiguousDecodedDomainStepEnd(ranges []decodedFileRange) uint64 {
	var end uint64
	for _, rng := range ranges {
		if rng.fromStep > end {
			break
		}
		if rng.toStep > end {
			end = rng.toStep
		}
	}
	return end
}

func populateSnapshotFile(t *testing.T, dirs datadir.Dirs, file string) {
	t.Helper()

	switch {
	case strings.HasSuffix(file, ".ef"), strings.HasSuffix(file, ".v"), strings.HasSuffix(file, ".kv"):
		comp, err := seg.NewCompressor(context.Background(), t.Name(), file, dirs.Tmp, seg.DefaultCfg, log.LvlDebug, log.New())
		require.NoError(t, err)
		defer comp.Close()
		comp.DisableFsync()
		require.NoError(t, comp.AddWord([]byte("word")))
		require.NoError(t, comp.Compress())
	case strings.HasSuffix(file, ".bt"):
		sampleFile := file + ".sample"
		comp, err := seg.NewCompressor(context.Background(), t.Name(), sampleFile, dirs.Tmp, seg.DefaultCfg, log.LvlDebug, log.New())
		require.NoError(t, err)
		defer comp.Close()
		comp.DisableFsync()
		require.NoError(t, comp.AddWord([]byte("key")))
		require.NoError(t, comp.AddWord([]byte("value")))
		require.NoError(t, comp.Compress())

		decomp, err := seg.NewDecompressor(sampleFile)
		require.NoError(t, err)
		defer decomp.Close()
		defer func() { require.NoError(t, dir.RemoveFile(sampleFile)) }()

		reader := seg.NewReader(decomp.MakeGetter(), seg.CompressNone)
		kveiFile := strings.TrimSuffix(file, ".bt") + ".kvei"
		index, err := btindex.CreateBtreeIndexWithDecompressor(file, kveiFile, 128, reader, uint32(1), background.NewProgressSet(), dirs.Tmp, log.New(), true, statecfg.AccessorBTree|statecfg.AccessorExistence)
		require.NoError(t, err)
		index.Close()
	case strings.HasSuffix(file, ".kvei"):
		filter, err := existence.NewFilter(0, file)
		require.NoError(t, err)
		defer filter.Close()
		filter.DisableFsync()
		require.NoError(t, filter.Build())
	case strings.HasSuffix(file, ".kvi"), strings.HasSuffix(file, ".vi"), strings.HasSuffix(file, ".efi"):
		salt := uint32(1)
		rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
			KeyCount:   1,
			BucketSize: 10,
			Salt:       &salt,
			TmpDir:     dirs.Tmp,
			IndexFile:  file,
			LeafSize:   8,
		}, log.New())
		require.NoError(t, err)
		defer rs.Close()
		rs.DisableFsync()
		require.NoError(t, rs.AddKey([]byte("first_key"), 0))
		require.NoError(t, rs.Build(context.Background()))
	default:
		t.Fatalf("unsupported snapshot file type: %s", file)
	}
}

func populateSchemaFiles(t *testing.T, dirs datadir.Dirs, schema *state.E3SnapSchema, stepSize uint64, ranges []decodedFileRange) {
	t.Helper()

	v := version.V1_0
	for _, rng := range ranges {
		from := state.RootNum(rng.fromStep * stepSize)
		to := state.RootNum(rng.toStep * stepSize)

		file, err := schema.DataFile(v, from, to)
		require.NoError(t, err)
		populateSnapshotFile(t, dirs, file)

		accessors := schema.AccessorList()
		if accessors.Has(statecfg.AccessorBTree) {
			file, err = schema.BtIdxFile(v, from, to)
			require.NoError(t, err)
			populateSnapshotFile(t, dirs, file)
		}
		if accessors.Has(statecfg.AccessorExistence) {
			file, err = schema.ExistenceFile(v, from, to)
			require.NoError(t, err)
			populateSnapshotFile(t, dirs, file)
		}
		if accessors.Has(statecfg.AccessorHashMap) {
			file, err = schema.AccessorIdxFile(v, from, to, 0)
			require.NoError(t, err)
			populateSnapshotFile(t, dirs, file)
		}
	}
}

func primeCoreStateFilesAheadOfDecoded(t *testing.T, dirs datadir.Dirs, stepSize, aheadToStep uint64) {
	t.Helper()

	for _, cfg := range []statecfg.DomainCfg{
		statecfg.Schema.AccountsDomain,
		statecfg.Schema.StorageDomain,
		statecfg.Schema.CodeDomain,
		statecfg.Schema.CommitmentDomain,
	} {
		domain, history, ii := state.SnapSchemaFromDomainCfg(cfg, dirs, stepSize)
		populateSchemaFiles(t, dirs, domain, stepSize, []decodedFileRange{{fromStep: 0, toStep: aheadToStep}})
		if history != nil {
			populateSchemaFiles(t, dirs, history, stepSize, []decodedFileRange{{fromStep: 0, toStep: aheadToStep}})
		}
		if ii != nil {
			populateSchemaFiles(t, dirs, ii, stepSize, []decodedFileRange{{fromStep: 0, toStep: aheadToStep}})
		}
	}
}

func primeDecodedPrefixFiles(t *testing.T, dirs datadir.Dirs, stepSize, prefixToStep uint64) {
	t.Helper()

	domain, history, ii := state.SnapSchemaFromDomainCfg(statecfg.Schema.DecodedStorageDomain, dirs, stepSize)
	ranges := make([]decodedFileRange, 0, prefixToStep)
	for step := uint64(0); step < prefixToStep; step++ {
		ranges = append(ranges, decodedFileRange{fromStep: step, toStep: step + 1})
	}
	populateSchemaFiles(t, dirs, domain, stepSize, ranges)
	if history != nil {
		populateSchemaFiles(t, dirs, history, stepSize, ranges)
	}
	if ii != nil {
		populateSchemaFiles(t, dirs, ii, stepSize, ranges)
	}
}

func TestS_SNAP_07_DecodedStateWritesUseDecodedStorageTemporalDomain(t *testing.T) {
	ctx := context.Background()
	db, _ := newDecodedStateExecCtxDB(t, 4)
	store := decodedstate.NewStoreFromDB(db)
	entry := decodedStateFixtureEntry()

	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	txBefore, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer txBefore.Rollback()
	beforeProgress := txBefore.Debug().DomainProgress(kv.DecodedStorageDomain)
	txBefore.Rollback()

	require.NoError(t, store.WriteEntries(1, []decodedstate.DecodedEntry{entry}))

	got, found, err := store.GetLatest(entry.Contract, entry.MappingSlot, entry.Keys[0])
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, entry.Value, got)

	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	afterProgress := tx.Debug().DomainProgress(kv.DecodedStorageDomain)
	require.Greater(
		t,
		afterProgress,
		beforeProgress,
		"decoded-state writes must advance decodedstorage domain progress so the aggregator can merge/freeze/prune them",
	)
}

func TestS_SNAP_08_DecodedStateWritesFreezeIntoDecodedStorageSnapshots(t *testing.T) {
	stepSize := uint64(4)
	db, dirs := newDecodedStateExecCtxDB(t, stepSize)
	store := decodedstate.NewStoreFromDB(db)
	entry := decodedStateFixtureEntry()

	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	require.NoError(t, store.WriteEntries(1, []decodedstate.DecodedEntry{entry}))
	require.NoError(t, db.(state.HasAgg).Agg().(*state.Aggregator).BuildFiles(stepSize))

	require.NotEmpty(
		t,
		decodedStorageSnapshotFiles(t, dirs),
		"decoded-state writes must become frozen decodedstorage snapshot files once the completed step is built",
	)
}

func TestS_SNAP_09_LiveCommitPathAdvancesDecodedStorageTemporalDomain(t *testing.T) {
	ctx := context.Background()
	db, _ := newDecodedStateExecCtxDB(t, 4)
	store := decodedstate.NewStoreFromDB(db)
	entry := decodedStateFixtureEntry()

	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	txBefore, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer txBefore.Rollback()
	beforeProgress := txBefore.Debug().DomainProgress(kv.DecodedStorageDomain)
	txBefore.Rollback()

	writeDecodedEntriesViaLiveCommitPath(t, db, 1, []decodedstate.DecodedEntry{entry})

	got, found, err := store.GetLatest(entry.Contract, entry.MappingSlot, entry.Keys[0])
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, entry.Value, got)

	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	afterProgress := tx.Debug().DomainProgress(kv.DecodedStorageDomain)
	require.Greater(
		t,
		afterProgress,
		beforeProgress,
		"live decoded commit-path writes must advance decodedstorage domain progress so the aggregator can merge/freeze/prune them",
	)
}

func TestS_SNAP_10_LiveCommitPathFreezesIntoDecodedStorageSnapshots(t *testing.T) {
	stepSize := uint64(4)
	db, dirs := newDecodedStateExecCtxDB(t, stepSize)
	store := decodedstate.NewStoreFromDB(db)
	entry := decodedStateFixtureEntry()

	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	writeDecodedEntriesViaLiveCommitPath(t, db, 1, []decodedstate.DecodedEntry{entry})
	require.NoError(t, db.(state.HasAgg).Agg().(*state.Aggregator).BuildFiles(stepSize))

	require.NotEmpty(
		t,
		decodedStorageSnapshotFiles(t, dirs),
		"live decoded commit-path writes must freeze into decodedstorage snapshot files once the completed step is built",
	)
}

func TestS_SNAP_15_DirectDecodedWritesDoNotFreezeIncompleteHotStep(t *testing.T) {
	stepSize := uint64(4)
	db, dirs := newDecodedStateExecCtxDBWithReorgDepth(t, stepSize, 1)
	store := decodedstate.NewStoreFromDB(db)

	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	appendCanonicalTxNumsThrough(t, db, 6)

	coldStepEntry := decodedStateFixtureEntry()
	coldStepEntry.Keys = []common.Hash{common.BigToHash(big.NewInt(1))}
	coldStepEntry.Value = common.BigToHash(big.NewInt(101))

	hotStepEntry := decodedStateFixtureEntry()
	hotStepEntry.Keys = []common.Hash{common.BigToHash(big.NewInt(2))}
	hotStepEntry.Value = common.BigToHash(big.NewInt(202))

	futureStepEntry := decodedStateFixtureEntry()
	futureStepEntry.Keys = []common.Hash{common.BigToHash(big.NewInt(5))}
	futureStepEntry.Value = common.BigToHash(big.NewInt(505))

	require.NoError(t, store.WriteEntries(1, []decodedstate.DecodedEntry{coldStepEntry}))
	require.NoError(t, store.WriteEntries(6, []decodedstate.DecodedEntry{hotStepEntry}))
	require.NoError(t, store.WriteEntries(8, []decodedstate.DecodedEntry{futureStepEntry}))
	require.NoError(t, db.(state.HasAgg).Agg().(*state.Aggregator).BuildFiles(6))

	require.NotEmpty(t, decodedStorageSnapshotFiles(t, dirs), "completed decodedstorage steps should still freeze")
	require.Equal(
		t,
		kv.Step(1),
		decodedStorageStepsInFiles(t, db),
		"decoded snapshot freezing must stop before the current hot step even if decoded progress already points into the next step",
	)
}

func TestS_SNAP_16_LiveCommitPathDoesNotFreezeIncompleteHotStep(t *testing.T) {
	stepSize := uint64(4)
	db, dirs := newDecodedStateExecCtxDBWithReorgDepth(t, stepSize, 1)

	appendCanonicalTxNumsThrough(t, db, 6)

	coldStepEntry := decodedStateFixtureEntry()
	coldStepEntry.Keys = []common.Hash{common.BigToHash(big.NewInt(3))}
	coldStepEntry.Value = common.BigToHash(big.NewInt(303))

	hotStepEntry := decodedStateFixtureEntry()
	hotStepEntry.Keys = []common.Hash{common.BigToHash(big.NewInt(4))}
	hotStepEntry.Value = common.BigToHash(big.NewInt(404))

	futureStepEntry := decodedStateFixtureEntry()
	futureStepEntry.Keys = []common.Hash{common.BigToHash(big.NewInt(6))}
	futureStepEntry.Value = common.BigToHash(big.NewInt(606))

	writeDecodedEntriesViaLiveCommitPath(t, db, 1, []decodedstate.DecodedEntry{coldStepEntry})
	writeDecodedEntriesViaLiveCommitPath(t, db, 6, []decodedstate.DecodedEntry{hotStepEntry})
	writeDecodedEntriesViaLiveCommitPath(t, db, 8, []decodedstate.DecodedEntry{futureStepEntry})
	require.NoError(t, db.(state.HasAgg).Agg().(*state.Aggregator).BuildFiles(6))

	require.NotEmpty(t, decodedStorageSnapshotFiles(t, dirs), "completed decodedstorage steps should still freeze on the live commit path")
	require.Equal(
		t,
		kv.Step(1),
		decodedStorageStepsInFiles(t, db),
		"live decoded commit-path freezing must stop before the incomplete hot step",
	)
}

func TestS_SNAP_17_RuntimeBuildBackfillsDecodedGapFromFirstMissingStep(t *testing.T) {
	stepSize := uint64(4)
	db, dirs := newDecodedStateExecCtxDB(t, stepSize)
	store := decodedstate.NewStoreFromDB(db)

	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	writeDecodedEntriesDirectly := func(blockNums ...uint64) {
		t.Helper()
		for _, blockNum := range blockNums {
			entry := decodedStateFixtureEntry()
			entry.Keys = []common.Hash{common.BigToHash(new(big.Int).SetUint64(blockNum))}
			entry.Value = common.BigToHash(new(big.Int).SetUint64(blockNum * 10))
			require.NoError(t, store.WriteEntries(blockNum, []decodedstate.DecodedEntry{entry}))
		}
	}

	writeDecodedEntriesDirectly(9, 13, 17, 21, 25)
	primeDecodedPrefixFiles(t, dirs, stepSize, 2)
	primeCoreStateFilesAheadOfDecoded(t, dirs, stepSize, 5)
	require.NoError(t, db.(state.HasAgg).Agg().(*state.Aggregator).OpenFolder())
	require.NoError(t, db.(state.HasAgg).Agg().(*state.Aggregator).BuildFiles(28))

	require.GreaterOrEqual(
		t,
		maxContiguousDecodedDomainStepEnd(decodedStorageDomainFileRanges(t, dirs)),
		uint64(6),
		"runtime snapshot build must backfill decodedstorage from its first missing frozen step instead of leaving a decoded-only gap once core state domains are already farther ahead",
	)
}

func TestS_SNAP_18_LiveDecodedCommitPathBackfillsDecodedGapFromFirstMissingStep(t *testing.T) {
	stepSize := uint64(4)
	db, dirs := newDecodedStateExecCtxDB(t, stepSize)

	for _, blockNum := range []uint64{9, 13, 17, 21, 25} {
		entry := decodedStateFixtureEntry()
		entry.Keys = []common.Hash{common.BigToHash(new(big.Int).SetUint64(blockNum))}
		entry.Value = common.BigToHash(new(big.Int).SetUint64(blockNum * 10))
		writeDecodedEntriesViaLiveCommitPath(t, db, blockNum, []decodedstate.DecodedEntry{entry})
	}
	primeDecodedPrefixFiles(t, dirs, stepSize, 2)
	primeCoreStateFilesAheadOfDecoded(t, dirs, stepSize, 5)
	require.NoError(t, db.(state.HasAgg).Agg().(*state.Aggregator).OpenFolder())
	require.NoError(t, db.(state.HasAgg).Agg().(*state.Aggregator).BuildFiles(28))

	require.GreaterOrEqual(
		t,
		maxContiguousDecodedDomainStepEnd(decodedStorageDomainFileRanges(t, dirs)),
		uint64(6),
		"live decoded commit-path data must rebuild missing decodedstorage frozen steps on the normal runtime build path after core state domains have already advanced farther",
	)
}

func TestS_SNAP_19_StartupTriggerBackfillsDecodedGapBeforeNextCoreStep(t *testing.T) {
	stepSize := uint64(4)
	db, dirs := newDecodedStateExecCtxDB(t, stepSize)
	store := decodedstate.NewStoreFromDB(db)

	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	for _, blockNum := range []uint64{9, 13, 17} {
		entry := decodedStateFixtureEntry()
		entry.Keys = []common.Hash{common.BigToHash(new(big.Int).SetUint64(blockNum))}
		entry.Value = common.BigToHash(new(big.Int).SetUint64(blockNum * 10))
		require.NoError(t, store.WriteEntries(blockNum, []decodedstate.DecodedEntry{entry}))
	}
	primeDecodedPrefixFiles(t, dirs, stepSize, 2)
	primeCoreStateFilesAheadOfDecoded(t, dirs, stepSize, 5)
	require.NoError(t, db.(state.HasAgg).Agg().(*state.Aggregator).OpenFolder())
	require.NoError(t, db.(state.HasAgg).Agg().(*state.Aggregator).BuildFiles(21))

	require.GreaterOrEqual(
		t,
		maxContiguousDecodedDomainStepEnd(decodedStorageDomainFileRanges(t, dirs)),
		uint64(5),
		"startup runtime build must heal decodedstorage gaps even before the next core-domain step becomes build-eligible",
	)
}
