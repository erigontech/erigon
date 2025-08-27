package state

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
)

func TestOpenFolder(t *testing.T) {
	// bunch of blocks/txs/headers files...
	// open folder
	// check dirtyFile presence

	dirs, db, log := setupDb(t)
	headerId, header := setupHeader(t, db, log, dirs)
	bodyId, bodies := setupBodies(t, db, log, dirs)

	agg := NewForkableAgg(context.Background(), dirs, db, log)
	agg.RegisterMarkedForkable(header)
	agg.RegisterMarkedForkable(bodies)

	rwtx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwtx.Rollback()

	aggTx := agg.BeginTemporalTx()
	defer aggTx.Close()

	headerTx := aggTx.Marked(headerId)
	bodyTx := aggTx.Marked(bodyId)

	amount := 56

	// populate forkables
	canonicalHashes := fillForkables(t, rwtx, headerTx, bodyTx, amount)

	// check GET
	checkGet := func(headerTx, bodyTx MarkedTxI, rwtx kv.RwTx) {
		for i := range amount {
			HEADER_M, BODY_M := 100, 101
			if i%3 == 0 {
				// just use different value
				HEADER_M, BODY_M = 200, 201
			}
			v, err := headerTx.Get(Num(i), rwtx)
			require.NoError(t, err)
			require.Equal(t, Num(i*HEADER_M).EncTo8Bytes(), v)

			v, err = bodyTx.Get(Num(i), rwtx)
			require.NoError(t, err)
			require.Equal(t, Num(i*BODY_M).EncTo8Bytes(), v)

			chash, err := rwtx.GetOne(kv.HeaderCanonical, RootNum(i).EncTo8Bytes())
			require.NoError(t, err)
			require.Equal(t, canonicalHashes[i], chash)
		}
	}

	checkGet(headerTx, bodyTx, rwtx)

	// create files
	aggTx.Close()
	require.NoError(t, rwtx.Commit())

	rwtx, err = db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwtx.Commit()
	checkGet(headerTx, bodyTx, rwtx)
	rwtx.Commit()

	ch := agg.BuildFiles(RootNum(amount))
	select {
	case <-ch:
	case <-time.After(time.Second * 10):
		t.Fatal("timeout")
	}

	headerF, bodyF := agg.marked[0], agg.marked[1]
	headerItems := headerF.snaps.dirtyFiles.Items()
	bodyItems := bodyF.snaps.dirtyFiles.Items()
	require.Equal(t, 5, len(headerItems))
	require.Equal(t, 5, len(bodyItems))
	for i := range headerItems {
		require.Equal(t, uint64(10*i), headerItems[i].startTxNum)
		require.Equal(t, uint64(10*(i+1)), headerItems[i].endTxNum)
	}
	for i := range bodyItems {
		require.Equal(t, uint64(10*i), bodyItems[i].startTxNum)
		require.Equal(t, uint64(10*(i+1)), bodyItems[i].endTxNum)
	}

	rwtx, err = db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwtx.Commit()

	aggTx = agg.BeginTemporalTx()
	defer aggTx.Close()

	headerTx = aggTx.Marked(headerId)
	bodyTx = aggTx.Marked(bodyId)
	checkGet(headerTx, bodyTx, rwtx)

	// let's try open folder now
	aggTx.Close()
	require.NoError(t, rwtx.Commit())

	agg = NewForkableAgg(context.Background(), dirs, db, log)
	agg.RegisterMarkedForkable(header)
	agg.RegisterMarkedForkable(bodies)

	require.NoError(t, agg.OpenFolder())
	headerF, bodyF = agg.marked[0], agg.marked[1]
	headerItems = headerF.snaps.dirtyFiles.Items()
	bodyItems = bodyF.snaps.dirtyFiles.Items()
	require.Equal(t, 5, len(headerItems))
	require.Equal(t, 5, len(bodyItems))
	for i := range headerItems {
		require.Equal(t, uint64(10*i), headerItems[i].startTxNum)
		require.Equal(t, uint64(10*(i+1)), headerItems[i].endTxNum)
	}
	for i := range bodyItems {
		require.Equal(t, uint64(10*i), bodyItems[i].startTxNum)
		require.Equal(t, uint64(10*(i+1)), bodyItems[i].endTxNum)
	}
}

func TestRecalcVisibleFilesAligned(t *testing.T) {
	// different configurations of forkables - aligned and not aligned

	dirs, db, log := setupDb(t)
	headerId, header := setupHeader(t, db, log, dirs)
	bodyId, bodies := setupBodies(t, db, log, dirs)

	agg := NewForkableAgg(context.Background(), dirs, db, log)
	agg.RegisterMarkedForkable(header)
	agg.RegisterMarkedForkable(bodies)

	rwtx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwtx.Commit()

	amount := 36

	// progress must be aligned
	// so, headers files is 0-1, 1-2, 2-3
	// bodies files is 0-1, 1-2
	// visiblefiles of headers = 0-1, 1-2
	// visiblefiles of bodies = 0-1, 1-2

	aggTx := agg.BeginTemporalTx()
	defer aggTx.Close()
	headerTx, bodyTx := aggTx.Marked(headerId), aggTx.Marked(bodyId)
	fillForkables(t, rwtx, headerTx, bodyTx, amount)

	// create files
	aggTx.Close()
	require.NoError(t, rwtx.Commit())
	ch := agg.BuildFiles(RootNum(amount))
	select {
	case <-ch:
	case <-time.After(time.Second * 10):
		t.Fatal("timeout")
	}

	// delete last bodies file...
	bodiesFiles := agg.marked[1].snaps.dirtyFiles.Items()
	require.Equal(t, 3, len(bodiesFiles))
	lastBodyFile := bodiesFiles[len(bodiesFiles)-1].decompressor.FilePath()
	agg.Close()
	require.NoError(t, dir.RemoveFile(lastBodyFile))

	// now open folder and check visiblefiles
	agg = NewForkableAgg(context.Background(), dirs, db, log)
	agg.RegisterMarkedForkable(header)
	agg.RegisterMarkedForkable(bodies)

	require.NoError(t, agg.OpenFolder())
	headerF, bodyF := agg.marked[0], agg.marked[1]
	headerItems := headerF.snaps.VisibleFiles()
	bodyItems := bodyF.snaps.VisibleFiles()
	require.Equal(t, 2, len(headerItems))
	require.Equal(t, 2, len(bodyItems))

}

func TestRecalcVisibleFilesUnaligned(t *testing.T) {
	dirs, db, log := setupDb(t)
	headerId, header := setupHeader(t, db, log, dirs)
	bodyId, bodies := setupBodies(t, db, log, dirs)
	bodies.unaligned = true

	agg := NewForkableAgg(context.Background(), dirs, db, log)
	agg.RegisterMarkedForkable(header)
	agg.RegisterMarkedForkable(bodies)

	rwtx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwtx.Commit()

	amount := 36

	// can have different unaligned progresses
	// so, headers is 0-1, 1-2, 2-3
	// bodies is 0-1, 1-2
	// visiblefiles of headers = 0-1, 1-2,2-3
	// visiblefiles of bodies = 0-1, 1-2
	// BuildFiles only builds pending bodies

	aggTx := agg.BeginTemporalTx()
	defer aggTx.Close()
	headerTx, bodyTx := aggTx.Marked(headerId), aggTx.Marked(bodyId)
	fillForkables(t, rwtx, headerTx, bodyTx, amount)

	// create files
	aggTx.Close()
	require.NoError(t, rwtx.Commit())
	ch := agg.BuildFiles(RootNum(amount))
	select {
	case <-ch:
	case <-time.After(time.Second * 10):
		t.Fatal("timeout")
	}

	// delete last bodies file...
	bodiesFiles := agg.marked[1].snaps.dirtyFiles.Items()
	require.Equal(t, 3, len(bodiesFiles))
	lastBodyFile := bodiesFiles[len(bodiesFiles)-1].decompressor.FilePath()
	agg.Close()
	require.NoError(t, dir.RemoveFile(lastBodyFile))

	// now open folder and check visiblefiles
	agg = NewForkableAgg(context.Background(), dirs, db, log)
	agg.RegisterMarkedForkable(header)
	agg.RegisterMarkedForkable(bodies)

	require.NoError(t, agg.OpenFolder())
	headerF, bodyF := agg.marked[0], agg.marked[1]
	headerItems := headerF.snaps.VisibleFiles()
	bodyItems := bodyF.snaps.VisibleFiles()
	require.Equal(t, 3, len(headerItems))
	require.Equal(t, 2, len(bodyItems))

	// buildFiles should call 2-3 for bodies, and not for headers
	hfreezer, bfreezer := &inspectingFreezer{t: t, currentFreezer: header.freezer}, &inspectingFreezer{t: t, currentFreezer: bodies.freezer}
	header.freezer = hfreezer
	bodies.freezer = bfreezer
	agg.Close()

	agg = NewForkableAgg(context.Background(), dirs, db, log)
	agg.RegisterMarkedForkable(header)
	agg.RegisterMarkedForkable(bodies)
	require.NoError(t, agg.OpenFolder())

	// nothing for headers
	bfreezer.Expect(20, 30)

	ch = agg.BuildFiles(RootNum(amount))
	select {
	case <-ch:
	case <-time.After(time.Second * 10):
		t.Fatal("timeout")
	}

	hfreezer.Check()
	bfreezer.Check()
}

func TestClose(t *testing.T) {
	// dirty files/visible files should be closed
	dirs, db, log := setupDb(t)
	headerId, header := setupHeader(t, db, log, dirs)
	bodyId, bodies := setupBodies(t, db, log, dirs)
	bodies.unaligned = true

	agg := NewForkableAgg(context.Background(), dirs, db, log)
	agg.RegisterMarkedForkable(header)
	agg.RegisterMarkedForkable(bodies)

	rwtx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwtx.Commit()

	amount := 36
	aggTx := agg.BeginTemporalTx()
	defer aggTx.Close()
	headerTx, bodyTx := aggTx.Marked(headerId), aggTx.Marked(bodyId)
	fillForkables(t, rwtx, headerTx, bodyTx, amount)

	// create files
	aggTx.Close()
	require.NoError(t, rwtx.Commit())
	ch := agg.BuildFiles(RootNum(amount))
	select {
	case <-ch:
	case <-time.After(time.Second * 10):
		t.Fatal("timeout")
	}

	checkRefCnt := func(expected int32) {
		for _, marked := range agg.marked {
			marked.snaps.dirtyFiles.Walk(func(f []*FilesItem) bool {
				for _, f := range f {
					require.Equal(t, expected, f.refcount.Load())
				}
				return true
			})
		}
	}

	checkRefCnt(0)

	aggTx = agg.BeginTemporalTx()
	defer aggTx.Close()
	checkRefCnt(1)
}

func TestRecalcVisibleFiles2(t *testing.T) {
	// ReferencingIntegrityChecker (need to optimise it first -- it does file exist check
	// everytime )
}

func TestForkableAggState(t *testing.T) {
	// check AlignedMaxRootNum, MaxRootNum, HasRootNumUpto
}

func TestMergedFileGet(t *testing.T) {
	// ideally - smallest step file => adduncompressed word (fast build)
	// merged file -- addWord (compressed)
	// this reflects in the GetFiles() as well...ensure that is the case, and correct logic is applied
	// we go with this simple logic..this is not something user should bother with.
	dirs, db, log := setupDb(t)
	headerId, header := setupHeader(t, db, log, dirs)
	bodyId, bodies := setupBodies(t, db, log, dirs)

	agg := NewForkableAgg(context.Background(), dirs, db, log)
	agg.RegisterMarkedForkable(header)
	agg.RegisterMarkedForkable(bodies)

	rwtx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwtx.Rollback()

	aggTx := agg.BeginTemporalTx()
	defer aggTx.Close()

	headerTx := aggTx.Marked(headerId)
	bodyTx := aggTx.Marked(bodyId)

	amount := 200 //rand.Int() % 5000
	t.Logf("amount of headers: %d", amount)

	// populate forkables
	canonicalHashes := fillForkables(t, rwtx, headerTx, bodyTx, amount)

	// check GET
	checkGet := func(headerTx, bodyTx MarkedTxI, rwtx kv.RwTx) {
		for i := range amount {
			HEADER_M, BODY_M := 100, 101
			if i%3 == 0 {
				// just use different value
				HEADER_M, BODY_M = 200, 201
			}
			v, err := headerTx.Get(Num(i), rwtx)
			require.NoError(t, err)
			require.Equal(t, Num(i*HEADER_M).EncTo8Bytes(), v)

			v, err = bodyTx.Get(Num(i), rwtx)
			require.NoError(t, err)
			require.Equal(t, Num(i*BODY_M).EncTo8Bytes(), v)

			chash, err := rwtx.GetOne(kv.HeaderCanonical, RootNum(i).EncTo8Bytes())
			require.NoError(t, err)
			require.Equal(t, canonicalHashes[i], chash)
		}
	}

	checkGet(headerTx, bodyTx, rwtx)

	// create files
	aggTx.Close()
	require.NoError(t, rwtx.Commit())

	rwtx, err = db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwtx.Commit()
	checkGet(headerTx, bodyTx, rwtx)
	rwtx.Commit()

	checkBuildFilesFn := func(mergeDisabled bool) {
		agg.SetMergeDisabled(mergeDisabled)
		for i := range amount {
			ch := agg.BuildFiles(RootNum(i + 1))
			select {
			case <-ch:
			case <-time.After(time.Second * 30):
				t.Fatal("timeout")
			}
		}

		snapCfg := Registry.SnapshotConfig(headerId)
		var nDirtyFiles, nVisibleFiles int
		if mergeDisabled {
			nDirtyFiles = (amount - int(snapCfg.SafetyMargin)) / int(snapCfg.MinimumSize)
			nVisibleFiles = nDirtyFiles
		} else {
			nVisibleFiles = int(calculateNumberOfFiles(uint64(amount), snapCfg))
			nDirtyFiles = nVisibleFiles
		}

		// check dirty files count
		headerF, bodyF := agg.marked[0], agg.marked[1]
		headerItems := headerF.snaps.dirtyFiles.Items()
		bodyItems := bodyF.snaps.dirtyFiles.Items()
		require.Equal(t, nDirtyFiles, len(headerItems))
		require.Equal(t, nDirtyFiles, len(bodyItems))

		// check visiblefiles count
		require.Equal(t, nVisibleFiles, len(headerF.snaps.visibleFiles()))
		require.Equal(t, nVisibleFiles, len(bodyF.snaps.visibleFiles()))

		aggTx = agg.BeginTemporalTx()
		defer aggTx.Close()
		rwtx, err = db.BeginRw(context.Background())
		require.NoError(t, err)
		defer rwtx.Commit()
		headerTx, bodyTx = aggTx.Marked(headerId), aggTx.Marked(bodyId)
		checkGet(headerTx, bodyTx, rwtx)
	}

	checkBuildFilesFn(true)
	checkBuildFilesFn(false)
}

func setupDb(tb testing.TB) (datadir.Dirs, kv.RwDB, log.Logger) {
	tb.Helper()
	logger := log.New()
	dirs := datadir.New(tb.TempDir())
	db := mdbx.New(kv.ChainDB, logger).InMem(dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	return dirs, db, logger
}

func setupHeader(t *testing.T, db kv.RwDB, log log.Logger, dirs datadir.Dirs) (ForkableId, *Forkable[MarkedTxI]) {
	t.Helper()
	headerId := registerEntity(dirs, "headers")

	builder := NewSimpleAccessorBuilder(NewAccessorArgs(true, false), headerId, log,
		WithIndexKeyFactory(NewSimpleIndexKeyFactory()))

	ma, err := NewMarkedForkable(headerId, kv.Headers, kv.HeaderCanonical, IdentityRootRelationInstance, log,
		App_WithPruneFrom(Num(1)),
		App_WithIndexBuilders(builder),
		App_WithUpdateCanonical())
	require.NoError(t, err)

	freezer := &SimpleMarkedFreezer{mfork: ma}
	ma.SetFreezer(freezer)
	t.Cleanup(func() {
		db.Close()
		cleanupFiles(t, ma.snaps, dirs)
	})

	return headerId, ma
}

func setupBodies(t *testing.T, db kv.RwDB, log log.Logger, dirs datadir.Dirs) (ForkableId, *Forkable[MarkedTxI]) {
	t.Helper()
	bodyId := registerEntity(dirs, "bodies")

	builder := NewSimpleAccessorBuilder(NewAccessorArgs(true, false), bodyId, log,
		WithIndexKeyFactory(NewSimpleIndexKeyFactory()))

	ma, err := NewMarkedForkable(bodyId, kv.BlockBody, kv.HeaderCanonical, IdentityRootRelationInstance, log,
		App_WithPruneFrom(Num(1)),
		App_WithIndexBuilders(builder))
	require.NoError(t, err)

	freezer := &SimpleMarkedFreezer{mfork: ma}
	ma.SetFreezer(freezer)
	t.Cleanup(func() {
		db.Close()
		cleanupFiles(t, ma.snaps, dirs)
	})

	return bodyId, ma
}

func registerEntity(dirs datadir.Dirs, name string) ForkableId {
	stepSize := uint64(10)
	return registerEntityWithSnapshotConfig(dirs, name, NewSnapshotConfig(&SnapshotCreationConfig{
		RootNumPerStep: 10,
		MergeStages:    []uint64{80, 160},
		MinimumSize:    10,
		SafetyMargin:   5,
	}, NewE2SnapSchemaWithStep(dirs, name, []string{name}, stepSize)))
}

func registerEntityWithSnapshotConfig(dirs datadir.Dirs, name string, cfg *SnapshotConfig) ForkableId {
	return RegisterForkable(name, dirs, nil, WithSnapshotConfig(cfg))
}

func calculateNumberOfFiles(amount uint64, snapConfig *SnapshotConfig) (nfiles uint64) {
	amount -= snapConfig.SafetyMargin
	for i := len(snapConfig.MergeStages) - 1; i >= 0; i-- {
		mergeStageSize := snapConfig.MergeStages[i]
		nfiles += amount / mergeStageSize
		amount %= mergeStageSize
	}

	nfiles += amount / snapConfig.MinimumSize

	return
}

type TRand struct {
	rnd *rand.Rand
}

func NewTRand() *TRand {
	seed := time.Now().UnixNano()
	src := rand.NewSource(seed)
	return &TRand{rnd: rand.New(src)}
}

func (tr *TRand) RandHash() common.Hash {
	return common.Hash(tr.RandBytes(32))
}

func (tr *TRand) RandBytes(size int) []byte {
	arr := make([]byte, size)
	for i := 0; i < size; i++ {
		arr[i] = byte(tr.rnd.Intn(256))
	}
	return arr
}

func fillForkables(t *testing.T, rwtx kv.RwTx, headerTx, bodyTx MarkedTxI, amount int) (canonicalHashes [][]byte) {
	t.Helper()
	tr := NewTRand()
	canonicalHashes = make([][]byte, amount)

	putter := func(headerTx, bodyTx MarkedTxI, i int, override bool) []byte {
		HEADER_M, BODY_M := 100, 101
		if override {
			// just use different value
			HEADER_M, BODY_M = 200, 201
		}
		hash := tr.RandHash().Bytes()
		err := headerTx.Put(Num(i), hash, Num(i*HEADER_M).EncTo8Bytes(), rwtx)
		require.NoError(t, err)

		err = bodyTx.Put(Num(i), hash, Num(i*BODY_M).EncTo8Bytes(), rwtx)
		require.NoError(t, err)
		return hash
	}

	for i := range amount {
		canonicalHashes[i] = putter(headerTx, bodyTx, i, false)
		if i%3 == 0 {
			canonicalHashes[i] = putter(headerTx, bodyTx, i, true)
		}
	}

	return canonicalHashes
}

type inspectingFreezer struct {
	// in order
	t                    *testing.T
	expectFrom, expectTo []uint64
	currentFreezer       Freezer
}

func (i *inspectingFreezer) Expect(from, to RootNum) {
	i.expectFrom = append(i.expectFrom, uint64(from))
	i.expectTo = append(i.expectTo, uint64(to))
}

func (i *inspectingFreezer) Freeze(ctx context.Context, from, to RootNum, coll Collector, db kv.RoDB) error {
	require.GreaterOrEqual(i.t, len(i.expectFrom), 1)
	require.Equal(i.t, i.expectFrom[0], uint64(from))
	require.Equal(i.t, i.expectTo[0], uint64(to))

	i.expectFrom = i.expectFrom[1:]
	i.expectTo = i.expectTo[1:]

	return i.currentFreezer.Freeze(ctx, from, to, coll, db)
}

func (i *inspectingFreezer) Check() {
	require.Equal(i.t, 0, len(i.expectFrom))
	require.Equal(i.t, 0, len(i.expectTo))
}
