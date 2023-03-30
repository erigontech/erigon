package stagedsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/ethdb/prune"
)

func TestIndexGenerator_GenerateIndex_SimpleCase(t *testing.T) {
	db := kv2.NewTestDB(t)
	cfg := StageHistoryCfg(db, prune.DefaultMode, t.TempDir())
	test := func(blocksNum int, csBucket string) func(t *testing.T) {
		return func(t *testing.T) {
			tx, err := db.BeginRw(context.Background())
			require.NoError(t, err)
			defer tx.Rollback()

			csInfo, ok := historyv2.Mapper[csBucket]
			if !ok {
				t.Fatal("incorrect cs bucket")
			}
			addrs, expecedIndexes := generateTestData(t, tx, csBucket, blocksNum)
			cfgCopy := cfg
			cfgCopy.bufLimit = 10
			cfgCopy.flushEvery = time.Microsecond
			err = promoteHistory("logPrefix", tx, csBucket, 0, uint64(blocksNum/2), cfgCopy, nil)
			require.NoError(t, err)
			err = promoteHistory("logPrefix", tx, csBucket, uint64(blocksNum/2), uint64(blocksNum), cfgCopy, nil)
			require.NoError(t, err)

			checkIndex(t, tx, csInfo.IndexBucket, addrs[0], expecedIndexes[string(addrs[0])])
			checkIndex(t, tx, csInfo.IndexBucket, addrs[1], expecedIndexes[string(addrs[1])])
			checkIndex(t, tx, csInfo.IndexBucket, addrs[2], expecedIndexes[string(addrs[2])])

		}
	}

	t.Run("account plain state", test(2100, kv.AccountChangeSet))
	t.Run("storage plain state", test(2100, kv.StorageChangeSet))

}

func TestIndexGenerator_Truncate(t *testing.T) {
	buckets := []string{kv.AccountChangeSet, kv.StorageChangeSet}
	tmpDir, ctx := t.TempDir(), context.Background()
	kv := kv2.NewTestDB(t)
	cfg := StageHistoryCfg(kv, prune.DefaultMode, t.TempDir())
	for i := range buckets {
		csbucket := buckets[i]

		tx, err := kv.BeginRw(context.Background())
		require.NoError(t, err)
		defer tx.Rollback()

		hashes, expected := generateTestData(t, tx, csbucket, 2100)
		mp := historyv2.Mapper[csbucket]
		indexBucket := mp.IndexBucket
		cfgCopy := cfg
		cfgCopy.bufLimit = 10
		cfgCopy.flushEvery = time.Microsecond
		err = promoteHistory("logPrefix", tx, csbucket, 0, uint64(2100), cfgCopy, nil)
		require.NoError(t, err)

		reduceSlice := func(arr []uint64, timestamtTo uint64) []uint64 {
			pos := sort.Search(len(arr), func(i int) bool {
				return arr[i] > timestamtTo
			})
			return arr[:pos]
		}

		//t.Run("truncate to 2050 "+csbucket, func(t *testing.T) {
		expected[string(hashes[0])] = reduceSlice(expected[string(hashes[0])], 2050)
		expected[string(hashes[1])] = reduceSlice(expected[string(hashes[1])], 2050)
		expected[string(hashes[2])] = reduceSlice(expected[string(hashes[2])], 2050)

		err = unwindHistory("logPrefix", tx, csbucket, 2050, cfg, nil)
		require.NoError(t, err)

		checkIndex(t, tx, indexBucket, hashes[0], expected[string(hashes[0])])
		checkIndex(t, tx, indexBucket, hashes[1], expected[string(hashes[1])])
		checkIndex(t, tx, indexBucket, hashes[2], expected[string(hashes[2])])
		//})

		//t.Run("truncate to 2000 "+csbucket, func(t *testing.T) {
		expected[string(hashes[0])] = reduceSlice(expected[string(hashes[0])], 2000)
		expected[string(hashes[1])] = reduceSlice(expected[string(hashes[1])], 2000)
		expected[string(hashes[2])] = reduceSlice(expected[string(hashes[2])], 2000)

		err = unwindHistory("logPrefix", tx, csbucket, 2000, cfg, nil)
		require.NoError(t, err)

		checkIndex(t, tx, indexBucket, hashes[0], expected[string(hashes[0])])
		checkIndex(t, tx, indexBucket, hashes[1], expected[string(hashes[1])])
		checkIndex(t, tx, indexBucket, hashes[2], expected[string(hashes[2])])
		//})

		//t.Run("truncate to 1999 "+csbucket, func(t *testing.T) {
		err = unwindHistory("logPrefix", tx, csbucket, 1999, cfg, nil)
		require.NoError(t, err)
		expected[string(hashes[0])] = reduceSlice(expected[string(hashes[0])], 1999)
		expected[string(hashes[1])] = reduceSlice(expected[string(hashes[1])], 1999)
		expected[string(hashes[2])] = reduceSlice(expected[string(hashes[2])], 1999)

		checkIndex(t, tx, indexBucket, hashes[0], expected[string(hashes[0])])
		checkIndex(t, tx, indexBucket, hashes[1], expected[string(hashes[1])])
		checkIndex(t, tx, indexBucket, hashes[2], expected[string(hashes[2])])
		bm, err := bitmapdb.Get64(tx, indexBucket, hashes[0], 1999, math.MaxUint32)
		require.NoError(t, err)
		if bm.GetCardinality() > 0 && bm.Maximum() > 1999 {
			t.Fatal(bm.Maximum())
		}
		bm, err = bitmapdb.Get64(tx, indexBucket, hashes[1], 1999, math.MaxUint32)
		require.NoError(t, err)
		if bm.GetCardinality() > 0 && bm.Maximum() > 1999 {
			t.Fatal()
		}
		//})

		//t.Run("truncate to 999 "+csbucket, func(t *testing.T) {
		expected[string(hashes[0])] = reduceSlice(expected[string(hashes[0])], 999)
		expected[string(hashes[1])] = reduceSlice(expected[string(hashes[1])], 999)
		expected[string(hashes[2])] = reduceSlice(expected[string(hashes[2])], 999)

		err = unwindHistory("logPrefix", tx, csbucket, 999, cfg, nil)
		if err != nil {
			t.Fatal(err)
		}
		bm, err = bitmapdb.Get64(tx, indexBucket, hashes[0], 999, math.MaxUint32)
		require.NoError(t, err)
		if bm.GetCardinality() > 0 && bm.Maximum() > 999 {
			t.Fatal()
		}
		bm, err = bitmapdb.Get64(tx, indexBucket, hashes[1], 999, math.MaxUint32)
		require.NoError(t, err)
		if bm.GetCardinality() > 0 && bm.Maximum() > 999 {
			t.Fatal()
		}

		checkIndex(t, tx, indexBucket, hashes[0], expected[string(hashes[0])])
		checkIndex(t, tx, indexBucket, hashes[1], expected[string(hashes[1])])
		checkIndex(t, tx, indexBucket, hashes[2], expected[string(hashes[2])])

		//})
		err = pruneHistoryIndex(tx, csbucket, "", tmpDir, 128, ctx)
		assert.NoError(t, err)
		expectNoHistoryBefore(t, tx, csbucket, 128)

		// double prune is safe
		err = pruneHistoryIndex(tx, csbucket, "", tmpDir, 128, ctx)
		assert.NoError(t, err)
		expectNoHistoryBefore(t, tx, csbucket, 128)
		tx.Rollback()
	}
}

func expectNoHistoryBefore(t *testing.T, tx kv.Tx, csbucket string, prunedTo uint64) {
	prefixLen := length.Addr
	if csbucket == kv.StorageChangeSet {
		prefixLen = length.Hash
	}
	afterPrune := 0
	err := tx.ForEach(historyv2.Mapper[csbucket].IndexBucket, nil, func(k, _ []byte) error {
		n := binary.BigEndian.Uint64(k[prefixLen:])
		require.True(t, n >= prunedTo)
		afterPrune++
		return nil
	})
	require.True(t, afterPrune > 0)
	assert.NoError(t, err)
}

func generateTestData(t *testing.T, tx kv.RwTx, csBucket string, numOfBlocks int) ([][]byte, map[string][]uint64) { //nolint
	csInfo, ok := historyv2.Mapper[csBucket]
	if !ok {
		t.Fatal("incorrect cs bucket")
	}
	var isPlain bool
	if kv.StorageChangeSet == csBucket || kv.AccountChangeSet == csBucket {
		isPlain = true
	}
	addrs, err := generateAddrs(3, isPlain)
	require.NoError(t, err)
	if kv.StorageChangeSet == csBucket {
		keys, innerErr := generateAddrs(3, false)
		require.NoError(t, innerErr)

		defaultIncarnation := make([]byte, 8)
		binary.BigEndian.PutUint64(defaultIncarnation, uint64(1))
		for i := range addrs {
			addrs[i] = append(addrs[i], defaultIncarnation...)
			addrs[i] = append(addrs[i], keys[i]...)
		}
	}

	res := make([]uint64, 0)
	res2 := make([]uint64, 0)
	res3 := make([]uint64, 0)

	for i := 0; i < numOfBlocks; i++ {
		cs := csInfo.New()
		err = cs.Add(addrs[0], []byte(strconv.Itoa(i)))
		require.NoError(t, err)

		res = append(res, uint64(i))

		if i%2 == 0 {
			err = cs.Add(addrs[1], []byte(strconv.Itoa(i)))
			require.NoError(t, err)
			res2 = append(res2, uint64(i))
		}
		if i%3 == 0 {
			err = cs.Add(addrs[2], []byte(strconv.Itoa(i)))
			require.NoError(t, err)
			res3 = append(res3, uint64(i))
		}
		err = csInfo.Encode(uint64(i), cs, func(k, v []byte) error {
			return tx.Put(csBucket, k, v)
		})
		require.NoError(t, err)
	}

	return addrs, map[string][]uint64{
		string(addrs[0]): res,
		string(addrs[1]): res2,
		string(addrs[2]): res3,
	}
}

func checkIndex(t *testing.T, db kv.Tx, bucket string, k []byte, expected []uint64) {
	t.Helper()
	k = dbutils.CompositeKeyWithoutIncarnation(k)
	m, err := bitmapdb.Get64(db, bucket, k, 0, math.MaxUint32)
	if err != nil {
		t.Fatal(err, common.Bytes2Hex(k))
	}
	val := m.ToArray()
	if !reflect.DeepEqual(val, expected) {
		fmt.Printf("get     : %v\n", val)
		fmt.Printf("expected: %v\n", toU32(expected))
		t.Fatal()
	}
}

func toU32(in []uint64) []uint32 {
	out := make([]uint32, len(in))
	for i := range in {
		out[i] = uint32(in[i])
	}
	return out
}

func generateAddrs(numOfAddrs int, isPlain bool) ([][]byte, error) {
	addrs := make([][]byte, numOfAddrs)
	for i := 0; i < numOfAddrs; i++ {
		key1, err := crypto.GenerateKey()
		if err != nil {
			return nil, err
		}
		addr := crypto.PubkeyToAddress(key1.PublicKey)
		if isPlain {
			addrs[i] = addr.Bytes()
			continue
		}
		hash, err := common.HashData(addr.Bytes())
		if err != nil {
			return nil, err
		}
		addrs[i] = hash.Bytes()
	}
	return addrs, nil
}

func TestRoaringCanParseRealDBValue(t *testing.T) {
	v := common.FromHex("0100000000000000000000003a30000001000000c700c00310000000c02c092e162e55315a315b315c310932103214321d321f32223226322d323c325a325b326132623267326c327332753278327f32853287329932ab32af32bb32c332c732c832cf32d232d732d832db32e232e532e932f032f632fe320f331333173325332e3333333433363344334e33583369336e337d338a3391339933a733ab33b033b533c133cb33d133ff3301340c34143421342e3434343a345f346734733476347d348734893497349a34a134a534b834ce34d534dc34df34eb34ed34f334f634023505350c3513353e3546354a354f355a356235713576357a358d35913598359e35a535bf3501361c362036243625362c3634363c36453648365636643665366b367036713679367c3682369f36a336b336be36cc36d436db3609370f37433759375d3766378337983799379c37a837a937af37b137b837bb37c037c337c737d537db37e037f3371c38263828382d3832383c383e3846384d3852386f3875387b388138843889388b389038b438c038c338ce38d738de38e638f8380e39273933393d394d39793981398639a839b639c039c339e039f239043a0f3a1f3a243a273a2a3a2b3a453a563a663a693a793a7d3a833a8b3ac43add3af63a033b0e3b283b2f3b413b443b5c3b803b8e3ba03bae3bc73bd03be03b0c3c133c1d3c1e3c333c3c3c3f3c4f3c693c7c3c933cbe3cca3cd93cdd3ce13ce93cf53c063d0b3d173d1d3d233d283d3d3d423d453d4d3d513d563d5e3d6a3d6e3d943dc43dd63de03df43df73d273e7c3ea13ea63edd3ee33ef23e423f4f3fcf40d340d940e440ed40f24000410b4116411a414f41694172417f4188418a41a041aa41c541cc41d241d841e541f341fc410e4214421e4224422a423042454258427f4281429042b742bf42c642d742e642eb42f042f642024310431443184323432b433043354338433e4343434f4355435943614366436e43704373437a437d438143854387438f4392439e43a843b843c643ce43d343dd43f043fb430d44124419441e44214425442e442f4433443b44584460446844744478447a448c449244a444a844b744c344cf44d944e744ec4407451145264541454c4576458545a045ad45b345bd45e145ea45f34505460c46114614461946494654465d4668466f4675467a467e4687469446ae46ba46c146c646c846d446da46de46ec46fb4602470b4716471f4727472a473147384771479e47c147d347d947ff487449824987498949c049c449d949eb49ff49094a0b4a0d4a8a4b934b974bb54bbc4bc04bc14bc64bc74bca4bd14bd54bdf4be24be34be74bee4bf04bf54b014c064c0a4c174c1e4c224c294c354c394c3e4c404c454c504c554c5b4c5d4c5e4c754c7f4c814c844c894c904c9b4ca44cab4cae4cb24cb44cc64cc84ccb4ccd4cd14cd64ceb4ced4cf84cfe4c054d0c4d124d154d194d224d2b4d334d3c4d4d4d574d5d4d604d674d704d734d794d804d864d954d984d9e4da14dac4dbb4dc14dc44dcc4dcf4dd54ddd4deb4dec4df04df74d094e114e144e294e2f4e364e394e404e484e514e594e5d4e684e6c4e704e754e774e814e8d4e954ea34ea94eaa4ead4eb34eb54ebb4ec54ec94ed14ed74ee04ee44ee94eeb4ef44efe4e014f054f124f1e4f304f334f374f394f3a4f4a4f5a4f604f674f6b4f754f774f7f4f8f4f954f9a4f9c4fa34fbf4fc84fe64ff04ffe4f06501f502d50305036503f504750525056505c50645068506a506e5073507b5088508a5094509a50a050a250af50b250b650cf50d650eb50f650f750fd50035105511451175119511c5125512a512c5135513d514351455153515a515f517651775178517b518051835186518b519151a251ad51b851bc51ca51cc51df51e251e651e851ed51ee51ef51fc51075224522952335234523d5248524c524f52505255525c526c5273527c528552aa52bb52c952d552db52e152e852f1520d533f5343534a535a5363536d5374537a53835386538b5396539e53a453ad539e54fe5448566656a558f6583f597f599b59b559dd59055a1e5a9b5afa5a045b0e5b175b2c5b315b335b395bf65b145c465c525c5d5c615c6b5c875c8d5cb45cba5cec5cf25cf75c055d585ea15ee55ebc5fee5f016019603a60426059605b609760a860d36015613c617061bc61c661d461e561ed61f36152626762a262bc62c162c562d462de62076330633c635f638063826388638d63cf63f363fa6311642e64cb64ea641765216538653e655365ba65bd65c965cf65d465d865d965dc65de65e565ea65ed65f465f965fc6500660c660f661a662b66366645664b664d6655667066776682668b66906692669366ad66b366b766b966d266d566fb66ff6604670867186723673767436749674d67526757676467af67e56716681e6825682c6835683c683e684e6855685c685d6867686f6873687a68e068236b4c6b8a6bcc6bdc6bde6bed6c1d6f236f596f6e6fbc6f8c708f7093709b70a27021714b71eb731b75377548755c7581759d75a575ab75b175237648764c765b76697682769376c176d6762c7740775f776d778777c977d477ef77f277ff7707780e78207821782e7833783b783f784d784f7853785a78717872787f7883788d7894789d78a078a378c578c978d078d378d778e678e878ea78f278f77809790c79")
	m := roaring64.New()
	err := m.UnmarshalBinary(v)
	require.NoError(t, err)
	require.Equal(t, uint64(961), m.GetCardinality())
	encoded, err := m.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, encoded, v)
}
