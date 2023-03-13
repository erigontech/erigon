package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb/prune"

	"github.com/stretchr/testify/require"
)

func genReceipts(t *testing.T, tx kv.RwTx, blocks uint64) (map[libcommon.Address]uint64, map[libcommon.Hash]uint64) {
	addrs := []libcommon.Address{{1}, {2}, {3}}
	topics := []libcommon.Hash{{1}, {2}, {3}}

	expectAddrs := map[libcommon.Address]uint64{}
	expectTopics := map[libcommon.Hash]uint64{}
	for i := range addrs {
		expectAddrs[addrs[i]] = 0
	}
	for i := range topics {
		expectTopics[topics[i]] = 0
	}

	var receipts types.Receipts
	for i := uint64(0); i < blocks; i++ {
		switch i % 3 {
		case 0:
			a, t1, t2 := addrs[i%3], topics[i%3], topics[(i+1)%3]
			receipts = types.Receipts{{
				Logs: []*types.Log{
					{
						Address: a,
						Topics:  []libcommon.Hash{t1, t2},
					},
					{
						Address: a,
						Topics:  []libcommon.Hash{t2},
					},
					{
						Address: a,
						Topics:  []libcommon.Hash{},
					},
				},
			}}
			expectAddrs[a]++
			expectTopics[t1]++
			expectTopics[t2]++

		case 1:
			a1, a2, t1, t2 := addrs[i%3], addrs[(i+1)%3], topics[i%3], topics[(i+1)%3]
			receipts = types.Receipts{{
				Logs: []*types.Log{
					{
						Address: a1,
						Topics:  []libcommon.Hash{t1, t2, t1, t2},
					},
				},
			}, {
				Logs: []*types.Log{
					{
						Address: a2,
						Topics:  []libcommon.Hash{t1, t2, t1, t2},
					},
					{
						Address: a1,
						Topics:  []libcommon.Hash{t1},
					},
				},
			}}
			expectAddrs[a1]++
			expectAddrs[a2]++
			expectTopics[t1]++
			expectTopics[t2]++
		case 2:
			receipts = types.Receipts{{}, {}, {}}
		}
		err := rawdb.AppendReceipts(tx, i, receipts)
		require.NoError(t, err)
	}
	return expectAddrs, expectTopics
}

func TestPromoteLogIndex(t *testing.T) {
	parallelArgs := []bool{false, true}
	for i := range parallelArgs {
		require, ctx := require.New(t), context.Background()
		db, tx := memdb.NewTestTx(t)

		expectAddrs, expectTopics := genReceipts(t, tx, 100)

		cfg := StageLogIndexCfg(nil, prune.DefaultMode, "")
		cfgCopy := cfg
		cfgCopy.bufLimit = 10
		cfgCopy.flushEvery = time.Nanosecond
		cfgCopy.db = db

		tx = CommitTx(t, db, tx)
		err := promoteLogIndex("logPrefix", tx, 0, 1000, cfgCopy, ctx, parallelArgs[i])
		require.NoError(err)

		// Check indices GetCardinality (in how many blocks they meet)
		for addr, expect := range expectAddrs {
			m, err := bitmapdb.Get(tx, kv.LogAddressIndex, addr[:], 0, 10_000_000)
			require.NoError(err)
			require.Equal(expect, m.GetCardinality())
		}
		for topic, expect := range expectTopics {
			m, err := bitmapdb.Get(tx, kv.LogTopicIndex, topic[:], 0, 10_000_000)
			require.NoError(err)
			require.Equal(expect, m.GetCardinality())
		}
	}
}

func TestPruneLogIndex(t *testing.T) {
	parallelArgs := []bool{false, true}
	for i := range parallelArgs {
		require, tmpDir, ctx := require.New(t), t.TempDir(), context.Background()
		db, tx := memdb.NewTestTx(t)

		_, _ = genReceipts(t, tx, 100)

		cfg := StageLogIndexCfg(nil, prune.DefaultMode, "")
		cfgCopy := cfg
		cfgCopy.bufLimit = 10
		cfgCopy.flushEvery = time.Nanosecond
		cfgCopy.db = db
		tx = CommitTx(t, db, tx)
		err := promoteLogIndex("logPrefix", tx, 0, 99, cfgCopy, ctx, parallelArgs[i])
		require.NoError(err)

		// Mode test
		err = pruneLogIndex("", tx, tmpDir, 50, ctx)
		require.NoError(err)

		{
			total := 0
			err = tx.ForEach(kv.LogAddressIndex, nil, func(k, v []byte) error {
				require.True(binary.BigEndian.Uint32(k[length.Addr:]) == 4294967295)
				total++
				return nil
			})
			require.NoError(err)
			require.True(total == 3)
		}
		{
			total := 0
			err = tx.ForEach(kv.LogTopicIndex, nil, func(k, v []byte) error {
				require.True(binary.BigEndian.Uint32(k[length.Hash:]) == 4294967295)
				total++
				return nil
			})
			require.NoError(err)
			require.True(total == 3)
		}
	}
}

func TestUnwindLogIndex(t *testing.T) {
	parallelArgs := []bool{false, true}
	for i := range parallelArgs {
		require, tmpDir, ctx := require.New(t), t.TempDir(), context.Background()
		db, tx := memdb.NewTestTx(t)

		expectAddrs, expectTopics := genReceipts(t, tx, 100)

		cfg := StageLogIndexCfg(nil, prune.DefaultMode, "")
		cfgCopy := cfg
		cfgCopy.bufLimit = 10
		cfgCopy.flushEvery = time.Nanosecond
		cfgCopy.db = db
		tx = CommitTx(t, db, tx)
		err := promoteLogIndex("logPrefix", tx, 0, 99, cfgCopy, ctx, parallelArgs[i])
		require.NoError(err)

		// Mode test
		err = pruneLogIndex("", tx, tmpDir, 50, ctx)
		require.NoError(err)

		// Unwind test
		err = unwindLogIndex("logPrefix", tx, 70, cfg, nil)
		require.NoError(err)

		for addr := range expectAddrs {
			m, err := bitmapdb.Get(tx, kv.LogAddressIndex, addr[:], 0, 10_000_000)
			require.NoError(err)
			require.True(m.Maximum() <= 700)
		}
		for topic := range expectTopics {
			m, err := bitmapdb.Get(tx, kv.LogTopicIndex, topic[:], 0, 10_000_000)
			require.NoError(err)
			require.True(m.Maximum() <= 700)
		}
	}
}

func TestLogIndexBitmap(t *testing.T) {
	k, _ := hex.DecodeString("0000000000000000000000005b26196305a77b398689eb30cc25913605240836")
	v, _ := hex.DecodeString("3b300000017d004f013c00d5100000da100000dd100200e4100700f6100100f9100100fc100000fe100300151105001c110300211107003c11030041110d007a1101007d11010081110200851105008c111500c2110a00ce110600d6111900f1110a00fd1103000312000032120d0043120100461201005c120000a7120800b1120000b3120100b6120000b8120800c2120000c4120200c8120d00d7121100001301000313130019130c00281305002f130200481300006713000069130300731300007513100087130000891303008e13060096130000981301009e130000ae130800b8130000ba130000bc130100bf130400c5130100c8130100")
	lastChunkBytes, _ := hex.DecodeString("3b300500067c0006007d00f9017e00a5037f00050080000100830002003500000043000000590200003b070000470700004b070000de0df70d0d0e7242754278427d4285008e261f00af260200b32604003d27010040270100432706004b2700004f270500562700007227000074270000762708008027010083270000852703008c2700008e270500952704009b270500a3270300a8270200ac270000ae270200b2270300b7270d00c6270100c9270300ce270200d2271200e6270100e9270400ef270100f2270500f9270400ff27000001280000032809000e28020012280500192802001d28050024280200282814003e28050045280a0051280300562804005c2800005e28010061280300662809007128000073280a007f280100822803008728070090280000ed330000f0330100f3330000483400008234000087340000893400008c3400008f340000913400009534000098340000e83a0000eb3a0000ed3a0000f03a0000f23a0000f43a0000f73a0300fd3a0000003b0000023b0000043b0000063b0000093b00000d3b0000103b0000143b0000173b01001a3b00001c3b00001e3b0100223b0100253b0000283b01002c3b0000303b0000343b0000393b00003d3b0000403b0000423b0000453b0000473b00004d3b0100513b0000573b0100af3b0000f63b0000f83b00004f3c00009a3c0000d43c00000c3d00005d3d0000913d000092460000ba460000944a0000e2810400e8810800f2810100f5810300fa811500118209001c82040022820400288204002e8203003382040039820500408201004382190062820f00738206007b82070038010e1500001215000014150000181501001c1500001e1500002015020024150000271501002a1502002f15050036150100391500003b1503004115020046150000481501004d1501005415000056150000591500005c1500005f150000611500006415010067150000691500006b1501006e1500007015000072150000761501007a1500007c1500007f1501008215000084150000871502008b1502008f1500009215020096150000981503009d150000a2150000aa150000ac150000ae150100b2150100b5150000b7150400bf150000c1150100c5150000c8150100cb150000ce150000d0150100d3150000d5150000d7150000d9150100dd150100e0150100e3150000e5150000e7150000e9150000eb150200ef150200f3150000f5150100f9150000fb150000fd1500000016020004160000071602000b1601000f16040015160200191601001c1605002416000027160000291600002b1600002d1600002f1601003216010035160200391601003c1601003f1600004116000043160200471601004b1601004e1601005116020055160000571602005b1602005f1600006116010064160300691600006b1602006f1600007116020075160100781602007c1600007e160000801600008216010086160100891600008c1603009116010095160100981600009a1600009c160700a5160100a9160200ae160100b1160500b8160100bb160000bd160000c0160200c4160400ca160300cf160100d2160400d8160100dc160000de160400e6160000e8160100eb160100ee160400f4160500fb1604000117000003170100061700000817070011170100141708001e17010021170100241705002b1701002e17060036170100391703003e1704004417000046170100491702004d170400531709005e17060066170000681704006e17040074170b0081170100841709008f1702009317010096170000981703009d170300a2170100a5170200a9170700b3170000b5170000b7170000b9170900c5170200c9170300ce170200d2170000d4170200d9170000db170300e0170300e5170500ec170400f2170000f4170600fc17020000180200041807000d180600151806001d18060025180100281802002c18060034180000361802003a1801003d18050044180300491801004c1802005018020055180200591804004419000057190000eb190800f5190000f7190100fa190200fe190800081a03000d1a0100101a0100171a0000191a0800231a0100261a06002e1a0000301a0100331a000080270000ae270200b2270000b4270100b7270400bd270600c5270300ca270700d3270000d5270900e0270400e6270000e8270400ee270200f2270000f4270300f9270400ff27010002280000042803000928060011280000132806001b2803002028020024280200282807003128010034280300392802003d280200412806004928080053280a005f280800692802006d28060075280100782814008e280300932809009e280300a3280100a6281100b9280100bc280f00cd280300d2280300573100005b3100005d310000603100006331000065310200693104006f31010072310000753104007c3101007f3100008131010085310000883100008a3100008c3100008e310000903100009231000095310000983102009e310000a1310200a53101002432000026320000283201002b3200002e3200003132000098440000969a000061336333653375337a338154091a9a1af70dedd60cd7")
	lastChunk := roaring.New()
	_, err := lastChunk.FromBuffer(lastChunkBytes)
	if err != nil {
		t.Fatal(err)
	}
	// lastChunk.RunOptimize()

	var currentBitmap = roaring.New()
	if _, err := currentBitmap.FromBuffer(v); err != nil {
		t.Fatal(err)
	}

	fmt.Println("lastChunk max", lastChunk.Maximum(), "min", lastChunk.Minimum())
	fmt.Println("currentBitmap max", currentBitmap.Maximum(), "min", currentBitmap.Minimum())
	currentBitmap.Or(lastChunk)

	var buf = bytes.NewBuffer(nil)
	bitmapdb.WalkChunkWithKeys(k, currentBitmap, bitmapdb.ChunkLimit, func(chunkKey []byte, chunk *roaring.Bitmap) error {
		buf.Reset()
		if _, err := chunk.WriteTo(buf); err != nil {
			return err
		}
		fmt.Println("Write", "chunkKey", hex.EncodeToString(chunkKey), "buf", buf.Bytes())
		fmt.Println("chunk max", chunk.Maximum(), "min", chunk.Minimum())
		return nil
	})
}
