package debug

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/lmdb-go/lmdb"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestDebugState(t *testing.T) {
	t.Skip()
	dbNew, err:=ethdb.Open("/media/b00ris/nvme/snsync_test/tg/chaindata/", false)
	if err != nil {
		t.Fatal(err)
	}

	addr := []byte{119, 218, 94, 108, 114, 251, 54, 188, 225, 217, 121, 143, 123, 205, 241, 209, 143, 69, 156, 46}
	t.Log(dbNew.Get(dbutils.PlainStateBucket, addr))
	//i:=0
	err = dbNew.ClearBuckets(dbutils.HashedAccountsBucket, dbutils.ContractCodeBucket,dbutils.HashedStorageBucket, dbutils.TrieOfAccountsBucket, dbutils.TrieOfStorageBucket)
	if err!=nil {
		t.Fatal()
	}

	if err := stages.SaveStageProgress(dbNew, stages.IntermediateHashes, 0); err != nil {
		t.Fatal(err)
	}
	if err := stages.SaveStageUnwind(dbNew, stages.IntermediateHashes, 0); err != nil {
		t.Fatal(err)
	}
	if err := stages.SaveStageProgress(dbNew, stages.HashState, 0); err != nil {
		t.Fatal(err)
	}
	if err := stages.SaveStageUnwind(dbNew, stages.HashState, 0); err != nil {
		t.Fatal(err)
	}
	//dbNew.Walk(dbutils.PlainStateBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
	//	fmt.Println(i, common.Bytes2Hex(k))
	//	i++
	//	return true, nil
	//})
	/*
		dht server on [::]:44419: falling back on starting nodes
		INFO [01-31|14:31:31.589] [6/14 HashState] ETL [1/2] Extracting    from=PLAIN-SCS current key=0000000000b377a927054b13b1b798b345b591a4d22e6562d47ea75a0000000000000001 alloc=561.84MiB sys=1.51GiB numGC=128
		INFO [01-31|14:31:42.382] [6/14 HashState] DONE                    in=1m40.546349661s
		INFO [01-31|14:31:42.382] [7/14 IntermediateHashes] Generating intermediate hashes from=11750432  to=11763530
		INFO [01-31|14:32:38.527] [7/14 IntermediateHashes] Calculating Merkle root current key=09040407...
		curr: 0f0f0f0f0f0302040d0a0a0c0d0906080a0a0a050a0b0f0b070b070d09080703090d000a0d0e0f01000f08080000020109050b040c0c0a00090301010f0f0f0210, succ: 0f0f0f0f0f0302040d0a0a0c0d0906080a0a0a050a0b0f0b070b070d09080703090d000a0d0e0f01000f08080000020109050b040c0c0a00090301010f0f0f0210, maxLen 65, groups: [111111111111111 111111111111111 111111111111111 111111111111111 111111111111111 111], precLen: 5, succLen: 65, buildExtensions: false
		panic: runtime error: index out of range [65] with length 65

		goroutine 47512 [running]:
		github.com/ledgerwatch/turbo-geth/turbo/trie.GenStructStep(0xc0344b7950, 0xc01337c750, 0x41, 0xc1, 0xc02810f290, 0x41, 0x81, 0x16fc640, 0xc00415b200, 0xc006a70d90, ...)
			github.com/ledgerwatch/turbo-geth/turbo/trie/gen_struct_step.go:120 +0x10fe
		github.com/ledgerwatch/turbo-geth/turbo/trie.(*RootHashAggregator).genStructAccount(0xc0015f8a00, 0x50, 0x40)
			github.com/ledgerwatch/turbo-geth/turbo/trie/trie_root.go:705 +0x1ea
		github.com/ledgerwatch/turbo-geth/turbo/trie.(*RootHashAggregator).Receive(0xc0015f8a00, 0xc0344b7b01, 0xc046a681c0, 0x40, 0x40, 0x0, 0x0, 0x0, 0xc0111a1800, 0x0, ...)
			github.com/ledgerwatch/turbo-geth/turbo/trie/trie_root.go:511 +0x985
		github.com/ledgerwatch/turbo-geth/turbo/trie.(*FlatDBTrieLoader).CalcTrieRoot(0xc0111a1680, 0x1700e20, 0xc043602050, 0xc0053358c0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0)
			github.com/ledgerwatch/turbo-geth/turbo/trie/trie_root.go:406 +0x54a
		github.com/ledgerwatch/turbo-geth/eth/stagedsync.incrementIntermediateHashes(0xc032fa4560, 0x17, 0xc01c3ba090, 0x1700e20, 0xc043602050, 0xb37f4a, 0x1, 0xc00021b380, 0x27, 0xe35d3d592a009418, ...)
			github.com/ledgerwatch/turbo-geth/eth/stagedsync/stage_interhashes.go:280 +0x76c
		github.com/ledgerwatch/turbo-geth/eth/stagedsync.SpawnIntermediateHashesStage(0xc01c3ba090, 0x1700d80, 0xc00020eb10, 0x12ac301, 0xc00021b380, 0x27, 0xc0053358c0, 0x0, 0x0)
			github.com/ledgerwatch/turbo-geth/eth/stagedsync/stage_interhashes.go:63 +0x82a
		github.com/ledgerwatch/turbo-geth/eth/stagedsync.DefaultStages.func7.1(0xc01c3ba090, 0x16c8b60, 0xc00417b440, 0x12, 0x16efba0)
			github.com/ledgerwatch/turbo-geth/eth/stagedsync/stagebuilder.go:219 +0x65
		github.com/ledgerwatch/turbo-geth/eth/stagedsync.(*State).runStage(0xc00417b440, 0xc00694bdb0, 0x16efba0, 0xc00020eb10, 0x16efba0, 0xc00020eb10, 0x0, 0x0)
			github.com/ledgerwatch/turbo-geth/eth/stagedsync/state.go:224 +0x155
		github.com/ledgerwatch/turbo-geth/eth/stagedsync.(*State).Run(0xc00417b440, 0x16f5b80, 0xc00020eb10, 0x16f5b80, 0xc00020eb10, 0xc0002fc828, 0x1700d80)
			github.com/ledgerwatch/turbo-geth/eth/stagedsync/state.go:200 +0x348
		github.com/ledgerwatch/turbo-geth/eth/downloader.(*Downloader).syncWithPeer(0xc00034a000, 0xc0078ad680, 0x14c7eca2fa3bd440, 0xc3fda98477e76e72, 0x5a977a09fef4a406, 0xe4da1299ed683533, 0xb37f46, 0xc000236fc0, 0xc013742c30, 0x0, ...)
			github.com/ledgerwatch/turbo-geth/eth/downloader/downloader.go:612 +0xc68
		github.com/ledgerwatch/turbo-geth/eth/downloader.(*Downloader).synchronise(0xc00034a000, 0xc0047fce70, 0x10, 0x14c7eca2fa3bd440, 0xc3fda98477e76e72, 0x5a977a09fef4a406, 0xe4da1299ed683533, 0xb37f46, 0x1, 0xc000236fc0, ...)
			github.com/ledgerwatch/turbo-geth/eth/downloader/downloader.go:461 +0x3a5
		github.com/ledgerwatch/turbo-geth/eth/downloader.(*Downloader).Synchronise(0xc00034a000, 0xc0047fce70, 0x10, 0x14c7eca2fa3bd440, 0xc3fda98477e76e72, 0x5a977a09fef4a406, 0xe4da1299ed683533, 0xb37f46, 0xc000000001, 0xc000236fc0, ...)
			github.com/ledgerwatch/turbo-geth/eth/downloader/downloader.go:371 +0xa6
		github.com/ledgerwatch/turbo-geth/eth.(*ProtocolManager).doSync(0xc001344500, 0xc013a8be80, 0x2193040, 0x16ec7a0)
			github.com/ledgerwatch/turbo-geth/eth/sync.go:315 +0x10f
		github.com/ledgerwatch/turbo-geth/eth.(*chainSyncer).startSync.func1(0xc003eb3320, 0xc013a8be80)
			github.com/ledgerwatch/turbo-geth/eth/sync.go:286 +0x38
		created by github.com/ledgerwatch/turbo-geth/eth.(*chainSyncer).startSync
			github.com/ledgerwatch/turbo-geth/eth/sync.go:286 +0x76


	*/
}

func TestDebug(t *testing.T) {
	//46147
	t.Skip()
	db, err:=ethdb.Open("/media/b00ris/nvme/fresh_sync/tg/chaindata/", true)
	if err != nil {
		t.Fatal(err)
	}

	dbNew, err:=ethdb.Open("/media/b00ris/nvme/snsync_test/tg/chaindata/", true)
	if err != nil {
		t.Fatal(err)
	}
	bKv, err := ethdb.NewLMDB().Flags(func(flags uint) uint { return flags | lmdb.Readonly }).Path("/media/b00ris/nvme/snsync_test/tg/snapshots" + "/bodies").WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return snapshotsync.BucketConfigs[snapshotsync.SnapshotType_bodies]
	}).Open()
	dbSn:=ethdb.NewObjectDatabase(bKv)


	snapshotKV, innerErr := snapshotsync.WrapBySnapshotsFromDir(dbNew.KV(),"/media/b00ris/nvme/snsync_test/tg/snapshots/", snapshotsync.SnapshotMode{
		Headers:  true,
		Bodies:   true,
		State:    true,
		Receipts: false,
	})
	if innerErr != nil {
		t.Fatal(err)
	}
	dbNew.SetKV(snapshotKV)

	blockNumber :=uint64(46147)

	hash, err:=rawdb.ReadCanonicalHash(db, blockNumber)
	if err != nil {
		t.Fatal(err)
	}
	hash2, err:=rawdb.ReadCanonicalHash(dbNew, blockNumber)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(hash.String())
	fmt.Println(hash2.String())

	bb1:=rawdb.ReadStorageBodyRLP(db, hash, blockNumber)
	bR1:=new(types.BodyForStorage)
	err = rlp.DecodeBytes(bb1,bR1)
	if err != nil {
		t.Fatal(err)
	}


	spew.Dump(bR1)

	bl1:=rawdb.ReadBody(db, hash, blockNumber)
	bl2:=rawdb.ReadBody(dbNew, hash, blockNumber)
	bl3:=rawdb.ReadBody(dbSn, hash, blockNumber)
	fmt.Println(reflect.DeepEqual(bl1, bl2), reflect.DeepEqual(bl1, bl3))

	spew.Dump(bl1)
	spew.Dump(bl3)
	spew.Dump(bl2)

	k,_,err:=db.Last(dbutils.EthTx)
	fmt.Println(binary.BigEndian.Uint64(k))
	k,_,err = dbSn.Last(dbutils.EthTx)
	if err!=nil {
		t.Fatal(err)
	}

	fmt.Println(binary.BigEndian.Uint64(k))

	hash3, err:=rawdb.ReadCanonicalHash(dbNew, 11000000)
	if err != nil {
		t.Fatal(err)
	}

	bb2:=rawdb.ReadStorageBodyRLP(dbNew, hash3, 11000000)
	bR2:=new(types.BodyForStorage)
	err = rlp.DecodeBytes(bb2,bR2)
	if err != nil {
		t.Fatal(err)
	}
	spew.Dump(bR2)



	mem:=ethdb.NewMemDatabase()
	snapshotKV, innerErr = snapshotsync.WrapBySnapshotsFromDir(mem.KV(),"/media/b00ris/nvme/snsync_test/tg/snapshots/", snapshotsync.SnapshotMode{
		Headers:  true,
		Bodies:   true,
		State:    true,
		Receipts: false,
	})
	if innerErr != nil {
		t.Fatal(err)
	}
	mem.SetKV(snapshotKV)

	k,_,err = mem.Last(dbutils.EthTx)
	if err!=nil {
		t.Fatal(err)
	}
	fmt.Println("k", k)
	fmt.Println(binary.BigEndian.Uint64(k), err)

	/*
		858581106
		858581106
		(*types.BodyForStorage)(0xc00036de30)({
		BaseTxId: (uint64) 858580934,
				TxAmount: (uint32) 173,
				Uncles: ([]*types.Header) {
			}
		})
	*/
	//b:=rawdb.ReadHeaderRLP(db, hash, 11_000_000)
	//t.Log(hash.String(), len(b))
	//h:=new(types.Header)
	//err = rlp.DecodeBytes(b, h)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//fmt.Println("-----")
	//fmt.Println(common.Bytes2Hex(b))
	//fmt.Println("-----")
	//fmt.Println(db.Get(dbutils.HeaderPrefix, dbutils.HeaderTDKey(11_000_000, hash)))
	//spew.Dump(h)


}

func TestDebug5(t *testing.T) {
	//46147
	t.Skip()
	kv, err := ethdb.NewLMDB().Path("/media/b00ris/nvme/snapshots/headers/").WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeadersBucket:              dbutils.BucketConfigItem{},
			dbutils.HeadersSnapshotInfoBucket: dbutils.BucketConfigItem{},
		}
	}).Flags(func(u uint) uint {
		return u | lmdb.Readonly
	}).Open()
	if err != nil {
		t.Fatal(err)
	}

	kv2:=ethdb.NewSnapshotKV().DB(ethdb.NewLMDB().InMem().MustOpen()).SnapshotDB([]string{dbutils.HeadersBucket}, kv).Open()
	db:=ethdb.NewObjectDatabase(kv2)
	var lastHash common.Hash
	num:=1
	for j:=0; j<5; j++ {
		err=db.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
			header:=new(types.Header)
			err:=rlp.DecodeBytes(v, header)
			if err!=nil {
				return false, err
			}
			for i:=0; i<=num; i++ {
				lastHash = header.Hash()
			}
			return true, nil
		})
		if err!=nil {
			t.Fatal(err)
		}

	}
	fmt.Println(lastHash.String())
}
/*
0x4d5e647b2d8d3a3c8c1561ebb88734bc5fc3c2941016f810cf218738c0ecd99e
--- PASS: TestDebug5 (126.46s)
0x4d5e647b2d8d3a3c8c1561ebb88734bc5fc3c2941016f810cf218738c0ecd99e
--- PASS: TestDebug5 (245.59s)

=== RUN   TestDebug5
Open
0 {[h] 0xc0004c8080}
0x4d5e647b2d8d3a3c8c1561ebb88734bc5fc3c2941016f810cf218738c0ecd99e
--- PASS: TestDebug5 (101.84s)

0x4d5e647b2d8d3a3c8c1561ebb88734bc5fc3c2941016f810cf218738c0ecd99e
--- PASS: TestDebug5 (92.76s)


0x4d5e647b2d8d3a3c8c1561ebb88734bc5fc3c2941016f810cf218738c0ecd99e
--- PASS: TestDebug5 (457.69s)
*/

// 0

func TestCheckValue(t *testing.T) {
	//46147
	t.Skip()
	db, err := ethdb.Open("/media/b00ris/nvme/fresh_sync/tg/chaindata/", true)
	if err != nil {
		t.Fatal(err)
	}
	numOfEmpty:=0
	i:=1000000
	err = db.Walk(dbutils.PlainStateBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if len(k)==20 {
			return true, nil
		}
		if len(v)==0 {
			numOfEmpty++
		}
		i--
		if i==0 {
			fmt.Println(len(k),common.Bytes2Hex(k), len(v))
			i=1000000
		}
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	t.Log(numOfEmpty)
}

func TestWalk(t *testing.T) {
	t.Skip()
	db, err := ethdb.Open("/media/b00ris/nvme/tmp/debug", true)
	kv := db.KV()
	mode:=snapshotsync.SnapshotMode{
		State: true,
	}

	kv, err = snapshotsync.WrapBySnapshotsFromDir(kv, "/media/b00ris/nvme/snapshots/", mode)
	if err != nil {
		t.Fatal(err)
	}

	db.SetKV(kv)

	err  = db.Walk(dbutils.PlainStateBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		fmt.Println(common.Bytes2Hex(k))
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}

}

func TestSizesCheck(t *testing.T) {
	t.Skip()
	//46147
	db, err := ethdb.Open("/media/b00ris/nvme/fresh_sync/tg/chaindata/", true)
	if err != nil {
		t.Fatal(err)
	}
	m:=make(map[int]uint64)
	i:=1000000
	numOfEmpty:=0
	err = db.Walk(dbutils.PlainStateBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		if len(k)==20 {
			return true, nil
		}
		m[len(v)]++
		i--
		if bytes.Equal(v, []byte{0}) {
			numOfEmpty++
		}
		if i==0 {
			fmt.Println(len(k),common.Bytes2Hex(k), len(v))
			i=1000000
		}
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	t.Log(numOfEmpty)
	t.Log(m)
	val, err:=json.Marshal(m)
	fmt.Println(val)
	fmt.Println(err)
}


func TestName(t *testing.T) {
	t.Skip()
	snapshotDir:="/media/b00ris/nvme/snapshots/state115"
	chaindataDir:="/media/b00ris/nvme/fresh_sync/tg/chaindata"
	tmpDbDir:="/media/b00ris/nvme/tmp/debug2"
	tmpDbDir2:="/media/b00ris/nvme/tmp/debug3"
	kv:=ethdb.NewLMDB().Path(snapshotDir).Flags(func(u uint) uint {
		return u|lmdb.Readonly
	}).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return snapshotsync.BucketConfigs[snapshotsync.SnapshotType_state]
	}).MustOpen()

	chaindata,err:=ethdb.Open(chaindataDir, true)
	if err!=nil {
		t.Fatal(err)
	}
	//tmpDb:=ethdb.NewMemDatabase()
	os.RemoveAll(tmpDbDir)
	os.RemoveAll(tmpDbDir2)
	tmpDb,err:=ethdb.Open(tmpDbDir, false)
	if err!=nil {
		t.Fatal(err)
	}
	snkv:=ethdb.NewSnapshotKV().DB(tmpDb.KV()).SnapshotDB([]string{dbutils.PlainStateBucket, dbutils.CodeBucket, dbutils.ContractCodeBucket}, kv).SnapshotDB([]string{dbutils.HeadersBucket, dbutils.HeaderCanonicalBucket, dbutils.HeaderTDBucket, dbutils.HeaderNumberBucket, dbutils.BlockBodyPrefix, dbutils.HeadHeaderKey, dbutils.Senders}, chaindata.KV()).Open()
	db:=ethdb.NewObjectDatabase(snkv)

	tx,err:=db.Begin(context.Background(), ethdb.RW)
	if err!=nil {
		t.Fatal(err)
	}
	blockNum:=uint64(11500001)
	limit:=uint64(1000)
	blockchain, err := core.NewBlockChain(tx, nil, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{
		NoReceipts: true,
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	cc := &core.TinyChainContext{}
	cc.SetDB(tx)
	cc.SetEngine(ethash.NewFaker())

	stateReaderWriter := NewDebugReaderWriter(state.NewPlainStateReader(tx), state.NewPlainStateWriter(tx, tx,blockNum))
	tt1:=time.Now()
	for i:=blockNum; i<blockNum+limit; i++ {
		fmt.Println("exsecuted", i)
		stateReaderWriter.UpdateWriter(state.NewPlainStateWriter(tx, tx, i))

		block, err:=rawdb.ReadBlockByNumber(chaindata, i)
		if err!=nil {
			t.Fatal(err)
		}
		_, err = core.ExecuteBlockEphemerally(blockchain.Config(), blockchain.GetVMConfig(), cc, cc.Engine(), block, stateReaderWriter, stateReaderWriter)
		if err != nil {
			t.Fatal(err)
		}

	}
	tx.Rollback()
	tt2:=time.Now()
	fmt.Println("End")
	spew.Dump("readAcc",len(stateReaderWriter.readAcc))
	spew.Dump("readStr",len(stateReaderWriter.readStorage))
	spew.Dump("createdContracts", len(stateReaderWriter.createdContracts))
	spew.Dump("deleted",len(stateReaderWriter.deletedAcc))

	//checkDB:=ethdb.NewMemDatabase()
	checkDB,err:=ethdb.Open(tmpDbDir2, false)
	if err!=nil {
		t.Fatal(err)
	}


	tt3:=time.Now()
	tx,err=checkDB.Begin(context.Background(), ethdb.RW)
	if err!=nil {
		t.Fatal(err)
	}
	accs:=stateReaderWriter.AllAccounts()
	for i:=range accs {
		v, err:=db.Get(dbutils.PlainStateBucket, i.Bytes())
		if err==nil && len(v)>0 {
			innerErr:=tx.Put(dbutils.PlainStateBucket, i.Bytes(), v)
			if innerErr!=nil {
				t.Fatal(innerErr)
			}
		}
	}
	for i:=range stateReaderWriter.readStorage {
		v, err:=db.Get(dbutils.PlainStateBucket, []byte(i))
		if err==nil && len(v)>0 {
			innerErr:=tx.Put(dbutils.PlainStateBucket, []byte(i), v)
			if innerErr!=nil {
				t.Fatal(innerErr)
			}
		}
	}
	for i:=range stateReaderWriter.readCodes {
		v, err:=db.Get(dbutils.CodeBucket, i.Bytes())
		if err==nil && len(v)>0 {
			innerErr := tx.Put(dbutils.CodeBucket, i.Bytes(), v)
			if innerErr != nil {
				t.Fatal(innerErr)
			}
		}
	}
	err=tx.Commit()
	if err!=nil {
		t.Fatal(err)
	}
	tt4:=time.Now()
	checkSnkv:=ethdb.NewSnapshotKV().DB(checkDB.KV()).SnapshotDB([]string{dbutils.HeadersBucket,dbutils.HeaderCanonicalBucket,dbutils.HeaderTDBucket, dbutils.HeaderNumberBucket, dbutils.BlockBodyPrefix, dbutils.HeadHeaderKey, dbutils.Senders}, chaindata.KV()).Open()
	checkDB.SetKV(checkSnkv)

	tx,err=checkDB.Begin(context.Background(), ethdb.RW)
	if err!=nil {
		t.Fatal(err)
	}
	blockchain, err = core.NewBlockChain(tx, nil, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{
		NoReceipts: true,
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	checkContext := &core.TinyChainContext{}
	checkContext.SetDB(tx)
	checkContext.SetEngine(ethash.NewFaker())

	for i:=blockNum; i<blockNum+limit; i++ {
		fmt.Println("checked", i)
		block, err:=rawdb.ReadBlockByNumber(chaindata, i)
		if err!=nil {
			t.Fatal(err)
		}
		_, err = core.ExecuteBlockEphemerally(blockchain.Config(), blockchain.GetVMConfig(), checkContext, cc.Engine(), block, state.NewPlainStateReader(tx),  state.NewPlainStateWriter(tx, tx,i))
		if err != nil {
			t.Fatal(err)
		}
	}
	err=tx.Commit()
	if err!=nil {
		t.Fatal(err)
	}
	t.Log("exsecution:", tt2.Sub(tt1))
	t.Log("writing to temp db:", tt4.Sub(tt3))
	t.Log("checking:", time.Since(tt4))



}


var _ state.StateReader = &DebugReaderWriter{}
var _ state.WriterWithChangeSets = &DebugReaderWriter{}

func NewDebugReaderWriter(r state.StateReader, w state.WriterWithChangeSets) *DebugReaderWriter {
	return &DebugReaderWriter{
		r:   r,
		w: w,
		readAcc: make(map[common.Address]struct{}),
		readStorage: make(map[string]struct{}),
		readCodes: make(map[common.Hash]struct{}),
		readIncarnations: make(map[common.Address]struct{}),

		updatedAcc: make(map[common.Address]struct{}),
		updatedStorage:make(map[string]struct{}),
		updatedCodes: make(map[common.Hash]struct{}),
		deletedAcc: make(map[common.Address]struct{}),
		createdContracts: make(map[common.Address]struct{}),


	}
}
type DebugReaderWriter struct {
	r state.StateReader
	w state.WriterWithChangeSets
	readAcc map[common.Address]struct{}
	readStorage map[string]struct{}
	readCodes map[common.Hash] struct{}
	readIncarnations map[common.Address] struct{}
	updatedAcc map[common.Address]struct{}
	updatedStorage map[string]struct{}
	updatedCodes map[common.Hash]struct{}
	deletedAcc map[common.Address]struct{}
	createdContracts map[common.Address]struct{}
}
func (d *DebugReaderWriter) UpdateWriter(w state.WriterWithChangeSets) {
	d.w = w
}

func (d *DebugReaderWriter) ReadAccountData(address common.Address) (*accounts.Account, error) {
	d.readAcc[address] = struct{}{}
	return d.r.ReadAccountData(address)
}

func (d *DebugReaderWriter) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	d.readStorage[string(dbutils.PlainGenerateCompositeStorageKey(address.Bytes(),incarnation, key.Bytes()))] = struct{}{}
	return d.r.ReadAccountStorage(address, incarnation, key)
}

func (d *DebugReaderWriter) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	d.readCodes[codeHash] = struct{}{}
	return d.r.ReadAccountCode(address, incarnation, codeHash)
}

func (d *DebugReaderWriter) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	return d.r.ReadAccountCodeSize(address, incarnation, codeHash)
}

func (d *DebugReaderWriter) ReadAccountIncarnation(address common.Address) (uint64, error) {
	d.readIncarnations[address] = struct{}{}
	return d.r.ReadAccountIncarnation(address)
}

func (d *DebugReaderWriter) WriteChangeSets() error {
	return d.w.WriteChangeSets()
}

func (d *DebugReaderWriter) WriteHistory() error {
	return d.w.WriteHistory()
}

func (d *DebugReaderWriter) UpdateAccountData(ctx context.Context, address common.Address, original, account *accounts.Account) error {
	d.updatedAcc[address] = struct{}{}
	return d.w.UpdateAccountData(ctx, address, original, account)
}

func (d *DebugReaderWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	d.updatedCodes[codeHash] = struct{}{}
	return d.w.UpdateAccountCode(address, incarnation, codeHash, code)
}

func (d *DebugReaderWriter) DeleteAccount(ctx context.Context, address common.Address, original *accounts.Account) error {
	d.deletedAcc[address]= struct{}{}
	return d.w.DeleteAccount(ctx, address, original)
}

func (d *DebugReaderWriter) WriteAccountStorage(ctx context.Context, address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	d.updatedStorage[string(dbutils.PlainGenerateCompositeStorageKey(address.Bytes(),incarnation, key.Bytes()))] = struct{}{}
	return d.w.WriteAccountStorage(ctx, address, incarnation, key, original, value)
}

func (d *DebugReaderWriter) CreateContract(address common.Address) error {
	d.createdContracts[address] = struct{}{}
	return d.w.CreateContract(address)
}

func (d *DebugReaderWriter) AllAccounts() map[common.Address]struct{}  {
	accs:=make(map[common.Address]struct{})
	for i:=range d.readAcc {
		accs[i]=struct{}{}
	}
	for i:=range d.updatedAcc {
		accs[i]=struct{}{}
	}
	for i:=range d.readIncarnations {
		accs[i]=struct{}{}
	}
	for i:=range d.deletedAcc {
		accs[i]=struct{}{}
	}
	for i:=range d.createdContracts {
		accs[i]=struct{}{}
	}
	return accs
}
func (d *DebugReaderWriter) AllStorage() map[string]struct{}  {
	st:=make(map[string]struct{})
	for i:=range d.readStorage {
		st[i]=struct{}{}
	}
	for i:=range d.updatedStorage {
		st[i]=struct{}{}
	}
	return st
}
func (d *DebugReaderWriter) AllCodes() map[common.Hash]struct{}  {
	c:=make(map[common.Hash]struct{})
	for i:=range d.readCodes {
		c[i]=struct{}{}
	}
	for i:=range d.updatedCodes {
		c[i]=struct{}{}
	}
	return c
}

func TestCustomHeadersSnapshot(t *testing.T) {
	t.Skip()
	pathKeys:="/media/b00ris/nvme/tmp/bn/index.sn"
	pathData:="/media/b00ris/nvme/tmp/bn/data.sn"
	os.Remove(pathKeys)
	os.Remove(pathData)
	fkeys, err:=os.Create(pathKeys)
	if err!=nil {
		t.Fatal(err)
	}
	fdata, err:=os.Create(pathData)
	if err!=nil {
		t.Fatal(err)
	}
	//_=f

	//keysNum:=0
	kv:=ethdb.NewLMDB().Path("/media/b00ris/nvme/sync115/tg/snapshots/headers/").Flags(func(u uint) uint {
		return u|lmdb.Readonly
	}).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return snapshotsync.BucketConfigs[snapshotsync.SnapshotType_headers]
	}).MustOpen()
	db:=ethdb.NewObjectDatabase(kv)

	b:=make([]byte,8)
	binary.BigEndian.PutUint64(b, 11500000)
	_, err = fkeys.Write(b)
	if err!=nil {
		t.Fatal(err)
	}
	i:=0
	currentDataLength :=uint64(0)
	err = db.Walk(dbutils.HeadersBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		_, err = fkeys.Write(common.CopyBytes(k))
		if err!=nil {
			t.Fatal(err)
		}

		ln:=make([]byte, 8)
		//point in data file
		binary.BigEndian.PutUint64(ln, currentDataLength)
		_, err = fkeys.Write(ln)
		if err!=nil {
			t.Fatal(err)
		}
		//len of data
		binary.BigEndian.PutUint64(ln, uint64(len(v)))
		_, err = fkeys.Write(ln)
		if err!=nil {
			t.Fatal(err)
		}
		currentDataLength +=uint64(len(v))
		_, err = fdata.Write(common.CopyBytes(v))
		if err!=nil {
			t.Fatal(err)
		}
		i++
		if i%10000 == 0 {
			fmt.Println(i, currentDataLength)
		}

		//fmt.Println(common.Bytes2Hex(k))
		//fmt.Println(len(k), len(v))
		//keysNum++
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	//t.Log(keysNum)
}

func TestCustomStateSnapshot(t *testing.T) {
	t.Skip()
	pathState :="/media/b00ris/nvme/tmp/bnstate/state.sn"
	//pathData:="/media/b00ris/nvme/tmp/bnstate/data.sn"
	os.Remove(pathState)
	//os.Remove(pathData)
	fkeys, err:=os.Create(pathState)
	if err!=nil {
		t.Fatal(err)
	}
	//fdata, err:=os.Create(pathData)
	//if err!=nil {
	//	t.Fatal(err)
	//}

	kv:=ethdb.NewLMDB().Path("/media/b00ris/nvme/sync115/tg/snapshots/state/").Flags(func(u uint) uint {
		return u|lmdb.Readonly
	}).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return snapshotsync.BucketConfigs[snapshotsync.SnapshotType_state]
	}).MustOpen()
	db:=ethdb.NewObjectDatabase(kv)
	var i uint64 =0
	var contractsWithStorageNum uint64 =0
	var accounts uint64 =0
	var storage uint64 =0
	prevKey:=[]byte{}

	err = db.Walk(dbutils.PlainStateBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		kkk:=common.CopyBytes(k)
		if len(prevKey)==20 && len(k)>20 {
			contractsWithStorageNum++
			_, err = fkeys.Write(kkk[:28])
			if err!=nil {
				t.Fatal(err)
			}
		}
		if len(k)==20 {
			accounts++
		}
		if len(k)>20 {
			storage++
			kkk=kkk[28:]

		}
		_, err = fkeys.Write(common.CopyBytes(kkk))
		if err!=nil {
			t.Fatal(err)
		}
		_, err = fkeys.Write([]byte{uint8(len(v))})
		if err!=nil {
			t.Fatal(err)
		}

		_, err = fkeys.Write(common.CopyBytes(v))
		if err!=nil {
			t.Fatal(err)
		}
		i++
		if i%1000000 == 0 {
			fmt.Println(i, len(k), common.Bytes2Hex(k), contractsWithStorageNum, accounts, storage)
		}
		prevKey=common.CopyBytes(k)
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	t.Log(contractsWithStorageNum)
	t.Log(i)
	/*
	508000000 60 ff488fd296c38a24cccc60b43dd7254810dab64e000000000000000198264eb92dda6c692c7363b9e3a8061d4b8b3b650599834b5970330da5feb4bc 13002513 112135508 395864492
	509000000 60 ffe02ee4c69edf1b340fcad64fbd6b37a7b9e2650000000000000001f4f5f27cdb929e95d582add19104f5bfdebb05df585b44d17caa79f36d371dca 13032579 112392744 396607256
	    debug_test.go:963: 13038776
	    debug_test.go:964: 509211221
	--- PASS: TestCustomStateSnapshot (2564.29s)
	 */
}

func TestSizesCheck0(t *testing.T) {
	t.Skip()
	for i:=1; i<=32; i++ {
		num:=1
		for j:=1;j<=i; j++ {
			num*=16
		}
		fmt.Println(i, (i+8)*num, (i+8)*num/1024)
	}
}
func TestSizesCheck1(t *testing.T) {
	t.Skip()
	pathWith :="/media/b00ris/nvme/tmp/with.json"
	pathWithout :="/media/b00ris/nvme/tmp/without.json"
	os.Remove(pathWithout)
	os.Remove(pathWith)
	fw,err:=os.Create(pathWith)
	if err!=nil {
		t.Fatal(err)
	}
	fwo,err:=os.Create(pathWithout)
	if err!=nil {
		t.Fatal(err)
	}


	kv:=ethdb.NewLMDB().Path("/media/b00ris/nvme/sync115/tg/snapshots/state/").Flags(func(u uint) uint {
		return u|lmdb.Readonly
	}).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return snapshotsync.BucketConfigs[snapshotsync.SnapshotType_state]
	}).MustOpen()
	db:=ethdb.NewObjectDatabase(kv)
	mpWithoutStorage:=make(map[int]map[string]uint64)
	mpWithStorage:=make(map[int]map[string]uint64)
	numOfBytes:=4
	for i:=0;i<=numOfBytes; i++ {
		mpWithStorage[i]= map[string]uint64{}
		mpWithoutStorage[i]= map[string]uint64{}
	}
	y:=uint64(0)
	err = db.Walk(dbutils.PlainStateBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		kk:=common.Bytes2Hex(k)
		if len(k)==20 {
			for i:=0; i<numOfBytes; i++ {
				mpWithoutStorage[i][kk[:i]]++
			}
		}
		for i:=0; i<numOfBytes; i++ {
			mpWithStorage[i][kk[:i]]++
		}
		y++
		if y%1000000 ==0 {
			fmt.Println(y, common.Bytes2Hex(k))
			fmt.Println()
			for i:=0; i<=numOfBytes; i++ {
				maxNum:=uint64(0)
				for _,v:=range mpWithoutStorage[i] {
					if v>maxNum {
						maxNum=v
					}
				}
				fmt.Println("i", i, maxNum)
			}
		}
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	fmt.Println(len(mpWithStorage))
	fmt.Println(len(mpWithoutStorage))
	err = json.NewEncoder(fw).Encode(mpWithStorage)
	if err!=nil {
		t.Fatal(err)
	}

	err = json.NewEncoder(fwo).Encode(mpWithoutStorage)
	if err!=nil {
		t.Fatal(err)
	}
/*
	509000000 ffe02ee4c69edf1b340fcad64fbd6b37a7b9e2650000000000000001f4f5f27cdb929e95d582add19104f5bfdebb05df585b44d17caa79f36d371dca

	i 0 112392744
	i 1 7271663
	i 2 603207
	i 3 53501
	i 4 0
	5
	5
	--- PASS: TestSizesCheck1 (411.60s)

*/
}

func TestName1(t *testing.T) {
	t.Skip()
	//kv:=ethdb.NewLMDB().Path("/media/b00ris/nvme/sync115/tg/snapshots/headers/").Flags(func(u uint) uint {
	//	return u|lmdb.Readonly
	//}).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
	//	return snapshotsync.BucketConfigs[snapshotsync.SnapshotType_headers]
	//}).Open()
	//db:=ethdb.NewObjectDatabase(kv)


	pathKeys:="/media/b00ris/nvme/tmp/bn/index.sn"
	pathData:="/media/b00ris/nvme/tmp/bn/data.sn"

	fkeys, err:=os.Open(pathKeys)
	if err!=nil {
		t.Fatal(err)
	}
	defer fkeys.Close()
	fdata, err:=os.Open(pathData)
	if err!=nil {
		t.Fatal(err)
	}
	defer fdata.Close()

	b:=make([]byte, 8)
	n,err:=fkeys.Read(b)
	if err!=nil {
		t.Fatal(err)
	}
	max:=binary.BigEndian.Uint64(b)
	key:=make([]byte, 40)
	var point, dataSize uint64
	_=point
	for i:=uint64(1); i < max; i++ {
		_, err=fkeys.Read(key)
		if err!=nil {
			t.Fatal(err)
		}

		//point
		_, err = fkeys.Read(b)
		if err!=nil {
			t.Fatal(err)
		}
		point = binary.BigEndian.Uint64(b)
		//size
		_, err = fkeys.Read(b)
		if err!=nil {
			t.Fatal(err)
		}
		dataSize = binary.BigEndian.Uint64(b)
		//dtOrig, err:=db.Get(dbutils.HeaderPrefix, key)
		//if err!=nil {
		//	t.Fatal(err)
		//}
		//if len(dtOrig)==0 {
		//	t.Log("not found", i)
		//}
		//
		dt:=make([]byte, dataSize)
		//t.Log("point", point, "size", dataSize)
		//_, err=fdata.Seek(int64(point), io.SeekCurrent)
		//if err!=nil {
		//	t.Fatal(err, i, binary.BigEnd;  20 an.Uint64(b))
		//}
		_, err = fdata.Read(dt)
		if err!=nil {
			t.Fatal(err)
		}
		//if bytes.Equal(dtOrig, dt)==false {
		//	t.Log("Body not equal")
		//	t.Log(common.Bytes2Hex(dtOrig))
		//	t.Log(common.Bytes2Hex(dt))
		//}
	}
	t.Log(max)
	t.Log(n)

	//_=db
}

func TestName3(t *testing.T) {
	t.Skip()
	kv:=ethdb.NewLMDB().Path("/media/b00ris/nvme/sync115/tg/snapshots/headers/").Flags(func(u uint) uint {
		return u|lmdb.Readonly
	}).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return snapshotsync.BucketConfigs[snapshotsync.SnapshotType_headers]
	}).MustOpen()
	db:=ethdb.NewObjectDatabase(kv)
	var k1,v1 []byte
	i:=0

	tt:=time.Now()
	err := db.Walk(dbutils.HeadersBucket,[]byte{}, 0, func(k, v []byte) (bool, error) {
		k1,v1 = k, v
		//fmt.Println("k:",common.Bytes2Hex(k))
		//fmt.Println("v:", common.Bytes2Hex(v))
		fmt.Println(len(v))
		//if i>3 {
		//	return false, nil
		//}
		i++
		return true, nil
	})
	if err!=nil {
		t.Fatal(err)
	}
	t.Log(time.Since(tt))
	_=k1
	_=v1
	/*
	--- PASS: TestName3 (14.06s)
	--- PASS: TestName3 (5.28s)
	--- PASS: TestName3 (5.02s)
	--- PASS: TestName3 (4.78s)
	--- PASS: TestName3 (5.03s)




	*/
}

//func TestBinKVCheck(t *testing.T) {
//	kv, err:=ethdb.NewBinKV("/media/b00ris/nvme/tmp/bn")
//	if err!=nil {
//		t.Fatal(err)
//	}
//	defer kv.Close()
//	db:=ethdb.NewObjectDatabase(kv)
//	var k1,v1 []byte
//	kv2:=ethdb.NewLMDB().Path("/media/b00ris/nvme/sync115/tg/snapshots/headers/").Flags(func(u uint) uint {
//		return u|lmdb.Readonly
//	}).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
//		return snapshotsync.BucketConfigs[snapshotsync.SnapshotType_headers]
//	}).Open()
//	tx,err:=kv2.Begin(context.Background(), ethdb.RO)
//	if err!=nil {
//		t.Fatal(err)
//	}
//	c:=tx.Cursor(dbutils.HeaderPrefix)
//	korig, vorig,err:=c.First()
//	if err!=nil {
//		t.Fatal(err)
//	}
//	//fmt.Println("k:",common.Bytes2Hex(k))
//	//fmt.Println("v:", common.Bytes2Hex(v))
//	//i:=0
//	err = db.Walk(dbutils.HeaderPrefix,[]byte{}, 0, func(k, v []byte) (bool, error) {
//		k1,v1 = k, v
//		//fmt.Println("k:",common.Bytes2Hex(k))
//		//fmt.Println("v:", common.Bytes2Hex(v))
//		if !bytes.Equal(k, korig) {
//			t.Log(common.Bytes2Hex(k),common.Bytes2Hex(korig))
//		}
//		if !bytes.Equal(v, vorig) {
//			t.Log("fail value for",common.Bytes2Hex(k), )
//		}
//		//if i>3 {
//		//	return false, nil
//		//}
//		//i++
//		korig, vorig,err = c.Next()
//		if err!=nil {
//			t.Fatal(err)
//		}
//		return true, nil
//	})
//	if err!=nil {
//		t.Fatal(err)
//	}
//	_=k1
//	_=v1
//	_=vorig
//
//}
//func TestBinKVCheckV2(t *testing.T) {
//	kv, err:=ethdb.NewBinKV("/media/b00ris/nvme/tmp/bn")
//	if err!=nil {
//		t.Fatal(err)
//	}
//	defer kv.Close()
//	tx,err:=kv.Begin(context.Background(), ethdb.RO)
//	if err != nil {
//		t.Fatal(err)
//	}
//	//defer tx.Rollback()
//	var k1,v1 []byte
//	kv2:=ethdb.NewLMDB().Path("/media/b00ris/nvme/sync115/tg/snapshots/headers/").Flags(func(u uint) uint {
//		return u|lmdb.Readonly
//	}).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
//		return snapshotsync.BucketConfigs[snapshotsync.SnapshotType_headers]
//	}).Open()
//	tx2,err:=kv2.Begin(context.Background(), ethdb.RO)
//	if err!=nil {
//		t.Fatal(err)
//	}
//
//	c:=tx.Cursor(dbutils.HeaderPrefix)
//	c2:=tx2.Cursor(dbutils.HeaderPrefix)
//	korig, vorig,err:=c2.First()
//	if err!=nil {
//		t.Fatal(err)
//	}
//	k, v,err:=c.First()
//	if err!=nil {
//		t.Fatal(err)
//	}
//
//
//	for ;!(k==nil&&v==nil&&korig==nil&&vorig==nil); {
//
//		if !bytes.Equal(k, korig) {
//			t.Fatal(common.Bytes2Hex(k),common.Bytes2Hex(korig))
//		}
//		if !bytes.Equal(v, vorig) {
//			t.Fatal("fail value for",common.Bytes2Hex(k), )
//		}
//		currK, currV,err:=c.Current()
//		if err!=nil {
//			t.Fatal(err)
//		}
//
//		currOrigK, currOrigV,err:=c2.Current()
//		if err!=nil {
//			t.Fatal(err)
//		}
//
//		if !bytes.Equal(currK, currOrigK) || !bytes.Equal(k, currK) {
//			t.Fatal("current k", common.Bytes2Hex(k),common.Bytes2Hex(korig), common.Bytes2Hex(currOrigK))
//		}
//		if !bytes.Equal(currV, currOrigV) || !bytes.Equal(v, currV) {
//			t.Fatal("current v",common.Bytes2Hex(k), )
//		}
//
//
//	          	k, v,err = c.Next()
//		if err!=nil {
//			t.Fatal(err)
//		}
//		korig, vorig,err = c2.Next()
//		if err!=nil {
//			t.Fatal(err)
//		}
//
//		if bytes.Equal(k, currK) {
//			t.Fatal("current k=next k", common.Bytes2Hex(k),common.Bytes2Hex(korig))
//		}
//
//	}
//	_=k1
//	_=v1
//	_=vorig
//
//}
//func TestBinKV(t *testing.T) {
//	kv, err:=ethdb.NewBinKV("/media/b00ris/nvme/tmp/bn")
//	if err!=nil {
//		t.Fatal(err)
//	}
//	defer kv.Close()
//	db:=ethdb.NewObjectDatabase(kv)
//	var k1,v1 []byte
//	tt:=time.Now()
//	err = db.Walk(dbutils.HeaderPrefix,[]byte{}, 0, func(k, v []byte) (bool, error) {
//		k1,v1 = k, v
//		return true, nil
//	})
//	if err!=nil {
//		t.Fatal(err)
//	}
//	t.Log(time.Since(tt))
//	_=k1
//	_=v1
//	/*
//	--- PASS: TestBinKV (35.39s)
//	--- PASS: TestBinKV (30.20s)
//	after multiple reads (200)
//	debug_test.go:1171: 3.972221805s
//	    debug_test.go:1171: 4.102059313s
//
//	20
//	    debug_test.go:1171: 5.950186325s
//	    debug_test.go:1171: 5.925421281s
//	    debug_test.go:1171: 6.020720685s
//
//50
//	    debug_test.go:1171: 4.74899121s
//	100
//	    debug_test.go:1171: 4.561637034s
//
//
//	lmdb
//		--- PASS: TestName3 (5.28s)
//		--- PASS: TestName3 (5.02s)
//		--- PASS: TestName3 (4.78s)
//		--- PASS: TestName3 (5.03s)
//
//
//
//	*/
//}
//func TestBinFirstAndLast(t *testing.T) {
//	kv, err:=ethdb.NewBinKV("/media/b00ris/nvme/tmp/bn")
//	if err!=nil {
//		t.Fatal(err)
//	}
//	defer kv.Close()
//	var kf1,vf1, kl1,vl1 []byte
//	var kf2,vf2, kl2,vl2 []byte
//
//	err = kv.View(context.Background(), func(tx ethdb.Tx) error {
//		c:=tx.Cursor(dbutils.HeaderPrefix)
//		kf1, vf1,err=c.First()
//		if err!=nil {
//			return err
//		}
//		kl1, vl1,err=c.Last()
//		if err!=nil {
//			return err
//		}
//		return nil
//	})
//	if err!=nil {
//		t.Fatal(err)
//	}
//	kv2:=ethdb.NewLMDB().Path("/media/b00ris/nvme/sync115/tg/snapshots/headers/").Flags(func(u uint) uint {
//		return u|lmdb.Readonly
//	}).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
//		return snapshotsync.BucketConfigs[snapshotsync.SnapshotType_headers]
//	}).Open()
//	kv2.View(context.Background(), func(tx ethdb.Tx) error {
//		c:=tx.Cursor(dbutils.HeaderPrefix)
//		kf2, vf2,err=c.First()
//		if err!=nil {
//			return err
//		}
//		kl2, vl2,err=c.Last()
//		if err!=nil {
//			return err
//		}
//		return nil
//	})
//
//	if !bytes.Equal(kf1, kf2) {
//		t.Fatal()
//	}
//	if !bytes.Equal(vf1, vf2) {
//		t.Fatal()
//	}
//	if !bytes.Equal(kl1, kl2) {
//		t.Fatal()
//	}
//	if !bytes.Equal(vl1, vl2) {
//		t.Fatal()
//	}
//}
//
//func TestCompareWithPostProcessing(t *testing.T) {
//	origDB,err:=ethdb.Open("/media/b00ris/nvme/fresh_sync/tg/chaindata", true)
//	if err!=nil {
//		t.Fatal(err)
//	}
//	hash,err:= rawdb.ReadCanonicalHash(origDB, 0)
//	if err!=nil {
//		t.Fatal(err)
//	}
//	val:=rawdb.ReadHeaderRLP(origDB, hash, 0)
//
//	kv, err:=ethdb.NewBinKV("/media/b00ris/nvme/tmp/bn")
//	if err!=nil {
//		t.Fatal(err)
//	}
//	defer kv.Close()
//
//	kv2:=ethdb.NewLMDB().Path("/media/b00ris/nvme/sync115/tg/snapshots/headers/").Flags(func(u uint) uint {
//		return u|lmdb.Readonly
//	}).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
//		return snapshotsync.BucketConfigs[snapshotsync.SnapshotType_headers]
//	}).Open()
//
//	pathBin:="/media/b00ris/nvme/tmp/TestCompareWithPostProcessingbin"
//	pathLmdb:="/media/b00ris/nvme/tmp/TestCompareWithPostProcessinglmdb"
//	os.RemoveAll(pathBin)
//	os.RemoveAll(pathLmdb)
//
//	db:=ethdb.NewObjectDatabase(ethdb.NewLMDB().Path(pathBin).Open())
//	err = db.Put(dbutils.HeaderPrefix, dbutils.HeaderKey(0, hash), val)
//	if err!=nil {
//		t.Fatal(err)
//	}
//	err = db.Put(dbutils.HeaderPrefix, dbutils.HeaderHashKey(0), hash.Bytes())
//	if err!=nil {
//		t.Fatal(err)
//	}
//	db2:=ethdb.NewObjectDatabase(ethdb.NewLMDB().Path(pathLmdb).Open())
//	err = db2.Put(dbutils.HeaderPrefix, dbutils.HeaderKey(0, hash), val)
//	if err!=nil {
//		t.Fatal(err)
//	}
//	err = db2.Put(dbutils.HeaderPrefix, dbutils.HeaderHashKey(0), hash.Bytes())
//	if err!=nil {
//		t.Fatal(err)
//	}
//
//
//	snkv:=ethdb.NewSnapshotKV().DB(db.KV()).SnapshotDB([]string{dbutils.HeaderPrefix}, kv).SnapshotDB([]string{dbutils.HeadersSnapshotInfoBucket}, kv2).Open()
//	db.SetKV(snkv)
//	tt:=time.Now()
//	err=snapshotsync.GenerateHeaderIndexes(context.Background(), db)
//	if err!=nil {
//		t.Fatal(err)
//	}
//	t.Log("bin", time.Since(tt))
//
//
//
//	snkv2:=ethdb.NewSnapshotKV().DB(db2.KV()).SnapshotDB([]string{dbutils.HeaderPrefix, dbutils.HeadersSnapshotInfoBucket}, kv2).Open()
//	db2.SetKV(snkv2)
//	tt=time.Now()
//	err=snapshotsync.GenerateHeaderIndexes(context.Background(), db2)
//	if err!=nil {
//		t.Fatal(err)
//	}
//	t.Log("lmdb", time.Since(tt))
//
//
//
//	tx,err:=snkv2.Begin(context.Background(), ethdb.RO)
//	c:=tx.Cursor(dbutils.HeaderPrefix)
//	korig, vorig,err:=c.First()
//	if err!=nil {
//		t.Fatal(err)
//	}
//	err = db.Walk(dbutils.HeaderPrefix,[]byte{}, 0, func(k, v []byte) (bool, error) {
//		if !bytes.Equal(k, korig) {
//			t.Fatal(common.Bytes2Hex(k),common.Bytes2Hex(korig))
//		}
//		if !bytes.Equal(v, vorig) {
//			t.Log("fail value for",common.Bytes2Hex(k), )
//		}
//		korig, vorig,err = c.Next()
//		if err!=nil {
//			t.Fatal(err)
//		}
//		return true, nil
//	})
//	 if err!=nil {
//	 	t.Fatal(err)
//	 }
///*
//   debug_test.go:1306: bin 5m59.086414218s
//   debug_test.go:1317: lmdb 6m17.007325735s
//
//    debug_test.go:1306: bin 5m57.108946392s
//    debug_test.go:1317: lmdb 6m4.577944312s
//
//    debug_test.go:1306: bin 5m55.714544551s
//    debug_test.go:1317: lmdb 6m3.122525096s
// */
//}
//
//func TestRandomReadLmdbTest(t *testing.T) {
//	origDB,err:=ethdb.Open("/media/b00ris/nvme/fresh_sync/tg/chaindata", true)
//	if err!=nil {
//		t.Fatal(err)
//	}
//	_=origDB
//	numOfReads:=1000000
//	keysList:=make([][]byte, 0, numOfReads)
//	for i:=0; i<numOfReads; i++ {
//		id:=rand.Int31n(11499998)+1
//		hash, err:=rawdb.ReadCanonicalHash(origDB, uint64(id))
//		if err!=nil {
//			t.Fatal(err)
//		}
//		keysList=append(keysList, dbutils.HeaderKey(uint64(id), hash))
//
//	}
//	//binkv, err:=ethdb.NewBinKV("/media/b00ris/nvme/tmp/bn")
//	//if err!=nil {
//	//	t.Fatal(err)
//	//}
//	//defer binkv.Close()
//	//bindb:=ethdb.NewObjectDatabase(binkv)
//	kv2:=ethdb.NewLMDB().Path("/media/b00ris/nvme/sync115/tg/snapshots/headers/").Flags(func(u uint) uint {
//		return u|lmdb.Readonly
//	}).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
//		return snapshotsync.BucketConfigs[snapshotsync.SnapshotType_headers]
//	}).Open()
//	lmDB:=ethdb.NewObjectDatabase(kv2)
//
//	var v []byte
//	tm:=time.Now()
//	for i,key:=range keysList {
//		v, err = lmDB.Get(dbutils.HeaderPrefix, key)
//		if err!=nil {
//			t.Fatal(err, i, common.Bytes2Hex(key))
//		}
//	}
//	lmdbSn:=time.Since(tm)
//	fmt.Println("lmdb", lmdbSn)
//	//tm=time.Now()
//	//for _,key:=range keysList {
//	//	v, err = bindb.Get(dbutils.HeaderPrefix, key)
//	//	if err!=nil {
//	//		t.Fatal(err)
//	//	}
//	//}
//	//bindbsn:=time.Since(tm)
//	//fmt.Println("bindb", bindbsn)
//	//_=bindb
//	_=lmDB
//	_=v
//}
//func TestRandomReadBinTest(t *testing.T) {
//	origDB,err:=ethdb.Open("/media/b00ris/nvme/fresh_sync/tg/chaindata", true)
//	if err!=nil {
//		t.Fatal(err)
//	}
//	_=origDB
//	numOfReads:=10000000
//	keysList:=make([][]byte, 0, numOfReads)
//	for i:=0; i<numOfReads; i++ {
//		id:=rand.Int31n(11499998)+1
//		hash, err:=rawdb.ReadCanonicalHash(origDB, uint64(id))
//		if err!=nil {
//			t.Fatal(err)
//		}
//		keysList=append(keysList, dbutils.HeaderKey(uint64(id), hash))
//
//	}
//	binkv, err:=ethdb.NewBinKV("/media/b00ris/nvme/tmp/bn")
//	if err!=nil {
//		t.Fatal(err)
//	}
//	defer binkv.Close()
//	bindb:=ethdb.NewObjectDatabase(binkv)
//	//kv2:=ethdb.NewLMDB().Path("/media/b00ris/nvme/sync115/tg/snapshots/headers/").Flags(func(u uint) uint {
//	//	return u|lmdb.Readonly
//	//}).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
//	//	return snapshotsync.BucketConfigs[snapshotsync.SnapshotType_headers]
//	//}).Open()
//	//lmDB:=ethdb.NewObjectDatabase(kv2)
//
//	var v []byte
//	//tm:=time.Now()
//	//for _,key:=range keysList {
//	//	v, err = lmDB.Get(dbutils.HeaderPrefix, key)
//	//	if err!=nil {
//	//		t.Fatal(err)
//	//	}
//	//}
//	//lmdbSn:=time.Since(tm)
//	//fmt.Println("lmdb", lmdbSn)
//	tm:=time.Now()
//	for i,key:=range keysList {
//		v, err = bindb.Get(dbutils.HeaderPrefix, key)
//		if err!=nil {
//			t.Fatal(err, i,  common.Bytes2Hex(key))
//		}
//		//v2,err:=origDB.Get(dbutils.HeaderPrefix, key)
//		//if err!=nil {
//		//	t.Fatal(err)
//		//}
//		//if !bytes.Equal(v, v2) {
//		//	t.Log(common.Bytes2Hex(v))
//		//	t.Log(common.Bytes2Hex(v2))
//		//	t.Fatal()
//		//}
//	}
//	bindbsn:=time.Since(tm)
//	fmt.Println("bindb", bindbsn)
//	_=bindb
//	//_=lmDB
//	_=v
//}
//
//func TestRandomReadBothTest(t *testing.T) {
//	origDB,err:=ethdb.Open("/media/b00ris/nvme/fresh_sync/tg/chaindata", true)
//	if err!=nil {
//		t.Fatal(err)
//	}
//	_=origDB
//	numOfKeys :=1000000
//	numOfCycles:=10
//	keysList:=make([][]byte, 0, numOfKeys)
//	for i:=0; i< numOfKeys; i++ {
//		id:=rand.Int31n(11499998)+1
//		hash, err:=rawdb.ReadCanonicalHash(origDB, uint64(id))
//		if err!=nil {
//			t.Fatal(err)
//		}
//		keysList=append(keysList, dbutils.HeaderKey(uint64(id), hash))
//
//	}
//	binkv, err:=ethdb.NewBinKV("/media/b00ris/nvme/tmp/bn")
//	if err!=nil {
//		t.Fatal(err)
//	}
//	defer binkv.Close()
//	bindb:=ethdb.NewObjectDatabase(binkv)
//	kv2:=ethdb.NewLMDB().Path("/media/b00ris/nvme/sync115/tg/snapshots/headers/").Flags(func(u uint) uint {
//		return u|lmdb.Readonly
//	}).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
//		return snapshotsync.BucketConfigs[snapshotsync.SnapshotType_headers]
//	}).Open()
//	lmDB:=ethdb.NewObjectDatabase(kv2)
//
//	//timingBin:=make([]time.Duration, numOfKeys)
//	//timingLmdb:=make([]time.Duration, numOfKeys)
//	var v1,v2 []byte
//	tm:=time.Now()
//	for j:=0;j<numOfCycles;j++ {
//		for i,key:=range keysList {
//			//tt1:=time.Now()
//			v1, err = bindb.Get(dbutils.HeaderPrefix, key)
//			//timingBin[i]=time.Since(tt1)
//			if err!=nil {
//				t.Fatal(err, i,  common.Bytes2Hex(key))
//			}
//		}
//	}
//	bindbsn:=time.Since(tm)
//	fmt.Println("bindb", bindbsn)
//
//	tm=time.Now()
//	for j:=0;j<numOfCycles;j++ {
//		for i, key := range keysList {
//			//tt1:=time.Now()
//			v2, err = lmDB.Get(dbutils.HeaderPrefix, key)
//			//timingLmdb[i]=time.Since(tt1)
//
//			if err != nil {
//				t.Fatal(err, i, common.Bytes2Hex(key))
//			}
//		}
//	}
//	lmdbSn:=time.Since(tm)
//	fmt.Println("lmdb", lmdbSn)
//
//	//for i,_:=range keysList {
//	//	fmt.Println(i, timingBin[i], timingLmdb[i])
//	//}
//	_=bindb
//	_=lmDB
//	_=v1
//	_=v2
//
//	/**
//	bindb 3.775510273s
//	lmdb 51.829369594s
//	0_o
//	bindb 3.786477907s
//	lmdb 2.69446405s
//	bindb 3.732588105s
//	lmdb 2.688271345s
//	bindb 3.75490917s
//	lmdb 2.661946802s
//	bindb 3.732664452s
//	lmdb 2.703882901s
//
//	1000000 keys x 10 times
//	bindb 35.32278571s
//	lmdb 24.659315931s
//
//	numOfKeys:=1000
//	numOfCycles:=10000
//	bindb 34.037538405s
//	lmdb 24.892599987s
//
//	numOfKeys :=100000
//	numOfCycles:=10000
//	bindb 58m52.936712543s
//	lmdb 41m46.417327109s
//
//
//	numOfKeys :=100000
//	numOfCycles:=10
//	bindb 18.425298557s
//	lmdb 14.996675294s
//
//	numOfKeys :=1000000
//	numOfCycles:=10
//	bindb 1m25.070405663s
//	lmdb 1m38.314928144s
//
//	 */
//	t.Log(os.Getpagesize())
//}
///*
//10000000
//lmdb 1m30.393778034s
//--- PASS: TestRandomReadTest (133.69s)
//lmdb 3m50.243489598s
//lmdb 1m25.326654356s
//
//10000000
//bindb 1m37.088543311s
//
//bindb 2m3.821595905s
//bindb 56.319379803s
//bindb 55.885618029s
//bindb 59.352367268s
//bindb 59.352367268s
//bindb 38.086106054s
//
//
//
//100000
//lmdb 715.672312ms
//bindb 741.688667ms
//
//1000 lmdb 114.472312ms
//1000 bindb 97.731523ms
//
//1000000
//lmdb 18.535345296s
//bindb 35.464465934s
//
//
//*/
//
//func TestWalkState(t *testing.T) {
//	kv:=ethdb.NewLMDB().Path("/media/b00ris/nvme/sync115/tg/snapshots/state/").Flags(func(u uint) uint {
//		return u|lmdb.Readonly
//	}).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
//		return snapshotsync.BucketConfigs[snapshotsync.SnapshotType_state]
//	}).Open()
//	db:=ethdb.NewObjectDatabase(kv)
//	//i:=0
//	var totalAccounts, totalStorage uint64
//	accLenMap:=make(map[int]uint64)
//	strLenMap:=make(map[int]uint64)
//	err:=db.Walk(dbutils.PlainStateBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
//		if !(len(k)==20 || len(k)==60) {
//			t.Fatal(len(k), common.Bytes2Hex(k))
//		}
//		if len(k) ==20 {
//			totalAccounts++
//			accLenMap[len(v)]++
//		}
//		if len(k)==60 {
//			totalStorage++
//			strLenMap[len(v)]++
//		}
//		return true, nil
//	})
//	if err!=nil {
//		t.Fatal(err)
//	}
//	var totalAccValues,totalStrValues uint64
//	for k,v:=range accLenMap {
//		totalAccValues+=uint64(k)*v
//	}
//	for k,v:=range strLenMap {
//		totalStrValues+=uint64(k)*v
//	}
//	t.Log("total acc", totalAccounts, "values size", totalAccValues, totalAccValues/1024/1024)
//	t.Log("total acc", totalStorage, "values size", totalStrValues, totalStrValues/1024/1024)
//	t.Log("total storage", totalStorage)
//
//	v,_:=json.Marshal(accLenMap)
//	t.Log("acc val len", string(v))
//	v2,_:=json.Marshal(strLenMap)
//	t.Log("storage val len", string(v2))
//}
//
//func TestName111(t *testing.T) {
//	totalAccounts := 112447517
//	accValues:=1405139034
//	totalStorage:= 396763704
//	totalStorageValues:=4171375605
//	fmt.Println("accounts hashed:")
//	fmt.Println("keys:  ",(totalAccounts*32)/1024/1024)
//	fmt.Println("values:", (accValues)/1024/1024)
//	fmt.Println("total: ",(totalAccounts*32+accValues)/1024/1024)
//	fmt.Println("accounts plain:")
//	fmt.Println("keys:  ",(totalAccounts*20)/1024/1024)
//	fmt.Println("values:", (accValues)/1024/1024)
//	fmt.Println("total: ",(totalAccounts*20+accValues)/1024/1024)
//	fmt.Println("storage hashed:")
//	fmt.Println("keys:  ",(totalStorage*64)/1024/1024)
//	fmt.Println("values:",(totalStorageValues)/1024/1024)
//	fmt.Println("total: ",(totalStorage*64+totalStorageValues)/1024/1024)
//	fmt.Println("storage plain:")
//	fmt.Println("keys:  ",(totalStorage*52)/1024/1024)
//	fmt.Println("values:",(totalStorageValues)/1024/1024)
//	fmt.Println("total: ",(totalStorage*52+totalStorageValues)/1024/1024)
//
//	fmt.Println("Total hashed:",(totalAccounts*32+accValues)/1024/1024+(totalStorage*64+totalStorageValues)/1024/1024)
//	fmt.Println("Total plain:",(totalAccounts*20+accValues)/1024/1024+(totalStorage*52+totalStorageValues)/1024/1024)
//}
//
///*
//I have a baseline for block 11500. Just only accounts and storage. Minor storage encoding optimisations.
//accounts hashed:
//keys:   3431
//values: 1340
//total:  4771
//accounts plain:
//keys:   2144
//values: 1340
//total:  3484
//storage hashed:
//keys:   24216
//values: 3978
//total:  28194
//storage plain:
//keys:   19675
//values: 3978
//total:  23654
//Total hashed: 32965
//Total plain: 27138
// */
//
//func TestHeaderSize(t *testing.T) {
//	t.Log(common.HashLength*5 + common.AddressLength + 8*3+256+common.HashLength+2*8+8)
//}
//
///*
//
//type Header struct {
//	ParentHash  common.Hash    `json:"parentHash"       gencodec:"required"`
//	UncleHash   common.Hash    `json:"sha3Uncles"       gencodec:"required"`
//	Coinbase    common.Address `json:"miner"            gencodec:"required"`
//	Root        common.Hash    `json:"stateRoot"        gencodec:"required"`
//	TxHash      common.Hash    `json:"transactionsRoot" gencodec:"required"`
//	ReceiptHash common.Hash    `json:"receiptsRoot"     gencodec:"required"`
//	Bloom       Bloom          `json:"logsBloom"        gencodec:"required"`
//	Difficulty  *big.Int       `json:"difficulty"       gencodec:"required"`
//	Number      *big.Int       `json:"number"           gencodec:"required"`
//	GasLimit    uint64         `json:"gasLimit"         gencodec:"required"`
//	GasUsed     uint64         `json:"gasUsed"          gencodec:"required"`
//	Time        uint64         `json:"timestamp"        gencodec:"required"`
//	Extra       []byte         `json:"extraData"        gencodec:"required"`
//	MixDigest   common.Hash    `json:"mixHash"`
//	Nonce       BlockNonce     `json:"nonce"`
//} */
//
//
