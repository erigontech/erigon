package ethdb

import (
	"bytes"
	"context"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"testing"
	"time"
)

func TestSnapshotGet(t *testing.T) {
	sn1 := NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeaderPrefix: dbutils.BucketConfigItem{},
		}
	}).InMem().MustOpen()
	err := sn1.Update(context.Background(), func(tx Tx) error {
		bucket := tx.Cursor(dbutils.HeaderPrefix)
		innerErr := bucket.Put(dbutils.HeaderKey(1, common.Hash{1}), []byte{1})
		if innerErr != nil {
			return innerErr
		}
		innerErr = bucket.Put(dbutils.HeaderKey(2, common.Hash{2}), []byte{2})
		if innerErr != nil {
			return innerErr
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	sn2 := NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.BlockBodyPrefix: dbutils.BucketConfigItem{},
		}
	}).InMem().MustOpen()
	err = sn2.Update(context.Background(), func(tx Tx) error {
		bucket := tx.Cursor(dbutils.BlockBodyPrefix)
		innerErr := bucket.Put(dbutils.BlockBodyKey(1, common.Hash{1}), []byte{1})
		if innerErr != nil {
			return innerErr
		}
		innerErr = bucket.Put(dbutils.BlockBodyKey(2, common.Hash{2}), []byte{2})
		if innerErr != nil {
			return innerErr
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	mainDB := NewLMDB().InMem().MustOpen()
	err = mainDB.Update(context.Background(), func(tx Tx) error {
		bucket := tx.Cursor(dbutils.HeaderPrefix)
		innerErr := bucket.Put(dbutils.HeaderKey(2, common.Hash{2}), []byte{22})
		if innerErr != nil {
			return innerErr
		}
		innerErr = bucket.Put(dbutils.HeaderKey(3, common.Hash{3}), []byte{33})
		if innerErr != nil {
			return innerErr
		}

		bucket = tx.Cursor(dbutils.BlockBodyPrefix)
		innerErr = bucket.Put(dbutils.BlockBodyKey(2, common.Hash{2}), []byte{22})
		if innerErr != nil {
			return innerErr
		}
		innerErr = bucket.Put(dbutils.BlockBodyKey(3, common.Hash{3}), []byte{33})
		if innerErr != nil {
			return innerErr
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	kv := NewSnapshotKV().For(dbutils.HeaderPrefix, dbutils.BucketConfigItem{}).SnapshotDB(sn1).DB(mainDB).MustOpen()
	kv = NewSnapshotKV().For(dbutils.BlockBodyPrefix, dbutils.BucketConfigItem{}).SnapshotDB(sn2).DB(kv).MustOpen()

	tx, err := kv.Begin(context.Background(), nil, false)
	if err != nil {
		t.Fatal(err)
	}

	v, err := tx.Get(dbutils.HeaderPrefix, dbutils.HeaderKey(1, common.Hash{1}))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte{1}) {
		t.Fatal(v)
	}

	v, err = tx.Get(dbutils.HeaderPrefix, dbutils.HeaderKey(2, common.Hash{2}))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte{22}) {
		t.Fatal(v)
	}

	v, err = tx.Get(dbutils.HeaderPrefix, dbutils.HeaderKey(3, common.Hash{3}))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte{33}) {
		t.Fatal(v)
	}

	v, err = tx.Get(dbutils.BlockBodyPrefix, dbutils.BlockBodyKey(1, common.Hash{1}))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte{1}) {
		t.Fatal(v)
	}

	v, err = tx.Get(dbutils.BlockBodyPrefix, dbutils.BlockBodyKey(2, common.Hash{2}))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte{22}) {
		t.Fatal(v)
	}

	v, err = tx.Get(dbutils.BlockBodyPrefix, dbutils.BlockBodyKey(3, common.Hash{3}))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte{33}) {
		t.Fatal(v)
	}


	headerCursor:=tx.Cursor(dbutils.HeaderPrefix)
	k,v,err:=headerCursor.Last()
	if err!=nil {
		t.Fatal(err)
	}
	if !(bytes.Equal(dbutils.HeaderKey(3, common.Hash{3}), k) && bytes.Equal(v, []byte{33})) {
		t.Fatal(k, v)
	}
	k,v,err=headerCursor.First()
	if !(bytes.Equal(dbutils.HeaderKey(1, common.Hash{1}), k) && bytes.Equal(v, []byte{1})) {
		t.Fatal(k, v)
	}

	k,v,err=headerCursor.Next()
	if !(bytes.Equal(dbutils.HeaderKey(2, common.Hash{2}), k) && bytes.Equal(v, []byte{22})) {
		t.Fatal(k, v)
	}

	k,v,err=headerCursor.Next()
	if !(bytes.Equal(dbutils.HeaderKey(3, common.Hash{3}), k) && bytes.Equal(v, []byte{33})) {
		t.Fatal(k, v)
	}

	k,v,err=headerCursor.Next()
	if !(bytes.Equal([]byte{}, k) && bytes.Equal(v, []byte{})) {
		t.Fatal(k, v)
	}
}


func TestSnapshotWritableTxAndGet(t *testing.T) {
	sn1 := NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeaderPrefix: dbutils.BucketConfigItem{},
		}
	}).InMem().MustOpen()
	err := sn1.Update(context.Background(), func(tx Tx) error {
		bucket := tx.Cursor(dbutils.HeaderPrefix)
		innerErr := bucket.Put(dbutils.HeaderKey(1, common.Hash{1}), []byte{1})
		if innerErr != nil {
			return innerErr
		}
		innerErr = bucket.Put(dbutils.HeaderKey(2, common.Hash{2}), []byte{2})
		if innerErr != nil {
			return innerErr
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	sn2 := NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.BlockBodyPrefix: dbutils.BucketConfigItem{},
		}
	}).InMem().MustOpen()
	err = sn2.Update(context.Background(), func(tx Tx) error {
		bucket := tx.Cursor(dbutils.BlockBodyPrefix)
		innerErr := bucket.Put(dbutils.BlockBodyKey(1, common.Hash{1}), []byte{1})
		if innerErr != nil {
			return innerErr
		}
		innerErr = bucket.Put(dbutils.BlockBodyKey(2, common.Hash{2}), []byte{2})
		if innerErr != nil {
			return innerErr
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	mainDB := NewLMDB().InMem().MustOpen()

	kv := NewSnapshotKV().For(dbutils.HeaderPrefix, dbutils.BucketConfigItem{}).SnapshotDB(sn1).DB(mainDB).MustOpen()
	kv = NewSnapshotKV().For(dbutils.BlockBodyPrefix, dbutils.BucketConfigItem{}).SnapshotDB(sn2).DB(kv).MustOpen()

	tx, err := kv.Begin(context.Background(), nil, true)
	if err != nil {
		t.Fatal(err)
	}

	v, err := tx.Get(dbutils.HeaderPrefix, dbutils.HeaderKey(1, common.Hash{1}))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte{1}) {
		t.Fatal(v)
	}

	v, err = tx.Get(dbutils.BlockBodyPrefix, dbutils.BlockBodyKey(1, common.Hash{1}))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte{1}) {
		t.Fatal(v)
	}

	err = tx.Cursor(dbutils.BlockBodyPrefix).Put(dbutils.BlockBodyKey(4, common.Hash{4}), []byte{2})
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Cursor(dbutils.HeaderPrefix).Put(dbutils.HeaderKey(4, common.Hash{4}), []byte{2})
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Commit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tx, err = kv.Begin(context.Background(), nil, false)
	if err != nil {
		t.Fatal(err)
	}
	c := tx.Cursor(dbutils.HeaderPrefix)
	t.Log(c.First())
	t.Log(c.Next())
	t.Log(c.Next())
	t.Log(c.Next())
	t.Log(c.Next())
	t.Log(c.Next())
}
//time 10m49.228776137s
/*
100000 39.910099ms
200000 61.826629ms
300000 83.042234ms
400000 104.978203ms
500000 126.703575ms
600000 149.099567ms
700000 172.037762ms
800000 197.457778ms
900000 222.776247ms
1000000 248.979594ms
1100000 274.928634ms
1200000 301.537176ms
1300000 328.506465ms
1400000 354.546391ms
1500000 381.912002ms
1600000 411.802059ms
1700000 441.322183ms
1800000 471.573883ms
1900000 500.502055ms
2000000 530.422036ms
2100000 560.067943ms
2200000 1.026550152s
2300000 1.692990255s
2400000 2.427723336s
2500000 3.19375135s
2600000 3.71390306s
2700000 4.456513061s
2800000 5.170786378s
2900000 5.95616417s
3000000 6.696230497s
3100000 7.418426077s
3200000 8.180247342s
3300000 9.001326409s
3400000 9.874638492s
3500000 10.910614023s
3600000 11.947200901s
3700000 13.310301424s
3800000 15.103322172s
3900000 17.14623032s
4000000 20.339145752s

 */
func TestWalk(t *testing.T) {
	db:=MustOpen("/media/b00ris/nvme/tgstaged/tg/chaindata/")

	i:=0
	tt:=time.Now()
	err := db.Walk(dbutils.BlockBodyPrefix, dbutils.EncodeBlockNumber(1), 0, func(k, v []byte) (bool, error) {
		i++
		if i%100000==0 {
			fmt.Println(i, time.Since(tt))
		}
		//blockNumber := binary.BigEndian.Uint64(k[:8])
		//blockHash := common.BytesToHash(k[8:])
		return true, nil
	})
	fmt.Println("time", time.Since(tt))
	if err!=nil {
		t.Fatal(err)
	}
}

/*
Before
GOROOT=/usr/local/go #gosetup
GOPATH=/home/b00ris/go #gosetup
/usr/local/go/bin/go test -c -o /tmp/___TestWalkSnRO_in_github_com_ledgerwatch_turbo_geth_ethdb github.com/ledgerwatch/turbo-geth/ethdb #gosetup
/usr/local/go/bin/go tool test2json -t /tmp/___TestWalkSnRO_in_github_com_ledgerwatch_turbo_geth_ethdb -test.v -test.run ^TestWalkSnRO$ #gosetup
=== RUN   TestWalkSnRO
100000 77.262075ms
200000 183.291986ms
300000 339.563162ms
400000 528.142696ms
500000 695.451206ms
600000 884.39604ms
700000 1.117182365s
800000 1.613314586s
900000 2.034991784s
1000000 2.468412674s
1100000 2.892406556s
1200000 3.346280315s
1300000 3.852970944s
1400000 4.316667161s
1500000 4.985639235s
1600000 5.571371962s
1700000 6.055384904s
1800000 6.605480218s
1900000 7.012082839s
2000000 7.498548897s
2100000 8.024043512s
2200000 8.600844114s
2300000 9.198403518s
2400000 9.926492915s
2500000 10.5640125s
2600000 11.15416078s
2700000 11.699313509s
2800000 12.269008059s
2900000 12.843861684s
3000000 13.347968911s
3100000 13.899501807s
3200000 14.454753015s
3300000 15.175515237s
3400000 16.010320023s
3500000 16.928790456s
3600000 17.882937637s
3700000 18.931279776s
3800000 20.191621875s
3900000 21.786439665s
4000000 25.162983895s
4100000 27.186015306s
4200000 34.35906874s
4300000 44.382486446s
4400000 54.56744236s
4500000 1m4.183879326s
4600000 1m14.093752333s
4700000 1m24.701466304s
4800000 1m35.319264615s
4900000 1m45.957472417s
5000000 1m56.537022677s
5100000 2m7.23161131s
5200000 2m17.790616638s
5300000 2m28.108714364s
5400000 2m36.009467493s
5500000 2m46.830557985s
5600000 2m57.462229904s
5700000 3m7.951242396s
5800000 3m18.401394126s
5900000 3m28.746494976s
6000000 3m39.412438049s
6100000 3m50.168817757s
6200000 3m57.330785109s
6300000 4m8.059985276s
6400000 4m18.399707239s
6500000 4m28.687618127s
6600000 4m38.667010007s
6700000 4m48.835969884s
6800000 4m58.79650357s
6900000 5m9.155444748s
7000000 5m19.572714712s
7100000 5m29.833801013s
7200000 5m39.926656068s
7300000 5m48.951459729s
7400000 5m59.72209688s
7500000 6m10.122473497s
7600000 6m20.40197564s
7700000 6m30.846137101s
7800000 6m40.632146753s
7900000 6m51.081690197s
8000000 7m1.523777011s
8100000 7m11.971128591s
8200000 7m22.193995249s
8300000 7m32.428956344s
8400000 7m42.713077451s
8500000 7m53.18743341s
8600000 8m3.359426284s
8700000 8m13.662925478s
8800000 8m23.943607955s
8900000 8m33.838375792s
9000000 8m43.895859318s
9100000 8m54.298641743s
9200000 9m4.575964265s
9300000 9m14.913859927s
9400000 9m25.249949588s
9500000 9m35.633668136s
9600000 9m45.616713933s
9700000 9m55.145642951s
9800000 10m5.374542287s
9900000 10m15.415551554s
10000000 10m25.31705943s
10100000 10m35.542068158s
10200000 10m45.5527169s
10300000 10m55.911302737s
10400000 11m5.921635009s
10500000 11m15.913481873s
10600000 11m26.227961649s
10700000 11m36.274487802s
10800000 11m46.713289537s
time 11m53.788153658s

Not Readonly
=== RUN   TestWalkSnRO
100000 102.670006ms
200000 242.147104ms
300000 416.145562ms
400000 595.087443ms
500000 757.309961ms
600000 956.008724ms
700000 1.18128446s
800000 1.542510764s
900000 1.910116492s
1000000 2.307357666s
1100000 2.679232437s
1200000 3.160972466s
1300000 3.622765185s
1400000 4.063051304s
1500000 4.708335532s
1600000 5.33312206s
1700000 6.019590308s
1800000 6.638953562s
1900000 7.094739453s
2000000 7.650012603s
2100000 8.24555339s
2200000 8.753004732s
2300000 9.351876308s
2400000 9.993100117s
2500000 10.563331961s
2600000 11.138059528s
2700000 11.633872458s
2800000 12.211165531s
2900000 12.85920053s
3000000 13.474259851s
3100000 14.025788377s
3200000 14.584853493s
3300000 15.262230703s
3400000 16.03849282s
3500000 16.860734024s
3600000 17.67639622s
3700000 18.71849768s
3800000 20.245915797s
3900000 23.346085917s
4000000 25.838722929s
4100000 28.339640757s
4200000 31.900810219s
4300000 42.034447085s
4400000 52.099735113s
4500000 1m1.910871198s
4600000 1m11.981844186s
4700000 1m22.175764528s
4800000 1m32.838954626s
4900000 1m43.369957322s
5000000 1m53.921312032s
5100000 2m4.708673079s
5200000 2m15.22573816s
5300000 2m25.676443472s
5400000 2m36.27373904s
5500000 2m47.070700632s
5600000 2m57.797470499s
5700000 3m8.558877552s
5800000 3m18.82653128s
5900000 3m29.605562246s
6000000 3m40.094555317s
6100000 3m50.636431744s
6200000 3m57.301063546s
6300000 4m8.011171206s
6400000 4m18.094222312s
6500000 4m28.541111595s
6600000 4m38.995702442s
6700000 4m48.975296196s
6800000 4m59.432355835s
6900000 5m9.885159085s
7000000 5m20.299299243s
7100000 5m30.662304895s
7200000 5m36.519784586s
7300000 5m44.708512472s
7400000 5m54.882654841s
7500000 6m5.076145565s
7600000 6m15.69509133s
7700000 6m25.955284708s
7800000 6m36.315361389s
7900000 6m46.619047996s
8000000 6m57.224733383s
8100000 7m7.601737601s
8200000 7m17.885485607s
8300000 7m28.111688295s
8400000 7m38.65784295s
8500000 7m49.018473622s
8600000 7m59.122026186s
8700000 8m9.495255722s
8800000 8m19.561946736s
8900000 8m29.933321897s
9000000 8m40.382550982s
9100000 8m50.835232585s
9200000 9m1.324496088s
9300000 9m11.368066105s
9400000 9m21.303253732s
9500000 9m31.528958582s
9600000 9m41.755380412s
9700000 9m52.018074544s
9800000 10m2.219924241s
9900000 10m12.420043145s
10000000 10m22.870552963s
10100000 10m32.872716346s
10200000 10m43.117519737s
10300000 10m53.436606284s
10400000 11m3.508674125s
10500000 11m13.628644373s
10600000 11m23.612161248s
10700000 11m33.662958861s
10800000 11m43.762269667s
time 11m50.597493005s
 */
func TestWalkSn(t *testing.T) {
	db:=MustOpen("/media/b00ris/nvme/snapshotsync/tg/chaindata/")
	sndb:=MustOpen("/media/b00ris/nvme/snapshotsync/tg/snapshots/bodies/")
	kv:=NewSnapshotKV().For(dbutils.BlockBodyPrefix, dbutils.BucketConfigItem{}).DB(db.kv).SnapshotDB(sndb.kv).MustOpen()
	db.SetKV(kv)
	i:=0
	tt:=time.Now()
	err := db.Walk(dbutils.BlockBodyPrefix, dbutils.EncodeBlockNumber(1), 0, func(k, v []byte) (bool, error) {
		i++
		if i%100000==0 {
			fmt.Println(i, time.Since(tt))
		}
		//blockNumber := binary.BigEndian.Uint64(k[:8])
		//blockHash := common.BytesToHash(k[8:])
		return true, nil
	})
	fmt.Println("time", time.Since(tt))
	if err!=nil {
		t.Fatal(err)
	}
}
func TestWalkSnRO(t *testing.T) {
	db:=MustOpen("/media/b00ris/nvme/snapshotsync/tg/chaindata/")
	snkv:=NewLMDB().ReadOnly().Path("/media/b00ris/nvme/snapshots/bodies_test").MustOpen()
	//sndb:=MustOpen("/media/b00ris/nvme/snapshotsync/tg/snapshots/bodies/")
	kv:=NewSnapshotKV().For(dbutils.BlockBodyPrefix, dbutils.BucketConfigItem{}).DB(db.kv).SnapshotDB(snkv).MustOpen()
	db.SetKV(kv)
	i:=0
	tt:=time.Now()
	err := db.Walk(dbutils.BlockBodyPrefix, dbutils.EncodeBlockNumber(1), 0, func(k, v []byte) (bool, error) {
		i++
		if i%100000==0 {
			fmt.Println(i, time.Since(tt))
		}
		//blockNumber := binary.BigEndian.Uint64(k[:8])
		//blockHash := common.BytesToHash(k[8:])
		return true, nil
	})
	fmt.Println("time", time.Since(tt))
	if err!=nil {
		t.Fatal(err)
	}
}
/*
4200000 3.831713833s
4300000 14.077745316s
4400000 24.723728851s
4500000 34.962839193s
 */
func TestWalkSn2(t *testing.T) {
	db:=MustOpen(" /media/b00ris/nvme/snapshotsync/tg/chaindata/")
	sndb:=MustOpen("/media/b00ris/nvme/snapshotsync/tg/snapshots/bodies/")
	sndb2:=MustOpen("/media/b00ris/nvme/snapshotsync/tg/snapshots/headers/")
	kv:=NewSnapshotKV().For(dbutils.BlockBodyPrefix, dbutils.BucketConfigItem{}).DB(db.kv).SnapshotDB(sndb.kv).MustOpen()
	kv=NewSnapshotKV().For(dbutils.HeaderPrefix, dbutils.BucketConfigItem{}).DB(kv).SnapshotDB(sndb2.kv).MustOpen()
	db.SetKV(kv)
	i:=0
	tt:=time.Now()
	err := db.Walk(dbutils.BlockBodyPrefix, dbutils.EncodeBlockNumber(1), 0, func(k, v []byte) (bool, error) {
		i++
		if i%100000==0 {
			fmt.Println(i, time.Since(tt))
		}
		//blockNumber := binary.BigEndian.Uint64(k[:8])
		//blockHash := common.BytesToHash(k[8:])
		return true, nil
	})
	fmt.Println("time", time.Since(tt))
	if err!=nil {
		t.Fatal(err)
	}
}