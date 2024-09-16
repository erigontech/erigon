package txpool

import (
	"context"
	"math/big"
	"os"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/gateway-fm/cdk-erigon-lib/kv/kvcache"
	"github.com/gateway-fm/cdk-erigon-lib/kv/mdbx"
	"github.com/gateway-fm/cdk-erigon-lib/txpool/txpoolcfg"
	"github.com/gateway-fm/cdk-erigon-lib/types"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/semaphore"
	"gotest.tools/v3/assert"
)

func Test_Persistency(t *testing.T) {
	dbPath := "/tmp/limbo-persistency"

	pSource := store(t, dbPath)

	restoreRo(t, dbPath, pSource)

	t.Cleanup(func() {
		os.RemoveAll(dbPath)
	})
}

func store(t *testing.T, dbPath string) *TxPool {
	db, tx, aclDb := initDb(t, dbPath, true)
	defer db.Close()
	defer tx.Rollback()
	defer aclDb.Close()

	newTxs := make(chan types.Announcements, 1024)
	defer close(newTxs)

	ethCfg := &ethconfig.Defaults
	ethCfg.Zk.Limbo = true

	pSource, err := New(make(chan types.Announcements), db, txpoolcfg.DefaultConfig, ethCfg, kvcache.NewDummy(), *uint256.NewInt(1101), big.NewInt(0), big.NewInt(0), aclDb)
	assert.NilError(t, err)

	parseCtx := types.NewTxParseContext(pSource.chainID)
	parseCtx.WithSender(false)

	pSource.limbo.invalidTxsMap["C4EDA275BD549EB9FB35DFFD867ADB346EC56E0A3A1CAF49C7804715A8A3019B"] = 0
	pSource.limbo.invalidTxsMap["FA5855E800870163CE572020268728703404BE78B3F13D772867127FFF27A6B6"] = 1

	txn := &types.TxSlot{}
	_, err = parseCtx.ParseTransaction(tx01Rlp, 0, txn, nil, false /* hasEnvelope */, nil)
	assert.NilError(t, err)
	senderId, _ := pSource.senders.getOrCreateID(common.BytesToAddress(tx01Sender))
	txn.SenderID = senderId
	pSource.limbo.limboSlots.Append(txn, tx01Sender, true)

	txn = &types.TxSlot{}
	_, err = parseCtx.ParseTransaction(tx02Rlp, 0, txn, nil, false /* hasEnvelope */, nil)
	assert.NilError(t, err)
	senderId, _ = pSource.senders.getOrCreateID(common.BytesToAddress(tx02Sender))
	txn.SenderID = senderId
	pSource.limbo.limboSlots.Append(txn, tx02Sender, true)

	pSource.limbo.uncheckedLimboBlocks = append(pSource.limbo.uncheckedLimboBlocks, &LimboBlockDetails{
		Witness:                 []byte{1, 4, 1},
		L1InfoTreeMinTimestamps: map[uint64]uint64{5: 6},
		BlockTimestamp:          51,
		BlockNumber:             14,
		BatchNumber:             5131,
		ForkId:                  11,
		Transactions: []*LimboBlockTransactionDetails{
			&LimboBlockTransactionDetails{
				Rlp:         []byte{100, 41, 151, 141, 13},
				StreamBytes: []byte{5, 15},
				Root:        common.BytesToHash([]byte{1, 2, 4, 134, 41, 15, 19, 105, 10, 214, 1, 2, 4, 134, 41, 15, 19, 105, 10, 214, 1, 2, 4, 134, 41, 15, 19, 105, 10, 214, 255, 0}),
				Hash:        common.HexToHash("FE4CEE305D5DF787F55057A0F37DEB0F5170A1E9BB361CFD1638BBEAFE66EE4B"),
				Sender:      common.HexToAddress("0x510b131a0b61aeff3c15bc73f03fa73fa12d9004"),
			},
			&LimboBlockTransactionDetails{
				Rlp:         []byte{120, 21, 121, 121, 213},
				StreamBytes: []byte{19, 170},
				Root:        common.BytesToHash([]byte{100, 20, 41, 134, 41, 15, 19, 105, 10, 214, 1, 21, 43, 134, 41, 15, 19, 105, 10, 214, 1, 2, 4, 134, 41, 15, 19, 105, 10, 214, 255, 0}),
				Hash:        common.HexToHash("10FB489D863DBAB249214BE9C128E37A54C3E57F514685191D45DCE8B0778A4F"),
				Sender:      common.HexToAddress("0x92f20480f6c693ab9fddfffff1e400532d24847b"),
			},
		},
	})
	pSource.limbo.uncheckedLimboBlocks = append(pSource.limbo.uncheckedLimboBlocks, &LimboBlockDetails{
		Witness:                 []byte{4, 1, 4},
		L1InfoTreeMinTimestamps: map[uint64]uint64{10: 20},
		BlockTimestamp:          1535151,
		BlockNumber:             114,
		BatchNumber:             65131,
		ForkId:                  111,
		Transactions: []*LimboBlockTransactionDetails{
			&LimboBlockTransactionDetails{
				Rlp:         []byte{100, 41, 151, 141, 13},
				StreamBytes: []byte{50, 150},
				Root:        common.BytesToHash([]byte{10, 20, 40, 34, 141, 15, 19, 105, 10, 214, 1, 2, 4, 134, 41, 15, 19, 105, 10, 214, 1, 2, 4, 134, 41, 15, 19, 105, 10, 214, 255, 0}),
				Hash:        common.HexToHash("A8B39BFE352B06575DBA0063170E5094FD12B61384E2276FA3A77C07CE726886"),
				Sender:      common.HexToAddress("0x510b131a0b61aeff3c15bc73f03fa73fa12d9004"),
			},
			&LimboBlockTransactionDetails{
				Rlp:         []byte{120, 21, 121, 121, 213},
				StreamBytes: []byte{191, 70},
				Root:        common.BytesToHash([]byte{40, 22, 20, 34, 241, 15, 19, 105, 10, 214, 1, 2, 4, 134, 41, 15, 19, 145, 10, 0, 1, 2, 4, 4, 41, 15, 19, 12, 10, 214, 0, 0}),
				Hash:        common.HexToHash("2504231FF7150135925D98A462A331522D9C2F712C85BEBC00B5BEBDF50DD7AA"),
				Sender:      common.HexToAddress("0x92f20480f6c693ab9fddfffff1e400532d24847b"),
			},
		},
	})

	pSource.limbo.invalidLimboBlocks = append(pSource.limbo.invalidLimboBlocks, &LimboBlockDetails{
		Witness:                 []byte{40, 45, 255},
		L1InfoTreeMinTimestamps: map[uint64]uint64{20: 30},
		BlockTimestamp:          9515151,
		BlockNumber:             138,
		BatchNumber:             165131,
		ForkId:                  222,
		Transactions: []*LimboBlockTransactionDetails{
			&LimboBlockTransactionDetails{
				Rlp:         []byte{101, 42, 198, 241, 133},
				StreamBytes: []byte{9, 213},
				Root:        common.BytesToHash([]byte{101, 201, 101, 34, 141, 15, 19, 105, 10, 214, 1, 2, 4, 134, 41, 15, 19, 105, 10, 214, 1, 2, 4, 134, 41, 15, 19, 105, 10, 214, 255, 0}),
				Hash:        common.HexToHash("A8B39BFE352B06575DBB0063170E5094FD12B61384E2276FA3A77C07CE726886"),
				Sender:      common.HexToAddress("0x510b131a0b61aeef3c15bc73f03fa73fa12d9004"),
			},
			&LimboBlockTransactionDetails{
				Rlp:         []byte{79, 76, 184, 159, 136},
				StreamBytes: []byte{91, 99},
				Root:        common.BytesToHash([]byte{40, 22, 20, 34, 241, 15, 19, 105, 101, 136, 1, 2, 4, 134, 41, 15, 19, 145, 10, 0, 1, 2, 4, 4, 41, 15, 19, 12, 10, 214, 0, 0}),
				Hash:        common.HexToHash("2504231FF7150135925D98A462A331522D9C2F712C85BEBCD0B5BEBDF50DD7AA"),
				Sender:      common.HexToAddress("0x92f20480f6c693ab9fddfffff1e400532d24847f"),
			},
		},
	})

	pSource.limbo.awaitingBlockHandling.Store(true)

	err = pSource.flushLockedLimbo(tx)
	assert.NilError(t, err)

	// restore
	pTarget, err := New(make(chan types.Announcements), db, txpoolcfg.DefaultConfig, ethCfg, kvcache.NewDummy(), *uint256.NewInt(1101), big.NewInt(0), big.NewInt(0), aclDb)
	assert.NilError(t, err)

	cacheView, err := pTarget._stateCache.View(context.Background(), tx)
	assert.NilError(t, err)

	tx.CreateBucket(TablePoolLimbo)
	err = pTarget.fromDBLimbo(context.Background(), tx, cacheView)
	assert.NilError(t, err)
	err = pTarget.fromDBLimbo(context.Background(), tx, cacheView)
	assert.NilError(t, err)

	assert.DeepEqual(t, pSource.limbo.invalidTxsMap, pTarget.limbo.invalidTxsMap)
	assert.DeepEqual(t, pSource.limbo.limboSlots, pTarget.limbo.limboSlots)
	assert.DeepEqual(t, pSource.limbo.uncheckedLimboBlocks, pTarget.limbo.uncheckedLimboBlocks)
	assert.DeepEqual(t, pSource.limbo.invalidLimboBlocks, pTarget.limbo.invalidLimboBlocks)
	assert.Equal(t, pSource.limbo.awaitingBlockHandling.Load(), pTarget.limbo.awaitingBlockHandling.Load())

	err = tx.Commit()
	assert.NilError(t, err)
	return pSource
}

func restoreRo(t *testing.T, dbPath string, pSource *TxPool) {
	db, tx, aclDb := initDb(t, dbPath, false)
	defer db.Close()
	defer tx.Rollback()
	defer aclDb.Close()

	err := tx.CreateBucket(TablePoolLimbo)
	assert.NilError(t, err)

	tx.Commit() // Close the tx because we don't need it

	newTxs := make(chan types.Announcements, 1024)
	defer close(newTxs)

	pTarget, err := New(newTxs, db, txpoolcfg.DefaultConfig, &ethconfig.Defaults, kvcache.NewDummy(), *uint256.NewInt(1101), big.NewInt(0), big.NewInt(0), aclDb)
	assert.NilError(t, err)

	err = db.View(context.Background(), func(tx kv.Tx) error {
		cacheView, err := pTarget._stateCache.View(context.Background(), tx)
		if err != nil {
			return err
		}

		return pTarget.fromDBLimbo(context.Background(), tx, cacheView)
	})
	assert.NilError(t, err)

	assert.DeepEqual(t, pSource.limbo.invalidTxsMap, pTarget.limbo.invalidTxsMap)
	assert.DeepEqual(t, pSource.limbo.limboSlots, pTarget.limbo.limboSlots)
	assert.DeepEqual(t, pSource.limbo.uncheckedLimboBlocks, pTarget.limbo.uncheckedLimboBlocks)
	assert.DeepEqual(t, pSource.limbo.invalidLimboBlocks, pTarget.limbo.invalidLimboBlocks)
	assert.Equal(t, pSource.limbo.awaitingBlockHandling.Load(), pTarget.limbo.awaitingBlockHandling.Load())

	err = tx.Commit()
	assert.NilError(t, err)
}

func initDb(t *testing.T, dbPath string, wipe bool) (kv.RwDB, kv.RwTx, kv.RwDB) {
	ctx := context.Background()

	if wipe {
		os.RemoveAll(dbPath)
	}

	dbOpts := mdbx.NewMDBX(log.Root()).Path(dbPath).Label(kv.ChainDB).GrowthStep(16 * datasize.MB).RoTxsLimiter(semaphore.NewWeighted(128))
	database, err := dbOpts.Open()
	if err != nil {
		t.Fatalf("Cannot create db %e", err)
	}

	txRw, err := database.BeginRw(ctx)
	if err != nil {
		t.Fatalf("Cannot create db transaction %e", err)
	}

	aclDB, err := OpenACLDB(ctx, dbPath)
	if err != nil {
		t.Fatalf("Cannot create acl db %e", err)
	}

	return database, txRw, aclDB
}

var tx01Rlp = []byte{249, 5, 168, 128, 128, 131, 22, 227, 96, 148, 42, 61, 211, 235, 131, 42, 249, 130, 236, 113, 102, 158, 23, 132, 36, 177, 13, 202, 46, 222, 128, 185, 5, 68, 44, 255, 208, 46, 239, 169, 249, 236, 66, 116, 220, 141, 242, 141, 64, 179, 216, 10, 113, 255, 153, 244, 221, 39, 219, 82, 195, 174, 36, 182, 60, 32, 237, 24, 49, 170, 173, 50, 40, 182, 118, 247, 211, 205, 66, 132, 165, 68, 63, 23, 241, 150, 43, 54, 228, 145, 179, 10, 64, 178, 64, 88, 73, 229, 151, 186, 95, 181, 180, 193, 25, 81, 149, 124, 111, 143, 100, 44, 74, 246, 28, 214, 178, 70, 64, 254, 198, 220, 127, 198, 7, 238, 130, 6, 169, 158, 146, 65, 13, 48, 33, 221, 185, 163, 86, 129, 92, 63, 172, 16, 38, 182, 222, 197, 223, 49, 36, 175, 186, 219, 72, 92, 155, 165, 163, 227, 57, 138, 4, 183, 186, 133, 172, 207, 209, 220, 96, 226, 78, 125, 171, 201, 252, 92, 223, 121, 218, 106, 128, 181, 96, 37, 148, 74, 2, 193, 152, 79, 179, 182, 219, 110, 128, 217, 14, 176, 30, 191, 201, 237, 39, 80, 12, 212, 223, 201, 121, 39, 45, 31, 9, 19, 204, 159, 102, 84, 13, 126, 128, 5, 129, 17, 9, 225, 207, 45, 136, 124, 34, 189, 135, 80, 211, 64, 22, 172, 60, 102, 181, 255, 16, 45, 172, 221, 115, 246, 176, 20, 231, 16, 181, 30, 128, 34, 175, 154, 25, 104, 255, 215, 1, 87, 228, 128, 99, 252, 51, 201, 122, 5, 15, 127, 100, 2, 51, 191, 100, 108, 201, 141, 149, 36, 198, 185, 43, 207, 58, 181, 111, 131, 152, 103, 204, 95, 127, 25, 107, 147, 186, 225, 226, 126, 99, 32, 116, 36, 69, 210, 144, 242, 38, 56, 39, 73, 139, 84, 254, 197, 57, 247, 86, 175, 206, 250, 212, 229, 8, 192, 152, 185, 167, 225, 216, 254, 177, 153, 85, 251, 2, 186, 150, 117, 88, 80, 120, 113, 9, 105, 211, 68, 15, 80, 84, 224, 249, 220, 62, 127, 224, 22, 224, 80, 239, 242, 96, 51, 79, 24, 165, 212, 254, 57, 29, 130, 9, 35, 25, 245, 150, 79, 46, 46, 183, 193, 195, 165, 248, 177, 58, 73, 226, 130, 246, 9, 195, 23, 168, 51, 251, 141, 151, 109, 17, 81, 124, 87, 29, 18, 33, 162, 101, 210, 90, 247, 120, 236, 248, 146, 52, 144, 198, 206, 235, 69, 10, 236, 220, 130, 226, 130, 147, 3, 29, 16, 199, 215, 59, 248, 94, 87, 191, 4, 26, 151, 54, 10, 162, 197, 217, 156, 193, 223, 130, 217, 196, 184, 116, 19, 234, 226, 239, 4, 143, 148, 180, 211, 85, 76, 234, 115, 217, 43, 15, 122, 249, 110, 2, 113, 198, 145, 226, 187, 92, 103, 173, 215, 198, 202, 243, 2, 37, 106, 222, 223, 122, 177, 20, 218, 10, 207, 232, 112, 212, 73, 163, 164, 137, 247, 129, 214, 89, 232, 190, 204, 218, 123, 206, 159, 78, 134, 24, 182, 189, 47, 65, 50, 206, 121, 140, 220, 122, 96, 231, 225, 70, 10, 114, 153, 227, 198, 52, 42, 87, 150, 38, 210, 39, 51, 229, 15, 82, 110, 194, 250, 25, 162, 43, 49, 232, 237, 80, 242, 60, 209, 253, 249, 76, 145, 84, 237, 58, 118, 9, 162, 241, 255, 152, 31, 225, 211, 181, 200, 7, 178, 129, 228, 104, 60, 198, 214, 49, 92, 249, 91, 154, 222, 134, 65, 222, 252, 179, 35, 114, 241, 193, 38, 227, 152, 239, 122, 90, 45, 206, 10, 138, 127, 104, 187, 116, 86, 15, 143, 113, 131, 124, 44, 46, 187, 203, 247, 255, 251, 66, 174, 24, 150, 241, 63, 124, 116, 121, 160, 180, 106, 40, 182, 245, 85, 64, 248, 148, 68, 246, 61, 224, 55, 142, 61, 18, 27, 224, 158, 6, 204, 157, 237, 28, 32, 230, 88, 118, 211, 106, 160, 198, 94, 150, 69, 100, 71, 134, 182, 32, 226, 221, 42, 214, 72, 221, 252, 191, 74, 126, 91, 26, 58, 78, 207, 231, 246, 70, 103, 163, 240, 183, 226, 244, 65, 133, 136, 237, 53, 162, 69, 140, 255, 235, 57, 185, 61, 38, 241, 141, 42, 177, 59, 220, 230, 174, 229, 142, 123, 153, 53, 158, 194, 223, 217, 90, 156, 22, 220, 0, 214, 239, 24, 183, 147, 58, 111, 141, 198, 92, 203, 85, 102, 113, 56, 119, 111, 125, 234, 16, 16, 112, 220, 135, 150, 227, 119, 77, 248, 79, 64, 174, 12, 130, 41, 208, 214, 6, 158, 92, 143, 57, 167, 194, 153, 103, 122, 9, 211, 103, 252, 123, 5, 227, 188, 56, 14, 230, 82, 205, 199, 37, 149, 247, 76, 123, 16, 67, 208, 225, 255, 186, 183, 52, 100, 140, 131, 141, 251, 5, 39, 217, 113, 182, 2, 188, 33, 108, 150, 25, 239, 10, 191, 90, 201, 116, 161, 237, 87, 244, 5, 10, 165, 16, 221, 156, 116, 245, 8, 39, 123, 57, 215, 151, 59, 178, 223, 204, 197, 238, 176, 97, 141, 184, 205, 116, 4, 111, 243, 55, 240, 167, 191, 44, 142, 3, 225, 15, 100, 44, 24, 134, 121, 141, 113, 128, 106, 177, 232, 136, 217, 229, 238, 135, 208, 131, 140, 86, 85, 203, 33, 198, 203, 131, 49, 59, 90, 99, 17, 117, 223, 244, 150, 55, 114, 204, 233, 16, 129, 136, 179, 74, 200, 124, 129, 196, 30, 102, 46, 228, 221, 45, 215, 178, 188, 112, 121, 97, 177, 230, 70, 196, 4, 118, 105, 220, 182, 88, 79, 13, 141, 119, 13, 175, 93, 126, 125, 235, 46, 56, 138, 178, 14, 37, 115, 209, 113, 168, 129, 8, 231, 157, 130, 14, 152, 242, 108, 11, 132, 170, 139, 47, 74, 164, 150, 141, 187, 129, 142, 163, 34, 147, 35, 124, 80, 186, 117, 238, 72, 95, 76, 34, 173, 242, 247, 65, 64, 11, 223, 141, 106, 156, 199, 223, 126, 202, 229, 118, 34, 22, 101, 215, 53, 132, 72, 129, 139, 180, 174, 69, 98, 132, 158, 148, 158, 23, 172, 22, 224, 190, 22, 104, 142, 21, 107, 92, 241, 94, 9, 140, 98, 124, 0, 86, 169, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 17, 49, 106, 154, 52, 76, 163, 188, 143, 106, 27, 7, 126, 74, 14, 28, 180, 158, 184, 232, 115, 81, 3, 96, 44, 0, 163, 246, 39, 162, 220, 117, 244, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 215, 251, 230, 61, 181, 32, 31, 113, 72, 47, 164, 126, 204, 75, 229, 229, 177, 37, 239, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 106, 148, 215, 79, 67, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 130, 8, 190, 160, 30, 164, 124, 192, 4, 230, 72, 163, 212, 85, 219, 126, 7, 180, 213, 92, 51, 82, 117, 96, 208, 157, 157, 149, 209, 15, 27, 82, 243, 88, 12, 175, 160, 103, 56, 187, 47, 176, 67, 165, 184, 198, 14, 249, 231, 69, 169, 120, 98, 204, 139, 154, 65, 115, 208, 81, 234, 84, 234, 218, 206, 239, 208, 206, 29}
var tx01Sender = []byte{215, 251, 230, 61, 181, 32, 31, 113, 72, 47, 164, 126, 204, 75, 229, 229, 177, 37, 239, 7}

var tx02Rlp = []byte{249, 5, 168, 128, 128, 131, 22, 227, 96, 148, 42, 61, 211, 235, 131, 42, 249, 130, 236, 113, 102, 158, 23, 132, 36, 177, 13, 202, 46, 222, 128, 185, 5, 68, 44, 255, 208, 46, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 109, 236, 220, 38, 66, 17, 4, 141, 156, 6, 100, 34, 106, 247, 143, 195, 80, 67, 128, 28, 113, 254, 248, 54, 179, 72, 111, 206, 229, 111, 243, 243, 180, 193, 25, 81, 149, 124, 111, 143, 100, 44, 74, 246, 28, 214, 178, 70, 64, 254, 198, 220, 127, 198, 7, 238, 130, 6, 169, 158, 146, 65, 13, 48, 71, 70, 182, 145, 184, 37, 189, 219, 146, 196, 79, 43, 6, 227, 66, 65, 128, 98, 97, 111, 12, 56, 114, 241, 6, 153, 32, 130, 220, 68, 45, 240, 172, 207, 209, 220, 96, 226, 78, 125, 171, 201, 252, 92, 223, 121, 218, 106, 128, 181, 96, 37, 148, 74, 2, 193, 152, 79, 179, 182, 219, 110, 128, 217, 14, 176, 30, 191, 201, 237, 39, 80, 12, 212, 223, 201, 121, 39, 45, 31, 9, 19, 204, 159, 102, 84, 13, 126, 128, 5, 129, 17, 9, 225, 207, 45, 136, 124, 34, 189, 135, 80, 211, 64, 22, 172, 60, 102, 181, 255, 16, 45, 172, 221, 115, 246, 176, 20, 231, 16, 181, 30, 128, 34, 175, 154, 25, 104, 255, 215, 1, 87, 228, 128, 99, 252, 51, 201, 122, 5, 15, 127, 100, 2, 51, 191, 100, 108, 201, 141, 149, 36, 198, 185, 43, 207, 58, 181, 111, 131, 152, 103, 204, 95, 127, 25, 107, 147, 186, 225, 226, 126, 99, 32, 116, 36, 69, 210, 144, 242, 38, 56, 39, 73, 139, 84, 254, 197, 57, 247, 86, 175, 206, 250, 212, 229, 8, 192, 152, 185, 167, 225, 216, 254, 177, 153, 85, 251, 2, 186, 150, 117, 88, 80, 120, 113, 9, 105, 211, 68, 15, 80, 84, 224, 249, 220, 62, 127, 224, 22, 224, 80, 239, 242, 96, 51, 79, 24, 165, 212, 254, 57, 29, 130, 9, 35, 25, 245, 150, 79, 46, 46, 183, 193, 195, 165, 248, 177, 58, 73, 226, 130, 246, 9, 195, 23, 168, 51, 251, 141, 151, 109, 17, 81, 124, 87, 29, 18, 33, 162, 101, 210, 90, 247, 120, 236, 248, 146, 52, 144, 198, 206, 235, 69, 10, 236, 220, 130, 226, 130, 147, 3, 29, 16, 199, 215, 59, 248, 94, 87, 191, 4, 26, 151, 54, 10, 162, 197, 217, 156, 193, 223, 130, 217, 196, 184, 116, 19, 234, 226, 239, 4, 143, 148, 180, 211, 85, 76, 234, 115, 217, 43, 15, 122, 249, 110, 2, 113, 198, 145, 226, 187, 92, 103, 173, 215, 198, 202, 243, 2, 37, 106, 222, 223, 122, 177, 20, 218, 10, 207, 232, 112, 212, 73, 163, 164, 137, 247, 129, 214, 89, 232, 190, 204, 218, 123, 206, 159, 78, 134, 24, 182, 189, 47, 65, 50, 206, 121, 140, 220, 122, 96, 231, 225, 70, 10, 114, 153, 227, 198, 52, 42, 87, 150, 38, 210, 39, 51, 229, 15, 82, 110, 194, 250, 25, 162, 43, 49, 232, 237, 80, 242, 60, 209, 253, 249, 76, 145, 84, 237, 58, 118, 9, 162, 241, 255, 152, 31, 225, 211, 181, 200, 7, 178, 129, 228, 104, 60, 198, 214, 49, 92, 249, 91, 154, 222, 134, 65, 222, 252, 179, 35, 114, 241, 193, 38, 227, 152, 239, 122, 90, 45, 206, 10, 138, 127, 104, 187, 116, 86, 15, 143, 113, 131, 124, 44, 46, 187, 203, 247, 255, 251, 66, 174, 24, 150, 241, 63, 124, 116, 121, 160, 180, 106, 40, 182, 245, 85, 64, 248, 148, 68, 246, 61, 224, 55, 142, 61, 18, 27, 224, 158, 6, 204, 157, 237, 28, 32, 230, 88, 118, 211, 106, 160, 198, 94, 150, 69, 100, 71, 134, 182, 32, 226, 221, 42, 214, 72, 221, 252, 191, 74, 126, 91, 26, 58, 78, 207, 231, 246, 70, 103, 163, 240, 183, 226, 244, 65, 133, 136, 237, 53, 162, 69, 140, 255, 235, 57, 185, 61, 38, 241, 141, 42, 177, 59, 220, 230, 174, 229, 142, 123, 153, 53, 158, 194, 223, 217, 90, 156, 22, 220, 0, 214, 239, 24, 183, 147, 58, 111, 141, 198, 92, 203, 85, 102, 113, 56, 119, 111, 125, 234, 16, 16, 112, 220, 135, 150, 227, 119, 77, 248, 79, 64, 174, 12, 130, 41, 208, 214, 6, 158, 92, 143, 57, 167, 194, 153, 103, 122, 9, 211, 103, 252, 123, 5, 227, 188, 56, 14, 230, 82, 205, 199, 37, 149, 247, 76, 123, 16, 67, 208, 225, 255, 186, 183, 52, 100, 140, 131, 141, 251, 5, 39, 217, 113, 182, 2, 188, 33, 108, 150, 25, 239, 10, 191, 90, 201, 116, 161, 237, 87, 244, 5, 10, 165, 16, 221, 156, 116, 245, 8, 39, 123, 57, 215, 151, 59, 178, 223, 204, 197, 238, 176, 97, 141, 184, 205, 116, 4, 111, 243, 55, 240, 167, 191, 44, 142, 3, 225, 15, 100, 44, 24, 134, 121, 141, 113, 128, 106, 177, 232, 136, 217, 229, 238, 135, 208, 131, 140, 86, 85, 203, 33, 198, 203, 131, 49, 59, 90, 99, 17, 117, 223, 244, 150, 55, 114, 204, 233, 16, 129, 136, 179, 74, 200, 124, 129, 196, 30, 102, 46, 228, 221, 45, 215, 178, 188, 112, 121, 97, 177, 230, 70, 196, 4, 118, 105, 220, 182, 88, 79, 13, 141, 119, 13, 175, 93, 126, 125, 235, 46, 56, 138, 178, 14, 37, 115, 209, 113, 168, 129, 8, 231, 157, 130, 14, 152, 242, 108, 11, 132, 170, 139, 47, 74, 164, 150, 141, 187, 129, 142, 163, 34, 147, 35, 124, 80, 186, 117, 238, 72, 95, 76, 34, 173, 242, 247, 65, 64, 11, 223, 141, 106, 156, 199, 223, 126, 202, 229, 118, 34, 22, 101, 215, 53, 132, 72, 129, 139, 180, 174, 69, 98, 132, 158, 148, 158, 23, 172, 22, 224, 190, 22, 104, 142, 21, 107, 92, 241, 94, 9, 140, 98, 124, 0, 86, 169, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 26, 146, 204, 150, 1, 187, 194, 161, 138, 197, 140, 8, 117, 89, 181, 95, 161, 212, 98, 45, 31, 249, 115, 192, 56, 79, 84, 172, 231, 153, 99, 147, 231, 244, 138, 97, 125, 121, 44, 79, 7, 2, 224, 77, 99, 242, 169, 37, 133, 165, 150, 70, 43, 96, 115, 61, 105, 196, 95, 17, 75, 185, 51, 235, 38, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 95, 131, 194, 3, 188, 124, 106, 166, 89, 243, 57, 54, 216, 239, 115, 134, 71, 17, 39, 238, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 35, 134, 242, 111, 193, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 130, 8, 190, 160, 39, 120, 213, 212, 147, 0, 55, 210, 153, 214, 109, 190, 166, 142, 178, 63, 121, 104, 69, 177, 191, 141, 197, 248, 49, 206, 229, 25, 19, 231, 153, 161, 160, 90, 231, 245, 104, 19, 146, 126, 121, 166, 102, 173, 49, 92, 12, 64, 185, 239, 22, 130, 250, 175, 172, 115, 14, 168, 66, 239, 168, 170, 111, 179, 232}
var tx02Sender = []byte{95, 131, 194, 3, 188, 124, 106, 166, 89, 243, 57, 54, 216, 239, 115, 134, 71, 17, 39, 238}
