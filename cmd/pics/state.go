package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
	"github.com/ledgerwatch/turbo-geth/cmd/pics/contracts"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/trie"
	"github.com/ledgerwatch/turbo-geth/visual"
)

func constructCodeMap(tds *state.TrieDbState) (map[common.Hash][]byte, error) {
	codeMap := make(map[common.Hash][]byte)
	if err := tds.Database().Walk(dbutils.CodeBucket, make([]byte, 32), 0, func(k, v []byte) (bool, error) {
		codeMap[common.BytesToHash(k)] = common.CopyBytes(v)
		return true, nil
	}); err != nil {
		return nil, err
	}
	return codeMap, nil
}

/*func statePicture(t *trie.Trie, number int, keyCompression int, codeCompressed bool, valCompressed bool,
	quadTrie bool, quadColors bool, highlights [][]byte) (*trie.Trie, error) {
	filename := fmt.Sprintf("state_%d.dot", number)
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	indexColors := visual.HexIndexColors
	fontColors := visual.HexFontColors
	if quadTrie {
		t = trie.HexToQuad(t)
	}
	if quadColors {
		indexColors = visual.QuadIndexColors
		fontColors = visual.QuadFontColors
	}
	visual.StartGraph(f, false)
	trie.Visual(t, f, &trie.VisualOpts{
		Highlights:     highlights,
		IndexColors:    indexColors,
		FontColors:     fontColors,
		Values:         true,
		CutTerminals:   keyCompression,
		CodeCompressed: codeCompressed,
		ValCompressed:  valCompressed,
		ValHex:         true,
	})
	visual.EndGraph(f)
	if err := f.Close(); err != nil {
		return nil, err
	}
	cmd := exec.Command("dot", "-Tpng:gd", "-O", filename)
	if output, err := cmd.CombinedOutput(); err != nil {
		fmt.Printf("error: %v, output: %s\n", err, output)
	}
	return t, nil
}*/

func keyTape(t *trie.Trie, number int) error {
	filename := fmt.Sprintf("state_%d.dot", number)
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	visual.StartGraph(f, false)
	fk := trie.FullKeys(t)
	sort.Strings(fk)
	for i, key := range fk {
		if len(key) == 129 {
			visual.Vertical(f, []byte(key), 0, fmt.Sprintf("q_%x", key), visual.QuadIndexColors, visual.QuadFontColors, 110)
		} else {
			visual.Vertical(f, []byte(key[128:]), 0, fmt.Sprintf("q_%x", key), visual.QuadIndexColors, visual.QuadFontColors, 100)
		}
		visual.Circle(f, fmt.Sprintf("e_%d", i), "...", false)
		fmt.Fprintf(f,
			`q_%x -> e_%d;
`, key, i)
	}
	visual.EndGraph(f)
	if err := f.Close(); err != nil {
		return err
	}
	cmd := exec.Command("dot", "-Tpng:gd", "-O", filename)
	if output, err := cmd.CombinedOutput(); err != nil {
		fmt.Printf("error: %v, output: %s\n", err, output)
	}
	return nil
}

var bucketLabels = map[string]string{
	string(dbutils.PreimagePrefix):              "Preimages",
	string(dbutils.BlockReceiptsPrefix):         "Receipts",
	string(dbutils.AccountsHistoryBucket):       "History of Accounts",
	string(dbutils.HeaderPrefix):                "Headers",
	string(dbutils.ConfigPrefix):                "Config",
	string(dbutils.BlockBodyPrefix):             "Block Bodies",
	string(dbutils.HeaderNumberPrefix):          "Header Numbers",
	string(dbutils.AccountChangeSetBucket):      "Account Change Sets",
	string(dbutils.StorageChangeSetBucket):      "Storage Change Sets",
	string(dbutils.CurrentStateBucket):          "Current State",
	string(dbutils.TxLookupPrefix):              "Transaction Index",
	string(dbutils.StorageHistoryBucket):        "History of Storage",
	string(dbutils.CodeBucket):                  "Code of Contracts",
	string(dbutils.Senders):                     "Senders",
	string(dbutils.SyncStageProgress):           "Sync Progress",
	string(dbutils.PlainStateBucket):            "PlainState",
	string(dbutils.IntermediateTrieHashBucket):  "Intermediate Hashes",
	string(dbutils.SyncStageUnwind):             "Unwind",
	string(dbutils.PlainAccountChangeSetBucket): "Account Changes",
	string(dbutils.PlainStorageChangeSetBucket): "Storage Changes",
	string(dbutils.IncarnationMapBucket):        "Incarnations",
}

/*dbutils.PlainContractCodeBucket,
dbutils.CodeBucket,
dbutils.AccountsHistoryBucket,
dbutils.StorageHistoryBucket,
dbutils.TxLookupPrefix,*/

func hexPalette() error {
	filename := "hex_palette.dot"
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	visual.StartGraph(f, true)
	p := common.FromHex("0x000102030405060708090a0b0c0d0e0f")
	visual.Horizontal(f, p, len(p), "p", visual.HexIndexColors, visual.HexFontColors, 0)
	visual.EndGraph(f)
	if err := f.Close(); err != nil {
		return err
	}
	cmd := exec.Command("dot", "-Tpng:gd", "-O", filename)
	if output, err := cmd.CombinedOutput(); err != nil {
		fmt.Printf("error: %v, output: %s\n", err, output)
	}
	return nil
}

func insertBlocksInStages(db ethdb.Database, config *params.ChainConfig, engine consensus.Engine, block *types.Block, bc *core.BlockChain) error {

	num := block.Number().Uint64()
	// Stage 1
	if _, _, err := stagedsync.InsertHeaderChain(db, []*types.Header{block.Header()}, config, engine, 1); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(db, stages.Headers, num, nil); err != nil {
		return err
	}
	// Stage 2
	if err := stagedsync.SpawnBlockHashStage(&stagedsync.StageState{
		BlockNumber: num - 1,
	}, db, nil); err != nil {
		return err
	}

	if err := stages.SaveStageProgress(db, stages.BlockHashes, num, nil); err != nil {
		return err
	}

	// Stage 3
	if _, err := bc.InsertBodyChain(context.TODO(), []*types.Block{block}); err != nil {
		return err
	}

	if err := stages.SaveStageProgress(db, stages.Bodies, num, nil); err != nil {
		return err
	}
	// Stage 4
	const batchSize = 10000
	const blockSize = 4096
	n := runtime.NumCPU()

	cfg := stagedsync.Stage3Config{
		BatchSize:       batchSize,
		BlockSize:       blockSize,
		BufferSize:      (blockSize * 10 / 20) * 10000, // 20*4096
		StartTrace:      false,
		Prof:            false,
		NumOfGoroutines: n,
		ReadChLen:       4,
		Now:             time.Now(),
	}
	if err := stagedsync.SpawnRecoverSendersStage(cfg, &stagedsync.StageState{
		BlockNumber: num - 1,
	}, db, config, 0, "", nil); err != nil {
		return err
	}

	if err := stages.SaveStageProgress(db, stages.Senders, num, nil); err != nil {
		return err
	}
	// Stage 5
	if err := stagedsync.SpawnExecuteBlocksStage(&stagedsync.StageState{
		BlockNumber: num - 1,
	}, db, config, bc, bc.GetVMConfig(), 0, nil, true, nil); err != nil {
		return err
	}

	if err := stages.SaveStageProgress(db, stages.Execution, num, nil); err != nil {
		return err
	}

	// Stage 6
	if err := stagedsync.SpawnIntermediateHashesStage(&stagedsync.StageState{
		BlockNumber: num - 1,
	}, db, "", nil); err != nil {
		return err
	}

	if err := stages.SaveStageProgress(db, stages.IntermediateHashes, num, nil); err != nil {
		return err
	}

	// Stage 7
	if err := stagedsync.SpawnHashStateStage(&stagedsync.StageState{
		BlockNumber: num - 1,
	}, db, "", nil); err != nil {
		return err
	}

	if err := stages.SaveStageProgress(db, stages.HashState, num, nil); err != nil {
		return err
	}

	// Stage 8
	if err := stagedsync.SpawnAccountHistoryIndex(&stagedsync.StageState{
		BlockNumber: num - 1,
	}, db, "", nil); err != nil {
		return err
	}

	if err := stages.SaveStageProgress(db, stages.AccountHistoryIndex, num, nil); err != nil {
		return err
	}

	// Stage 9
	if err := stagedsync.SpawnStorageHistoryIndex(&stagedsync.StageState{
		BlockNumber: num - 1,
	}, db, "", nil); err != nil {
		return err
	}

	if err := stages.SaveStageProgress(db, stages.StorageHistoryIndex, num, nil); err != nil {
		return err
	}

	// Stage 10
	if err := stagedsync.SpawnTxLookup(&stagedsync.StageState{}, db, "", nil); err != nil {
		return err
	}

	if err := stages.SaveStageProgress(db, stages.TxLookup, num, nil); err != nil {
		return err
	}

	return nil
}

func stateDatabaseComparison(first ethdb.KV, second ethdb.KV, number int) error {
	filename := fmt.Sprintf("changes_%d.dot", number)
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	i := 0
	visual.StartGraph(f, true)
	m := make(map[string][]int)
	noValues := make(map[int]struct{})
	perBucketFiles := make(map[string]*os.File)

	if err = second.View(context.Background(), func(readTx ethdb.Tx) error {
		return first.View(context.Background(), func(firstTx ethdb.Tx) error {
			for _, bucketName := range dbutils.Buckets {
				b := readTx.Bucket(bucketName)
				firstB := firstTx.Bucket(bucketName)
				if err2 := b.Cursor().Walk(func(k, v []byte) (bool, error) {
					if firstB != nil {
						if firstV, _ := firstB.Get(k); firstV != nil && bytes.Equal(v, firstV) {
							// Skip the record that is the same as in the first Db
							return true, nil
						}
					}
					// Produce pair of nodes
					keyKeyBytes := &trie.Keybytes{
						Data:        k,
						Odd:         false,
						Terminating: false,
					}
					valKeyBytes := &trie.Keybytes{
						Data:        v,
						Odd:         false,
						Terminating: false,
					}
					val := valKeyBytes.ToHex()
					key := keyKeyBytes.ToHex()
					var f1 *os.File
					var ok bool
					if f1, ok = perBucketFiles[string(bucketName)]; !ok {
						f1, err = os.Create(fmt.Sprintf("changes_%d_%s_%d.dot", number, bucketName, len(perBucketFiles)))
						if err != nil {
							return false, err
						}
						visual.StartGraph(f1, true)
						var clusterLabel string
						var ok bool
						if clusterLabel, ok = bucketLabels[string(bucketName)]; !ok {
							clusterLabel = string(bucketName)
						}
						visual.StartCluster(f1, 0, clusterLabel)
						perBucketFiles[string(common.CopyBytes(bucketName))] = f1
					}
					visual.Horizontal(f1, key, len(key), fmt.Sprintf("k_%d", i), visual.HexIndexColors, visual.HexFontColors, 0)
					if len(val) > 0 {
						if len(val) > 64 {
							visual.HexBox(f1, fmt.Sprintf("v_%d", i), val, 64, false /*compresses*/, true /*highlighted*/)
						} else {
							visual.Horizontal(f1, val, len(val), fmt.Sprintf("v_%d", i), visual.HexIndexColors, visual.HexFontColors, 0)
						}
						// Produce edge
						fmt.Fprintf(f1, "k_%d -> v_%d;\n", i, i)
					} else {
						noValues[i] = struct{}{}
					}
					visual.Horizontal(f, key, 0, fmt.Sprintf("k_%d", i), visual.HexIndexColors, visual.HexFontColors, 0)
					if len(val) > 0 {
						if len(val) > 64 {
							visual.HexBox(f, fmt.Sprintf("v_%d", i), val, 64, false /*compressed*/, false /*highlighted*/)
						} else {
							visual.Horizontal(f, val, 0, fmt.Sprintf("v_%d", i), visual.HexIndexColors, visual.HexFontColors, 0)
						}
						// Produce edge
						fmt.Fprintf(f, "k_%d -> v_%d;\n", i, i)
					} else {
						noValues[i] = struct{}{}
					}
					lst := m[string(bucketName)]
					lst = append(lst, i)
					m[string(common.CopyBytes(bucketName))] = lst
					i++
					return true, nil
				}); err2 != nil {
					return err2
				}
			}
			return nil
		})
	}); err != nil {
		return err
	}
	n := 0
	for prefix, lst := range m {
		var clusterLabel string
		var ok bool
		if clusterLabel, ok = bucketLabels[prefix]; !ok {
			clusterLabel = prefix
		}
		if len(lst) == 0 {
			continue
		}
		visual.StartCluster(f, n, clusterLabel)
		for _, item := range lst {
			if _, ok1 := noValues[item]; ok1 {
				fmt.Fprintf(f, "k_%d;", item)
			} else {
				fmt.Fprintf(f, "k_%d;v_%d;", item, item)
			}
		}
		fmt.Fprintf(f, "\n")
		visual.EndCluster(f)
		n++
	}
	visual.EndGraph(f)
	if err := f.Close(); err != nil {
		return err
	}
	cmd := exec.Command("dot", "-Tpng:gd", "-O", filename)
	if output, err := cmd.CombinedOutput(); err != nil {
		fmt.Printf("error: %v, output: %s\n", err, output)
	}
	for _, f1 := range perBucketFiles {
		fmt.Fprintf(f1, "\n")
		visual.EndCluster(f1)
		visual.EndGraph(f1)
		if err := f1.Close(); err != nil {
			return err
		}
		cmd := exec.Command("dot", "-Tpng:gd", "-O", f1.Name())
		if output, err := cmd.CombinedOutput(); err != nil {
			fmt.Printf("error: %v, output: %s\n", err, output)
		}
	}
	return nil
}

func initialState1() error {
	fmt.Printf("Initial state 1\n")
	// Configure and generate a sample block chain
	db := ethdb.NewMemDatabase()
	defer db.Close()
	kv := db.KV()
	var (
		key, _   = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		key1, _  = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
		key2, _  = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
		address  = crypto.PubkeyToAddress(key.PublicKey)
		address1 = crypto.PubkeyToAddress(key1.PublicKey)
		address2 = crypto.PubkeyToAddress(key2.PublicKey)
		theAddr  = common.Address{1}
		gspec    = &core.Genesis{
			Config: params.MainnetChainConfig,
			Alloc: core.GenesisAlloc{
				address:  {Balance: big.NewInt(9000000000000000000)},
				address1: {Balance: big.NewInt(200000000000000000)},
				address2: {Balance: big.NewInt(300000000000000000)},
			},
		}
		// this code generates a log
		signer = types.HomesteadSigner{}
	)
	// Create intermediate hash bucket since it is mandatory now
	_, genesisHash, _, err := core.SetupGenesisBlock(db, gspec, true, false)
	if err != nil {
		return err
	}
	genesis := rawdb.ReadBlock(db, genesisHash, 0)
	if err != nil {
		return err
	}
	genesisDb := db.MemCopy()
	defer genesisDb.Close()

	engine := ethash.NewFaker()

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOpts := bind.NewKeyedTransactor(key)
	transactOpts1 := bind.NewKeyedTransactor(key1)
	transactOpts2 := bind.NewKeyedTransactor(key2)

	var tokenContract *contracts.Token
	// We generate the blocks without plainstant because it's not supported in core.GenerateChain
	blocks, _, err := core.GenerateChain(gspec.Config, genesis, engine, genesisDb, 8, func(i int, block *core.BlockGen) {
		var (
			tx  *types.Transaction
			txs []*types.Transaction
		)

		ctx := gspec.Config.WithEIPsFlags(context.Background(), block.Number())
		switch i {
		case 0:
			tx, err = types.SignTx(types.NewTransaction(0, theAddr, uint256.NewInt().SetUint64(1000000000000000), 21000, new(uint256.Int), nil), signer, key)
			err = contractBackend.SendTransaction(ctx, tx)
			if err != nil {
				panic(err)
			}
		case 1:
			tx, err = types.SignTx(types.NewTransaction(1, theAddr, uint256.NewInt().SetUint64(1000000000000000), 21000, new(uint256.Int), nil), signer, key)
			err = contractBackend.SendTransaction(ctx, tx)
			if err != nil {
				panic(err)
			}
		case 2:
			_, tx, tokenContract, err = contracts.DeployToken(transactOpts, contractBackend, address1)
		case 3:
			tx, err = tokenContract.Mint(transactOpts1, address2, big.NewInt(10))
		case 4:
			tx, err = tokenContract.Transfer(transactOpts2, address, big.NewInt(3))
		case 5:
			// Muliple transactions sending small amounts of ether to various accounts
			var j uint64
			var toAddr common.Address
			nonce := block.TxNonce(address)
			for j = 1; j <= 32; j++ {
				binary.BigEndian.PutUint64(toAddr[:], j)
				tx, err = types.SignTx(types.NewTransaction(nonce, toAddr, uint256.NewInt().SetUint64(1000000000000000), 21000, new(uint256.Int), nil), signer, key)
				if err != nil {
					panic(err)
				}
				err = contractBackend.SendTransaction(ctx, tx)
				if err != nil {
					panic(err)
				}
				txs = append(txs, tx)
				nonce++
			}
		case 6:
			_, tx, tokenContract, err = contracts.DeployToken(transactOpts, contractBackend, address1)
			if err != nil {
				panic(err)
			}
			txs = append(txs, tx)
			tx, err = tokenContract.Mint(transactOpts1, address2, big.NewInt(100))
			if err != nil {
				panic(err)
			}
			txs = append(txs, tx)
			// Muliple transactions sending small amounts of ether to various accounts
			var j uint64
			var toAddr common.Address
			for j = 1; j <= 32; j++ {
				binary.BigEndian.PutUint64(toAddr[:], j)
				tx, err = tokenContract.Transfer(transactOpts2, toAddr, big.NewInt(1))
				if err != nil {
					panic(err)
				}
				txs = append(txs, tx)
			}
		case 7:
			var toAddr common.Address
			nonce := block.TxNonce(address)
			binary.BigEndian.PutUint64(toAddr[:], 4)
			tx, err = types.SignTx(types.NewTransaction(nonce, toAddr, uint256.NewInt().SetUint64(1000000000000000), 21000, new(uint256.Int), nil), signer, key)
			if err != nil {
				panic(err)
			}
			err = contractBackend.SendTransaction(ctx, tx)
			if err != nil {
				panic(err)
			}
			txs = append(txs, tx)
			binary.BigEndian.PutUint64(toAddr[:], 12)
			tx, err = tokenContract.Transfer(transactOpts2, toAddr, big.NewInt(1))
			if err != nil {
				panic(err)
			}
			txs = append(txs, tx)
		}

		if err != nil {
			panic(err)
		}
		if txs == nil && tx != nil {
			txs = append(txs, tx)
		}

		for _, tx := range txs {
			block.AddTx(tx)
		}
		contractBackend.Commit()
	}, true)
	if err != nil {
		return err
	}
	db.Close()
	// We reset the DB and use the generated blocks
	core.UsePlainStateExecution = true
	db = ethdb.NewMemDatabase()
	kv = db.KV()
	snapshotDB := db.MemCopy()

	_, genesisHash, _, err = core.SetupGenesisBlock(db, gspec, true, false)
	if err != nil {
		return err
	}
	genesis = rawdb.ReadBlock(db, genesisHash, 0)
	engine = ethash.NewFaker()

	if err != nil {
		return err
	}

	txCacher := core.NewTxSenderCacher(runtime.NumCPU())
	blockchain, err := core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil, txCacher)
	if err != nil {
		return err
	}
	defer blockchain.Stop()
	blockchain.EnableReceipts(true)

	if err = hexPalette(); err != nil {
		return err
	}

	/*if _, err = statePicture(t, 0, 0, false, false, false, false, nil); err != nil {
		return err
	}

	if _, err = statePicture(t, 1, 48, false, false, false, false, nil); err != nil {
		return err
	}*/

	if err = stateDatabaseComparison(snapshotDB.KV(), kv, 0); err != nil {
		return err
	}

	snapshotDB.Close()

	// BLOCK 1
	snapshotDB = db.MemCopy()

	if err = insertBlocksInStages(db, gspec.Config, engine, blocks[0], blockchain); err != nil {
		return err
	}

	if err = stateDatabaseComparison(snapshotDB.KV(), kv, 1); err != nil {
		return err
	}
	snapshotDB.Close()
	/*if _, err = statePicture(t, 2, 48, false, false, false, false, nil); err != nil {
		return err
	}*/

	// BLOCK 2
	snapshotDB = db.MemCopy()
	if err = insertBlocksInStages(db, gspec.Config, engine, blocks[1], blockchain); err != nil {
		return err
	}

	if err = stateDatabaseComparison(snapshotDB.KV(), kv, 2); err != nil {
		return err
	}
	snapshotDB.Close()
	/*if _, err = statePicture(t, 3, 48, false, false, false, false, nil); err != nil {
		return err
	}*/
	// BLOCK 3
	snapshotDB = db.MemCopy()

	if err = insertBlocksInStages(db, gspec.Config, engine, blocks[2], blockchain); err != nil {
		return err
	}

	if err = stateDatabaseComparison(snapshotDB.KV(), kv, 3); err != nil {
		return err
	}
	snapshotDB.Close()
	/*
		if _, err = statePicture(t, 4, 48, false, false, false, false, nil); err != nil {
			return err
		}
		if _, err = statePicture(t, 5, 48, true, false, false, false, nil); err != nil {
			return err
		}
		if _, err = statePicture(t, 6, 48, true, true, false, false, nil); err != nil {
			return err
		}
	*/
	// BLOCK 4
	snapshotDB = db.MemCopy()

	if err = insertBlocksInStages(db, gspec.Config, engine, blocks[3], blockchain); err != nil {
		return err
	}

	if err = stateDatabaseComparison(snapshotDB.KV(), kv, 4); err != nil {
		return err
	}
	snapshotDB.Close()
	/*if _, err = statePicture(t, 7, 48, true, true, false, false, nil); err != nil {
		return err
	}*/

	// BLOCK 5
	snapshotDB = db.MemCopy()

	if err = insertBlocksInStages(db, gspec.Config, engine, blocks[4], blockchain); err != nil {
		return err
	}

	if err = stateDatabaseComparison(snapshotDB.KV(), kv, 5); err != nil {
		return err
	}
	snapshotDB.Close()
	/*
		if _, err = statePicture(t, 8, 54, true, true, false, false, nil); err != nil {
			return err
		}
	*/

	// BLOCK 6
	snapshotDB = db.MemCopy()

	if err = insertBlocksInStages(db, gspec.Config, engine, blocks[5], blockchain); err != nil {
		return err
	}

	if err = stateDatabaseComparison(snapshotDB.KV(), kv, 6); err != nil {
		return err
	}
	snapshotDB.Close()

	/*
		if _, err = statePicture(t, 9, 54, true, true, false, false, nil); err != nil {
			return err
		}
		if _, err = statePicture(t, 10, 110, true, true, true, true, nil); err != nil {
			return err
		}
	*/

	// BLOCK 7
	snapshotDB = db.MemCopy()

	if err = insertBlocksInStages(db, gspec.Config, engine, blocks[6], blockchain); err != nil {
		return err
	}

	if err = stateDatabaseComparison(snapshotDB.KV(), kv, 7); err != nil {
		return err
	}
	snapshotDB.Close()
	// _, err = statePicture(t, 11, 110, true, true, true, true, nil)
	if err != nil {
		return err
	}

	// tds.SetResolveReads(true)
	// BLOCK 8
	snapshotDB = db.MemCopy()

	if err = insertBlocksInStages(db, gspec.Config, engine, blocks[7], blockchain); err != nil {
		return err
	}

	if err = stateDatabaseComparison(snapshotDB.KV(), kv, 8); err != nil {
		return err
	}
	snapshotDB.Close()
	/*
		if _, err = statePicture(t, 12, 110, true, true, true, true, nil); err != nil {
			return err
		}

		rl := trie.NewRetainList(0)
		touches, storageTouches := tds.ExtractTouches()
		var touchQuads = make([][]byte, len(touches))
		for _, touch := range touches {
			touchQuad := trie.KeyToQuad(touch)
			rl.AddHex(touchQuad)
			touchQuads = append(touchQuads, touchQuad)
		}
		for _, touch := range storageTouches {
			touchQuad := trie.KeyToQuad(touch)
			rl.AddHex(touchQuad)
			touchQuads = append(touchQuads, touchQuad)
		}

		var witness *trie.Witness

		if witness, err = quadTrie.ExtractWitness(false, rl); err != nil {
			return err
		}

		var witnessTrie *trie.Trie

		if witnessTrie, err = trie.BuildTrieFromWitness(witness, false, false); err != nil {
			return err
		}
		if _, err = statePicture(witnessTrie, 13, 110, true, true, false, true, touchQuads); err != nil {
			return err
		}

		if err = keyTape(witnessTrie, 14); err != nil {
			return err
		}

		// Repeat the block witness illustration, but without any highlighted keys
		if _, err = statePicture(witnessTrie, 15, 110, true, true, false, true, nil); err != nil {
			return err
		}
	*/

	kv2 := ethdb.NewMemDatabase().KV()
	defer kv2.Close()
	if err = stateDatabaseComparison(kv2, kv, 9); err != nil {
		return err
	}
	return nil
}
