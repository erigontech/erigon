package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
	"github.com/ledgerwatch/turbo-geth/cmd/pics/contracts"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/trie"
	"github.com/ledgerwatch/turbo-geth/visual"
	"math/big"
	"os"
	"os/exec"
	"sort"
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

func statePicture(t *trie.Trie, codeMap map[common.Hash][]byte, number int, keyCompression int, codeCompressed bool, valCompressed bool,
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
		CodeMap:        codeMap,
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
}

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

func stateDatabaseMap(db *bolt.DB, number int) error {
	filename := fmt.Sprintf("state_%d.dot", number)
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	i := 0
	visual.StartGraph(f, true)

	if err := db.View(func(readTx *bolt.Tx) error {
		return readTx.ForEach(func(name []byte, b *bolt.Bucket) error {

			visual.StartCluster(f, i, string(name))

			if err := b.ForEach(func(k, v []byte) error {
				fmt.Fprintf(f, `k_%d -> v_%d`, i, i)
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
				key := keyKeyBytes.ToHex()
				val := valKeyBytes.ToHex()
				visual.Horizontal(f, key, 0, fmt.Sprintf("k_%d", i), visual.HexIndexColors, visual.HexFontColors, 110)
				if len(val) > 0 {
					if len(val) > 32 {
						shortenedVal := val[:32]
						visual.Horizontal(f, shortenedVal, 0, fmt.Sprintf("v_%d", i), visual.HexIndexColors, visual.HexFontColors, 100)
					} else {
						visual.Horizontal(f, val, 0, fmt.Sprintf("v_%d", i), visual.HexIndexColors, visual.HexFontColors, 110)
					}
				} else {
					visual.Circle(f, fmt.Sprintf("v_%d", i), "...", false)
				}
				i++
				return nil
			}); err != nil {
				return err
			}
			visual.EndCluster(f)
			return nil
		})
	}); err != nil {
		panic(err)
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

func initialState1() error {
	fmt.Printf("Initial state 1\n")
	// Configure and generate a sample block chain
	var (
		db, dbBolt = ethdb.NewMemDatabase2()
		key, _     = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		key1, _    = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
		key2, _    = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
		address    = crypto.PubkeyToAddress(key.PublicKey)
		address1   = crypto.PubkeyToAddress(key1.PublicKey)
		address2   = crypto.PubkeyToAddress(key2.PublicKey)
		theAddr    = common.Address{1}
		gspec      = &core.Genesis{
			Config: &params.ChainConfig{
				ChainID:             big.NewInt(1),
				HomesteadBlock:      new(big.Int),
				EIP155Block:         new(big.Int),
				EIP158Block:         big.NewInt(1),
				EIP2027Block:        big.NewInt(4),
				ConstantinopleBlock: big.NewInt(1),
			},
			Alloc: core.GenesisAlloc{
				address:  {Balance: big.NewInt(9000000000000000000)},
				address1: {Balance: big.NewInt(200000000000000000)},
				address2: {Balance: big.NewInt(300000000000000000)},
			},
		}
		genesis   = gspec.MustCommit(db)
		genesisDb = db.MemCopy()
		// this code generates a log
		signer = types.HomesteadSigner{}
	)

	engine := ethash.NewFaker()
	blockchain, err := core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil)
	if err != nil {
		return err
	}
	blockchain.EnableReceipts(true)

	tds, err := blockchain.GetTrieDbState()
	if err != nil {
		return err
	}
	t := tds.Trie()
	var codeMap map[common.Hash][]byte
	if codeMap, err = constructCodeMap(tds); err != nil {
		return err
	}
	if _, err = statePicture(t, codeMap, 0, 0, false, false, false, false, nil); err != nil {
		return err
	}
	if _, err = statePicture(t, codeMap, 1, 48, false, false, false, false, nil); err != nil {
		return err
	}

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOpts := bind.NewKeyedTransactor(key)
	transactOpts1 := bind.NewKeyedTransactor(key1)
	transactOpts2 := bind.NewKeyedTransactor(key2)

	var tokenContract *contracts.Token

	blocks, _ := core.GenerateChain(context.Background(), gspec.Config, genesis, engine, genesisDb, 8, func(i int, block *core.BlockGen) {
		var (
			tx  *types.Transaction
			txs []*types.Transaction
		)

		ctx := gspec.Config.WithEIPsFlags(context.Background(), block.Number())

		switch i {
		case 0:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, big.NewInt(1000000000000000), 21000, new(big.Int), nil), signer, key)
			err = contractBackend.SendTransaction(ctx, tx)
			if err != nil {
				panic(err)
			}
		case 1:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, big.NewInt(1000000000000000), 21000, new(big.Int), nil), signer, key)
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
				tx, err = types.SignTx(types.NewTransaction(nonce, toAddr, big.NewInt(1000000000000000), 21000, new(big.Int), nil), signer, key)
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
			tx, err = types.SignTx(types.NewTransaction(nonce, toAddr, big.NewInt(1000000000000000), 21000, new(big.Int), nil), signer, key)
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
	})

	// BLOCK 1
	if _, err = blockchain.InsertChain(types.Blocks{blocks[0]}); err != nil {
		return err
	}
	if codeMap, err = constructCodeMap(tds); err != nil {
		return err
	}
	if _, err = statePicture(t, codeMap, 2, 48, false, false, false, false, nil); err != nil {
		return err
	}

	// BLOCK 2
	if _, err = blockchain.InsertChain(types.Blocks{blocks[1]}); err != nil {
		return err
	}
	if codeMap, err = constructCodeMap(tds); err != nil {
		return err
	}
	if _, err = statePicture(t, codeMap, 3, 48, false, false, false, false, nil); err != nil {
		return err
	}

	// BLOCK 3
	if _, err = blockchain.InsertChain(types.Blocks{blocks[2]}); err != nil {
		return err
	}
	if codeMap, err = constructCodeMap(tds); err != nil {
		return err
	}
	if _, err = statePicture(t, codeMap, 4, 48, false, false, false, false, nil); err != nil {
		return err
	}
	if _, err = statePicture(t, codeMap, 5, 48, true, false, false, false, nil); err != nil {
		return err
	}
	if _, err = statePicture(t, codeMap, 6, 48, true, true, false, false, nil); err != nil {
		return err
	}

	// BLOCK 4
	if _, err = blockchain.InsertChain(types.Blocks{blocks[3]}); err != nil {
		return err
	}
	if codeMap, err = constructCodeMap(tds); err != nil {
		return err
	}
	if _, err = statePicture(t, codeMap, 7, 48, true, true, false, false, nil); err != nil {
		return err
	}

	// BLOCK 5
	if _, err = blockchain.InsertChain(types.Blocks{blocks[4]}); err != nil {
		return err
	}
	if codeMap, err = constructCodeMap(tds); err != nil {
		return err
	}
	if _, err = statePicture(t, codeMap, 8, 54, true, true, false, false, nil); err != nil {
		return err
	}

	// BLOCK 6
	if _, err = blockchain.InsertChain(types.Blocks{blocks[5]}); err != nil {
		return err
	}
	if codeMap, err = constructCodeMap(tds); err != nil {
		return err
	}
	if _, err = statePicture(t, codeMap, 9, 54, true, true, false, false, nil); err != nil {
		return err
	}
	if _, err = statePicture(t, codeMap, 10, 110, true, true, true, true, nil); err != nil {
		return err
	}

	// BLOCK 7
	if _, err = blockchain.InsertChain(types.Blocks{blocks[6]}); err != nil {
		return err
	}
	if codeMap, err = constructCodeMap(tds); err != nil {
		return err
	}
	quadTrie, err := statePicture(t, codeMap, 11, 110, true, true, true, true, nil)
	if err != nil {
		return err
	}

	tds.SetResolveReads(true)
	// BLOCK 8
	if _, err = blockchain.InsertChain(types.Blocks{blocks[7]}); err != nil {
		return err
	}
	if codeMap, err = constructCodeMap(tds); err != nil {
		return err
	}
	if _, err = statePicture(t, codeMap, 12, 110, true, true, true, true, nil); err != nil {
		return err
	}

	rs := trie.NewResolveSet(0)
	storageRs := trie.NewResolveSet(0)
	touches, storageTouches := tds.ExtractTouches()
	var touchQuads = make([][]byte, len(touches))
	for _, touch := range touches {
		touchQuad := trie.KeyToQuad(touch)
		rs.AddHex(touchQuad)
		touchQuads = append(touchQuads, touchQuad)
	}
	for _, touch := range storageTouches {
		touchQuad := trie.KeyToQuad(touch)
		storageRs.AddHex(touchQuad)
		touchQuads = append(touchQuads, touchQuad)
	}
	bwb := trie.NewBlockWitnessBuilder(false)
	if codeMap, err = constructCodeMap(tds); err != nil {
		return err
	}
	if err = bwb.MakeBlockWitness(quadTrie, rs, storageRs, codeMap); err != nil {
		return err
	}
	var witness bytes.Buffer
	if err = bwb.WriteTo(&witness); err != nil {
		return err
	}
	var witnessTrie *trie.Trie
	var witnessCodeMap map[common.Hash][]byte
	if witnessTrie, witnessCodeMap, err = trie.BlockWitnessToTrie(witness.Bytes(), false); err != nil {
		return err
	}
	if _, err = statePicture(witnessTrie, witnessCodeMap, 13, 110, true, true, false /*already quad*/, true, touchQuads); err != nil {
		return err
	}

	if err = keyTape(witnessTrie, 14); err != nil {
		return err
	}

	// Repeat the block witness illustration, but without any highlighted keys
	if _, err = statePicture(witnessTrie, witnessCodeMap, 15, 110, true, true, false /*already quad*/, true, nil); err != nil {
		return err
	}

	if err = stateDatabaseMap(dbBolt, 16); err != nil {
		return err
	}
	return nil
}
