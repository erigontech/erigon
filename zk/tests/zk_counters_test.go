package vm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/ethash/ethashcfg"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/eth/ethconsensusconfig"
	"github.com/ledgerwatch/erigon/node/nodecfg"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/stages/mock"
	seq "github.com/ledgerwatch/erigon/zk/sequencer"
	"github.com/ledgerwatch/erigon/zk/tx"
	zktypes "github.com/ledgerwatch/erigon/zk/types"
	"github.com/ledgerwatch/erigon/zkevm/hex"
	"github.com/ledgerwatch/log/v3"
	"github.com/status-im/keycard-go/hexutils"
)

const (
	root                = "./testdata"
	transactionGasLimit = 30000000
)

var noop = state.NewNoopWriter()

type vector struct {
	BatchL2Data        string `json:"batchL2Data"`
	BatchL2DataDecoded []byte
	Genesis            []struct {
		Address  string                      `json:"address"`
		Nonce    string                      `json:"nonce"`
		Balance  string                      `json:"balance"`
		PvtKey   string                      `json:"pvtKey"`
		ByteCode string                      `json:"bytecode"`
		Storage  map[common.Hash]common.Hash `json:"storage,omitempty"`
	} `json:"genesis"`
	VirtualCounters struct {
		Steps    int `json:"steps"`
		Arith    int `json:"arith"`
		Binary   int `json:"binary"`
		MemAlign int `json:"memAlign"`
		Keccaks  int `json:"keccaks"`
		Padding  int `json:"padding"`
		Poseidon int `json:"poseidon"`
		Sha256   int `json:"sha256"`
	} `json:"virtualCounters"`
	SequencerAddress string `json:"sequencerAddress"`
	ChainId          int64  `json:"chainID"`
	ForkId           uint64 `json:"forkID"`
	ExpectedOldRoot  string `json:"expectedOldRoot"`
	ExpectedNewRoot  string `json:"expectedNewRoot"`
	SmtDepths        []int  `json:"smtDepths"`
	Txs              [2]struct {
		Type            int    `json:"type"`
		DeltaTimestamp  string `json:"deltaTimestamp"`
		IndexL1InfoTree int    `json:"indexL1InfoTree"`
		L1Info          *struct {
			GlobalExitRoot string `json:"globalExitRoot"`
			BlockHash      string `json:"blockHash"`
			Timestamp      string `json:"timestamp"`
		} `json:"l1Info"`
	} `json:"txs"`
}

type testVector struct {
	idx      int
	fileName string
	vector   vector
}

func Test_RunTestVectors(t *testing.T) {
	// we need to ensure we're running in a sequencer context to wrap the jump table
	os.Setenv(seq.SEQUENCER_ENV_KEY, "1")
	defer os.Setenv(seq.SEQUENCER_ENV_KEY, "0")

	m := mock.Mock(t)
	blockReader, _ := m.BlocksIO()

	testVectors, err := testVectors()
	if err != nil {
		t.Fatalf("could not get vector tests: %v", err)
	}

	for idx, test := range testVectors {
		t.Run(test.fileName, func(t *testing.T) {
			runTest(t, blockReader, testVectors[idx])
		})
	}
}

func testVectors() ([]testVector, error) {
	files, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("could not read directory %s: %v", root, err)
	}

	var tests []testVector
	for _, file := range files {
		var vectors []vector
		contents, err := os.ReadFile(fmt.Sprintf("%s/%s", root, file.Name()))
		if err != nil {
			return nil, fmt.Errorf("could not read file %s: %v", file.Name(), err)
		}

		if err = json.Unmarshal(contents, &vectors); err != nil {
			return nil, fmt.Errorf("could not unmarshal file %s: %v", file.Name(), err)
		}
		for i := len(vectors) - 1; i >= 0; i-- {
			tests = append(tests, testVector{
				idx:      i,
				fileName: file.Name(),
				vector:   vectors[i],
			})
		}
	}

	return tests, nil
}

func runTest(t *testing.T, blockReader services.FullBlockReader, test testVector) {
	// arrange
	decodedBlocks, err := decodeBlocks(test.vector, test.fileName)
	if err != nil {
		t.Fatalf("could not decode blocks: %v", err)
	}

	db, tx := memdb.NewTestTx(t)
	defer db.Close()
	defer tx.Rollback()

	for _, table := range kv.ChaindataTables {
		if err = tx.CreateBucket(table); err != nil {
			t.Fatalf("could not create bucket: %v", err)
		}
	}

	genesisBlock, err := writeGenesisState(&test.vector, decodedBlocks, test.idx, tx)
	if err != nil {
		t.Fatalf("could not write genesis state: %v", err)
	}

	genesisRoot := genesisBlock.Root()
	expectedGenesisRoot := common.HexToHash(test.vector.ExpectedOldRoot)
	if genesisRoot != expectedGenesisRoot {
		t.Fatal("genesis root did not match expected")
	}

	header := &types.Header{
		Number:     big.NewInt(1),
		Difficulty: big.NewInt(0),
	}

	chainCfg := chainConfig(test.vector.ChainId)
	ethashCfg := newEthashConfig()
	vmCfg := newVmConfig()

	engine := newEngine(chainCfg, ethashCfg, blockReader, db)
	ibs := state.New(state.NewPlainStateReader(tx))

	shouldVerifyMerkleProof := false
	if test.vector.Txs[0].Type == 11 {
		if test.vector.Txs[0].IndexL1InfoTree != 0 {
			shouldVerifyMerkleProof = true
		}
		if err := updateGER(test.vector, ibs, chainCfg); err != nil {
			t.Fatalf("could not update ger: %v", err)
		}
	}

	batchCollector := vm.NewBatchCounterCollector(test.vector.SmtDepths[0], uint16(test.vector.ForkId), 0.6, false, nil)

	if err = applyTransactionsToDecodedBlocks(
		decodedBlocks,
		test.vector,
		chainCfg,
		vmCfg,
		header,
		tx,
		engine,
		ibs,
		batchCollector,
	); err != nil {
		t.Fatalf("could not apply transactions: %v", err)
	}

	// act
	errors, err := testVirtualCounters(test.vector, batchCollector, shouldVerifyMerkleProof)
	if err != nil {
		t.Fatalf("could not test virtual counters: %v", err)
	}

	// assert
	if len(errors) > 0 {
		t.Errorf("counter mismath in file %s: %s \n", test.fileName, strings.Join(errors, " "))
	}
}

func decodeBlocks(v vector, fileName string) ([]tx.DecodedBatchL2Data, error) {
	batchL2DataDecoded, err := hex.DecodeHex(v.BatchL2Data)
	if err != nil {
		return nil, fmt.Errorf("could not decode batchL2Data: %v", err)
	}

	decodedBlocks, err := tx.DecodeBatchL2Blocks(batchL2DataDecoded, v.ForkId)
	if err != nil {
		return nil, fmt.Errorf("could not decode batchL2Blocks: %v", err)
	}
	fmt.Println(decodedBlocks[0].Transactions)
	if len(decodedBlocks) == 0 {
		fmt.Printf("found no blocks in file %s", fileName)
	}
	for _, block := range decodedBlocks {
		if len(block.Transactions) == 0 {
			fmt.Printf("found no transactions in file %s", fileName)
		}
	}

	return decodedBlocks, nil
}

func writeGenesisState(v *vector, decodedBlocks []tx.DecodedBatchL2Data, idx int, tx kv.RwTx) (*types.Block, error) {
	genesisAccounts := map[common.Address]types.GenesisAccount{}

	for _, g := range v.Genesis {
		addr := common.HexToAddress(g.Address)
		key, err := hex.DecodeHex(g.PvtKey)
		if err != nil {
			return nil, fmt.Errorf("could not decode private key: %v", err)
		}
		nonce, err := strconv.ParseUint(g.Nonce, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("could not parse nonce: %v", err)
		}
		balance, ok := new(big.Int).SetString(g.Balance, 10)
		if !ok {
			return nil, errors.New("could not parse balance")
		}
		code, err := hex.DecodeHex(g.ByteCode)
		if err != nil {
			return nil, fmt.Errorf("could not decode bytecode: %v", err)
		}
		acc := types.GenesisAccount{
			Balance:    balance,
			Nonce:      nonce,
			PrivateKey: key,
			Code:       code,
			Storage:    g.Storage,
		}
		genesisAccounts[addr] = acc
	}

	genesis := &types.Genesis{
		Alloc: genesisAccounts,
		Config: &chain.Config{
			ChainID: big.NewInt(v.ChainId),
		},
	}

	genesisBlock, _, sparseTree, err := core.WriteGenesisState(genesis, tx, fmt.Sprintf("%s/temp-%v", os.TempDir(), idx), log.New())
	if err != nil {
		return nil, fmt.Errorf("could not write genesis state: %v", err)
	}
	smtDepth := sparseTree.GetDepth()
	for len(v.SmtDepths) < len(decodedBlocks) {
		v.SmtDepths = append(v.SmtDepths, smtDepth)
	}
	if len(v.SmtDepths) == 0 {
		v.SmtDepths = append(v.SmtDepths, smtDepth)
	}

	return genesisBlock, nil
}

func chainConfig(chainId int64) *chain.Config {
	chainConfig := params.ChainConfigByChainName("hermez-dev")
	chainConfig.ChainID = big.NewInt(chainId)

	chainConfig.ForkID4Block = big.NewInt(0)
	chainConfig.ForkID5DragonfruitBlock = big.NewInt(0)
	chainConfig.ForkID6IncaBerryBlock = big.NewInt(0)
	chainConfig.ForkID7EtrogBlock = big.NewInt(0)
	chainConfig.ForkID88ElderberryBlock = big.NewInt(0)

	return chainConfig
}

func newEthashConfig() *ethashcfg.Config {
	return &ethashcfg.Config{
		CachesInMem:      1,
		CachesLockMmap:   true,
		DatasetDir:       "./dataset",
		DatasetsInMem:    1,
		DatasetsOnDisk:   1,
		DatasetsLockMmap: true,
		PowMode:          ethashcfg.ModeFake,
		NotifyFull:       false,
		Log:              nil,
	}
}

func newVmConfig() vm.ZkConfig {
	return vm.ZkConfig{
		Config: vm.Config{
			Debug:         false,
			Tracer:        nil,
			NoRecursion:   false,
			NoBaseFee:     false,
			SkipAnalysis:  false,
			TraceJumpDest: false,
			NoReceipts:    false,
			ReadOnly:      false,
			StatelessExec: false,
			RestoreState:  false,
			ExtraEips:     nil,
		},
	}
}

func updateGER(v vector, ibs *state.IntraBlockState, chainCfg *chain.Config) error {
	parentRoot := common.Hash{}
	deltaTimestamp, err := strconv.ParseUint(v.Txs[0].DeltaTimestamp, 10, 64)
	if err != nil {
		return fmt.Errorf("could not parse delta timestamp: %v", err)
	}
	ibs.PreExecuteStateSet(chainCfg, 1, deltaTimestamp, &parentRoot)

	if v.Txs[0].L1Info == nil {
		return nil
	}

	// handle writing to the ger manager contract
	timestamp, err := strconv.ParseUint(v.Txs[0].L1Info.Timestamp, 10, 64)
	if err != nil {
		return fmt.Errorf("could not parse timestamp: %v", err)
	}
	ger := string(v.Txs[0].L1Info.GlobalExitRoot)
	blockHash := string(v.Txs[0].L1Info.BlockHash)

	hexutil.Remove0xPrefixIfExists(&ger)
	hexutil.Remove0xPrefixIfExists(&blockHash)

	l1info := &zktypes.L1InfoTreeUpdate{
		GER:        common.BytesToHash(hexutils.HexToBytes(ger)),
		ParentHash: common.BytesToHash(hexutils.HexToBytes(blockHash)),
		Timestamp:  timestamp,
	}
	// first check if this ger has already been written
	l1BlockHash := ibs.ReadGerManagerL1BlockHash(l1info.GER)
	if l1BlockHash == (common.Hash{}) {
		// not in the contract so let's write it!
		ibs.WriteGerManagerL1BlockHash(l1info.GER, l1info.ParentHash)
	}

	return nil
}

func applyTransactionsToDecodedBlocks(
	decodedBlocks []tx.DecodedBatchL2Data,
	v vector,
	chainCfg *chain.Config,
	vmCfg vm.ZkConfig,
	header *types.Header,
	tx kv.RwTx,
	engine consensus.Engine,
	ibs *state.IntraBlockState,
	batchCollector *vm.BatchCounterCollector,
) error {
	sequencerAddress := common.HexToAddress(v.SequencerAddress)

	getHeader := func(hash common.Hash, number uint64) *types.Header { return rawdb.ReadHeader(tx, hash, number) }
	blockHashFunc := core.GetHashFn(header, getHeader)

	blockStarted := false
	for i, block := range decodedBlocks {
		for _, transaction := range block.Transactions {
			vmCfg.Config.SkipAnalysis = core.SkipAnalysis(chainCfg, header.Number.Uint64())

			blockContext := core.NewEVMBlockContext(header, blockHashFunc, engine, &sequencerAddress)

			if !blockStarted {
				overflow, err := batchCollector.StartNewBlock(false)
				if err != nil {
					return fmt.Errorf("could not start new block: %v", err)
				}
				if overflow {
					return fmt.Errorf("unexpected overflow")
				}
				blockStarted = true
			}
			txCounters := vm.NewTransactionCounter(transaction, v.SmtDepths[i], uint16(v.ForkId), 0.6, false)
			overflow, err := batchCollector.AddNewTransactionCounters(txCounters)
			if err != nil {
				return fmt.Errorf("could not add new transaction counters: %v", err)
			}

			gasPool := new(core.GasPool).AddGas(transactionGasLimit)

			vmCfg.CounterCollector = txCounters.ExecutionCounters()

			evm := vm.NewZkEVM(blockContext, evmtypes.TxContext{}, ibs, chainCfg, vmCfg)

			_, result, err := core.ApplyTransaction_zkevm(
				chainCfg,
				engine,
				evm,
				gasPool,
				ibs,
				noop,
				header,
				transaction,
				&header.GasUsed,
				zktypes.EFFECTIVE_GAS_PRICE_PERCENTAGE_MAXIMUM,
				true,
			)
			if err != nil {
				// this could be deliberate in the test so just move on and note it
				fmt.Println("err handling tx", err)
				continue
			}
			if overflow {
				return fmt.Errorf("unexpected overflow")
			}

			if err = txCounters.ProcessTx(ibs, result.ReturnData); err != nil {
				return fmt.Errorf("could not process tx: %v", err)
			}

			batchCollector.UpdateExecutionAndProcessingCountersCache(txCounters)
		}
	}

	return nil
}

func newEngine(chainCfg *chain.Config, ethashCfg *ethashcfg.Config, blockReader services.FullBlockReader, db kv.RwDB) consensus.Engine {
	logger := log.New()
	return ethconsensusconfig.CreateConsensusEngine(
		context.Background(),
		&nodecfg.Config{Dirs: datadir.New("./datadir")},
		chainCfg,
		ethashCfg,
		[]string{},
		true,
		heimdall.NewHeimdallClient("", logger),
		true,
		blockReader,
		db.ReadOnly(),
		logger,
	)
}

func testVirtualCounters(v vector, batchCollector *vm.BatchCounterCollector, shouldVerifyMerkleProof bool) ([]string, error) {
	combined, err := batchCollector.CombineCollectors(shouldVerifyMerkleProof)
	if err != nil {
		return nil, fmt.Errorf("could not combine collectors: %v", err)
	}

	vc := v.VirtualCounters

	var errors []string
	if vc.Keccaks != combined[vm.K].Used() {
		errors = append(errors, fmt.Sprintf("K=%v:%v", combined[vm.K].Used(), vc.Keccaks))
	}
	if vc.Arith != combined[vm.A].Used() {
		errors = append(errors, fmt.Sprintf("A=%v:%v", combined[vm.A].Used(), vc.Arith))
	}
	if vc.Binary != combined[vm.B].Used() {
		errors = append(errors, fmt.Sprintf("B=%v:%v", combined[vm.B].Used(), vc.Binary))
	}
	if vc.Padding != combined[vm.D].Used() {
		errors = append(errors, fmt.Sprintf("D=%v:%v", combined[vm.D].Used(), vc.Padding))
	}
	if vc.Sha256 != combined[vm.SHA].Used() {
		errors = append(errors, fmt.Sprintf("SHA=%v:%v", combined[vm.SHA].Used(), vc.Sha256))
	}
	if vc.MemAlign != combined[vm.M].Used() {
		errors = append(errors, fmt.Sprintf("M=%v:%v", combined[vm.M].Used(), vc.MemAlign))
	}
	if vc.Poseidon != combined[vm.P].Used() {
		errors = append(errors, fmt.Sprintf("P=%v:%v", combined[vm.P].Used(), vc.Poseidon))
	}
	if vc.Steps != combined[vm.S].Used() {
		errors = append(errors, fmt.Sprintf("S=%v:%v", combined[vm.S].Used(), vc.Steps))
	}

	return errors, nil
}
