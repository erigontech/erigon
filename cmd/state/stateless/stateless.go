package stateless

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ledgerwatch/turbo-geth/common/debug"
	chart "github.com/wcharczuk/go-chart"
	"github.com/wcharczuk/go-chart/drawing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/consensus/misc"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/trie"
	"github.com/ledgerwatch/turbo-geth/visual"
)

var chartColors = []drawing.Color{
	chart.ColorBlack,
	chart.ColorRed,
	chart.ColorBlue,
	chart.ColorYellow,
	chart.ColorOrange,
	chart.ColorGreen,
}

func runBlock(dbstate *state.Stateless, chainConfig *params.ChainConfig,
	bcb core.ChainContext, header *types.Header, block *types.Block, trace bool, checkRoot bool,
) error {
	vmConfig := vm.Config{}
	engine := ethash.NewFullFaker()
	statedb := state.New(dbstate)
	statedb.SetTrace(trace)
	gp := new(core.GasPool).AddGas(block.GasLimit())
	usedGas := new(uint64)
	var receipts types.Receipts
	if chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}
	for _, tx := range block.Transactions() {
		receipt, err := core.ApplyTransaction(chainConfig, bcb, nil, gp, statedb, dbstate, header, tx, usedGas, vmConfig)
		if err != nil {
			return fmt.Errorf("tx %x failed: %v", tx.Hash(), err)
		}
		receipts = append(receipts, receipt)
	}
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	if _, err := engine.FinalizeAndAssemble(chainConfig, header, statedb, block.Transactions(), block.Uncles(), receipts); err != nil {
		return fmt.Errorf("finalize of block %d failed: %v", block.NumberU64(), err)
	}
	dbstate.SetBlockNr(block.NumberU64())

	ctx := chainConfig.WithEIPsFlags(context.Background(), header.Number)
	if err := statedb.CommitBlock(ctx, dbstate); err != nil {
		return fmt.Errorf("commiting block %d failed: %v", block.NumberU64(), err)
	}
	if checkRoot {
		if err := dbstate.CheckRoot(header.Root); err != nil {
			fmt.Printf("block hash = %x\n", block.Hash())
			return fmt.Errorf("error processing block %d: %v", block.NumberU64(), err)
		}
	}
	return nil
}

func statePicture(t *trie.Trie, codeMap map[common.Hash][]byte, number uint64) error {
	filename := fmt.Sprintf("state_%d.dot", number)
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	indexColors := visual.HexIndexColors
	fontColors := visual.HexFontColors
	visual.StartGraph(f, false)
	trie.Visual(t, f, &trie.VisualOpts{
		Highlights:     nil,
		IndexColors:    indexColors,
		FontColors:     fontColors,
		Values:         true,
		CutTerminals:   0,
		CodeMap:        codeMap,
		CodeCompressed: false,
		ValCompressed:  false,
		ValHex:         true,
	})
	visual.EndGraph(f)
	if err := f.Close(); err != nil {
		return err
	}
	return nil
}

func parseStarkBlockFile(starkBlocksFile string) (map[uint64]struct{}, error) {
	dat, err := ioutil.ReadFile(starkBlocksFile)
	if err != nil {
		return nil, err
	}
	blockStrs := strings.Split(string(dat), "\n")
	m := make(map[uint64]struct{})
	for _, blockStr := range blockStrs {
		if len(blockStr) == 0 {
			continue
		}
		if b, err1 := strconv.ParseUint(blockStr, 10, 64); err1 == nil {
			m[b] = struct{}{}
		} else {
			return nil, err1
		}

	}
	return m, nil
}

func starkData(witness *trie.Witness, starkStatsBase string, blockNum uint64) error {
	filename := fmt.Sprintf("%s_%d.txt", starkStatsBase, blockNum)
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	if err = trie.StarkStats(witness, f, false); err != nil {
		return err
	}
	return nil
}

type CreateDbFunc func(string) (ethdb.Database, error)

func Stateless(
	blockNum uint64,
	chaindata string,
	statefile string,
	triesize uint32,
	tryPreRoot bool,
	interval uint64,
	ignoreOlderThan uint64,
	witnessThreshold uint64,
	statsfile string,
	verifySnapshot bool,
	binary bool,
	createDb CreateDbFunc,
	starkBlocksFile string,
	starkStatsBase string,
	useStatelessResolver bool,
	witnessDatabasePath string) {

	state.MaxTrieCacheGen = triesize
	startTime := time.Now()
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()

	timeF, err := os.Create("timings.txt")
	if err != nil {
		panic(err)
	}
	defer timeF.Close()
	fmt.Fprintf(timeF, "blockNr,exec,resolve,stateless_exec,calc_root,mod_root\n")

	ethDb, err := createDb(chaindata)
	check(err)
	defer ethDb.Close()
	chainConfig := params.MainnetChainConfig

	stats, err := NewStatsFile(statsfile)
	check(err)
	defer stats.Close()

	vmConfig := vm.Config{}
	engine := ethash.NewFullFaker()
	bcb, err := core.NewBlockChain(ethDb, nil, chainConfig, engine, vm.Config{}, nil)
	check(err)
	stateDb, err := createDb(statefile)
	check(err)
	defer stateDb.Close()
	var starkBlocks map[uint64]struct{}
	if starkBlocksFile != "" {
		starkBlocks, err = parseStarkBlockFile(starkBlocksFile)
		check(err)
	}
	var preRoot common.Hash
	if blockNum == 1 {
		_, _, _, err = core.SetupGenesisBlock(stateDb, core.DefaultGenesisBlock())
		check(err)
		genesisBlock, _, _, err := core.DefaultGenesisBlock().ToBlock(nil)
		check(err)
		preRoot = genesisBlock.Header().Root
	} else {
		block := bcb.GetBlockByNumber(blockNum - 1)
		fmt.Printf("Block number: %d\n", blockNum-1)
		fmt.Printf("Block root hash: %x\n", block.Root())
		preRoot = block.Root()

		if verifySnapshot {
			fmt.Println("Verifying snapshot..")
			checkRoots(stateDb, preRoot, blockNum-1)
			fmt.Println("Verifying snapshot... OK")
		}
	}
	batch := stateDb.NewBatch()
	defer func() {
		if _, err = batch.Commit(); err != nil {
			fmt.Printf("Failed to commit batch: %v\n", err)
		}
	}()
	tds, err := state.NewTrieDbState(preRoot, batch, blockNum-1)
	check(err)
	if blockNum > 1 {
		tds.Rebuild()
	}
	tds.SetResolveReads(false)
	tds.SetNoHistory(true)
	tds.EnableIntermediateCache(debug.IsIntermediateTrieCache())
	interrupt := false
	var blockWitness []byte
	var bw *trie.Witness

	processed := 0
	blockProcessingStartTime := time.Now()

	defer func() { fmt.Printf("stoppped at block number: %d\n", blockNum) }()

	var witnessDBWriter *WitnessDBWriter
	var witnessDBReader *WitnessDBReader

	if useStatelessResolver && witnessDatabasePath == "" {
		panic("to use stateless resolver, set the witness DB path")
	}

	if witnessDatabasePath != "" {
		var db ethdb.Database
		db, err = createDb(witnessDatabasePath)
		check(err)
		defer db.Close()

		if useStatelessResolver {
			witnessDBReader = NewWitnessDBReader(db)
			fmt.Printf("Will use the stateless resolver with DB: %s\n", witnessDatabasePath)
		} else {
			statsFilePath := fmt.Sprintf("%v.stats.csv", witnessDatabasePath)

			var file *os.File
			file, err = os.OpenFile(statsFilePath, os.O_RDWR|os.O_CREATE, os.ModePerm)
			check(err)
			defer file.Close()

			statsFileCsv := csv.NewWriter(file)
			defer statsFileCsv.Flush()

			witnessDBWriter, err = NewWitnessDBWriter(db, statsFileCsv)
			check(err)
			fmt.Printf("witnesses will be stored to a db at path: %s\n\tstats: %s\n", witnessDatabasePath, statsFilePath)
		}

	}

	for !interrupt {
		trace := blockNum == 50492 // false // blockNum == 545080
		tds.SetResolveReads(blockNum >= witnessThreshold)
		block := bcb.GetBlockByNumber(blockNum)
		if block == nil {
			break
		}
		execStart := time.Now()
		statedb := state.New(tds)
		gp := new(core.GasPool).AddGas(block.GasLimit())
		usedGas := new(uint64)
		header := block.Header()
		tds.StartNewBuffer()
		var receipts types.Receipts
		if chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0 {
			misc.ApplyDAOHardFork(statedb)
		}
		for i, tx := range block.Transactions() {
			statedb.Prepare(tx.Hash(), block.Hash(), i)
			receipt, err := core.ApplyTransaction(chainConfig, bcb, nil, gp, statedb, tds.TrieStateWriter(), header, tx, usedGas, vmConfig)
			if err != nil {
				fmt.Printf("tx %x failed: %v\n", tx.Hash(), err)
				return
			}
			if !chainConfig.IsByzantium(header.Number) {
				tds.StartNewBuffer()
			}
			receipts = append(receipts, receipt)
		}
		// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
		if _, err = engine.FinalizeAndAssemble(chainConfig, header, statedb, block.Transactions(), block.Uncles(), receipts); err != nil {
			fmt.Printf("Finalize of block %d failed: %v\n", blockNum, err)
			return
		}

		execTime1 := time.Since(execStart)
		execStart = time.Now()

		ctx := chainConfig.WithEIPsFlags(context.Background(), header.Number)
		if err := statedb.FinalizeTx(ctx, tds.TrieStateWriter()); err != nil {
			fmt.Printf("FinalizeTx of block %d failed: %v\n", blockNum, err)
			return
		}

		if witnessDBReader != nil {
			tds.SetBlockNr(blockNum)
			err = tds.ResolveStateTrieStateless(witnessDBReader)
			if err != nil {
				fmt.Printf("Failed to statelessly resolve state trie: %v\n", err)
				return
			}
		} else {
			var resolveWitnesses []*trie.Witness
			if resolveWitnesses, err = tds.ResolveStateTrie(witnessDBWriter != nil); err != nil {
				fmt.Printf("Failed to resolve state trie: %v\n", err)
				return
			}

			if len(resolveWitnesses) > 0 {
				witnessDBWriter.MustUpsert(blockNum, state.MaxTrieCacheGen, resolveWitnesses)
			}
		}
		execTime2 := time.Since(execStart)
		blockWitness = nil
		if blockNum >= witnessThreshold {
			// Witness has to be extracted before the state trie is modified
			var blockWitnessStats *trie.BlockWitnessStats
			bw, err = tds.ExtractWitness(trace, binary /* is binary */)
			if err != nil {
				fmt.Printf("error extracting witness for block %d: %v\n", blockNum, err)
				return
			}

			var buf bytes.Buffer
			blockWitnessStats, err = bw.WriteTo(&buf)
			if err != nil {
				fmt.Printf("error extracting witness for block %d: %v\n", blockNum, err)
				return
			}
			blockWitness = buf.Bytes()
			err = stats.AddRow(blockNum, blockWitnessStats)
			check(err)
		}
		finalRootFail := false
		execStart = time.Now()
		if blockNum >= witnessThreshold && blockWitness != nil { // blockWitness == nil means the extraction fails
			var s *state.Stateless
			var w *trie.Witness
			w, err = trie.NewWitnessFromReader(bytes.NewReader(blockWitness), false)
			bw.WriteDiff(w, os.Stdout)
			if err != nil {
				fmt.Printf("error deserializing witness for block %d: %v\n", blockNum, err)
				return
			}
			if _, ok := starkBlocks[blockNum-1]; ok {
				err = starkData(w, starkStatsBase, blockNum-1)
				check(err)
			}
			s, err = state.NewStateless(preRoot, w, blockNum-1, trace, binary /* is binary */)
			if err != nil {
				fmt.Printf("Error making stateless2 for block %d: %v\n", blockNum, err)
				filename := fmt.Sprintf("right_%d.txt", blockNum-1)
				f, err1 := os.Create(filename)
				if err1 == nil {
					defer f.Close()
					tds.PrintTrie(f)
				}
				return
			}
			if _, ok := starkBlocks[blockNum-1]; ok {
				err = statePicture(s.GetTrie(), s.GetCodeMap(), blockNum-1)
				check(err)
			}
			if err = runBlock(s, chainConfig, bcb, header, block, trace, !binary); err != nil {
				fmt.Printf("Error running block %d through stateless2: %v\n", blockNum, err)
				finalRootFail = true
			}
		}
		execTime3 := time.Since(execStart)
		execStart = time.Now()
		var preCalculatedRoot common.Hash
		if tryPreRoot {
			preCalculatedRoot, err = tds.CalcTrieRoots(blockNum == 1)
			if err != nil {
				fmt.Printf("failed to calculate preRoot for block %d: %v\n", blockNum, err)
				return
			}
		}
		execTime4 := time.Since(execStart)
		execStart = time.Now()
		roots, err := tds.UpdateStateTrie()
		if err != nil {
			fmt.Printf("failed to calculate IntermediateRoot: %v\n", err)
			return
		}
		execTime5 := time.Since(execStart)
		if tryPreRoot && tds.LastRoot() != preCalculatedRoot {
			filename := fmt.Sprintf("right_%d.txt", blockNum)
			f, err1 := os.Create(filename)
			if err1 == nil {
				defer f.Close()
				tds.PrintTrie(f)
			}
			fmt.Printf("block %d, preCalculatedRoot %x != lastRoot %x\n", blockNum, preCalculatedRoot, tds.LastRoot())
			return
		}
		if finalRootFail {
			filename := fmt.Sprintf("right_%d.txt", blockNum)
			f, err1 := os.Create(filename)
			if err1 == nil {
				defer f.Close()
				tds.PrintTrie(f)
			}
			return
		}
		if !chainConfig.IsByzantium(header.Number) {
			for i, receipt := range receipts {
				receipt.PostState = roots[i].Bytes()
			}
		}
		nextRoot := roots[len(roots)-1]
		if nextRoot != block.Root() {
			fmt.Printf("Root hash does not match for block %d, expected %x, was %x\n", blockNum, block.Root(), nextRoot)
			return
		}
		tds.SetBlockNr(blockNum)

		err = statedb.CommitBlock(ctx, tds.DbStateWriter())
		if err != nil {
			fmt.Printf("Commiting block %d failed: %v", blockNum, err)
			return
		}

		willSnapshot := interval > 0 && blockNum > 0 && blockNum >= ignoreOlderThan && blockNum%interval == 0

		if batch.BatchSize() >= 100000 || willSnapshot {
			if _, err := batch.Commit(); err != nil {
				fmt.Printf("Failed to commit batch: %v\n", err)
				return
			}
			tds.PruneTries(false)
		}

		if willSnapshot {
			// Snapshots of the state will be written to the same directory as the state file
			fmt.Printf("\nSaving snapshot at block %d, hash %x\n", blockNum, block.Root())

			saveSnapshot(stateDb, fmt.Sprintf("%s_%d", statefile, blockNum), createDb)
		}

		preRoot = header.Root
		blockNum++
		processed++

		if blockNum%10 == 0 {
			// overwrite terminal line, if no snapshot was made and not the first line
			if blockNum > 0 && !willSnapshot {
				fmt.Printf("\r")
			}

			secondsSinceStart := time.Since(blockProcessingStartTime) / time.Second
			if secondsSinceStart < 1 {
				secondsSinceStart = 1
			}
			blocksPerSecond := float64(processed) / float64(secondsSinceStart)

			fmt.Printf("Processed %d blocks (%v blocks/sec)", blockNum, blocksPerSecond)
		}
		fmt.Fprintf(timeF, "%d,%d,%d,%d,%d,%d\n", blockNum,
			execTime1.Nanoseconds(),
			execTime2.Nanoseconds(),
			execTime3.Nanoseconds(),
			execTime4.Nanoseconds(),
			execTime5.Nanoseconds())
		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			fmt.Println("interrupted, please wait for cleanup...")
		default:
		}
	}
	fmt.Printf("Processed %d blocks\n", blockNum)
	fmt.Printf("Next time specify -block %d\n", blockNum)
	fmt.Printf("Stateless client analysis took %s\n", time.Since(startTime))
}
