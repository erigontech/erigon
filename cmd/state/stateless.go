package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"
	"time"

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
)

var chartColors = []drawing.Color{
	chart.ColorBlack,
	chart.ColorRed,
	chart.ColorBlue,
	chart.ColorYellow,
	chart.ColorOrange,
	chart.ColorGreen,
}

func runBlock(tds *state.TrieDbState, dbstate *state.Stateless, chainConfig *params.ChainConfig,
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
	if err := dbstate.CheckRoot(header.Root, checkRoot); err != nil {
		filename := fmt.Sprintf("right_%d.txt", block.NumberU64())
		f, err1 := os.Create(filename)
		if err1 == nil {
			defer f.Close()
			tds.PrintTrie(f)
		}
		return fmt.Errorf("error processing block %d: %v", block.NumberU64(), err)
	}
	return nil
}

func writeStats(w io.Writer, blockNum uint64, blockProof trie.BlockProof) {
	var totalCShorts, totalCValues, totalCodes, totalShorts, totalValues int
	for _, short := range blockProof.CShortKeys {
		l := len(short)
		if short[l-1] == 16 {
			l -= 1
		}
		l = l/2 + 1
		totalCShorts += l
	}
	for _, value := range blockProof.CValues {
		totalCValues += len(value)
	}
	for _, code := range blockProof.Codes {
		totalCodes += len(code)
	}
	for _, short := range blockProof.ShortKeys {
		l := len(short)
		if short[l-1] == 16 {
			l -= 1
		}
		l = l/2 + 1
		totalShorts += l
	}
	for _, value := range blockProof.Values {
		totalValues += len(value)
	}
	fmt.Fprintf(w, "%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
		blockNum, len(blockProof.Contracts), len(blockProof.CMasks), len(blockProof.CHashes), len(blockProof.CShortKeys), len(blockProof.CValues), len(blockProof.Codes),
		len(blockProof.Masks), len(blockProof.Hashes), len(blockProof.ShortKeys), len(blockProof.Values), totalCShorts, totalCValues, totalCodes, totalShorts, totalValues,
	)
}

func stateless(genLag, consLag int) {
	//state.MaxTrieCacheGen = 64*1024
	startTime := time.Now()
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()

	ethDb, err := ethdb.NewBoltDatabase("/Volumes/tb4/turbo-geth/geth/chaindata")
	//ethDb, err := ethdb.NewBoltDatabase("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata1")
	//ethDb, err := ethdb.NewBoltDatabase("/home/akhounov/.ethereum/geth/chaindata1")
	check(err)
	defer ethDb.Close()
	chainConfig := params.MainnetChainConfig
	//slFile, err := os.OpenFile("/Volumes/tb4/turbo-geth/stateless.csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	slFile, err := os.OpenFile("stateless.csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	check(err)
	defer slFile.Close()
	w := bufio.NewWriter(slFile)
	defer w.Flush()
	slfFile, err := os.OpenFile(fmt.Sprintf("stateless_%d.csv", consLag), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	check(err)
	defer slfFile.Close()
	wf := bufio.NewWriter(slfFile)
	vmConfig := vm.Config{}
	engine := ethash.NewFullFaker()
	bcb, err := core.NewBlockChain(ethDb, nil, chainConfig, engine, vm.Config{}, nil)
	check(err)
	stateDb, err := ethdb.NewBoltDatabase("/Volumes/tb4/turbo-geth-copy/state")
	check(err)
	db := stateDb.DB()
	blockNum := uint64(*block)
	var preRoot common.Hash
	if blockNum == 1 {
		_, _, _, err = core.SetupGenesisBlock(stateDb, core.DefaultGenesisBlock())
		check(err)
		genesisBlock, _, _, err := core.DefaultGenesisBlock().ToBlock(nil)
		check(err)
		preRoot = genesisBlock.Header().Root
	} else {
		//load_snapshot(db, fmt.Sprintf("/Volumes/tb4/turbo-geth-copy/state_%d", blockNum-1))
		//loadCodes(db, ethDb)
		block := bcb.GetBlockByNumber(blockNum - 1)
		fmt.Printf("Block number: %d\n", blockNum-1)
		fmt.Printf("Block root hash: %x\n", block.Root())
		preRoot = block.Root()
		check_roots(stateDb, db, preRoot, blockNum-1)
	}
	batch := stateDb.NewBatch()
	tds, err := state.NewTrieDbState(preRoot, batch, blockNum-1)
	check(err)
	if blockNum > 1 {
		tds.Rebuild()
	}
	tds.SetResolveReads(false)
	tds.SetNoHistory(true)
	interrupt := false
	var thresholdBlock uint64 = 5000000
	//prev := make(map[uint64]*state.Stateless)
	var proofGen *state.Stateless  // Generator of proofs
	var proofCons *state.Stateless // Consumer of proofs
	for !interrupt {
		trace := false // blockNum == 1807
		if trace {
			filename := fmt.Sprintf("right_%d.txt", blockNum-1)
			f, err1 := os.Create(filename)
			if err1 == nil {
				defer f.Close()
				tds.PrintTrie(f)
				//tds.PrintStorageTrie(f, common.HexToHash("1a4fa162e70315921486693f1d5943b7704232081b39206774caa567d63f633f"))
			}
		}
		tds.SetResolveReads(blockNum >= thresholdBlock)
		block := bcb.GetBlockByNumber(blockNum)
		if block == nil {
			break
		}
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

		ctx := chainConfig.WithEIPsFlags(context.Background(), header.Number)
		if err := statedb.FinalizeTx(ctx, tds.TrieStateWriter()); err != nil {
			fmt.Printf("FinalizeTx of block %d failed: %v\n", blockNum, err)
			return
		}

		roots, err := tds.ComputeTrieRoots()
		if err != nil {
			fmt.Printf("Failed to calculate IntermediateRoot: %v\n", err)
			return
		}
		if !chainConfig.IsByzantium(header.Number) {
			for i, receipt := range receipts {
				receipt.PostState = roots[i].Bytes()
			}
		}
		nextRoot := roots[len(roots)-1]
		//fmt.Printf("Next root %x\n", nextRoot)
		if nextRoot != block.Root() {
			fmt.Printf("Root hash does not match for block %d, expected %x, was %x\n", blockNum, block.Root(), nextRoot)
		}
		tds.SetBlockNr(blockNum)

		err = statedb.CommitBlock(ctx, tds.DbStateWriter())
		if err != nil {
			fmt.Errorf("Commiting block %d failed: %v", blockNum, err)
			return
		}
		if _, err := batch.Commit(); err != nil {
			fmt.Printf("Failed to commit batch: %v\n", err)
			return
		}
		if (blockNum > 2000000 && blockNum%500000 == 0) || (blockNum > 4000000 && blockNum%100000 == 0) {
			save_snapshot(db, fmt.Sprintf("/Volumes/tb4/turbo-geth-copy/state_%d", blockNum))
		}
		if blockNum >= thresholdBlock {
			blockProof := tds.ExtractProofs(trace)
			dbstate, err := state.NewStateless(preRoot, blockProof, block.NumberU64()-1, trace)
			if err != nil {
				fmt.Printf("Error making state for block %d: %v\n", blockNum, err)
			} else {
				if err := runBlock(tds, dbstate, chainConfig, bcb, header, block, trace, true); err != nil {
					fmt.Printf("Error running block %d through stateless0: %v\n", blockNum, err)
				} else {
					writeStats(w, blockNum, blockProof)
				}
			}
			if proofCons == nil {
				proofCons, err = state.NewStateless(preRoot, blockProof, block.NumberU64()-1, false)
				if err != nil {
					fmt.Printf("Error making proof consumer for block %d: %v\n", blockNum, err)
				}
			}
			if proofGen == nil {
				proofGen, err = state.NewStateless(preRoot, blockProof, block.NumberU64()-1, false)
				if err != nil {
					fmt.Printf("Error making proof generator for block %d: %v\n", blockNum, err)
				}
			}
			if proofGen != nil && proofCons != nil {
				if blockNum > uint64(consLag) {
					pBlockProof := proofGen.ThinProof(blockProof, block.NumberU64()-1, blockNum-uint64(consLag), trace)
					if err := proofCons.ApplyProof(preRoot, pBlockProof, block.NumberU64()-1, false); err != nil {
						fmt.Printf("Error applying thin proof to consumer: %v\n", err)
						return
					}
					writeStats(wf, blockNum, pBlockProof)
				} else {
					if err := proofCons.ApplyProof(preRoot, blockProof, block.NumberU64()-1, false); err != nil {
						fmt.Printf("Error applying proof to consumer: %v\n", err)
						return
					}
				}
				if err := runBlock(tds, proofCons, chainConfig, bcb, header, block, trace, false); err != nil {
					fmt.Printf("Error running block %d through proof consumer: %v\n", blockNum, err)
				}
				if blockNum > uint64(consLag) {
					proofCons.Prune(blockNum-uint64(consLag), false)
				}
			}
			if proofGen != nil {
				if err := proofGen.ApplyProof(preRoot, blockProof, block.NumberU64()-1, false); err != nil {
					fmt.Printf("Error applying proof to generator: %v\n", err)
					return
				}
				if err := runBlock(tds, proofGen, chainConfig, bcb, header, block, trace, false); err != nil {
					fmt.Printf("Error running block %d through proof generator: %v\nn", blockNum, err)
				}
				if blockNum > uint64(genLag) {
					proofGen.Prune(blockNum-uint64(genLag), false)
				}
			}
		}
		preRoot = header.Root
		blockNum++
		if blockNum%1000 == 0 {
			tds.PruneTries(true)
			fmt.Printf("Processed %d blocks\n", blockNum)
		}
		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			fmt.Println("interrupted, please wait for cleanup...")
		default:
		}
	}
	stateDb.Close()
	fmt.Printf("Processed %d blocks\n", blockNum)
	fmt.Printf("Next time specify -block %d\n", blockNum)
	fmt.Printf("Stateless client analysis took %s\n", time.Since(startTime))
}

func stateless_chart_key_values(filename string, right []int, chartFileName string, start int, startColor int) {
	file, err := os.Open(filename)
	check(err)
	defer file.Close()
	reader := csv.NewReader(bufio.NewReader(file))
	var blocks []float64
	var vals [22][]float64
	count := 0
	for records, _ := reader.Read(); len(records) == 16; records, _ = reader.Read() {
		count++
		if count < start {
			continue
		}
		blocks = append(blocks, parseFloat64(records[0])/1000000.0)
		for i := 0; i < 22; i++ {
			cProofs := 4.0*parseFloat64(records[2]) + 32.0*parseFloat64(records[3]) + parseFloat64(records[11]) + parseFloat64(records[12])
			proofs := 4.0*parseFloat64(records[7]) + 32.0*parseFloat64(records[8]) + parseFloat64(records[14]) + parseFloat64(records[15])
			switch i {
			case 1, 6:
				vals[i] = append(vals[i], 4.0*parseFloat64(records[i+1]))
			case 2, 7:
				vals[i] = append(vals[i], 32.0*parseFloat64(records[i+1]))
			case 15:
				vals[i] = append(vals[i], cProofs)
			case 16:
				vals[i] = append(vals[i], proofs)
			case 17:
				vals[i] = append(vals[i], cProofs+proofs+parseFloat64(records[13]))
			case 18:
				vals[i] = append(vals[i], 4.0*parseFloat64(records[2])+4.0*parseFloat64(records[7]))
			case 19:
				vals[i] = append(vals[i], 4.0*parseFloat64(records[2])+4.0*parseFloat64(records[7])+
					parseFloat64(records[3])+parseFloat64(records[4])+parseFloat64(records[10])+parseFloat64(records[11]))
			case 20:
				vals[i] = append(vals[i], 4.0*parseFloat64(records[2])+4.0*parseFloat64(records[7])+
					parseFloat64(records[4])+parseFloat64(records[5])+parseFloat64(records[10])+parseFloat64(records[11])+
					32.0*parseFloat64(records[3])+32.0*parseFloat64(records[8]))
			case 21:
				vals[i] = append(vals[i], 4.0*parseFloat64(records[2])+4.0*parseFloat64(records[7])+
					parseFloat64(records[4])+parseFloat64(records[5])+parseFloat64(records[10])+parseFloat64(records[11])+
					32.0*parseFloat64(records[3])+32.0*parseFloat64(records[8])+parseFloat64(records[13]))
			default:
				vals[i] = append(vals[i], parseFloat64(records[i+1]))
			}
		}
	}
	var windowSums [22]float64
	var window int = 1024
	var movingAvgs [22][]float64
	for i := 0; i < 22; i++ {
		movingAvgs[i] = make([]float64, len(blocks)-(window-1))
	}
	for j := 0; j < len(blocks); j++ {
		for i := 0; i < 22; i++ {
			windowSums[i] += vals[i][j]
		}
		if j >= window {
			for i := 0; i < 22; i++ {
				windowSums[i] -= vals[i][j-window]
			}
		}
		if j >= window-1 {
			for i := 0; i < 22; i++ {
				movingAvgs[i][j-window+1] = windowSums[i] / float64(window)
			}
		}
	}
	movingBlock := blocks[window-1:]
	seriesNames := [22]string{
		"Number of contracts",
		"Contract masks",
		"Contract hashes",
		"Number of contract leaf keys",
		"Number of contract leaf vals",
		"Number of contract codes",
		"Masks",
		"Hashes",
		"Number of leaf keys",
		"Number of leaf values",
		"Total size of contract leaf keys",
		"Total size of contract leaf vals",
		"Total size of codes",
		"Total size of leaf keys",
		"Total size of leaf vals",
		"Block proofs (contracts only)",
		"Block proofs (without contracts)",
		"Block proofs (total)",
		"Structure (total)",
		"Leaves (total)",
		"Hashes (total)",
		"Code (total)",
	}
	var currentColor int = startColor
	var series []chart.Series
	for _, r := range right {
		s := &chart.ContinuousSeries{
			Name: seriesNames[r],
			Style: chart.Style{
				Show:        true,
				StrokeWidth: 0.0,
				StrokeColor: chartColors[currentColor],
				FillColor:   chartColors[currentColor],
				//FillColor:   chartColors[currentColor].WithAlpha(100),
			},
			XValues: movingBlock,
			YValues: movingAvgs[r],
		}
		currentColor++
		series = append(series, s)
	}

	graph1 := chart.Chart{
		Width:  1280,
		Height: 720,
		Background: chart.Style{
			Padding: chart.Box{
				Top: 50,
			},
		},
		YAxis: chart.YAxis{
			Name:      "kBytes",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%d kB", int(v.(float64)/1024.0))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorBlack,
				StrokeWidth: 1.0,
			},
			//GridLines: days(),
		},
		/*
			YAxisSecondary: chart.YAxis{
				NameStyle: chart.StyleShow(),
				Style: chart.StyleShow(),
				TickStyle: chart.Style{
					TextRotationDegrees: 45.0,
				},
				ValueFormatter: func(v interface{}) string {
					return fmt.Sprintf("%d", int(v.(float64)))
				},
			},
		*/
		XAxis: chart.XAxis{
			Name: "Blocks, million",
			Style: chart.Style{
				Show: true,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.3fm", v.(float64))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorAlternateGray,
				StrokeWidth: 1.0,
			},
			//GridLines: blockMillions(),
		},
		Series: series,
	}

	graph1.Elements = []chart.Renderable{chart.LegendThin(&graph1)}

	buffer := bytes.NewBuffer([]byte{})
	err = graph1.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile(chartFileName, buffer.Bytes(), 0644)
	check(err)
}
