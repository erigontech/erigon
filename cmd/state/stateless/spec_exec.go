package stateless

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/wcharczuk/go-chart"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/core/vm/stack"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

type CombTracer struct {
	loadedS      map[common.Address]map[common.Hash]struct{}
	wroteS       map[common.Address]map[common.Hash]struct{}
	totalSstores int
	nakedSstores int
	totalSloads  int
	nakedSloads  int
	loaded       map[common.Address]struct{}
	wrote        map[common.Address]struct{}
	totalWrites  int
	nakedWrites  int
	totalReads   int
	nakedReads   int
}

func NewCombTracer() *CombTracer {
	return &CombTracer{
		loadedS: make(map[common.Address]map[common.Hash]struct{}),
		wroteS:  make(map[common.Address]map[common.Hash]struct{}),
		loaded:  make(map[common.Address]struct{}),
		wrote:   make(map[common.Address]struct{}),
	}
}

func (ct *CombTracer) ResetCounters() {
	ct.totalSstores = 0
	ct.nakedSstores = 0
	ct.totalSloads = 0
	ct.nakedSloads = 0
	ct.totalWrites = 0
	ct.nakedWrites = 0
	ct.totalReads = 0
	ct.nakedReads = 0
}

func (ct *CombTracer) ResetSets() {
	ct.loadedS = make(map[common.Address]map[common.Hash]struct{})
	ct.wroteS = make(map[common.Address]map[common.Hash]struct{})
	ct.loaded = make(map[common.Address]struct{})
	ct.wrote = make(map[common.Address]struct{})
}

func (ct *CombTracer) CaptureStart(depth int, from common.Address, to common.Address, call bool, input []byte, gas uint64, value *big.Int) error {
	return nil
}
func (ct *CombTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, _ *stack.ReturnStack, rData []byte, contract *vm.Contract, depth int, err error) error {
	if op == vm.SSTORE {
		addr := contract.Address()
		if stack.Len() == 0 {
			return nil
		}
		loc := common.Hash(stack.Back(0).Bytes32())
		if l1, ok1 := ct.loadedS[addr]; ok1 {
			if _, ok2 := l1[loc]; !ok2 {
				ct.nakedSstores++
				l1[loc] = struct{}{}
			}
		} else {
			ct.nakedSstores++
			l2 := make(map[common.Hash]struct{})
			l2[loc] = struct{}{}
			ct.loadedS[addr] = l2
		}
		if l1, ok1 := ct.wroteS[addr]; ok1 {
			if _, ok2 := l1[loc]; !ok2 {
				ct.totalSstores++
				l1[loc] = struct{}{}
			}
		} else {
			ct.totalSstores++
			l2 := make(map[common.Hash]struct{})
			l2[loc] = struct{}{}
			ct.wroteS[addr] = l2
		}
	} else if op == vm.SLOAD {
		addr := contract.Address()
		if stack.Len() == 0 {
			return nil
		}
		loc := common.Hash(stack.Back(0).Bytes32())
		if l1, ok1 := ct.loadedS[addr]; ok1 {
			if _, ok2 := l1[loc]; !ok2 {
				ct.nakedSloads++
				l1[loc] = struct{}{}
			}
		} else {
			ct.nakedSloads++
			l2 := make(map[common.Hash]struct{})
			l2[loc] = struct{}{}
			ct.loadedS[addr] = l2
		}
		ct.totalSloads++
	}
	return nil
}
func (ct *CombTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, _ *stack.ReturnStack, contract *vm.Contract, depth int, err error) error {
	return nil
}
func (ct *CombTracer) CaptureEnd(depth int, output []byte, gasUsed uint64, t time.Duration, err error) error {
	return nil
}
func (ct *CombTracer) CaptureCreate(creator common.Address, creation common.Address) error {
	return nil
}
func (ct *CombTracer) CaptureAccountRead(account common.Address) error {
	if _, ok := ct.loaded[account]; !ok {
		ct.nakedReads++
		ct.loaded[account] = struct{}{}
	}
	ct.totalReads++
	return nil
}
func (ct *CombTracer) CaptureAccountWrite(account common.Address) error {
	if _, ok := ct.loaded[account]; !ok {
		ct.nakedWrites++
		ct.loaded[account] = struct{}{}
	}
	ct.totalWrites++
	return nil
}

//nolint:deadcode,unused
func speculativeExecution(blockNum uint64) {
	startTime := time.Now()
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()

	ethDb := ethdb.MustOpen("/Volumes/tb41/turbo-geth-10/geth/chaindata")
	defer ethDb.Close()
	ethTx, err1 := ethDb.KV().Begin(context.Background(), nil, false)
	check(err1)
	defer ethTx.Rollback()
	chainConfig := params.MainnetChainConfig
	depFile, err := os.OpenFile("/Volumes/tb41/turbo-geth/spec_execution.csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	check(err)
	defer depFile.Close()
	w := bufio.NewWriter(depFile)
	defer w.Flush()
	ct1 := NewCombTracer()
	ct2 := NewCombTracer()
	ct3 := NewCombTracer()
	vmConfig1 := vm.Config{Tracer: ct1, Debug: true}
	vmConfig2 := vm.Config{Tracer: ct2, Debug: true}
	vmConfig3 := vm.Config{Tracer: ct3, Debug: true}
	txCacher := core.NewTxSenderCacher(runtime.NumCPU())
	bc, err := core.NewBlockChain(ethDb, nil, chainConfig, ethash.NewFaker(), vmConfig1, nil, txCacher)
	check(err)
	defer bc.Stop()
	interrupt := false
	for !interrupt {
		block := bc.GetBlockByNumber(blockNum)
		if block == nil {
			break
		}
		dbstate := state.NewPlainDBState(ethTx, block.NumberU64()-1)

		// First pass - execute transactions in sequence
		statedb1 := state.New(dbstate)
		statedb1.SetTracer(ct1)
		signer := types.MakeSigner(chainConfig, block.Number())
		ct1.ResetCounters()
		ct1.ResetSets()
		for _, tx := range block.Transactions() {
			// Assemble the transaction call message and return if the requested offset
			msg, _ := tx.AsMessage(signer)
			context := core.NewEVMContext(msg, block.Header(), bc, nil)
			// Not yet the searched for transaction, execute on top of the current state
			vmenv := vm.NewEVM(context, statedb1, chainConfig, vmConfig1)
			if _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.Gas())); err != nil {
				panic(fmt.Errorf("tx %x failed: %v", tx.Hash(), err))
			}
		}
		totalWrites := ct1.totalWrites
		nakedWrites := ct1.nakedWrites
		totalReads := ct1.totalReads
		nakedReads := ct1.nakedReads
		totalSstores := ct1.totalSstores
		nakedSstores := ct1.nakedSstores
		totalSloads := ct1.totalSloads
		nakedSloads := ct1.nakedSloads

		// Second pass - execute all transactions on the initial state, building up the load and loadS cache
		ct2.ResetCounters()
		ct2.ResetSets()
		for _, tx := range block.Transactions() {
			statedb2 := state.New(dbstate)
			statedb2.SetTracer(ct2)
			// Assemble the transaction call message and return if the requested offset
			msg, _ := tx.AsMessage(signer)
			msg = types.NewMessage(msg.From(), msg.To(), msg.Nonce(), msg.Value(), msg.Gas(), msg.GasPrice(), msg.Data(), false)
			context := core.NewEVMContext(msg, block.Header(), bc, nil)
			// Not yet the searched for transaction, execute on top of the current state
			vmenv := vm.NewEVM(context, statedb2, chainConfig, vmConfig2)
			if _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.Gas())); err != nil {
				panic(fmt.Errorf("tx %x failed: %v", tx.Hash(), err))
			}
		}

		// Third pass - execute all transactions on the initial state
		maxNakedReads := 0
		maxNakedSloads := 0
		ct3.ResetCounters()
		ct3.ResetSets()
		for _, tx := range block.Transactions() {
			statedb3 := state.New(dbstate)
			statedb3.SetTracer(ct3)
			// Assemble the transaction call message and return if the requested offset
			msg, _ := tx.AsMessage(signer)
			msg = types.NewMessage(msg.From(), msg.To(), msg.Nonce(), msg.Value(), msg.Gas(), msg.GasPrice(), msg.Data(), false)
			context := core.NewEVMContext(msg, block.Header(), bc, nil)
			// Not yet the searched for transaction, execute on top of the current state
			vmenv := vm.NewEVM(context, statedb3, chainConfig, vmConfig3)
			if _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.Gas())); err != nil {
				panic(fmt.Errorf("tx %x failed: %v", tx.Hash(), err))
			}
			if ct3.nakedReads > maxNakedReads {
				maxNakedReads = ct3.nakedReads
			}
			if ct3.nakedSloads > maxNakedSloads {
				maxNakedSloads = ct3.nakedSloads
			}
			ct3.ResetCounters()
			ct3.ResetSets()
		}

		// Fourth pass - execute all transactions sequentially, but with the caches made at the second pass
		statedb4 := state.New(dbstate)
		statedb4.SetTracer(ct2)
		ct2.ResetCounters()
		for _, tx := range block.Transactions() {
			// Assemble the transaction call message and return if the requested offset
			msg, _ := tx.AsMessage(signer)
			context := core.NewEVMContext(msg, block.Header(), bc, nil)
			// Not yet the searched for transaction, execute on top of the current state
			vmenv := vm.NewEVM(context, statedb4, chainConfig, vmConfig2)
			if _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.Gas())); err != nil {
				panic(fmt.Errorf("tx %x failed: %v", tx.Hash(), err))
			}
		}

		fmt.Fprintf(w, "%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n", blockNum,
			totalWrites, nakedWrites, totalReads, nakedReads,
			totalSstores, nakedSstores, totalSloads, nakedSloads,
			maxNakedReads, maxNakedSloads, ct2.nakedReads, ct2.nakedSloads)
		blockNum++
		if blockNum%1000 == 0 {
			fmt.Printf("Processed %d blocks\n", blockNum)
		}
		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			fmt.Println("interrupted, please wait for cleanup...")
		default:
		}
	}
	fmt.Printf("Processed %d blocks\n", blockNum)
	fmt.Printf("Next time specify -block %d\n", blockNum)
	fmt.Printf("Speculative execution analysis took %s\n", time.Since(startTime))
}

func specExecChart1() {
	seFile, err := os.Open("/Volumes/tb41/turbo-geth/spec_execution.csv")
	check(err)
	defer seFile.Close()
	seReader := csv.NewReader(bufio.NewReader(seFile))
	var blocks []float64
	var nakedReads []float64
	var nakedSloads []float64
	var maxNakedReads []float64
	var maxNakedSloads []float64
	var exNakedReads []float64
	var exNakedSloads []float64
	for records, _ := seReader.Read(); records != nil; records, _ = seReader.Read() {
		blocks = append(blocks, parseFloat64(records[0])/1000000.0)
		nakedReads = append(nakedReads, parseFloat64(records[4]))
		nakedSloads = append(nakedSloads, parseFloat64(records[8]))
		maxNakedReads = append(maxNakedReads, parseFloat64(records[9]))
		maxNakedSloads = append(maxNakedSloads, parseFloat64(records[10]))
		exNakedReads = append(exNakedReads, parseFloat64(records[11]))
		exNakedSloads = append(exNakedSloads, parseFloat64(records[12]))
	}
	var nakedReadsGroup float64 = 0
	var nakedSloadsGroup float64 = 0
	var maxReadsGroup float64 = 0
	var maxSloadsGroup float64 = 0
	var exNakedReadsGroup float64 = 0
	var exNakedSloadsGroup float64 = 0
	var i int
	var window int = 1024
	b := make([]float64, len(blocks)-window+1)
	nrs := make([]float64, len(blocks)-window+1)
	mrs := make([]float64, len(blocks)-window+1)
	ers := make([]float64, len(blocks)-window+1)
	nls := make([]float64, len(blocks)-window+1)
	mls := make([]float64, len(blocks)-window+1)
	els := make([]float64, len(blocks)-window+1)
	for i = 0; i < len(blocks); i++ {
		nakedReadsGroup += nakedReads[i]
		nakedSloadsGroup += nakedSloads[i]
		maxReadsGroup += maxNakedReads[i]
		maxSloadsGroup += maxNakedSloads[i]
		exNakedReadsGroup += exNakedReads[i]
		exNakedSloadsGroup += exNakedSloads[i]
		if i >= window {
			nakedReadsGroup -= nakedReads[i-window]
			nakedSloadsGroup -= nakedSloads[i-window]
			maxReadsGroup -= maxNakedReads[i-window]
			maxSloadsGroup -= maxNakedSloads[i-window]
			exNakedReadsGroup -= exNakedReads[i-window]
			exNakedSloadsGroup -= exNakedSloads[i-window]
		}
		if i >= window-1 {
			b[i-window+1] = blocks[i]
			nrs[i-window+1] = nakedReadsGroup / float64(window)
			nls[i-window+1] = nakedSloadsGroup / float64(window)
			mrs[i-window+1] = maxReadsGroup / float64(window)
			mls[i-window+1] = maxSloadsGroup / float64(window)
			ers[i-window+1] = exNakedReadsGroup / float64(window)
			els[i-window+1] = exNakedSloadsGroup / float64(window)
			mrs[i-window+1] += ers[i-window+1]
			mls[i-window+1] += ers[i-window+1]
		}
	}
	nakedSeries := &chart.ContinuousSeries{
		Name: fmt.Sprintf("Naked account reads per block, moving average with window %d", window),
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorBlack,
		},
		XValues: b,
		YValues: nrs,
	}
	maxSeries := &chart.ContinuousSeries{
		Name: fmt.Sprintf("Max reads per tx in the block, moving average with window %d", window),
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorBlue,
			FillColor:   chart.ColorBlue.WithAlpha(100),
		},
		XValues: b,
		YValues: mrs,
	}
	exSeries := &chart.ContinuousSeries{
		Name: fmt.Sprintf("Extra reads per block, moving average with window %d", window),
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorRed,
			FillColor:   chart.ColorRed.WithAlpha(100),
		},
		XValues: b,
		YValues: ers,
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
			Name:      "operations",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.1f", v.(float64))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorAlternateGray,
				StrokeWidth: 1.0,
			},
			//GridLines: operations(),
		},
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
			GridLines: blockMillions(),
		},
		Series: []chart.Series{
			nakedSeries,
			maxSeries,
			exSeries,
		},
	}

	graph1.Elements = []chart.Renderable{chart.LegendThin(&graph1)}

	buffer := bytes.NewBuffer([]byte{})
	err = graph1.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile("spec_exec.png", buffer.Bytes(), 0644)
	check(err)

	naked4Series := &chart.ContinuousSeries{
		Name: fmt.Sprintf("Naked account reads per block, moving average with window %d", window),
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorBlack,
		},
		XValues: b[4000000:],
		YValues: nrs[4000000:],
	}
	max4Series := &chart.ContinuousSeries{
		Name: fmt.Sprintf("Max reads per tx in the block, moving average with window %d", window),
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorBlue,
			FillColor:   chart.ColorBlue.WithAlpha(100),
		},
		XValues: b[4000000:],
		YValues: mrs[4000000:],
	}
	ex4Series := &chart.ContinuousSeries{
		Name: fmt.Sprintf("Extra reads per block, moving average with window %d", window),
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorRed,
			FillColor:   chart.ColorRed.WithAlpha(100),
		},
		XValues: b[4000000:],
		YValues: ers[4000000:],
	}
	graph4m := chart.Chart{
		Width:  1280,
		Height: 720,
		Background: chart.Style{
			Padding: chart.Box{
				Top: 50,
			},
		},
		YAxis: chart.YAxis{
			Name:      "operations",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.1f", v.(float64))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorAlternateGray,
				StrokeWidth: 1.0,
			},
			//GridLines: operations(),
		},
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
			GridLines: blockMillions(),
		},
		Series: []chart.Series{
			naked4Series,
			max4Series,
			ex4Series,
		},
	}

	graph4m.Elements = []chart.Renderable{chart.LegendThin(&graph4m)}

	buffer = bytes.NewBuffer([]byte{})
	err = graph4m.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile("spec_exec_4m.png", buffer.Bytes(), 0644)
	check(err)

	nakedLSeries := &chart.ContinuousSeries{
		Name: fmt.Sprintf("Naked SLOADs per block, moving average with window %d", window),
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorBlue,
		},
		XValues: b,
		YValues: nls,
	}
	maxLSeries := &chart.ContinuousSeries{
		Name: fmt.Sprintf("Max SLOADs per tx in the block, moving average with window %d", window),
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorYellow,
			FillColor:   chart.ColorYellow.WithAlpha(100),
		},
		XValues: b,
		YValues: mls,
	}
	exLSeries := &chart.ContinuousSeries{
		Name: fmt.Sprintf("Extra SLOADs per block, moving average with window %d", window),
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorRed,
			FillColor:   chart.ColorRed.WithAlpha(100),
		},
		XValues: b,
		YValues: els,
	}
	graphL := chart.Chart{
		Width:  1280,
		Height: 720,
		Background: chart.Style{
			Padding: chart.Box{
				Top: 50,
			},
		},
		YAxis: chart.YAxis{
			Name:      "operations",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.1f", v.(float64))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorAlternateGray,
				StrokeWidth: 1.0,
			},
			//GridLines: operations(),
		},
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
			GridLines: blockMillions(),
		},
		Series: []chart.Series{
			nakedLSeries,
			maxLSeries,
			exLSeries,
		},
	}

	graphL.Elements = []chart.Renderable{chart.LegendThin(&graphL)}

	buffer = bytes.NewBuffer([]byte{})
	err = graphL.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile("spec_exec_L.png", buffer.Bytes(), 0644)
	check(err)
}
