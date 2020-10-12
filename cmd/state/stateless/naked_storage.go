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
	"github.com/wcharczuk/go-chart/drawing"

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

type StorageTracer struct {
	loaded       map[common.Address]map[common.Hash]struct{}
	totalSstores int
	nakedSstores int
	totalSloads  int
	nakedSloads  int
}

func NewStorageTracer() *StorageTracer {
	return &StorageTracer{
		loaded: make(map[common.Address]map[common.Hash]struct{}),
	}
}

func (st *StorageTracer) CaptureStart(depth int, from common.Address, to common.Address, call bool, input []byte, gas uint64, value *big.Int) error {
	return nil
}
func (st *StorageTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, _ *stack.ReturnStack, rData []byte, contract *vm.Contract, depth int, err error) error {
	if op == vm.SSTORE {
		addr := contract.Address()
		if stack.Len() == 0 {
			return nil
		}
		loc := common.Hash(stack.Back(0).Bytes32())
		if l1, ok1 := st.loaded[addr]; ok1 {
			if _, ok2 := l1[loc]; !ok2 {
				st.nakedSstores++
				l1[loc] = struct{}{}
			}
		} else {
			st.nakedSstores++
			l2 := make(map[common.Hash]struct{})
			l2[loc] = struct{}{}
			st.loaded[addr] = l2
		}
		st.totalSstores++
	} else if op == vm.SLOAD {
		addr := contract.Address()
		if stack.Len() == 0 {
			return nil
		}
		loc := common.Hash(stack.Back(0).Bytes32())
		if l1, ok1 := st.loaded[addr]; ok1 {
			if _, ok2 := l1[loc]; !ok2 {
				st.nakedSloads++
				l1[loc] = struct{}{}
			}
		} else {
			st.nakedSloads++
			l2 := make(map[common.Hash]struct{})
			l2[loc] = struct{}{}
			st.loaded[addr] = l2
		}
		st.totalSloads++
	}
	return nil
}
func (st *StorageTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, _ *stack.ReturnStack, contract *vm.Contract, depth int, err error) error {
	return nil
}
func (st *StorageTracer) CaptureEnd(depth int, output []byte, gasUsed uint64, t time.Duration, err error) error {
	return nil
}
func (st *StorageTracer) CaptureCreate(creator common.Address, creation common.Address) error {
	return nil
}
func (st *StorageTracer) CaptureAccountRead(account common.Address) error {
	return nil
}
func (st *StorageTracer) CaptureAccountWrite(account common.Address) error {
	return nil
}

//nolint:deadcode,unused
func storageReadWrites(blockNum uint64) {
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
	srwFile, err := os.OpenFile("/Volumes/tb41/turbo-geth/storage_read_writes.csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	check(err)
	defer srwFile.Close()
	w := bufio.NewWriter(srwFile)
	defer w.Flush()
	st := NewStorageTracer()
	vmConfig := vm.Config{Tracer: st, Debug: true}
	txCacher := core.NewTxSenderCacher(runtime.NumCPU())
	bc, err := core.NewBlockChain(ethDb, nil, chainConfig, ethash.NewFaker(), vmConfig, nil, txCacher)
	check(err)
	defer bc.Stop()
	interrupt := false
	totalSstores := 0
	nakedSstores := 0
	totalSloads := 0
	nakedSloads := 0
	for !interrupt {
		block := bc.GetBlockByNumber(blockNum)
		if block == nil {
			break
		}
		dbstate := state.NewPlainDBState(ethTx, block.NumberU64()-1)
		statedb := state.New(dbstate)
		signer := types.MakeSigner(chainConfig, block.Number())
		st.loaded = make(map[common.Address]map[common.Hash]struct{})
		st.totalSstores = 0
		st.nakedSstores = 0
		st.totalSloads = 0
		st.nakedSloads = 0
		for _, tx := range block.Transactions() {
			// Assemble the transaction call message and return if the requested offset
			msg, _ := tx.AsMessage(signer)
			context := core.NewEVMContext(msg, block.Header(), bc, nil)
			// Not yet the searched for transaction, execute on top of the current state
			vmenv := vm.NewEVM(context, statedb, chainConfig, vmConfig)
			if _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.Gas())); err != nil {
				panic(fmt.Errorf("tx %x failed: %v", tx.Hash(), err))
			}
		}
		fmt.Fprintf(w, "%d,%d,%d,%d,%d\n", blockNum, st.totalSstores, st.nakedSstores, st.totalSloads, st.nakedSloads)
		totalSstores += st.totalSstores
		nakedSstores += st.nakedSstores
		totalSloads += st.totalSloads
		nakedSloads += st.nakedSloads
		blockNum++
		if blockNum%1000 == 0 {
			fmt.Printf("Processed %d blocks, totalSstores %d, nakedSstores %d, totalSloads %d, nakedSloads %d\n", blockNum, totalSstores, nakedSstores, totalSloads, nakedSloads)
		}
		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			fmt.Println("interrupted, please wait for cleanup...")
		default:
		}
	}
	fmt.Printf("Processed %d blocks, totalSstores %d, nakedSstores %d, totalSloads %d, nakedSloads %d\n", blockNum, totalSstores, nakedSstores, totalSloads, nakedSloads)
	fmt.Printf("Next time specify -block %d\n", blockNum)
	fmt.Printf("Storage read/write analysis took %s\n", time.Since(startTime))
}

func operations() []chart.GridLine {
	return []chart.GridLine{
		{Value: 5.0},
		{Value: 10.0},
		{Value: 15.0},
		{Value: 20.0},
		{Value: 25.0},
		{Value: 30.0},
		{Value: 35.0},
		{Value: 40.0},
		{Value: 45.0},
	}
}

func nakedSstoreChart() {
	swrFile, err := os.Open("/Volumes/tb4/turbo-geth/storage_read_writes.csv")
	check(err)
	defer swrFile.Close()
	swrReader := csv.NewReader(bufio.NewReader(swrFile))
	var blocks []float64
	var totalSstores []float64
	var nakedSstores []float64
	for records, _ := swrReader.Read(); records != nil; records, _ = swrReader.Read() {
		blocks = append(blocks, parseFloat64(records[0])/1000000.0)
		totalSstores = append(totalSstores, parseFloat64(records[1]))
		nakedSstores = append(nakedSstores, parseFloat64(records[2]))
	}
	var totalSstoresGroup float64 = 0
	var nakedSstoresGroup float64 = 0
	var i int
	var window int = 1024
	b := make([]float64, len(blocks)-window+1)
	ts := make([]float64, len(blocks)-window+1)
	ns := make([]float64, len(blocks)-window+1)
	for i = 0; i < len(blocks); i++ {
		totalSstoresGroup += totalSstores[i]
		nakedSstoresGroup += nakedSstores[i]
		if i >= window {
			totalSstoresGroup -= totalSstores[i-window]
			nakedSstoresGroup -= nakedSstores[i-window]
		}
		if i >= window-1 {
			b[i-window+1] = blocks[i]
			ts[i-window+1] = totalSstoresGroup / float64(window)
			ns[i-window+1] = nakedSstoresGroup / float64(window)
		}
	}
	check(err)
	totalSeries := &chart.ContinuousSeries{
		Name: fmt.Sprintf("Total SSTOREs per block, moving average with window %d", window),
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorBlue,
			FillColor:   chart.ColorBlue.WithAlpha(100),
		},
		XValues: b,
		YValues: ts,
	}
	nakedSeries := &chart.ContinuousSeries{
		Name: fmt.Sprintf("Naked SSTOREs per block, moving average with window %d", window),
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorBlack,
		},
		XValues: b,
		YValues: ns,
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
			totalSeries,
			nakedSeries,
		},
	}

	graph1.Elements = []chart.Renderable{chart.LegendThin(&graph1)}

	buffer := bytes.NewBuffer([]byte{})
	err = graph1.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile("naked_sstores.png", buffer.Bytes(), 0644)
	check(err)
}

func nakedSloadChart() {
	swrFile, err := os.Open("/Volumes/tb4/turbo-geth/storage_read_writes.csv")
	check(err)
	defer swrFile.Close()
	swrReader := csv.NewReader(bufio.NewReader(swrFile))
	var blocks []float64
	var totalSloads []float64
	var nakedSloads []float64
	for records, _ := swrReader.Read(); records != nil; records, _ = swrReader.Read() {
		blocks = append(blocks, parseFloat64(records[0])/1000000.0)
		totalSloads = append(totalSloads, parseFloat64(records[3]))
		nakedSloads = append(nakedSloads, parseFloat64(records[4]))
	}
	var totalSloadsGroup float64 = 0
	var nakedSloadsGroup float64 = 0
	var i int
	var window int = 1024
	b := make([]float64, len(blocks)-window+1)
	ts := make([]float64, len(blocks)-window+1)
	ns := make([]float64, len(blocks)-window+1)
	for i = 0; i < len(blocks); i++ {
		totalSloadsGroup += totalSloads[i]
		nakedSloadsGroup += nakedSloads[i]
		if i >= window {
			totalSloadsGroup -= totalSloads[i-window]
			nakedSloadsGroup -= nakedSloads[i-window]
		}
		if i >= window-1 {
			b[i-window+1] = blocks[i]
			ts[i-window+1] = totalSloadsGroup / float64(window)
			ns[i-window+1] = nakedSloadsGroup / float64(window)
		}
	}
	check(err)
	totalSeries := &chart.ContinuousSeries{
		Name: fmt.Sprintf("Total SLOADs per block, moving average with window %d", window),
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorGreen,
			FillColor:   chart.ColorGreen.WithAlpha(100),
		},
		XValues: b,
		YValues: ts,
	}
	nakedSeries := &chart.ContinuousSeries{
		Name: fmt.Sprintf("Naked SLOADs per block, moving average with window %d", window),
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorBlack,
		},
		XValues: b,
		YValues: ns,
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
			totalSeries,
			nakedSeries,
		},
	}

	graph1.Elements = []chart.Renderable{chart.LegendThin(&graph1)}

	buffer := bytes.NewBuffer([]byte{})
	err = graph1.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile("naked_sloads.png", buffer.Bytes(), 0644)
	check(err)
}

func naked_storage_vs_blockproof() {
	swrFile, err := os.Open("/Volumes/tb41/turbo-geth/storage_read_writes.csv")
	check(err)
	defer swrFile.Close()
	swrReader := csv.NewReader(bufio.NewReader(swrFile))
	var blocks []float64
	var totalSstores []float64
	var nakedSstores []float64
	var totalSloads []float64
	var nakedSloads []float64
	var totalNaked []float64
	for records, _ := swrReader.Read(); records != nil; records, _ = swrReader.Read() {
		blocks = append(blocks, parseFloat64(records[0])/1000000.0)
		totalSstores = append(totalSstores, parseFloat64(records[1]))
		nakedSstores = append(nakedSstores, parseFloat64(records[2]))
		totalSloads = append(totalSloads, parseFloat64(records[3]))
		nakedSloads = append(nakedSloads, parseFloat64(records[4]))
		totalNaked = append(totalNaked, parseFloat64(records[2])+parseFloat64(records[4]))
	}

	file, err := os.Open("/Volumes/tb41/turbo-geth/stateless.csv")
	check(err)
	defer file.Close()
	reader := csv.NewReader(bufio.NewReader(file))
	var blocks2 []float64
	var vals [18][]float64
	for records, _ := reader.Read(); records != nil; records, _ = reader.Read() {
		if len(records) < 16 {
			break
		}
		blocks2 = append(blocks2, parseFloat64(records[0])/1000000.0)
		for i := 0; i < 18; i++ {
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
			default:
				vals[i] = append(vals[i], parseFloat64(records[i+1]))
			}
		}
	}
	limit := len(blocks)
	if len(blocks2) < limit {
		limit = len(blocks2)
	}
	var min float64 = 100000000.0
	var max float64
	for i := 0; i < limit; i++ {
		if totalNaked[i] < min {
			min = totalNaked[i]
		}
		if totalNaked[i] > max {
			max = totalNaked[i]
		}
	}
	fmt.Printf("limit: %d, min total naked: %f, max total naked: %f\n", limit, min, max)
	colorByBlock := func(xr, yr chart.Range, index int, x, y float64) drawing.Color {
		if index < 1000000 {
			return chart.ColorGreen.WithAlpha(100)
		} else if index < 2000000 {
			return chart.ColorBlue.WithAlpha(100)
		} else if index < 3000000 {
			return chart.ColorRed.WithAlpha(100)
		} else if index < 4000000 {
			return chart.ColorBlack.WithAlpha(100)
		} else if index < 5000000 {
			return chart.ColorYellow.WithAlpha(100)
		} else if index < 6000000 {
			return chart.ColorOrange.WithAlpha(100)
		} else {
			return chart.ColorCyan.WithAlpha(100)
		}
	}
	hashSeries := &chart.ContinuousSeries{
		Name: "Block proof hashes (for contracts) vs naked SLOADs and naked SSTOREs",
		Style: chart.Style{
			Show:             true,
			StrokeWidth:      chart.Disabled,
			DotWidth:         1,
			DotColorProvider: colorByBlock,
		},
		XValues: totalNaked[:limit],
		YValues: vals[2][:limit],
	}
	graph1 := chart.Chart{
		Width:  1280,
		Height: 720,
		Background: chart.Style{
			Padding: chart.Box{
				Top: 50,
			},
		},
		XAxis: chart.XAxis{
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
		},
		YAxis: chart.YAxis{
			Name: "block proof hashes (contracts)",
			Style: chart.Style{
				Show: true,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%d kB", int(v.(float64)/1024.0))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorAlternateGray,
				StrokeWidth: 1.0,
			},
		},
		Series: []chart.Series{
			hashSeries,
		},
	}
	graph1.Elements = []chart.Renderable{chart.LegendThin(&graph1)}

	buffer := bytes.NewBuffer([]byte{})
	err = graph1.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile("naked_storage_vs_blockproof.png", buffer.Bytes(), 0644)
	check(err)
}
