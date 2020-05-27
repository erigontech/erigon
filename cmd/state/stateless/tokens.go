package stateless

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"math"
	"math/big"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

// NOTE: This file is not the part of the Turbo-Geth binary. It i s part of the experimental utility, state
// to perform data analysis related to the state growth, state rent, and statelesss clients

type TokenTracer struct {
	tokens    map[common.Address]struct{}
	addrs     []common.Address
	startMode []bool
}

func NewTokenTracer() TokenTracer {
	return TokenTracer{
		tokens:    make(map[common.Address]struct{}),
		addrs:     make([]common.Address, 16384),
		startMode: make([]bool, 16384),
	}
}

func (tt TokenTracer) CaptureStart(depth int, from common.Address, to common.Address, call bool, input []byte, gas uint64, value *big.Int) error {
	if len(input) < 68 {
		return nil
	}
	if _, ok := tt.tokens[to]; ok {
		return nil
	}
	//a9059cbb is transfer method ID
	if input[0] != byte(0xa9) || input[1] != byte(5) || input[2] != byte(0x9c) || input[3] != byte(0xbb) {
		return nil
	}
	// Next 12 bytes are zero, because the first argument is an address
	for i := 4; i < 16; i++ {
		if input[i] != byte(0) {
			return nil
		}
	}
	tt.addrs[depth] = to
	tt.startMode[depth] = true
	return nil
}
func (tt TokenTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *vm.Stack, contract *vm.Contract, depth int, err error) error {
	return nil
}
func (tt TokenTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *vm.Stack, contract *vm.Contract, depth int, err error) error {
	return nil
}
func (tt TokenTracer) CaptureEnd(depth int, output []byte, gasUsed uint64, t time.Duration, err error) error {
	if !tt.startMode[depth] {
		return nil
	}
	tt.startMode[depth] = false
	if err != nil {
		return nil
	}
	if len(output) != 0 && len(output) != 32 {
		return nil
	}
	if len(output) == 32 {
		allZeros := true
		for i := 0; i < 32; i++ {
			if output[i] != byte(0) {
				allZeros = false
				break
			}
		}
		if allZeros {
			return nil
		}
	}
	addr := tt.addrs[depth]
	if _, ok := tt.tokens[addr]; !ok {
		tt.tokens[addr] = struct{}{}
		if len(tt.tokens)%100 == 0 {
			fmt.Printf("Found %d token contracts so far\n", len(tt.tokens))
		}
	}
	return nil
}
func (tt TokenTracer) CaptureCreate(creator common.Address, creation common.Address) error {
	return nil
}
func (tt TokenTracer) CaptureAccountRead(account common.Address) error {
	return nil
}
func (tt TokenTracer) CaptureAccountWrite(account common.Address) error {
	return nil
}

//nolint:deadcode,unused
func makeTokens(blockNum uint64) {
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()

	//ethDb, err := ethdb.NewBoltDatabase("/home/akhounov/.ethereum/geth/chaindata")
	ethDb, err := ethdb.NewBoltDatabase("/Volumes/tb41/turbo-geth/geth/chaindata")
	//ethDb, err := ethdb.NewBoltDatabase("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata")
	check(err)
	defer ethDb.Close()
	chainConfig := params.MainnetChainConfig
	tt := NewTokenTracer()
	vmConfig := vm.Config{Tracer: tt, Debug: true}
	bc, err := core.NewBlockChain(ethDb, nil, chainConfig, ethash.NewFaker(), vmConfig, nil, nil)
	check(err)
	if blockNum > 1 {
		tokenFile, err := os.Open("/Volumes/tb41/turbo-geth/tokens.csv")
		check(err)
		tokenReader := csv.NewReader(bufio.NewReader(tokenFile))
		for records, _ := tokenReader.Read(); records != nil; records, _ = tokenReader.Read() {
			tt.tokens[common.HexToAddress(records[0])] = struct{}{}
		}
		tokenFile.Close()
	}
	interrupt := false
	for !interrupt {
		block := bc.GetBlockByNumber(blockNum)
		if block == nil {
			break
		}
		dbstate := state.NewDbState(ethDb.AbstractKV(), block.NumberU64()-1)
		statedb := state.New(dbstate)
		signer := types.MakeSigner(chainConfig, block.Number())
		for _, tx := range block.Transactions() {
			// Assemble the transaction call message and return if the requested offset
			msg, _ := tx.AsMessage(signer)
			context := core.NewEVMContext(msg, block.Header(), bc, nil)
			// Not yet the searched for transaction, execute on top of the current state
			vmenv := vm.NewEVM(context, statedb, chainConfig, vmConfig)
			if _, err = core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.Gas())); err != nil {
				panic(fmt.Errorf("tx %x failed: %v", tx.Hash(), err))
			}
		}
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
	fmt.Printf("Writing dataset...\n")
	f, err := os.Create("/Volumes/tb41/turbo-geth/tokens.csv")
	check(err)
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	for token := range tt.tokens {
		fmt.Fprintf(w, "%x\n", token)
	}
	fmt.Printf("Next time specify -block %d\n", blockNum)
}

func makeTokenBalances() {
	//ethDb, err := ethdb.NewBoltDatabase("/home/akhounov/.ethereum/geth/chaindata")
	ethDb, err := ethdb.NewBoltDatabase("/Volumes/tb41/turbo-geth/geth/chaindata")
	//ethDb, err := ethdb.NewBoltDatabase("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata")
	check(err)
	defer ethDb.Close()
	bc, err := core.NewBlockChain(ethDb, nil, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{}, nil, nil)
	check(err)
	currentBlock := bc.CurrentBlock()
	currentBlockNr := currentBlock.NumberU64()
	fmt.Printf("Current block number: %d\n", currentBlockNr)

	pdb, err := bolt.Open("/Volumes/tb41/turbo-geth/sha3preimages", 0600, &bolt.Options{})
	check(err)
	defer pdb.Close()
	bucket := []byte("sha3")

	var tokens []common.Address
	tokenFile, err := os.Open("/Volumes/tb41/turbo-geth/tokens.csv")
	check(err)
	tokenReader := csv.NewReader(bufio.NewReader(tokenFile))
	for records, _ := tokenReader.Read(); records != nil; records, _ = tokenReader.Read() {
		tokens = append(tokens, common.HexToAddress(records[0]))
	}
	tokenFile.Close()
	//tokens = append(tokens, common.HexToAddress("0xB8c77482e45F1F44dE1745F52C74426C631bDD52"))
	caller := common.HexToAddress("0x742d35cc6634c0532925a3b844bc454e4438f44e")
	f, err := os.Create("/Volumes/tb41/turbo-geth/token_balances.csv")
	check(err)
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	var a accounts.Account
	for _, token := range tokens {
		// Exclude EOAs and removed accounts
		if ok, err := rawdb.ReadAccount(ethDb, common.BytesToHash(crypto.Keccak256(token[:])), &a); err != nil {
			panic(err)
		} else if !ok {
			continue
		}
		if a.IsEmptyCodeHash() {
			// Only processing contracts
			continue
		}
		fmt.Printf("Analysing token %x...", token)
		count := 0
		addrCount := 0
		dbstate := state.NewDbState(ethDb.AbstractKV(), currentBlockNr)
		statedb := state.New(dbstate)
		msg := types.NewMessage(
			caller,
			&token,
			0,
			big.NewInt(0),    // value
			math.MaxUint64/2, // gaslimit
			big.NewInt(100000),
			common.FromHex(fmt.Sprintf("0x70a08231000000000000000000000000%x", common.HexToAddress("0xe477292f1b3268687a29376116b0ed27a9c76170"))),
			false, // checkNonce
		)
		chainConfig := params.MainnetChainConfig
		vmConfig := vm.Config{EnablePreimageRecording: true}
		context := core.NewEVMContext(msg, currentBlock.Header(), bc, nil)
		// Not yet the searched for transaction, execute on top of the current state
		vmenv := vm.NewEVM(context, statedb, chainConfig, vmConfig)
		result, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(math.MaxUint64))
		if err != nil {
			fmt.Printf("Call failed with error: %v\n", err)
		}
		if result.Failed() {
			fmt.Printf("Call failed\n")
		}
		pi := statedb.Preimages()
		var base byte
		var plen int
		for _, preimage := range pi {
			if len(preimage) == 64 {
				allZerosBase := true
				for i := 32; i < 63; i++ {
					if preimage[i] != byte(0) {
						allZerosBase = false
						break
					}
				}
				if !allZerosBase {
					fmt.Printf(" base > 255\n")
					continue
				}
				base = preimage[63]
				plen++
			}
		}
		if plen != 1 {
			fmt.Printf(" balanceOf preimages: %d\n", plen)
		}
		err = ethDb.Walk(dbutils.CurrentStateBucket, token[:], 160, func(k, v []byte) (bool, error) {
			var key []byte
			key, err = ethDb.Get(dbutils.PreimagePrefix, k[20:])
			var preimage []byte
			if key != nil {
				err := pdb.View(func(tx *bolt.Tx) error {
					b := tx.Bucket(bucket)
					if b == nil {
						return nil
					}
					preimage, _ = b.Get(key)
					if preimage != nil {
						preimage = common.CopyBytes(preimage)
					}
					return nil
				})
				if err != nil {
					return false, err
				}
			}
			if preimage != nil && len(preimage) == 64 {
				allZerosKey := true
				for i := 0; i < 12; i++ {
					if preimage[i] != byte(0) {
						allZerosKey = false
						break
					}
				}
				allZerosBase := true
				for i := 32; i < 63; i++ {
					if preimage[i] != byte(0) {
						allZerosBase = false
						break
					}
				}
				if allZerosKey && allZerosBase && plen == 1 && preimage[63] == base {
					balance := common.BytesToHash(v).Big()
					fmt.Fprintf(w, "%x,%x,%d\n", token, preimage[12:32], balance)
					addrCount++
				}
			}
			count++
			return true, nil
		})
		if err != nil {
			fmt.Printf("Error walking: %v\n", err)
			return
		}
		fmt.Printf(" %d storage items, holders: %d, base: %d\n", count, addrCount, base)
	}
}

type TokenBalance struct {
	token   common.Address
	holder  common.Address
	balance *big.Int
}

func tokenBalances() {
	addrFile, err := os.Open("addresses.csv")
	check(err)
	defer addrFile.Close()
	addrReader := csv.NewReader(bufio.NewReader(addrFile))
	names := make(map[common.Address]string)
	for records, _ := addrReader.Read(); records != nil; records, _ = addrReader.Read() {
		names[common.HexToAddress(records[0])] = records[1]
	}
	var tokenBalances []TokenBalance
	tbFile, err := os.Open("/Volumes/tb41/turbo-geth/token_balances.csv")
	check(err)
	tbReader := csv.NewReader(bufio.NewReader(tbFile))
	for records, _ := tbReader.Read(); records != nil; records, _ = tbReader.Read() {
		balance := big.NewInt(0)
		if err := balance.UnmarshalText([]byte(records[2])); err != nil {
			panic(err)
		}
		tokenBalances = append(tokenBalances, TokenBalance{
			token:   common.HexToAddress(records[0]),
			holder:  common.HexToAddress(records[1]),
			balance: balance,
		})
	}
	tbFile.Close()
	fmt.Printf("(token, holder) pairs: %d\n", len(tokenBalances))
	tokens := make(map[common.Address]int)
	holders := make(map[common.Address]int)
	for _, tb := range tokenBalances {
		tokens[tb.token]++
		holders[tb.holder]++
	}
	fmt.Printf("Tokens: %d\n", len(tokens))
	fmt.Printf("Holders: %d\n", len(holders))
	tokenSorter := NewIntSorterAddr(len(tokens))
	idx := 0
	for token, count := range tokens {
		tokenSorter.ints[idx] = count
		tokenSorter.values[idx] = token
		idx++
	}
	sort.Sort(tokenSorter)
	fmt.Printf("Top 100 tokens by number of holders:\n")
	for i := 0; i < 100; i++ {
		if name, ok := names[tokenSorter.values[i]]; ok {
			fmt.Printf("%d,%s,%d\n", i+1, name, tokenSorter.ints[i])
		} else {
			fmt.Printf("%d,%x,%d\n", i+1, tokenSorter.values[i], tokenSorter.ints[i])
		}
	}
	holderSorter := NewIntSorterAddr(len(holders))
	idx = 0
	for holder, count := range holders {
		holderSorter.ints[idx] = count
		holderSorter.values[idx] = holder
		idx++
	}
	sort.Sort(holderSorter)
	fmt.Printf("Top 100 holder by number of tokens:\n")
	for i := 0; i < 100; i++ {
		if name, ok := names[holderSorter.values[i]]; ok {
			fmt.Printf("%d,%s,%d\n", i+1, name, holderSorter.ints[i])
		} else {
			fmt.Printf("%d,%x,%d\n", i+1, holderSorter.values[i], holderSorter.ints[i])
		}
	}
}

func makeTokenAllowances() {
	//ethDb, err := ethdb.NewBoltDatabase("/home/akhounov/.ethereum/geth/chaindata")
	ethDb, err := ethdb.NewBoltDatabase("/Volumes/tb41/turbo-geth/geth/chaindata")
	//ethDb, err := ethdb.NewBoltDatabase("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata")
	check(err)
	defer ethDb.Close()
	bc, err := core.NewBlockChain(ethDb, nil, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{}, nil, nil)
	check(err)
	currentBlock := bc.CurrentBlock()
	currentBlockNr := currentBlock.NumberU64()
	fmt.Printf("Current block number: %d\n", currentBlockNr)

	pdb, err := bolt.Open("/Volumes/tb41/turbo-geth/sha3preimages", 0600, &bolt.Options{})
	check(err)
	defer pdb.Close()
	bucket := []byte("sha3")

	var tokens []common.Address
	tokenFile, err := os.Open("/Volumes/tb41/turbo-geth/tokens.csv")
	check(err)
	tokenReader := csv.NewReader(bufio.NewReader(tokenFile))
	for records, _ := tokenReader.Read(); records != nil; records, _ = tokenReader.Read() {
		tokens = append(tokens, common.HexToAddress(records[0]))
	}
	tokenFile.Close()
	//tokens = append(tokens, common.HexToAddress("0xB8c77482e45F1F44dE1745F52C74426C631bDD52"))
	caller := common.HexToAddress("0x742d35cc6634c0532925a3b844bc454e4438f44e")
	f, err := os.Create("/Volumes/tb41/turbo-geth/token_allowances.csv")
	check(err)
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	var a accounts.Account
	for _, token := range tokens {
		// Exclude EOAs and removed accounts
		if ok, err := rawdb.ReadAccount(ethDb, common.BytesToHash(crypto.Keccak256(token[:])), &a); err != nil {
			panic(err)
		} else if !ok {
			continue
		}
		if a.IsEmptyCodeHash() {
			// Only processing contracts
			continue
		}
		fmt.Printf("Analysing token %x...", token)
		count := 0
		addrCount := 0
		dbstate := state.NewDbState(ethDb.AbstractKV(), currentBlockNr)
		statedb := state.New(dbstate)
		msg := types.NewMessage(
			caller,
			&token,
			0,
			big.NewInt(0),    // value
			math.MaxUint64/2, // gaslimit
			big.NewInt(100000),
			common.FromHex(fmt.Sprintf("0xdd62ed3e000000000000000000000000%x000000000000000000000000%x",
				common.HexToAddress("0xe477292f1b3268687a29376116b0ed27a9c76170"),
				common.HexToAddress("0xe477292f1b3268687a29376116b0ed25a9c76170"),
			)),
			false, // checkNonce
		)
		chainConfig := params.MainnetChainConfig
		vmConfig := vm.Config{EnablePreimageRecording: true}
		context := core.NewEVMContext(msg, currentBlock.Header(), bc, nil)
		// Not yet the searched for transaction, execute on top of the current state
		vmenv := vm.NewEVM(context, statedb, chainConfig, vmConfig)
		result, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(math.MaxUint64))
		if err != nil {
			fmt.Printf("Call failed with error: %v\n", err)
		}
		if result.Failed() {
			fmt.Printf("Call failed\n")
		}
		pi := statedb.Preimages()
		var base byte
		var foundBase bool
		var base2 common.Hash
		var foundBase2 bool
		lenpi := 0
		for hash, preimage := range pi {
			if len(preimage) != 64 {
				continue
			}
			allZerosBase := true
			for i := 32; i < 63; i++ {
				if preimage[i] != byte(0) {
					allZerosBase = false
					break
				}
			}
			if allZerosBase {
				foundBase2 = true
				base = preimage[63]
				base2 = hash
			}
			lenpi++
		}
		if lenpi == 2 && foundBase2 {
			for _, preimage := range pi {
				if len(preimage) != 64 {
					continue
				}
				if bytes.Equal(preimage[32:], base2[:]) {
					foundBase = true
				}
			}
		}
		if !foundBase {
			fmt.Printf("allowance base not found\n")
			continue
		}
		err = ethDb.Walk(dbutils.CurrentStateBucket, token[:], 160, func(k, v []byte) (bool, error) {
			var key []byte
			key, err = ethDb.Get(dbutils.PreimagePrefix, k[20:])
			var index2 common.Hash
			var preimage []byte
			if key != nil {
				err := pdb.View(func(tx *bolt.Tx) error {
					b := tx.Bucket(bucket)
					if b == nil {
						return nil
					}
					preimage, _ = b.Get(key)
					if preimage != nil && len(preimage) == 64 {
						copy(index2[:], preimage[:32])
						preimage, _ = b.Get(preimage[32:])
					}
					return nil
				})
				if err != nil {
					return false, err
				}
			}
			if preimage != nil && len(preimage) == 64 {
				allZerosIndex2 := true
				for i := 0; i < 12; i++ {
					if index2[i] != byte(0) {
						allZerosIndex2 = false
						break
					}
				}
				allZerosKey := true
				for i := 0; i < 12; i++ {
					if preimage[i] != byte(0) {
						allZerosKey = false
						break
					}
				}
				allZerosBase := true
				for i := 32; i < 63; i++ {
					if preimage[i] != byte(0) {
						allZerosBase = false
						break
					}
				}
				if allZerosIndex2 && allZerosKey && allZerosBase && foundBase && preimage[63] == base {
					allowance := common.BytesToHash(v).Big()
					fmt.Fprintf(w, "%x,%x,%x,%d\n", token, preimage[12:32], index2[12:], allowance)
					addrCount++
				}
			}
			count++
			return true, nil
		})
		if err != nil {
			fmt.Printf("Error walking: %v\n", err)
			return
		}
		fmt.Printf(" %d storage items, allowances: %d, base: %d\n", count, addrCount, base)
	}
}
