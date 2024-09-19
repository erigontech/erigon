package smt

import (
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"strconv"
	"strings"
	"testing"

	"context"

	"github.com/ledgerwatch/erigon/smt/pkg/utils"
)

type Address struct {
	Address  string            `json:"address"`
	Balance  string            `json:"balance"`
	Nonce    string            `json:"nonce"`
	Bytecode string            `json:"bytecode,omitempty"`
	Storage  map[string]string `json:"storage,omitempty"`
}

type TestCase struct {
	Addresses    []Address `json:"addresses"`
	ExpectedRoot string    `json:"expectedRoot"`
}

type Genesis struct {
	L1Config struct {
		ChainID                           int    `json:"chainId"`
		PolygonZkEVMAddress               string `json:"polygonZkEVMAddress"`
		MaticTokenAddress                 string `json:"maticTokenAddress"`
		PolygonZkEVMGlobalExitRootAddress string `json:"polygonZkEVMGlobalExitRootAddress"`
	} `json:"l1Config"`
	Root               string           `json:"root"`
	GenesisBlockNumber int              `json:"genesisBlockNumber"`
	Genesis            []GenesisElement `json:"genesis"`
}

type GenesisElement struct {
	ContractName string            `json:"contractName,omitempty"`
	Balance      string            `json:"balance"`
	Nonce        string            `json:"nonce"`
	Address      string            `json:"address"`
	Bytecode     string            `json:"bytecode,omitempty"`
	Storage      map[string]string `json:"storage,omitempty"`
	AccountName  string            `json:"accountName,omitempty"`
}

func TestGenesis(t *testing.T) {
	runGenesisTest(t, "testdata/mainnet-genesis.json")
}

func BenchmarkGenesis(b *testing.B) {
	for i := 0; i < b.N; i++ {
		runGenesisTest(b, "testdata/mainnet-genesis.json")
	}
}

func runGenesisTest(tb testing.TB, filename string) {

	data, err := os.ReadFile(filename)
	if err != nil {
		tb.Fatal("Failed to open file: ", err)
	}

	var genesis Genesis
	err = json.Unmarshal(data, &genesis)
	if err != nil {
		tb.Fatal("Failed to parse json: ", err)
	}

	smt := NewSMT(nil, false)

	for _, addr := range genesis.Genesis {
		fmt.Println(addr.ContractName)
		bal, _ := new(big.Int).SetString(addr.Balance, 10)
		non, _ := new(big.Int).SetString(addr.Nonce, 10)
		// add balance and nonce
		_, _ = smt.SetAccountState(addr.Address, bal, non)
		// add bytecode if defined
		if addr.Bytecode != "" {
			_ = smt.SetContractBytecode(addr.Address, addr.Bytecode)
		}
		// add storage if defined
		if len(addr.Storage) > 0 {
			_, _ = smt.SetContractStorage(addr.Address, addr.Storage, nil)
		}
	}

	base := 10
	root := genesis.Root
	if strings.HasPrefix(root, "0x") {
		root = root[2:]
		base = 16
	}

	smt.CheckOrphanedNodes(context.Background())

	//smt.PrintTree()

	expected, _ := new(big.Int).SetString(root, base)
	fmt.Println("Expected root: ", genesis.Root)
	fmt.Println("Actual root: ", fmt.Sprintf("0x%x", smt.LastRoot()))

	if expected.Cmp(smt.LastRoot()) != 0 {
		tb.Errorf("Expected root does not match. Got 0x%x, want %s", smt.LastRoot(), root)
	}
}

func Test_Data(t *testing.T) {
	runTestVectors(t, "testdata/data.json")
	runTestVectors(t, "testdata/data2.json")
}

func runTestVectors(t *testing.T, filename string) {
	// load test data from disk
	data, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal("Failed to open file: ", err)
	}

	var testCases []TestCase
	err = json.Unmarshal(data, &testCases)
	if err != nil {
		t.Fatal("Failed to parse json: ", err)
	}

	for k, tc := range testCases {
		t.Run(strconv.Itoa(k), func(t *testing.T) {
			smt := NewSMT(nil, false)
			for _, addr := range tc.Addresses {

				bal, _ := new(big.Int).SetString(addr.Balance, 10)
				non, _ := new(big.Int).SetString(addr.Nonce, 10)
				// add balance and nonce
				_, _ = smt.SetAccountState(addr.Address, bal, non)
				// add bytecode if defined
				if addr.Bytecode != "" {
					_ = smt.SetContractBytecode(addr.Address, addr.Bytecode)
				}
				// add storage if defined
				if len(addr.Storage) > 0 {
					_, _ = smt.SetContractStorage(addr.Address, addr.Storage, nil)
				}
			}

			base := 10
			if strings.HasPrefix(tc.ExpectedRoot, "0x") {
				tc.ExpectedRoot = tc.ExpectedRoot[2:]
				base = 16
			}

			expected, _ := new(big.Int).SetString(tc.ExpectedRoot, base)

			if expected.Cmp(smt.LastRoot()) != 0 {
				t.Errorf("Expected root does not match. Got 0x%x, want 0x%x", smt.LastRoot(), expected)
			}
		})
	}
}

func Test_FullGenesisTest_Deprecated(t *testing.T) {
	s := NewSMT(nil, false)

	e := utils.NodeKey{
		13946701032480821596,
		7064140841355949548,
		9886191939484862127,
		5990325670619938515,
	}
	expected := e.ToBigInt()

	ethAddr := "0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D"
	balance := big.Int{}
	balance.SetString("100000000000000000000", 10)
	nonce := big.NewInt(0)

	newRoot, err := s.SetAccountState(ethAddr, &balance, nonce)
	if err != nil {
		t.Errorf("setAccountState failed: %v", err)
	}

	if newRoot.Cmp(expected) != 0 {
		t.Errorf("setAccountState failed: expected %v, got %v", e, newRoot)
	}
}

// this tests the subset of functionality to hash the bytecode only
func Test_SetContractBytecode_HashBytecode(t *testing.T) {
	byteCode := "0x1234"

	expected := "0x9257c9a31308a7cb046aba1a95679dd7e3ad695b6900e84a6470b401b1ea416e"

	hashedBytecode := utils.HashContractBytecode(byteCode)
	if hashedBytecode != expected {
		t.Errorf("setContractBytecode failed: expected %v, got %v", expected, hashedBytecode)
	}
}
