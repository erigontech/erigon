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

	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/gateway-fm/cdk-erigon-lib/kv/mdbx"
	db2 "github.com/ledgerwatch/erigon/smt/pkg/db"
)

func TestGenesisMdbx(t *testing.T) {
	runGenesisTestMdbx(t, "testdata/mainnet-genesis.json")
}

func TestSMT_Mdbx_AddRemove1Element(t *testing.T) {
	sdb, _, err := getTempMdbx()
	if err != nil {
		t.Errorf("Failed to create temp db: %v", err)
	}
	//defer dbi.Close()

	s := NewSMT(sdb, false)

	r, _ := s.InsertBI(big.NewInt(1), big.NewInt(2))
	if r.Mode != "insertNotFound" {
		t.Errorf("Mode is not insert, got %v", r.Mode)
	}

	r, _ = s.InsertBI(big.NewInt(1), big.NewInt(0))
	if r.Mode != "deleteLast" {
		t.Errorf("Mode is not deleteFound, got %v", r.Mode)
	}

	if r.NewRootScalar.ToBigInt().Cmp(big.NewInt(0)) != 0 {
		t.Errorf("Last root is not 0, got %v", r.NewRootScalar.ToBigInt())
	}
}

func TestSMT_Mdbx_AddRemove3Elements(t *testing.T) {
	sdb, _, err := getTempMdbx()
	if err != nil {
		t.Errorf("Failed to create temp db: %v", err)
	}

	s := NewSMT(sdb, false)
	N := 3
	var r *SMTResponse

	for i := 0; i < N; i++ {
		r, _ = s.InsertBI(big.NewInt(int64(i)), big.NewInt(int64(i+1000)))
	}

	fmt.Println("done writing to tree")

	for i := 0; i < N; i++ {
		r, _ = s.InsertBI(big.NewInt(int64(i)), big.NewInt(0))
		if r.Mode != "deleteFound" && i != N-1 {
			t.Errorf("Mode is not deleteFound, got %v", r.Mode)
		} else if r.Mode != "deleteLast" && i == N-1 {
			t.Errorf("Mode is not deleteLast, got %v", r.Mode)
		}
	}

	newR := s.LastRoot()

	if newR.Cmp(big.NewInt(0)) != 0 {
		t.Errorf("Root hash is not zero, got %v", toHex(r.NewRootScalar.ToBigInt()))
	}
}

func TestSMT_Mdbx_AddRemove128Elements(t *testing.T) {
	sdb, _, err := getTempMdbx()
	if err != nil {
		t.Errorf("Failed to create temp db: %v", err)
	}

	s := NewSMT(sdb, false)
	N := 128
	var r *SMTResponse

	for i := 0; i < N; i++ {
		r, _ = s.InsertBI(big.NewInt(int64(i)), big.NewInt(int64(i+1000)))
	}

	fmt.Println("done writing to tree")

	for i := 0; i < N; i++ {
		r, _ = s.InsertBI(big.NewInt(int64(i)), big.NewInt(0))
		if r.Mode != "deleteFound" && i != N-1 {
			t.Errorf("Mode is not deleteFound, got %v", r.Mode)
		} else if r.Mode != "deleteLast" && i == N-1 {
			t.Errorf("Mode is not deleteLast, got %v", r.Mode)
		}
	}

	newR := s.LastRoot()

	if newR.Cmp(big.NewInt(0)) != 0 {
		t.Errorf("Root hash is not zero, got %v", toHex(r.NewRootScalar.ToBigInt()))
	}
}

func TestSMT_Mdbx_MultipleInsert(t *testing.T) {
	sdb, _, err := getTempMdbx()
	if err != nil {
		t.Errorf("Failed to create temp db: %v", err)
	}

	testCases := []struct {
		root  *big.Int
		key   *big.Int
		value *big.Int
		want  string
		mode  string
	}{
		{
			big.NewInt(0),
			big.NewInt(1),
			big.NewInt(1),
			"0xb26e0de762d186d2efc35d9ff4388def6c96ec15f942d83d779141386fe1d2e1",
			"insertNotFound",
		},
		{
			nil,
			big.NewInt(2),
			big.NewInt(2),
			"0xa399847134a9987c648deabc85a7310fbe854315cbeb6dc3a7efa1a4fa2a2c86",
			"insertFound",
		},
		{
			nil,
			big.NewInt(3),
			big.NewInt(3),
			"0xb5a4b1b7a8c3a7c11becc339bbd7f639229cd14f14f76ee3a0e9170074399da4",
			"insertFound",
		},
		{
			big.NewInt(0),
			big.NewInt(3),
			big.NewInt(0),
			"0xa399847134a9987c648deabc85a7310fbe854315cbeb6dc3a7efa1a4fa2a2c86",
			"deleteFound",
		},
	}

	var root *big.Int
	for i, testCase := range testCases {
		if i > 0 {
			testCase.root = root
		}

		tr := new(big.Int).SetInt64(0)
		if i > 0 {
			x, _ := new(big.Int).SetString(testCases[i-1].want[2:], 16)
			tr = x
		}

		s := NewSMT(sdb, false)

		s.SetLastRoot(tr)
		r, err := s.InsertBI(testCase.key, testCase.value)
		if err != nil {
			t.Errorf("Test case %d: Insert failed: %v", i, err)
			continue
		}

		got := toHex(r.NewRootScalar.ToBigInt())
		if got != testCase.want {
			t.Errorf("Test case %d: Root hash is not as expected, got %v, want %v", i, got, testCase.want)
		}
		if testCase.mode != r.Mode {
			t.Errorf("Test case %d: Mode is not as expected, got %v, want %v", i, r.Mode, testCase.mode)
		}

		root = r.NewRootScalar.ToBigInt()
	}
}

func BenchmarkGenesisMdbx(b *testing.B) {
	for i := 0; i < b.N; i++ {
		runGenesisTestMdbx(b, "testdata/mainnet-genesis.json")
	}
}

func runGenesisTestMdbx(tb testing.TB, filename string) {

	data, err := os.ReadFile(filename)
	if err != nil {
		tb.Fatal("Failed to open file: ", err)
	}

	var genesis Genesis
	err = json.Unmarshal(data, &genesis)
	if err != nil {
		tb.Fatal("Failed to parse json: ", err)
	}

	dbi, err := mdbx.NewTemporaryMdbx()
	if err != nil {
		tb.Fatal("Failed to open db: ", err)
	}
	tx, err := dbi.BeginRw(context.Background())
	if err != nil {
		tb.Fatal("Failed to open db: ", err)
	}
	sdb := db2.NewEriDb(tx)
	err = db2.CreateEriDbBuckets(tx)
	if err != nil {
		tb.Fatal("Failed to create db buckets: ", err)
	}

	smt := NewSMT(sdb, false)

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

func Test_Data_Mdbx(t *testing.T) {
	runTestVectorsMdbx(t, "testdata/data.json")
	runTestVectorsMdbx(t, "testdata/data2.json")
}

func runTestVectorsMdbx(t *testing.T, filename string) {
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

func getTempMdbx() (*db2.EriDb, kv.RwDB, error) {
	dbi, err := mdbx.NewTemporaryMdbx()
	if err != nil {
		return nil, nil, err
	}
	tx, err := dbi.BeginRw(context.Background())
	if err != nil {
		return nil, nil, err
	}
	sdb := db2.NewEriDb(tx)
	err = db2.CreateEriDbBuckets(tx)
	if err != nil {
		return nil, nil, err
	}
	return sdb, dbi, nil
}
