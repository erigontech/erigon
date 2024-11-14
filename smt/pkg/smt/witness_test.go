package smt_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/smt/pkg/db"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/stretchr/testify/require"
)

func prepareSMT(t *testing.T) (*smt.SMT, *trie.RetainList) {
	t.Helper()

	contract := libcommon.HexToAddress("0x71dd1027069078091B3ca48093B00E4735B20624")
	balance := uint256.NewInt(1000000000)
	sKey := libcommon.HexToHash("0x5")
	sVal := uint256.NewInt(0xdeadbeef)

	_, tx := memdb.NewTestTx(t)

	tds := state.NewTrieDbState(libcommon.Hash{}, tx, 0, state.NewPlainStateReader(tx))

	w := tds.TrieStateWriter()

	intraBlockState := state.New(tds)

	tds.StartNewBuffer()

	tds.SetResolveReads(false)

	intraBlockState.CreateAccount(contract, true)

	code := []byte{0x01, 0x02, 0x03, 0x04}
	intraBlockState.SetCode(contract, code)
	intraBlockState.AddBalance(contract, balance)
	intraBlockState.SetState(contract, &sKey, *sVal)

	err := intraBlockState.FinalizeTx(&chain.Rules{}, tds.TrieStateWriter())
	require.NoError(t, err, "error finalising 1st tx")

	err = intraBlockState.CommitBlock(&chain.Rules{}, w)
	require.NoError(t, err, "error committing block")

	inclusions := make(map[libcommon.Address][]libcommon.Hash)
	rl, err := tds.ResolveSMTRetainList(inclusions)
	require.NoError(t, err, "error resolving state trie")

	memdb := db.NewMemDb()

	smtTrie := smt.NewSMT(memdb, false)

	_, err = smtTrie.SetAccountState(contract.String(), balance.ToBig(), uint256.NewInt(1).ToBig())
	require.NoError(t, err)

	err = smtTrie.SetContractBytecode(contract.String(), hex.EncodeToString(code))
	require.NoError(t, err)

	err = memdb.AddCode(code)
	require.NoError(t, err, "error adding code to memdb")

	storage := make(map[string]string, 0)

	for i := 0; i < 100; i++ {
		k := libcommon.HexToHash(fmt.Sprintf("0x%d", i)).String()
		storage[k] = k
	}

	storage[sKey.String()] = sVal.String()

	_, err = smtTrie.SetContractStorage(contract.String(), storage, nil)
	require.NoError(t, err)

	return smtTrie, rl
}

func findNode(t *testing.T, w *trie.Witness, addr libcommon.Address, storageKey libcommon.Hash, nodeType int) []byte {
	t.Helper()

	for _, operator := range w.Operators {
		switch op := operator.(type) {
		case *trie.OperatorSMTLeafValue:
			if op.NodeType == uint8(nodeType) && bytes.Equal(op.Address, addr.Bytes()) {
				if nodeType == utils.SC_STORAGE {
					if bytes.Equal(op.StorageKey, storageKey.Bytes()) {
						return op.Value
					}
				} else {
					return op.Value
				}
			}
		}
	}

	return nil
}

func TestSMTWitnessRetainList(t *testing.T) {
	smtTrie, rl := prepareSMT(t)

	contract := libcommon.HexToAddress("0x71dd1027069078091B3ca48093B00E4735B20624")
	sKey := libcommon.HexToHash("0x5")
	sVal := uint256.NewInt(0xdeadbeef)

	witness, err := smt.BuildWitness(smtTrie, rl, context.Background())
	require.NoError(t, err, "error building witness")

	foundCode := findNode(t, witness, contract, libcommon.Hash{}, utils.SC_CODE)
	foundBalance := findNode(t, witness, contract, libcommon.Hash{}, utils.KEY_BALANCE)
	foundNonce := findNode(t, witness, contract, libcommon.Hash{}, utils.KEY_NONCE)
	foundStorage := findNode(t, witness, contract, sKey, utils.SC_STORAGE)

	require.NotNil(t, foundCode)
	require.NotNil(t, foundBalance)
	require.NotNil(t, foundNonce)
	require.NotNil(t, foundStorage)

	require.Equal(t, foundStorage, sVal.Bytes(), "witness contains unexpected storage value")
}

func TestSMTWitnessRetainListEmptyVal(t *testing.T) {
	smtTrie, rl := prepareSMT(t)

	contract := libcommon.HexToAddress("0x71dd1027069078091B3ca48093B00E4735B20624")
	balance := uint256.NewInt(1000000000)
	sKey := libcommon.HexToHash("0x5")

	// Set nonce to 0
	_, err := smtTrie.SetAccountState(contract.String(), balance.ToBig(), uint256.NewInt(0).ToBig())
	require.NoError(t, err)

	witness, err := smt.BuildWitness(smtTrie, rl, context.Background())
	require.NoError(t, err, "error building witness")

	foundCode := findNode(t, witness, contract, libcommon.Hash{}, utils.SC_CODE)
	foundBalance := findNode(t, witness, contract, libcommon.Hash{}, utils.KEY_BALANCE)
	foundNonce := findNode(t, witness, contract, libcommon.Hash{}, utils.KEY_NONCE)
	foundStorage := findNode(t, witness, contract, sKey, utils.SC_STORAGE)

	// Code, balance and storage should be present in the witness
	require.NotNil(t, foundCode)
	require.NotNil(t, foundBalance)
	require.NotNil(t, foundStorage)

	// Nonce should not be in witness
	require.Nil(t, foundNonce, "witness contains unexpected operator")
}

// TestWitnessToSMT tests that the SMT built from a witness matches the original SMT
func TestWitnessToSMT(t *testing.T) {
	smtTrie, rl := prepareSMT(t)

	witness, err := smt.BuildWitness(smtTrie, rl, context.Background())
	require.NoError(t, err, "error building witness")

	newSMT, err := smt.BuildSMTfromWitness(witness)
	require.NoError(t, err, "error building SMT from witness")

	root, err := newSMT.Db.GetLastRoot()
	require.NoError(t, err, "error getting last root from db")

	// newSMT.Traverse(context.Background(), root, func(prefix []byte, k utils.NodeKey, v utils.NodeValue12) (bool, error) {
	// 	fmt.Printf("[After] path: %v, hash: %x\n", prefix, libcommon.BigToHash(k.ToBigInt()))
	// 	return true, nil
	// })

	expectedRoot, err := smtTrie.Db.GetLastRoot()
	require.NoError(t, err, "error getting last root")

	// assert that the roots are the same
	require.Equal(t, expectedRoot, root, "SMT root mismatch")
}

// TestWitnessToSMTStateReader tests that the SMT built from a witness matches the state
func TestWitnessToSMTStateReader(t *testing.T) {
	smtTrie, rl := prepareSMT(t)

	sKey := libcommon.HexToHash("0x5")

	expectedRoot, err := smtTrie.Db.GetLastRoot()
	require.NoError(t, err, "error getting last root")

	witness, err := smt.BuildWitness(smtTrie, rl, context.Background())
	require.NoError(t, err, "error building witness")

	newSMT, err := smt.BuildSMTfromWitness(witness)
	require.NoError(t, err, "error building SMT from witness")

	root, err := newSMT.Db.GetLastRoot()
	require.NoError(t, err, "error getting the last root from db")

	require.Equal(t, expectedRoot, root, "SMT root mismatch")

	contract := libcommon.HexToAddress("0x71dd1027069078091B3ca48093B00E4735B20624")

	expectedAcc, err := smtTrie.ReadAccountData(contract)
	require.NoError(t, err)

	newAcc, err := newSMT.ReadAccountData(contract)
	require.NoError(t, err)

	expectedAccCode, err := smtTrie.ReadAccountCode(contract, 0, expectedAcc.CodeHash)
	require.NoError(t, err)

	newAccCode, err := newSMT.ReadAccountCode(contract, 0, newAcc.CodeHash)
	require.NoError(t, err)

	expectedAccCodeSize, err := smtTrie.ReadAccountCodeSize(contract, 0, expectedAcc.CodeHash)
	require.NoError(t, err)

	newAccCodeSize, err := newSMT.ReadAccountCodeSize(contract, 0, newAcc.CodeHash)
	require.NoError(t, err)

	expectedStorageValue, err := smtTrie.ReadAccountStorage(contract, 0, &sKey)
	require.NoError(t, err)

	newStorageValue, err := newSMT.ReadAccountStorage(contract, 0, &sKey)
	require.NoError(t, err)

	// assert that the account data is the same
	require.Equal(t, expectedAcc, newAcc)

	// assert that account code is the same
	require.Equal(t, expectedAccCode, newAccCode)

	// assert that the account code size is the same
	require.Equal(t, expectedAccCodeSize, newAccCodeSize)

	// assert that the storage value is the same
	require.Equal(t, expectedStorageValue, newStorageValue)
}
