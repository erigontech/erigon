package smtv2

import (
	"math/big"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/holiman/uint256"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/types/accounts"
	"gotest.tools/v3/assert"
)

type storageChange struct {
	key common.Hash
	old []byte
	new []byte
}

type accountChange struct {
	addr           common.Address
	isCreate       bool
	old            accounts.Account
	new            accounts.Account
	oldCode        []byte
	newCode        []byte
	storageChanges []storageChange
}

type testCase struct {
	accountChanges []accountChange
	expected       []ChangeSetEntry
}

// we use a function here to return te test cases as calls like 'AddrKeyBalance` need an init function to run at the package level
// or the keys will not be set correctly in the expected results
func getTestCases() map[string]testCase {
	var testCases = map[string]testCase{
		"new account - just balance": {
			accountChanges: []accountChange{
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: true,
					old:      accounts.Account{},
					new:      accounts.Account{Nonce: 0, Balance: *uint256.NewInt(1)},
				},
			},
			expected: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Add,
					Key:   AddrKeyBalance(common.HexToAddress("0x1")),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
		},
		"new account - just nonce": {
			accountChanges: []accountChange{
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: true,
					old:      accounts.Account{},
					new:      accounts.Account{Nonce: 1, Balance: *uint256.NewInt(0)},
				},
			},
			expected: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Add,
					Key:   AddrKeyNonce(common.HexToAddress("0x1")),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
		},
		"new account - all values": {
			accountChanges: []accountChange{
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: true,
					old:      accounts.Account{},
					new:      accounts.Account{Nonce: 1, Balance: *uint256.NewInt(1)},
				},
			},
			expected: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Add,
					Key:   AddrKeyNonce(common.HexToAddress("0x1")),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
				{
					Type:  ChangeSetEntryType_Add,
					Key:   AddrKeyBalance(common.HexToAddress("0x1")),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
		},
		"account change - just nonce": {
			accountChanges: []accountChange{
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: false,
					old:      accounts.Account{Nonce: 0, Balance: *uint256.NewInt(0)},
					new:      accounts.Account{Nonce: 1, Balance: *uint256.NewInt(0)},
				},
			},
			expected: []ChangeSetEntry{
				{
					Type:          ChangeSetEntryType_Add,
					Key:           AddrKeyNonce(common.HexToAddress("0x1")),
					Value:         ScalarToSmtValue8FromBits(big.NewInt(1)),
					OriginalValue: ScalarToSmtValue8FromBits(big.NewInt(0)),
				},
			},
		},
		"account change - nonce to 0": {
			accountChanges: []accountChange{
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: false,
					old:      accounts.Account{Nonce: 1, Balance: *uint256.NewInt(0)},
					new:      accounts.Account{Nonce: 0, Balance: *uint256.NewInt(0)},
				},
			},
			expected: []ChangeSetEntry{
				{
					Type:          ChangeSetEntryType_Delete,
					Key:           AddrKeyNonce(common.HexToAddress("0x1")),
					Value:         ScalarToSmtValue8FromBits(big.NewInt(0)),
					OriginalValue: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
		},
		"account change - just balance": {
			accountChanges: []accountChange{
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: false,
					old:      accounts.Account{Nonce: 0, Balance: *uint256.NewInt(1)},
					new:      accounts.Account{Nonce: 0, Balance: *uint256.NewInt(2)},
				},
			},
			expected: []ChangeSetEntry{
				{
					Type:          ChangeSetEntryType_Change,
					Key:           AddrKeyBalance(common.HexToAddress("0x1")),
					Value:         ScalarToSmtValue8FromBits(big.NewInt(2)),
					OriginalValue: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
		},
		"account change - all values": {
			accountChanges: []accountChange{
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: false,
					old:      accounts.Account{Nonce: 0, Balance: *uint256.NewInt(1)},
					new:      accounts.Account{Nonce: 1, Balance: *uint256.NewInt(2)},
				},
			},
			expected: []ChangeSetEntry{
				{
					Type:          ChangeSetEntryType_Add,
					Key:           AddrKeyNonce(common.HexToAddress("0x1")),
					Value:         ScalarToSmtValue8FromBits(big.NewInt(1)),
					OriginalValue: ScalarToSmtValue8FromBits(big.NewInt(0)),
				},
				{
					Type:          ChangeSetEntryType_Change,
					Key:           AddrKeyBalance(common.HexToAddress("0x1")),
					Value:         ScalarToSmtValue8FromBits(big.NewInt(2)),
					OriginalValue: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
		},
		"account change - balance to zero": {
			accountChanges: []accountChange{
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: false,
					old:      accounts.Account{Nonce: 0, Balance: *uint256.NewInt(1)},
					new:      accounts.Account{Nonce: 0, Balance: *uint256.NewInt(0)},
				},
			},
			expected: []ChangeSetEntry{
				{
					Type:          ChangeSetEntryType_Delete,
					Key:           AddrKeyBalance(common.HexToAddress("0x1")),
					Value:         ScalarToSmtValue8FromBits(big.NewInt(0)),
					OriginalValue: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
		},
		"multiple account changes in a single batch - ending with only adds": {
			accountChanges: []accountChange{
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: true,
					old:      accounts.Account{},
					new:      accounts.Account{Nonce: 0, Balance: *uint256.NewInt(100)},
				},
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: false,
					old:      accounts.Account{},
					new:      accounts.Account{Nonce: 1, Balance: *uint256.NewInt(50)},
				},
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: false,
					old:      accounts.Account{},
					new:      accounts.Account{Nonce: 2, Balance: *uint256.NewInt(20)},
				},
			},
			expected: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Add,
					Key:   AddrKeyNonce(common.HexToAddress("0x1")),
					Value: ScalarToSmtValue8FromBits(big.NewInt(2)),
				},
				{
					Type:  ChangeSetEntryType_Add,
					Key:   AddrKeyBalance(common.HexToAddress("0x1")),
					Value: ScalarToSmtValue8FromBits(big.NewInt(20)),
				},
			},
		},
		"multiple account changes in a single batch - nonce added but 0 balance not included - only shows end state": {
			accountChanges: []accountChange{
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: true,
					old:      accounts.Account{},
					new:      accounts.Account{Nonce: 0, Balance: *uint256.NewInt(100)},
				},
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: false,
					old:      accounts.Account{Nonce: 0, Balance: *uint256.NewInt(0)},
					new:      accounts.Account{Nonce: 1, Balance: *uint256.NewInt(50)},
				},
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: false,
					old:      accounts.Account{Nonce: 0, Balance: *uint256.NewInt(0)},
					new:      accounts.Account{Nonce: 2, Balance: *uint256.NewInt(0)},
				},
			},
			expected: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Add,
					Key:   AddrKeyNonce(common.HexToAddress("0x1")),
					Value: ScalarToSmtValue8FromBits(big.NewInt(2)),
				},
			},
		},
		"existing account multiple account changes in a single batch - nonce added but 0 balance not included - only shows end state": {
			accountChanges: []accountChange{
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: false,
					old:      accounts.Account{Nonce: 10, Balance: *uint256.NewInt(100)},
					new:      accounts.Account{Nonce: 11, Balance: *uint256.NewInt(80)},
				},
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: false,
					old:      accounts.Account{Nonce: 11, Balance: *uint256.NewInt(80)},
					new:      accounts.Account{Nonce: 12, Balance: *uint256.NewInt(60)},
				},
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: false,
					old:      accounts.Account{Nonce: 12, Balance: *uint256.NewInt(60)},
					new:      accounts.Account{Nonce: 13, Balance: *uint256.NewInt(40)},
				},
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: false,
					old:      accounts.Account{Nonce: 13, Balance: *uint256.NewInt(40)},
					new:      accounts.Account{Nonce: 13, Balance: *uint256.NewInt(1000)},
				},
			},
			expected: []ChangeSetEntry{
				{
					Type:          ChangeSetEntryType_Change,
					Key:           AddrKeyNonce(common.HexToAddress("0x1")),
					Value:         ScalarToSmtValue8FromBits(big.NewInt(13)),
					OriginalValue: ScalarToSmtValue8FromBits(big.NewInt(10)),
				},
				{
					Type:          ChangeSetEntryType_Change,
					Key:           AddrKeyBalance(common.HexToAddress("0x1")),
					Value:         ScalarToSmtValue8FromBits(big.NewInt(1000)),
					OriginalValue: ScalarToSmtValue8FromBits(big.NewInt(100)),
				},
			},
		},
		"existing account - nonce increase and balance to 0 - change to nonce delete to balance": {
			accountChanges: []accountChange{
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: false,
					old:      accounts.Account{Nonce: 1, Balance: *uint256.NewInt(100)},
					new:      accounts.Account{Nonce: 2, Balance: *uint256.NewInt(0)},
				},
			},
			expected: []ChangeSetEntry{
				{
					Type:          ChangeSetEntryType_Change,
					Key:           AddrKeyNonce(common.HexToAddress("0x1")),
					Value:         ScalarToSmtValue8FromBits(big.NewInt(2)),
					OriginalValue: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
				{
					Type:          ChangeSetEntryType_Delete,
					Key:           AddrKeyBalance(common.HexToAddress("0x1")),
					Value:         ScalarToSmtValue8FromBits(big.NewInt(0)),
					OriginalValue: ScalarToSmtValue8FromBits(big.NewInt(100)),
				},
			},
		},
		"new contract - with code": {
			accountChanges: []accountChange{
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: true,
					old:      accounts.Account{},
					new:      accounts.Account{Nonce: 0, Balance: *uint256.NewInt(0)},
					newCode:  []byte("123"),
				},
			},
			expected: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Add,
					Key:   AddrKeyCode(common.HexToAddress("0x1")),
					Value: mustGetCodeValue([]byte("123")),
				},
				{
					Type:  ChangeSetEntryType_Add,
					Key:   AddrKeyLength(common.HexToAddress("0x1")),
					Value: mustGetContractLength([]byte("123")),
				},
			},
		},
		"existing contract - code change": {
			accountChanges: []accountChange{
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: false,
					old:      accounts.Account{},
					new:      accounts.Account{},
					oldCode:  []byte("0x123"),
					newCode:  []byte("0x456"),
				},
			},
			expected: []ChangeSetEntry{
				{
					Type:          ChangeSetEntryType_Change,
					Key:           AddrKeyCode(common.HexToAddress("0x1")),
					Value:         mustGetCodeValue([]byte("0x456")),
					OriginalValue: mustGetCodeValue([]byte("0x123")),
				},
				{
					Type:          ChangeSetEntryType_Change,
					Key:           AddrKeyLength(common.HexToAddress("0x1")),
					Value:         mustGetContractLength([]byte("0x456")),
					OriginalValue: mustGetContractLength([]byte("0x123")),
				},
			},
		},
		"new storage key - with value": {
			accountChanges: []accountChange{
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: true,
					old:      accounts.Account{},
					new:      accounts.Account{},
					storageChanges: []storageChange{
						{
							key: common.HexToHash("0x123"),
							old: nil,
							new: []byte("0x789"),
						},
					},
				},
			},
			expected: []ChangeSetEntry{
				{
					Type:          ChangeSetEntryType_Add,
					Key:           KeyContractStorageWithoutBig(common.HexToAddress("0x1"), common.HexToHash("0x123")),
					Value:         mustGetStorageValue([]byte("0x789")),
					OriginalValue: mustGetStorageValue([]byte("")),
				},
			},
		},
		"existing storage slot - going to zero - is deleted": {
			accountChanges: []accountChange{
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: true,
					old:      accounts.Account{},
					new:      accounts.Account{},
					storageChanges: []storageChange{
						{
							key: common.HexToHash("0x123"),
							old: []byte("0x456"),
							new: []byte{},
						},
					},
				},
			},
			expected: []ChangeSetEntry{
				{
					Type:          ChangeSetEntryType_Delete,
					Key:           KeyContractStorageWithoutBig(common.HexToAddress("0x1"), common.HexToHash("0x123")),
					Value:         SmtValue8{},
					OriginalValue: mustGetStorageValue([]byte("0x456")),
				},
			},
		},
		"existing storage slot - changes": {
			accountChanges: []accountChange{
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: true,
					old:      accounts.Account{},
					new:      accounts.Account{},
					storageChanges: []storageChange{
						{
							key: common.HexToHash("0x123"),
							old: []byte("0x456"),
							new: []byte("0x789"),
						},
					},
				},
			},
			expected: []ChangeSetEntry{
				{
					Type:          ChangeSetEntryType_Change,
					Key:           KeyContractStorageWithoutBig(common.HexToAddress("0x1"), common.HexToHash("0x123")),
					Value:         mustGetStorageValue([]byte("0x789")),
					OriginalValue: mustGetStorageValue([]byte("0x456")),
				},
			},
		},
		"new account with an immediate change to the same account - series of adds output": {
			accountChanges: []accountChange{
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: true,
					old:      accounts.Account{},
					new:      accounts.Account{Nonce: 0, Balance: *uint256.NewInt(100)},
				},
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: false,
					old:      accounts.Account{Nonce: 0, Balance: *uint256.NewInt(100)},
					new:      accounts.Account{Nonce: 1, Balance: *uint256.NewInt(50)},
				},
			},
			expected: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Add,
					Key:   AddrKeyNonce(common.HexToAddress("0x1")),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
				{
					Type:  ChangeSetEntryType_Add,
					Key:   AddrKeyBalance(common.HexToAddress("0x1")),
					Value: ScalarToSmtValue8FromBits(big.NewInt(50)),
				},
			},
		},
		"storage slot starts empty and ends empty with some changes in between - no-op": {
			accountChanges: []accountChange{
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: false,
					old:      accounts.Account{Nonce: 0, Balance: *uint256.NewInt(100)},
					new:      accounts.Account{Nonce: 0, Balance: *uint256.NewInt(100)},
					storageChanges: []storageChange{
						{
							key: common.HexToHash("0x123"),
							old: nil,
							new: []byte("0x789"),
						},
						{
							key: common.HexToHash("0x123"),
							old: []byte("0x789"),
							new: []byte("0x456"),
						},
						{
							key: common.HexToHash("0x123"),
							old: []byte("0x456"),
							new: []byte{},
						},
					},
				},
			},
			expected: []ChangeSetEntry{},
		},
		"account balance from 0 to something then to 0 - no-op": {
			accountChanges: []accountChange{
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: true,
					old:      accounts.Account{},
					new:      accounts.Account{Nonce: 0, Balance: *uint256.NewInt(100)},
				},
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: false,
					old:      accounts.Account{Nonce: 0, Balance: *uint256.NewInt(100)},
					new:      accounts.Account{Nonce: 0, Balance: *uint256.NewInt(200)},
				},
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: false,
					old:      accounts.Account{Nonce: 0, Balance: *uint256.NewInt(200)},
					new:      accounts.Account{Nonce: 0, Balance: *uint256.NewInt(1000)},
				},
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: false,
					old:      accounts.Account{Nonce: 0, Balance: *uint256.NewInt(1000)},
					new:      accounts.Account{Nonce: 0, Balance: *uint256.NewInt(0)},
				},
			},
			expected: []ChangeSetEntry{},
		},
		"existing account balance going from 0 to something -> add": {
			accountChanges: []accountChange{
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: false,
					old:      accounts.Account{Nonce: 0, Balance: *uint256.NewInt(0)},
					new:      accounts.Account{Nonce: 0, Balance: *uint256.NewInt(100)},
				},
			},
			expected: []ChangeSetEntry{
				{
					Type:          ChangeSetEntryType_Add,
					Key:           AddrKeyBalance(common.HexToAddress("0x1")),
					Value:         ScalarToSmtValue8FromBits(big.NewInt(100)),
					OriginalValue: ScalarToSmtValue8FromBits(big.NewInt(0)),
				},
			},
		},
		"existing account nonce going from 0 to something -> add": {
			accountChanges: []accountChange{
				{
					addr:     common.HexToAddress("0x1"),
					isCreate: false,
					old:      accounts.Account{Nonce: 0, Balance: *uint256.NewInt(0)},
					new:      accounts.Account{Nonce: 1, Balance: *uint256.NewInt(0)},
				},
			},
			expected: []ChangeSetEntry{
				{
					Type:          ChangeSetEntryType_Add,
					Key:           AddrKeyNonce(common.HexToAddress("0x1")),
					Value:         ScalarToSmtValue8FromBits(big.NewInt(1)),
					OriginalValue: ScalarToSmtValue8FromBits(big.NewInt(0)),
				},
			},
		},
	}
	return testCases
}

func TestChangesetMapper(t *testing.T) {
	for name, testCase := range getTestCases() {
		t.Run(name, func(t *testing.T) {
			mapper := NewChangesetMapper()
			for _, change := range testCase.accountChanges {
				if change.isCreate {
					mapper.ProcessNewAccount(change.addr, change.new, change.newCode)
				} else {
					mapper.ProcessAccountChange(change.addr, change.old, change.new)
					if len(change.oldCode) > 0 || len(change.newCode) > 0 {
						mapper.ProcessAccountCodeChange(change.addr, change.oldCode, change.newCode)
					}
				}

				for _, storageChange := range change.storageChanges {
					mapper.ProcessStorageChange(change.addr, storageChange.key, storageChange.old, storageChange.new)
				}
			}

			changeSet, err := mapper.GenerateChangeEntries()
			if err != nil {
				t.Fatalf("error generating change tape: %v", err)
			}
			if len(changeSet) != len(testCase.expected) {
				t.Fatalf("expected %d change set entries, got %d", len(testCase.expected), len(changeSet))
			}

			// sort the change set lexographically by key
			slices.SortFunc(testCase.expected, func(i, j ChangeSetEntry) int {
				return lexographhicalCheckPaths(i.Key.GetPath(), j.Key.GetPath())
			})

			assert.DeepEqual(t, changeSet, testCase.expected, bigIntComparer, cmpopts.IgnoreUnexported(ChangeSetEntry{}))
		})
	}
}
