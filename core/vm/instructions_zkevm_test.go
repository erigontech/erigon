package vm

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	types2 "github.com/gateway-fm/cdk-erigon-lib/types"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/core/vm/stack"
	"github.com/ledgerwatch/erigon/params"
	"encoding/hex"
)

func TestApplyHexPadBug(t *testing.T) {
	type testScenario struct {
		data     string
		mSize    int
		bug      bool
		expected string
	}

	scenarios := map[string]testScenario{
		// expected verified via zkevm node rpc
		"cardona-block-3177498": {
			data:     "0x010203",
			mSize:    3,
			bug:      true,
			expected: "102030",
		},
		"longHexWithBug": {
			data:     "0x0102030405060708090a0b0c0d0e0f0102030405060708090a0b0c0d0e0f0102030405060708090a0b0c0d0e0f0102030405060708090a0b0c0d0e0f",
			mSize:    64,
			bug:      true,
			expected: "0102030405060708090a0b0c0d0e0f0102030405060708090a0b0c0d0e0f010230405060708090a0b0c0d0e0f0102030405060708090a0b0c0d0e0f0",
		},
		"singleByteHexWithoutBug": {
			data:     "0x11",
			mSize:    1,
			bug:      false,
			expected: "11",
		},
		"longHexWithoutBug1": {
			data:     "0x00000000000000000000000000000000000000000000000000000000000001ff",
			mSize:    32,
			bug:      false,
			expected: "00000000000000000000000000000000000000000000000000000000000001ff",
		},
		"longHexWithoutBug2": {
			data:     "0x00000000000000000000000000000000000000000000000000005af3107a4000",
			mSize:    32,
			bug:      false,
			expected: "00000000000000000000000000000000000000000000000000005af3107a4000",
		},
		"randomHexWithoutBug": {
			data:     "0x060303e606c27f9cddd90a7f129f525c83a0be7108fd5209174a77ffa7809e1c",
			mSize:    32,
			bug:      false,
			expected: "060303e606c27f9cddd90a7f129f525c83a0be7108fd5209174a77ffa7809e1c",
		},
		// expected verified via zkevm node rpc
		"cardona-901721": {
			data:     "0x000c0d08908319bb1f124d95d1e890847956013e989b3a0d8cbcb3a923dc12010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000008908319bb1f124d95d1e890847956013e989b3a0d8cbcb3a923dc81f23aa91d",
			mSize:    128,
			bug:      false,
			expected: "000c0d08908319bb1f124d95d1e890847956013e989b3a0d8cbcb3a923dc12010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000008908319bb1f124d95d1e890847956013e989b3a0d8cbcb3a923dc81f23aa91d",
		},
		// expected verified via zkevm node rpc
		"cardona-1498495": {
			data:     "0x0000000000000000000000000000000000000000000000000000000005f7cf60000000000000000000000000e6386158ecc340d79439f0398a1085502c13993b00000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000140000000000000000000000067a061fb72554db38869a048db9d915600000bc70200000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000000005f5ca880000000000000000000000000000000000000000000000000000000005f5d7820000000000000000000000000000000000000000000000000000000005f7cf600000000000000000000000000000000000000000000000000000000005f7cf6000000000000000000000000000000000000000000000000000000000000000040300010200000000000000000000000000000000000000000000000000000000",
			mSize:    384,
			bug:      false,
			expected: "0000000000000000000000000000000000000000000000000000000005f7cf60000000000000000000000000e6386158ecc340d79439f0398a1085502c13993b00000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000140000000000000000000000067a061fb72554db38869a048db9d915600000bc70200000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000000005f5ca880000000000000000000000000000000000000000000000000000000005f5d7820000000000000000000000000000000000000000000000000000000005f7cf600000000000000000000000000000000000000000000000000000000005f7cf6000000000000000000000000000000000000000000000000000000000000000040300010200000000000000000000000000000000000000000000000000000000",
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			d := common.FromHex(scenario.data)
			result, bug, err := applyHexPadBug(d, uint64(scenario.mSize), 0)
			if err != nil {
				t.Fatalf("Error in applyHexPadBug: %v", err)
			}

			resultHex := hex.EncodeToString(result)

			if scenario.bug != bug {
				t.Errorf("Expected %t but got %t", scenario.bug, bug)
			} else {
				if resultHex != scenario.expected {
					t.Errorf("Expected %s but got %s", scenario.expected, resultHex)
				}
			}
		})
	}
}

func TestBlockhashV2(t *testing.T) {

	gethashFn := func(bn uint64) libcommon.Hash {
		return libcommon.BigToHash(new(big.Int).SetUint64(bn))
	}

	var (
		env            = NewEVM(evmtypes.BlockContext{GetHash: gethashFn}, evmtypes.TxContext{}, TestIntraBlockState{}, params.TestChainConfig, Config{})
		stack          = stack.New()
		evmInterpreter = NewZKEVMInterpreter(env, NewZkConfig(env.Config(), nil))
		pc             = uint64(0)
	)

	tests := []struct {
		blockNumber uint64
		expected    libcommon.Hash
	}{
		{
			blockNumber: 2,
			expected:    libcommon.BigToHash(new(big.Int).SetUint64(2)),
		}, {
			blockNumber: 2000,
			expected:    libcommon.BigToHash(new(big.Int).SetUint64(2000)),
		}, {
			blockNumber: 2000000000,
			expected:    libcommon.BigToHash(new(big.Int).SetUint64(2000000000)),
		},
	}

	for i, test := range tests {
		expected := new(uint256.Int).SetBytes(test.expected.Bytes())
		blockNumberBytes := new(uint256.Int).SetUint64(test.blockNumber)
		stack.Push(blockNumberBytes)

		opBlockhash_zkevm(&pc, evmInterpreter, &ScopeContext{nil, stack, nil})
		actual := stack.Pop()

		fmt.Println(actual)

		if actual.Cmp(expected) != 0 {
			t.Errorf("Testcase %d, expected  %x, got %x", i, expected, actual)
		}
	}
}

func TestDifficultyV2(t *testing.T) {
	var (
		env            = NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, params.TestChainConfig, Config{})
		stack          = stack.New()
		evmInterpreter = NewEVMInterpreter(env, env.Config())
		pc             = uint64(0)
	)

	zeroInt := new(big.Int).SetUint64(0)
	v, _ := uint256.FromBig(zeroInt)

	tests := []struct {
		expected *uint256.Int
	}{
		{
			expected: v,
		},
	}

	for i, test := range tests {
		expected := new(uint256.Int).SetBytes(test.expected.Bytes())

		opDifficulty_zkevm(&pc, evmInterpreter, &ScopeContext{nil, stack, nil})
		actual := stack.Pop()

		if actual.Cmp(expected) != 0 {
			t.Errorf("Testcase %d, expected  %x, got %x", i, expected, actual)
		}
	}
}

func TestExtCodeHashV2(t *testing.T) {
	var (
		env            = NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, TestIntraBlockState{}, params.TestChainConfig, Config{})
		stack          = stack.New()
		evmInterpreter = NewEVMInterpreter(env, env.Config())
		pc             = uint64(0)
	)
	tests := []struct {
		address  string
		expected libcommon.Hash
	}{
		{"0000000000000000000000000000000000000000000000000000000000000000",
			libcommon.HexToHash("0xtest"),
		}, {"000000000000000000000000000000000000000000000000000000000fffffff",
			libcommon.HexToHash("0xtest"),
		},
	}

	for i, test := range tests {
		address := new(uint256.Int).SetBytes(common.Hex2Bytes(test.address))
		expected := new(uint256.Int).SetBytes(test.expected.Bytes())
		stack.Push(address)
		opExtCodeHash_zkevm(&pc, evmInterpreter, &ScopeContext{nil, stack, nil})
		actual := stack.Pop()
		if actual.Cmp(expected) != 0 {
			t.Errorf("Testcase %d, expected  %x, got %x", i, expected, actual)
		}
	}
}

type TestIntraBlockState struct{}

func (ibs TestIntraBlockState) HasLiveAccount(addr libcommon.Address) bool {
	//TODO implement me
	panic("implement me")
}

func (ibs TestIntraBlockState) HasLiveState(addr libcommon.Address, key *libcommon.Hash) bool {
	//TODO implement me
	panic("implement me")
}

func (ibs TestIntraBlockState) AddLog(log *types.Log) {
	//TODO implement me
	panic("implement me")
}

func (ibs TestIntraBlockState) GetLogs(hash libcommon.Hash) []*types.Log {
	//TODO implement me
	panic("implement me")
}

func (ibs TestIntraBlockState) GetBlockStateRoot(blockNum uint64) libcommon.Hash {
	return libcommon.BigToHash(new(big.Int).SetUint64(blockNum))
}

func (ibs TestIntraBlockState) GetBlockNumber() *uint256.Int {
	//TODO implement me
	panic("implement me")
}

func (ibs TestIntraBlockState) CreateAccount(libcommon.Address, bool) {}
func (ibs TestIntraBlockState) GetTxCount() (uint64, error)           { return 0, nil }

func (ibs TestIntraBlockState) SubBalance(libcommon.Address, *uint256.Int) {}
func (ibs TestIntraBlockState) AddBalance(libcommon.Address, *uint256.Int) {}
func (ibs TestIntraBlockState) GetBalance(libcommon.Address) *uint256.Int  { return nil }
func (ibs TestIntraBlockState) GetNonce(libcommon.Address) uint64          { return 0 }
func (ibs TestIntraBlockState) SetNonce(libcommon.Address, uint64)         {}
func (ibs TestIntraBlockState) GetCodeHash(libcommon.Address) libcommon.Hash {
	return libcommon.HexToHash("0xtest")
}
func (ibs TestIntraBlockState) GetCode(libcommon.Address) []byte                                   { return nil }
func (ibs TestIntraBlockState) SetCode(libcommon.Address, []byte)                                  {}
func (ibs TestIntraBlockState) GetCodeSize(libcommon.Address) int                                  { return 0 }
func (ibs TestIntraBlockState) AddRefund(uint64)                                                   {}
func (ibs TestIntraBlockState) SubRefund(uint64)                                                   {}
func (ibs TestIntraBlockState) GetRefund() uint64                                                  { return 0 }
func (ibs TestIntraBlockState) GetCommittedState(libcommon.Address, *libcommon.Hash, *uint256.Int) {}
func (ibs TestIntraBlockState) GetState(address libcommon.Address, slot *libcommon.Hash, outValue *uint256.Int) {
}
func (ibs TestIntraBlockState) SetState(libcommon.Address, *libcommon.Hash, uint256.Int) {}
func (ibs TestIntraBlockState) Selfdestruct(libcommon.Address) bool                      { return false }
func (ibs TestIntraBlockState) HasSelfdestructed(libcommon.Address) bool                 { return false }
func (ibs TestIntraBlockState) Exist(libcommon.Address) bool                             { return false }
func (ibs TestIntraBlockState) Empty(libcommon.Address) bool                             { return false }
func (ibs TestIntraBlockState) PrepareAccessList(sender libcommon.Address, dest *libcommon.Address, precompiles []libcommon.Address, txAccesses types2.AccessList) {
}
func (ibs TestIntraBlockState) AddressInAccessList(addr libcommon.Address) bool { return false }
func (ibs TestIntraBlockState) SlotInAccessList(addr libcommon.Address, slot libcommon.Hash) (addressOk bool, slotOk bool) {
	return false, false
}
func (ibs TestIntraBlockState) AddAddressToAccessList(addr libcommon.Address)                   {}
func (ibs TestIntraBlockState) AddSlotToAccessList(addr libcommon.Address, slot libcommon.Hash) {}
func (ibs TestIntraBlockState) RevertToSnapshot(int)                                            {}
func (ibs TestIntraBlockState) Snapshot() int                                                   { return 0 }
func (ibs TestIntraBlockState) AddLog_zkEvm(*types.Log)                                         {}
