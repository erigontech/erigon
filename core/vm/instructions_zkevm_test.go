package vm

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/core/vm/stack"
	"github.com/ledgerwatch/erigon/params"
)

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

func (ibs TestIntraBlockState) SeenAccount(addr libcommon.Address) bool {
	//TODO implement me
	panic("implement me")
}

func (ibs TestIntraBlockState) GetLogs(hash libcommon.Hash) []*types.Log {
	//TODO implement me
	panic("implement me")
}

func (ibs TestIntraBlockState) GetBlockStateRoot(blockNum *uint256.Int) *uint256.Int {
	return uint256.NewInt(0).Set(blockNum)
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
func (ibs TestIntraBlockState) AddAddressToAccessList(addr libcommon.Address) bool { return false }
func (ibs TestIntraBlockState) AddSlotToAccessList(addr libcommon.Address, slot libcommon.Hash) (bool, bool) {
	return false, false
}
func (ibs TestIntraBlockState) RevertToSnapshot(int)    {}
func (ibs TestIntraBlockState) Snapshot() int           { return 0 }
func (ibs TestIntraBlockState) AddLog_zkEvm(*types.Log) {}

func (ibs TestIntraBlockState) GetTransientState(addr common.Address, key common.Hash) uint256.Int {
	return uint256.Int{}
}

func (ibs TestIntraBlockState) SetTransientState(addr common.Address, key common.Hash, value uint256.Int) {
}

func (ibs TestIntraBlockState) Prepare(rules *chain.Rules, sender, coinbase common.Address, dest *common.Address,
	precompiles []common.Address, txAccesses types2.AccessList) {
}

func (ibs TestIntraBlockState) Selfdestruct6780(common.Address) {}

func (ibs TestIntraBlockState) SetDisableBalanceInc(disable bool) {}

func (ibs TestIntraBlockState) IsDirtyJournal(addr common.Address) bool { return false }
