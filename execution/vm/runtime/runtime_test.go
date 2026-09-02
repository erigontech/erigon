// Copyright 2015 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package runtime

import (
	"fmt"
	"math/big"
	"os"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/tracing/tracers/logger"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/asm"
	"github.com/erigontech/erigon/execution/vm/program"
)

func TestDefaults(t *testing.T) {
	t.Parallel()
	cfg := new(Config)
	setDefaults(cfg)

	if cfg.Difficulty == nil {
		t.Error("expected difficulty to be non nil")
	}
	if cfg.GasLimit == 0 {
		t.Error("didn't expect gaslimit to be zero")
	}
	if cfg.GetHashFn == nil {
		t.Error("expected time to be non nil")
	}
}

func TestEVM(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("crashed with: %v", r)
		}
	}()

	if _, _, err := Execute([]byte{
		byte(vm.DIFFICULTY),
		byte(vm.TIMESTAMP),
		byte(vm.GASLIMIT),
		byte(vm.PUSH1),
		byte(vm.ORIGIN),
		byte(vm.BLOCKHASH),
		byte(vm.COINBASE),
	}, nil, nil, t.TempDir()); err != nil {
		t.Fatal("didn't expect error", err)
	}
}

func TestExecute(t *testing.T) {
	t.Parallel()
	ret, _, err := Execute([]byte{
		byte(vm.PUSH1), 10,
		byte(vm.PUSH1), 0,
		byte(vm.MSTORE),
		byte(vm.PUSH1), 32,
		byte(vm.PUSH1), 0,
		byte(vm.RETURN),
	}, nil, nil, t.TempDir())
	if err != nil {
		t.Fatal("didn't expect error", err)
	}

	num := new(big.Int).SetBytes(ret)
	if num.Cmp(big.NewInt(10)) != 0 {
		t.Error("Expected 10, got", num)
	}
}

func TestCall(t *testing.T) {
	t.Parallel()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, domains := temporaltest.NewTestTxSD(t, db)

	state := state.New(state.NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})))
	defer state.Close()
	address := accounts.InternAddress(common.HexToAddress("0xaa"))
	require.NoError(t, state.SetCode(address, []byte{
		byte(vm.PUSH1), 10,
		byte(vm.PUSH1), 0,
		byte(vm.MSTORE),
		byte(vm.PUSH1), 32,
		byte(vm.PUSH1), 0,
		byte(vm.RETURN),
	}, tracing.CodeChangeUnspecified))

	ret, _, err := Call(address, nil, &Config{State: state})
	if err != nil {
		t.Fatal("didn't expect error", err)
	}

	num := new(big.Int).SetBytes(ret)
	if num.Cmp(big.NewInt(10)) != 0 {
		t.Error("Expected 10, got", num)
	}
}

func TestCreateInsufficientBalanceLeavesGasUntouched(t *testing.T) {
	t.Parallel()
	statedb := state.New(state.NewNoopReader())
	defer statedb.Close()
	const gasLimit = uint64(500_000)
	_, _, gasRemaining, err := Create(
		[]byte{byte(vm.STOP)},
		&Config{
			GasLimit: gasLimit,
			Value:    *uint256.NewInt(1),
			State:    statedb,
		},
		0,
	)
	require.ErrorIs(t, err, vm.ErrInsufficientBalance)
	require.Equal(t, mdgas.MdGas{Execution: gasLimit}, gasRemaining)
}

func TestCreateInsufficientBalancePreservesPreAmsterdamTrace(t *testing.T) {
	t.Parallel()
	statedb := state.New(state.NewNoopReader())
	defer statedb.Close()
	var entered []byte
	exited := 0
	hooks := &tracing.Hooks{
		OnEnter: func(_ int, typ byte, _, _ accounts.Address, _ bool, _ []byte, _ uint64, _ uint256.Int, _ []byte) {
			entered = append(entered, typ)
		},
		OnExit: func(_ int, _ []byte, _ uint64, _ error, _ bool) {
			exited++
		},
	}
	_, _, _, err := Create(
		[]byte{byte(vm.STOP)},
		&Config{
			ChainConfig: chain.TestChainOsakaConfig,
			EVMConfig:   vm.Config{Tracer: hooks},
			GasLimit:    500_000,
			Value:       *uint256.NewInt(1),
			State:       statedb,
		},
		0,
	)
	require.ErrorIs(t, err, vm.ErrInsufficientBalance)
	require.Equal(t, []byte{byte(vm.CREATE)}, entered)
	require.Equal(t, 1, exited)
}

func TestCreateRuntimeOutOfGasEmitsCallGasChanges(t *testing.T) {
	t.Parallel()
	statedb := state.New(state.NewNoopReader())
	defer statedb.Close()
	type gasChange struct {
		old    uint64
		new    uint64
		reason tracing.GasChangeReason
	}
	var gasChanges []gasChange
	hooks := &tracing.Hooks{
		OnGasChange: func(old, newGas uint64, reason tracing.GasChangeReason) {
			if reason == tracing.GasChangeCallInitialBalance || reason == tracing.GasChangeCallFailedExecution {
				gasChanges = append(gasChanges, gasChange{old: old, new: newGas, reason: reason})
			}
		},
	}
	gasLimit := uint64(params.StateGasNewAccount - 1)
	_, _, _, err := Create(
		[]byte{byte(vm.STOP)},
		&Config{
			EVMConfig: vm.Config{Tracer: hooks},
			GasLimit:  gasLimit,
			State:     statedb,
		},
		0,
	)
	require.ErrorIs(t, err, vm.ErrRuntimeOutOfGas)
	require.Equal(
		t,
		[]gasChange{
			{old: 0, new: gasLimit, reason: tracing.GasChangeCallInitialBalance},
			{old: gasLimit, new: 0, reason: tracing.GasChangeCallFailedExecution},
		},
		gasChanges,
	)
}

func TestCallChargesAmsterdamNewAccountStateGas(t *testing.T) {
	t.Parallel()

	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, domains := temporaltest.NewTestTxSD(t, db)
	statedb := state.New(state.NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})))
	defer statedb.Close()

	sender := accounts.InternAddress(common.HexToAddress("0x1000"))
	recipient := accounts.InternAddress(common.HexToAddress("0x2000"))
	value := uint256.NewInt(1)
	require.NoError(t, statedb.SetBalance(sender, *value, tracing.BalanceChangeUnspecified))

	const gasLimit = uint64(500_000)
	_, gasRemaining, err := Call(recipient, nil, &Config{
		ChainConfig: chain.AllProtocolChanges,
		Origin:      sender,
		GasLimit:    gasLimit,
		Value:       *value,
		State:       statedb,
	})
	require.NoError(t, err)
	require.Equal(t, mdgas.MdGas{Execution: gasLimit - params.StateGasNewAccount}, gasRemaining)
}

func TestCallChargesAmsterdamDelegationTargetAccess(t *testing.T) {
	t.Parallel()

	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, domains := temporaltest.NewTestTxSD(t, db)
	statedb := state.New(state.NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})))
	defer statedb.Close()

	recipient := accounts.InternAddress(common.HexToAddress("0x2000"))
	delegatedTo := accounts.InternAddress(common.HexToAddress("0x3000"))
	require.NoError(t, statedb.SetCode(recipient, types.AddressToDelegation(delegatedTo), tracing.CodeChangeUnspecified))
	require.NoError(t, statedb.SetCode(delegatedTo, []byte{byte(vm.STOP)}, tracing.CodeChangeUnspecified))

	const gasLimit = uint64(100_000)
	_, gasRemaining, err := Call(recipient, nil, &Config{
		ChainConfig: chain.AllProtocolChanges,
		GasLimit:    gasLimit,
		State:       statedb,
	})
	require.NoError(t, err)
	require.Equal(t, mdgas.MdGas{Execution: gasLimit - params.ColdAccountAccessCostEIP8038}, gasRemaining)
	require.True(t, statedb.AddressInAccessList(delegatedTo))
}

func TestCallWarmsPragueDelegationTarget(t *testing.T) {
	t.Parallel()

	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, domains := temporaltest.NewTestTxSD(t, db)
	statedb := state.New(state.NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})))
	defer statedb.Close()

	recipient := accounts.InternAddress(common.HexToAddress("0x2000"))
	delegatedTo := accounts.InternAddress(common.HexToAddress("0x3000"))
	require.NoError(t, statedb.SetCode(recipient, types.AddressToDelegation(delegatedTo), tracing.CodeChangeUnspecified))
	require.NoError(t, statedb.SetCode(delegatedTo, []byte{byte(vm.STOP)}, tracing.CodeChangeUnspecified))

	_, _, err := Call(recipient, nil, &Config{
		ChainConfig: chain.TestChainOsakaConfig,
		GasLimit:    100_000,
		State:       statedb,
	})
	require.NoError(t, err)
	require.True(t, statedb.AddressInAccessList(delegatedTo))
}

func fakeHeader(n uint64, parentHash common.Hash) *types.Header {
	return &types.Header{
		Coinbase:   common.HexToAddress("0x00000000000000000000000000000000deadbeef"),
		Number:     *uint256.NewInt(n),
		ParentHash: parentHash,
		Time:       n,
		Nonce:      types.BlockNonce{0x1},
		Extra:      []byte{},
		GasLimit:   100000,
	}
}

// FakeChainHeaderReader implements consensus.ChainHeaderReader interface
type FakeChainHeaderReader struct{}

func (cr *FakeChainHeaderReader) GetHeaderByHash(hash common.Hash) *types.Header {
	return nil
}
func (cr *FakeChainHeaderReader) GetHeaderByNumber(number uint64) *types.Header {
	return cr.GetHeaderByHash(common.BigToHash(new(big.Int).SetUint64(number)))
}
func (cr *FakeChainHeaderReader) Config() *chain.Config                 { return nil }
func (cr *FakeChainHeaderReader) CurrentHeader() *types.Header          { return nil }
func (cr *FakeChainHeaderReader) CurrentFinalizedHeader() *types.Header { return nil }
func (cr *FakeChainHeaderReader) CurrentSafeHeader() *types.Header      { return nil }

// GetHeader returns a fake header with the parentHash equal to the number - 1
func (cr *FakeChainHeaderReader) GetHeader(hash common.Hash, number uint64) *types.Header {
	return &types.Header{
		Coinbase:   common.HexToAddress("0x00000000000000000000000000000000deadbeef"),
		Number:     *uint256.NewInt(number),
		ParentHash: common.BigToHash(new(big.Int).SetUint64(number - 1)),
		Time:       number,
		Nonce:      types.BlockNonce{0x1},
		Extra:      []byte{},
		GasLimit:   100000,
	}
}
func (cr *FakeChainHeaderReader) GetBlock(hash common.Hash, number uint64) *types.Block {
	return nil
}
func (cr *FakeChainHeaderReader) HasBlock(hash common.Hash, number uint64) bool { return false }
func (cr *FakeChainHeaderReader) GetTd(hash common.Hash, number uint64) *uint256.Int {
	return nil
}
func (cr *FakeChainHeaderReader) FrozenBlocks() uint64 { return 0 }

type dummyChain struct {
	counter int
}

// Engine retrieves the chain's rules engine.
func (d *dummyChain) Engine() rules.Engine {
	return nil
}

// GetHeader returns the hash corresponding to their hash.
func (d *dummyChain) GetHeader(h common.Hash, n uint64) (*types.Header, error) {
	d.counter++
	parentHash := common.Hash{}
	s := common.LeftPadBytes(new(big.Int).SetUint64(n-1).Bytes(), 32)
	copy(parentHash[:], s)

	//parentHash := common.Hash{byte(n - 1)}
	//fmt.Printf("GetHeader(%x, %d) => header with parent %x\n", h, n, parentHash)
	return fakeHeader(n, parentHash), nil
}

// TestBlockhash tests the blockhash operation. It's a bit special, since it internally
// requires access to a chain reader.
func TestBlockhash(t *testing.T) {
	t.Parallel()
	// Current head
	n := uint64(1000)
	parentHash := common.Hash{}
	s := common.LeftPadBytes(new(big.Int).SetUint64(n-1).Bytes(), 32)
	copy(parentHash[:], s)
	header := fakeHeader(n, parentHash)

	// This is the contract we're using. It requests the blockhash for current num (should be all zeroes),
	// then iteratively fetches all blockhashes back to n-260.
	// It returns
	// 1. the first (should be zero)
	// 2. the second (should be the parent hash)
	// 3. the last non-zero hash
	// By making the chain reader return hashes which correlate to the number, we can
	// verify that it obtained the right hashes where it should

	/*

		pragma solidity ^0.5.3;
		contract Hasher{

			function test() public view returns (bytes32, bytes32, bytes32){
				uint256 x = block.number;
				bytes32 first;
				bytes32 last;
				bytes32 zero;
				zero = blockhash(x); // Should be zeroes
				first = blockhash(x-1);
				for(uint256 i = 2 ; i < 260; i++){
					bytes32 hash = blockhash(x - i);
					if (uint256(hash) != 0){
						last = hash;
					}
				}
				return (zero, first, last);
			}
		}

	*/
	// The contract above
	data := common.Hex2Bytes("6080604052348015600f57600080fd5b50600436106045576000357c010000000000000000000000000000000000000000000000000000000090048063f8a8fd6d14604a575b600080fd5b60506074565b60405180848152602001838152602001828152602001935050505060405180910390f35b600080600080439050600080600083409050600184034092506000600290505b61010481101560c35760008186034090506000816001900414151560b6578093505b5080806001019150506094565b508083839650965096505050505090919256fea165627a7a72305820462d71b510c1725ff35946c20b415b0d50b468ea157c8c77dff9466c9cb85f560029")
	// The method call to 'test()'
	input := common.Hex2Bytes("f8a8fd6d")
	chain := &dummyChain{}
	cfg := &Config{
		GetHashFn:   protocol.GetHashFn(header, chain.GetHeader),
		BlockNumber: header.Number.Uint64(),
	}
	setDefaults(cfg)
	pragueTime := uint64(1)
	cfg.ChainConfig.PragueTime = &pragueTime
	ret, _, err := Execute(data, input, cfg, t.TempDir())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(ret) != 96 {
		t.Fatalf("expected returndata to be 96 bytes, got %d", len(ret))
	}

	zero := new(big.Int).SetBytes(ret[0:32])
	first := new(big.Int).SetBytes(ret[32:64])
	last := new(big.Int).SetBytes(ret[64:96])
	if zero.Sign() != 0 {
		t.Fatalf("expected zeroes, got %x", ret[0:32])
	}
	if first.Uint64() != 999 {
		t.Fatalf("second block should be 999, got %d (%x)", first, ret[32:64])
	}
	if last.Uint64() != 744 {
		t.Fatalf("last block should be 744, got %d (%x)", last, ret[64:96])
	}
	if exp, got := 255, chain.counter; exp != got {
		t.Errorf("suboptimal; too much chain iteration, expected %d, got %d", exp, got)
	}
}

// TestEip2929Cases contains various testcases that are used for
// EIP-2929 about gas repricings
func TestEip2929Cases(t *testing.T) {

	tmpdir := t.TempDir()
	id := 1
	prettyPrint := func(comment string, code []byte) {
		instrs := make([]string, 0)
		it := asm.NewInstructionIterator(code)
		for it.Next() {
			if it.Arg() != nil && 0 < len(it.Arg()) {
				instrs = append(instrs, fmt.Sprintf("%v 0x%x", it.Op(), it.Arg()))
			} else {
				instrs = append(instrs, fmt.Sprintf("%v", it.Op()))
			}
		}
		ops := strings.Join(instrs, ", ")
		//fmt.Printf("### Case %d\n\n", id)
		//fmt.Printf("%v\n\nBytecode: \n```\n0x%x\n```\nOperations: \n```\n%v\n```\n\n",
		//	comment,
		//	code, ops)
		_ = ops
		id++
		cfg := &Config{
			EVMConfig: vm.Config{
				Tracer:    logger.NewMarkdownLogger(nil, os.Stdout).Hooks(),
				ExtraEips: []int{2929},
			},
		}
		setDefaults(cfg)
		//nolint:errcheck
		Execute(code, nil, cfg, tmpdir)
	}

	{ // First eip testcase
		code := []byte{
			// Three checks against a precompile
			byte(vm.PUSH1), 1, byte(vm.EXTCODEHASH), byte(vm.POP),
			byte(vm.PUSH1), 2, byte(vm.EXTCODESIZE), byte(vm.POP),
			byte(vm.PUSH1), 3, byte(vm.BALANCE), byte(vm.POP),
			// Three checks against a non-precompile
			byte(vm.PUSH1), 0xf1, byte(vm.EXTCODEHASH), byte(vm.POP),
			byte(vm.PUSH1), 0xf2, byte(vm.EXTCODESIZE), byte(vm.POP),
			byte(vm.PUSH1), 0xf3, byte(vm.BALANCE), byte(vm.POP),
			// Same three checks (should be cheaper)
			byte(vm.PUSH1), 0xf2, byte(vm.EXTCODEHASH), byte(vm.POP),
			byte(vm.PUSH1), 0xf3, byte(vm.EXTCODESIZE), byte(vm.POP),
			byte(vm.PUSH1), 0xf1, byte(vm.BALANCE), byte(vm.POP),
			// Check the origin, and the 'this'
			byte(vm.ORIGIN), byte(vm.BALANCE), byte(vm.POP),
			byte(vm.ADDRESS), byte(vm.BALANCE), byte(vm.POP),

			byte(vm.STOP),
		}
		prettyPrint("This checks `EXT`(codehash,codesize,balance) of precompiles, which should be `100`, "+
			"and later checks the same operations twice against some non-precompiles. "+
			"Those are cheaper second time they are accessed. Lastly, it checks the `BALANCE` of `origin` and `this`.", code)
	}

	{ // EXTCODECOPY
		code := []byte{
			// extcodecopy( 0xff,0,0,0,0)
			byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, //length, codeoffset, memoffset
			byte(vm.PUSH1), 0xff, byte(vm.EXTCODECOPY),
			// extcodecopy( 0xff,0,0,0,0)
			byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, //length, codeoffset, memoffset
			byte(vm.PUSH1), 0xff, byte(vm.EXTCODECOPY),
			// extcodecopy( this,0,0,0,0)
			byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, //length, codeoffset, memoffset
			byte(vm.ADDRESS), byte(vm.EXTCODECOPY),

			byte(vm.STOP),
		}
		prettyPrint("This checks `extcodecopy( 0xff,0,0,0,0)` twice, (should be expensive first time), "+
			"and then does `extcodecopy( this,0,0,0,0)`.", code)
	}

	{ // SLOAD + SSTORE
		code := []byte{

			// Add slot `0x1` to access list
			byte(vm.PUSH1), 0x01, byte(vm.SLOAD), byte(vm.POP), // SLOAD( 0x1) (add to access list)
			// Write to `0x1` which is already in access list
			byte(vm.PUSH1), 0x11, byte(vm.PUSH1), 0x01, byte(vm.SSTORE), // SSTORE( loc: 0x01, val: 0x11)
			// Write to `0x2` which is not in access list
			byte(vm.PUSH1), 0x11, byte(vm.PUSH1), 0x02, byte(vm.SSTORE), // SSTORE( loc: 0x02, val: 0x11)
			// Write again to `0x2`
			byte(vm.PUSH1), 0x11, byte(vm.PUSH1), 0x02, byte(vm.SSTORE), // SSTORE( loc: 0x02, val: 0x11)
			// Read slot in access list (0x2)
			byte(vm.PUSH1), 0x02, byte(vm.SLOAD), // SLOAD( 0x2)
			// Read slot in access list (0x1)
			byte(vm.PUSH1), 0x01, byte(vm.SLOAD), // SLOAD( 0x1)
		}
		prettyPrint("This checks `sload( 0x1)` followed by `sstore(loc: 0x01, val:0x11)`, then 'naked' sstore:"+
			"`sstore(loc: 0x02, val:0x11)` twice, and `sload(0x2)`, `sload(0x1)`. ", code)
	}
	{ // Call variants
		code := []byte{
			// identity precompile
			byte(vm.PUSH1), 0x0, byte(vm.DUP1), byte(vm.DUP1), byte(vm.DUP1), byte(vm.DUP1),
			byte(vm.PUSH1), 0x04, byte(vm.PUSH1), 0x0, byte(vm.CALL), byte(vm.POP),

			// random account - call 1
			byte(vm.PUSH1), 0x0, byte(vm.DUP1), byte(vm.DUP1), byte(vm.DUP1), byte(vm.DUP1),
			byte(vm.PUSH1), 0xff, byte(vm.PUSH1), 0x0, byte(vm.CALL), byte(vm.POP),

			// random account - call 2
			byte(vm.PUSH1), 0x0, byte(vm.DUP1), byte(vm.DUP1), byte(vm.DUP1), byte(vm.DUP1),
			byte(vm.PUSH1), 0xff, byte(vm.PUSH1), 0x0, byte(vm.STATICCALL), byte(vm.POP),
		}
		prettyPrint("This calls the `identity`-precompile (cheap), then calls an account (expensive) and `staticcall`s the same"+
			"account (cheap)", code)
	}
}

// TestCreate2CollisionWithEIP7702Delegation verifies that CREATE2 to an address
// with an EIP-7702 delegation designator triggers ErrContractAddressCollision,
// matching geth's behavior. The delegation designator (0xef0100 ++ address) is
// non-empty code and must be treated as an occupied account.
// See https://github.com/ethereum-bounty/erigon/issues/2
func TestCreate2CollisionWithEIP7702Delegation(t *testing.T) {
	t.Parallel()

	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, domains := temporaltest.NewTestTxSD(t, db)
	statedb := state.New(state.NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})))
	defer statedb.Close()

	sender := accounts.InternAddress(common.HexToAddress("0x1234"))
	require.NoError(t, statedb.CreateAccount(sender, true))
	require.NoError(t, statedb.AddBalance(sender, *uint256.NewInt(1e18), tracing.BalanceChangeUnspecified))

	// Initcode that just returns empty runtime code (PUSH1 0, PUSH1 0, RETURN).
	initcode := []byte{byte(vm.PUSH1), 0, byte(vm.PUSH1), 0, byte(vm.RETURN)}

	// Compute the CREATE2 target address: keccak256(0xff ++ factory ++ salt ++ keccak256(initcode))[12:]
	salt := uint256.NewInt(0)
	factoryAddr := common.HexToAddress("0xfac0")
	create2Addr := types.CreateAddress2(factoryAddr, salt.Bytes32(), accounts.InternCodeHash(crypto.Keccak256Hash(initcode)))
	delegatedAddr := accounts.InternAddress(create2Addr)

	// Set an EIP-7702 delegation on the target address (points to some arbitrary empty account).
	delegationTarget := common.HexToAddress("0xdead")
	delegationCode := types.AddressToDelegation(accounts.InternAddress(delegationTarget))
	require.NoError(t, statedb.CreateAccount(delegatedAddr, true))
	require.NoError(t, statedb.SetCode(delegatedAddr, delegationCode, tracing.CodeChangeUnspecified))

	// Build a factory contract that executes CREATE2 with the initcode and salt=0.
	// The factory is placed at factoryAddr.
	factory := program.New()
	factory.Create2(initcode, salt)
	// Push the result to storage slot 0 for inspection.
	factory.Push(0).Op(vm.SSTORE)

	require.NoError(t, statedb.CreateAccount(accounts.InternAddress(factoryAddr), true))
	require.NoError(t, statedb.SetCode(accounts.InternAddress(factoryAddr), factory.Bytes(), tracing.CodeChangeUnspecified))

	cfg := &Config{
		State:  statedb,
		Origin: sender,
	}

	_, _, err := Call(accounts.InternAddress(factoryAddr), nil, cfg)
	require.NoError(t, err) // the CALL itself succeeds; CREATE2 failure is internal

	// The CREATE2 should have failed (collision), so the factory's SSTORE
	// should have stored the zero address (CREATE2 returns 0 on failure).
	val, err := statedb.GetState(accounts.InternAddress(factoryAddr), accounts.StorageKey{})
	require.NoError(t, err)
	require.True(t, val.IsZero(), "CREATE2 should have returned 0 (collision), but got %x", val)

	// Also verify that the delegation code on the target address is still intact.
	code, err := statedb.GetCode(delegatedAddr)
	require.NoError(t, err)
	require.Equal(t, delegationCode, code, "delegation code should be unchanged")
}

// TestCreateCollisionWithEIP7702Delegation verifies that CREATE (not just CREATE2)
// also collides with an EIP-7702 delegated account.
func TestCreateCollisionWithEIP7702Delegation(t *testing.T) {
	t.Parallel()

	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, domains := temporaltest.NewTestTxSD(t, db)
	statedb := state.New(state.NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})))
	defer statedb.Close()

	sender := accounts.InternAddress(common.HexToAddress("0x1234"))
	require.NoError(t, statedb.CreateAccount(sender, true))
	require.NoError(t, statedb.AddBalance(sender, *uint256.NewInt(1e18), tracing.BalanceChangeUnspecified))

	// Initcode that returns empty runtime code.
	initcode := []byte{byte(vm.PUSH1), 0, byte(vm.PUSH1), 0, byte(vm.RETURN)}

	// Compute the CREATE target address: keccak256(rlp([factory, nonce]))[12:]
	// Factory nonce starts at 1 (after SpuriousDragon sets it during create).
	// But for the factory contract already deployed, its nonce is 0 initially;
	// CREATE uses current nonce then increments. So target = CreateAddress(factory, 1)
	// because the factory itself has nonce=1 set by SpuriousDragon during deployment.
	// We'll use a simpler approach: just precompute and set delegation on the target.
	factoryAddr := common.HexToAddress("0xfac1")
	factoryAcct := accounts.InternAddress(factoryAddr)

	// Factory nonce will be 1 (set by SpuriousDragon on CreateAccount).
	// CREATE uses nonce of the calling contract. The factory already exists with nonce=0.
	// EVM increments nonce before CREATE, but here the factory's nonce is 0.
	// Actually in the EVM, CREATE does: target = CreateAddress(caller, callerNonce), then increments.
	// Since our factory is pre-deployed with nonce 0, CREATE target = CreateAddress(factoryAddr, 0).
	createAddr := types.CreateAddress(factoryAddr, 0)
	delegatedAddr := accounts.InternAddress(createAddr)

	// Set an EIP-7702 delegation on the target address.
	delegationTarget := common.HexToAddress("0xdead")
	delegationCode := types.AddressToDelegation(accounts.InternAddress(delegationTarget))
	require.NoError(t, statedb.CreateAccount(delegatedAddr, true))
	require.NoError(t, statedb.SetCode(delegatedAddr, delegationCode, tracing.CodeChangeUnspecified))

	// Build a factory that executes CREATE with the initcode.
	factory := program.New()
	factory.MstoreSmall(initcode, 0)
	factory.Push(len(initcode)). // size
					Push(32 - len(initcode)). // offset (right-aligned in the 32-byte word)
					Push(0).                  // value
					Op(vm.CREATE)
	factory.Push(0).Op(vm.SSTORE) // store result in slot 0

	require.NoError(t, statedb.CreateAccount(factoryAcct, true))
	require.NoError(t, statedb.SetCode(factoryAcct, factory.Bytes(), tracing.CodeChangeUnspecified))

	cfg := &Config{
		State:  statedb,
		Origin: sender,
	}

	_, _, err := Call(factoryAcct, nil, cfg)
	require.NoError(t, err)

	// CREATE should have failed (collision), returning 0.
	val, err := statedb.GetState(factoryAcct, accounts.StorageKey{})
	require.NoError(t, err)
	require.True(t, val.IsZero(), "CREATE should have returned 0 (collision), but got %x", val)
}

// TestGasTracingNoUnderflowOnStateGas verifies that the OnGasChange tracer
// callback receives correct (non-underflowing) gas values when an opcode
// charges state gas under EIP-8037 multi-dimensional gas (Amsterdam rules).
//
// The bug: the interpreter accumulated both execution and state dynamic gas into
// a single `cost` variable, then computed `gasCopy - cost` for the tracer
// callback. Because `gasCopy` only captured execution gas, the subtraction
// underflowed when state gas was non-zero (e.g. SSTORE creating a new slot).
func TestGasTracingNoUnderflowOnStateGas(t *testing.T) {
	t.Parallel()

	// Track all OnGasChange calls and check for underflow.
	type gasChange struct {
		oldGas uint64
		newGas uint64
		reason tracing.GasChangeReason
	}
	var gasChanges []gasChange

	hooks := &tracing.Hooks{
		OnGasChange: func(old, newGas uint64, reason tracing.GasChangeReason) {
			gasChanges = append(gasChanges, gasChange{old, newGas, reason})
			// The key invariant: new gas must never exceed old gas for a
			// consumption event (GasChangeCallOpCode). A uint64 underflow
			// would produce a very large value.
			if reason == tracing.GasChangeCallOpCode && newGas > old {
				t.Errorf("OnGasChange underflow: old=%d new=%d reason=%s", old, newGas, reason)
			}
		},
	}

	// Build bytecode: SSTORE(slot=0, value=1) then STOP.
	// Under Amsterdam with an empty slot this triggers state gas.
	code := []byte{
		byte(vm.PUSH1), 1, // value = 1
		byte(vm.PUSH1), 0, // slot = 0
		byte(vm.SSTORE), // creates new slot -> charges state gas
		byte(vm.STOP),
	}

	cfg := &Config{
		EVMConfig: vm.Config{Tracer: hooks},
		GasLimit:  10_000_000,
	}

	_, _, err := Execute(code, nil, cfg, t.TempDir())
	require.NoError(t, err)

	// Verify we actually observed at least one GasChangeCallOpCode event
	// (the SSTORE should have triggered it).
	found := false
	for _, gc := range gasChanges {
		if gc.reason == tracing.GasChangeCallOpCode {
			found = true
			break
		}
	}
	require.True(t, found, "expected at least one GasChangeCallOpCode event from SSTORE")
}

// TestSystemCallZeroValueSkipsTransferChecks verifies that a system call
// (caller = SystemAddress, value = 0) executes successfully without triggering
// CanTransfer or Transfer balance-change hooks on the caller. It also asserts:
//   - SYSTEM_ADDRESS was touched and exists after the call (positive check on the
//     caller-side empty-account creation for Gnosis/AuRa; see PR 5645, Issue 18276).
//   - SYSTEM_ADDRESS remains an empty account after the call.
//   - SYSTEM_ADDRESS is absent from the BAL produced by the call's tx IO.
//   - No balance-change tracer events fire for SYSTEM_ADDRESS as a result of
//     the zero-value transfer path.
func TestSystemCallZeroValueSkipsTransferChecks(t *testing.T) {
	t.Parallel()

	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, domains := temporaltest.NewTestTxSD(t, db)
	statedb := state.New(state.NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})))
	defer statedb.Close()

	systemAddr := params.SystemAddress
	target := accounts.InternAddress(common.HexToAddress("0xbeef"))

	// Deploy a trivial contract at the target that returns 0x42.
	require.NoError(t, statedb.CreateAccount(target, true))
	require.NoError(t, statedb.SetCode(target, []byte{
		byte(vm.PUSH1), 0x42,
		byte(vm.PUSH1), 0,
		byte(vm.MSTORE),
		byte(vm.PUSH1), 32,
		byte(vm.PUSH1), 0,
		byte(vm.RETURN),
	}, tracing.CodeChangeUnspecified))

	// Track balance-change events on SYSTEM_ADDRESS.
	type balChange struct {
		addr   accounts.Address
		oldBal uint256.Int
		newBal uint256.Int
		reason tracing.BalanceChangeReason
	}
	var balChanges []balChange

	hooks := &tracing.Hooks{
		OnBalanceChange: func(addr accounts.Address, prev, newBal uint256.Int, reason tracing.BalanceChangeReason) {
			if addr == systemAddr {
				balChanges = append(balChanges, balChange{addr, prev, newBal, reason})
			}
		},
	}

	cfg := &Config{
		State:     statedb,
		Origin:    systemAddr,
		EVMConfig: vm.Config{Tracer: hooks},
		GasLimit:  10_000_000,
	}
	setDefaults(cfg)

	vmenv := NewEnv(cfg)
	rules := vmenv.ChainRules()
	statedb.Prepare(rules, systemAddr, cfg.Coinbase, target, vm.ActivePrecompiles(rules), nil)

	ret, _, _, err := vmenv.Call(
		systemAddr,
		target,
		nil,
		mdgas.SplitTxnGasLimit(cfg.GasLimit, 0, rules),
		uint256.Int{}, // value = 0
		false,
	)
	require.NoError(t, err)

	// The contract should have returned 0x42.
	require.Equal(t, 32, len(ret))
	require.Equal(t, byte(0x42), ret[31])

	// Positive check: SYSTEM_ADDRESS must exist (Gnosis/AuRa invariant).
	exists, err := statedb.Exist(systemAddr)
	require.NoError(t, err)
	require.True(t, exists, "SYSTEM_ADDRESS should exist after a zero-value syscall")

	// SYSTEM_ADDRESS must remain empty after the touch.
	empty, err := statedb.Empty(systemAddr)
	require.NoError(t, err)
	require.True(t, empty, "SYSTEM_ADDRESS should remain empty after a zero-value syscall")

	// The call-level BAL must not include SYSTEM_ADDRESS when the syscall only
	// performs the sender-side touch and no actual account access.
	var io state.VersionedIO
	statedb.MergeTxIOInto(&io, statedb.VersionedWrites())
	bal := io.AsBlockAccessList()
	for _, accountChanges := range bal {
		require.NotEqual(t, systemAddr, accountChanges.Address,
			"SYSTEM_ADDRESS should be absent from the BAL after a zero-value syscall")
	}

	// No balance-change events should have fired for SYSTEM_ADDRESS
	// from the zero-value call path.
	require.Empty(t, balChanges, "no balance-change events expected for SYSTEM_ADDRESS on zero-value syscall, got %v", balChanges)
}

// LOG's BlockNumber comes from the EVM context. Entry points that do not go
// through SetTxContext leave the state's block number at zero, so the opcode
// must not source it from there.
func TestLogBlockNumberFromEVMContext(t *testing.T) {
	t.Parallel()

	cfg := &Config{BlockNumber: 42}
	_, ibs, err := Execute([]byte{
		byte(vm.PUSH1), 0,
		byte(vm.PUSH1), 0,
		byte(vm.LOG0),
	}, nil, cfg, t.TempDir())
	require.NoError(t, err)

	logs := ibs.GetRawLogs(0)
	require.Len(t, logs, 1)
	require.Equal(t, hexutil.Uint64(42), logs[0].BlockNumber)
}
