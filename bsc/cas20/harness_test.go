// Copyright 2026 The Erigon Authors
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
package cas20

import (
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

type testState struct {
	*ibsState
	txs map[common.Hash]int
}

func newTestStateDB(t testing.TB) *testState {
	t.Helper()
	ibs := state.New(state.NewNoopReader())
	t.Cleanup(ibs.Close)
	return &testState{ibsState: newIBSState(ibs), txs: map[common.Hash]int{}}
}

func (s *testState) GetCode(addr common.Address) []byte {
	code, err := s.ibs.GetCode(accounts.InternAddress(addr))
	if err != nil {
		panic(err)
	}
	return code
}

func (s *testState) GetRefund() uint64 { return s.ibs.GetRefund() }

func (s *testState) Logs() []*types.Log { return s.ibs.Logs() }

func (s *testState) Finalise(bool) {
	bc := cas20BlockContext(1)
	if err := s.ibs.FinalizeTx(bc.Rules(cas20TestChainConfig()), state.NewNoopWriter()); err != nil {
		panic(err)
	}
}

func cas20BlockContext(time uint64) evmtypes.BlockContext {
	return evmtypes.BlockContext{
		CanTransfer: func(evmtypes.IntraBlockState, accounts.Address, uint256.Int) (bool, error) { return true, nil },
		Transfer: func(evmtypes.IntraBlockState, accounts.Address, accounts.Address, uint256.Int, bool, *chain.Rules) error {
			return nil
		},
		BlockNumber: 1,
		Time:        time,
	}
}

func NewGasBudget(gas uint64) mdgas.MdGas { return mdgas.MdGas{Execution: gas} }

func cas20TestChainConfig() *chain.Config {
	return &chain.Config{
		ChainID:               uint256.NewInt(1),
		HomesteadBlock:        common.NewUint64(0),
		TangerineWhistleBlock: common.NewUint64(0),
		SpuriousDragonBlock:   common.NewUint64(0),
		ByzantiumBlock:        common.NewUint64(0),
		ConstantinopleBlock:   common.NewUint64(0),
		PetersburgBlock:       common.NewUint64(0),
		IstanbulBlock:         common.NewUint64(0),
		BerlinBlock:           common.NewUint64(0),
		LondonBlock:           common.NewUint64(0),
		ShanghaiTime:          common.NewUint64(0),
		CancunTime:            common.NewUint64(0),
		PragueTime:            common.NewUint64(0),
		OsakaTime:             common.NewUint64(0),
		JennerTime:            common.NewUint64(0),
		Parlia:                &chain.ParliaConfig{},
	}
}

func cas20Addr(variant, id byte) common.Address {
	var a common.Address
	a[0], a[1] = cas20MarkerPrefix[0], cas20MarkerPrefix[1]
	a[10] = variant
	a[19] = id
	return a
}

func cas20Call(sel [4]byte, args ...common.Hash) []byte {
	out := append([]byte{}, sel[:]...)
	for _, a := range args {
		out = append(out, a[:]...)
	}
	return out
}

func TestMain(m *testing.M) {
	vm.SetStatefulPrecompiles(Enabled, Resolve)
	os.Exit(m.Run())
}

// SetTxContext gives every transaction hash a block position of its own, since
// Erigon keys logs by position, not by hash.
func (s *testState) SetTxContext(txHash common.Hash, _ int) {
	i, ok := s.txs[txHash]
	if !ok {
		i = len(s.txs) + 1
		s.txs[txHash] = i
	}
	s.ibs.SetTxContext(1, i)
}

func (s *testState) GetLogs(txHash common.Hash, blockNumber uint64, blockHash common.Hash, _ uint64) []*types.Log {
	return s.ibs.GetLogs(s.txs[txHash], txHash, blockNumber, blockHash)
}

func (s *testState) Exist(addr common.Address) bool {
	ok, err := s.ibs.Exist(accounts.InternAddress(addr))
	if err != nil {
		panic(err)
	}
	return ok
}

func seedCAS20Activation(s StateDB) {
	seedCAS20Sentinel(s, CAS20ActivationRegistryAddress)
	seedCAS20Sentinel(s, CAS20PolicyRegistryAddress)
}

type testEVM struct{ *vm.EVM }

func newTestEVM(bc evmtypes.BlockContext, s *testState, cfg *chain.Config) *testEVM {
	var ibs *state.IntraBlockState
	if s != nil {
		ibs = s.ibs
	}
	return &testEVM{vm.NewEVM(bc, evmtypes.TxContext{}, ibs, cfg, vm.Config{})}
}

func (e *testEVM) SetPrecompiles(set map[common.Address]cas20Precompile) {
	out := make(vm.PrecompiledContracts, len(set))
	for addr, p := range set {
		out[accounts.InternAddress(addr)] = vmPrecompile{p}
	}
	e.EVM.SetPrecompiles(out)
}

func u256Value(v *uint256.Int) uint256.Int {
	if v == nil {
		return uint256.Int{}
	}
	return *v
}

func (e *testEVM) Call(caller, to common.Address, input []byte, gas mdgas.MdGas, value *uint256.Int) ([]byte, mdgas.MdGas, error) {
	ret, left, _, err := e.EVM.Call(accounts.InternAddress(caller), accounts.InternAddress(to), input, gas, u256Value(value), false)
	return ret, left, err
}

func (e *testEVM) StaticCall(caller, to common.Address, input []byte, gas mdgas.MdGas) ([]byte, mdgas.MdGas, error) {
	ret, left, _, err := e.EVM.StaticCall(accounts.InternAddress(caller), accounts.InternAddress(to), input, gas)
	return ret, left, err
}

func (e *testEVM) CallCode(caller, to common.Address, input []byte, gas mdgas.MdGas, value *uint256.Int) ([]byte, mdgas.MdGas, error) {
	ret, left, _, err := e.EVM.CallCode(accounts.InternAddress(caller), accounts.InternAddress(to), input, gas, u256Value(value))
	return ret, left, err
}

func (e *testEVM) DelegateCall(origin, caller, to common.Address, input []byte, gas mdgas.MdGas, value *uint256.Int) ([]byte, mdgas.MdGas, error) {
	ret, left, _, err := e.EVM.DelegateCall(accounts.InternAddress(caller), accounts.InternAddress(origin), accounts.InternAddress(to), input, u256Value(value), gas)
	return ret, left, err
}

// IntermediateRoot stands in for a state root: a digest of every account and
// non-zero slot the block has written so far.
func (s *testState) IntermediateRoot(bool) common.Hash {
	d := digestWriter{}
	bc := cas20BlockContext(1)
	if err := s.ibs.MakeWriteSet(bc.Rules(cas20TestChainConfig()), d); err != nil {
		panic(err)
	}
	keys := make([]string, 0, len(d))
	for k := range d {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buf []byte
	for _, k := range keys {
		buf = append(buf, k...)
		buf = append(buf, d[k]...)
	}
	return keccakHash(buf)
}

type digestWriter map[string]string

func (d digestWriter) UpdateAccountData(addr accounts.Address, _, acc *accounts.Account) error {
	d["a"+addr.String()] = fmt.Sprintf("%d/%s/%s/%d", acc.Nonce, acc.Balance.String(), acc.CodeHash.String(), acc.Incarnation)
	return nil
}

func (d digestWriter) UpdateAccountCode(addr accounts.Address, _ uint64, codeHash accounts.CodeHash, _ []byte) error {
	d["c"+addr.String()] = codeHash.String()
	return nil
}

func (d digestWriter) DeleteAccount(addr accounts.Address, _ *accounts.Account) error {
	d["a"+addr.String()] = "deleted"
	return nil
}

func (d digestWriter) WriteAccountStorage(addr accounts.Address, _ uint64, key accounts.StorageKey, _, value uint256.Int) error {
	k := "s" + addr.String() + key.String()
	if value.IsZero() {
		delete(d, k)
		return nil
	}
	d[k] = value.Hex()
	return nil
}

func (d digestWriter) CreateContract(accounts.Address) error { return nil }

// run calls f on the state and then reverts it, which is what the upstream tests
// get from running f on a copy.
func (s *testState) run(f func()) {
	snap := s.ibs.PushSnapshot()
	defer s.ibs.PopSnapshot(snap)
	f()
	s.ibs.RevertToSnapshot(snap, nil)
}

func (e *testEVM) cas20Enabled() bool { return Enabled(e.ChainRules()) }

func (e *testEVM) precompile(addr common.Address) (vm.StatefulPrecompile, bool) {
	if !Enabled(e.ChainRules()) {
		return nil, false
	}
	return Resolve(accounts.InternAddress(addr))
}

func runStatefulPrecompiledContract(evm *testEVM, p cas20Precompile, caller, self common.Address, input []byte, gas mdgas.MdGas, readOnly, directCall bool, value *uint256.Int) ([]byte, mdgas.MdGas, error) {
	env := vm.PrecompileEnv{
		EVM:        evm.EVM,
		Self:       accounts.InternAddress(self),
		Caller:     accounts.InternAddress(caller),
		Value:      u256Value(value),
		Gas:        &gas,
		ReadOnly:   readOnly,
		DirectCall: directCall,
	}
	ret, err := vmPrecompile{p}.RunStateful(&env, input)
	return ret, gas, err
}

func seedCAS20Sentinel(s StateDB, addr common.Address) {
	if hadNoCode(s, addr) {
		s.SetCode(addr, CAS20MarkerCode, tracing.CodeChangeUnspecified)
	}
}
