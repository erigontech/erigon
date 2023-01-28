// Code generated - DO NOT EDIT.
// This file is a generated binding and any manual changes will be lost.

package contracts

import (
	"math/big"
	"strings"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	ethereum "github.com/ledgerwatch/erigon"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/event"
)

// Reference imports to suppress errors if they are not otherwise used.
var (
	_ = big.NewInt
	_ = strings.NewReader
	_ = ethereum.NotFound
	_ = bind.Bind
	_ = common.Big1
	_ = types.BloomLookup
	_ = event.NewSubscription
)

// TestcontractABI is the input ABI used to generate the binding from.
const TestcontractABI = "[{\"inputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"constructor\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"name\":\"balances\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"newBalance\",\"type\":\"uint256\"}],\"name\":\"create\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"newBalance\",\"type\":\"uint256\"}],\"name\":\"createAndException\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"newBalance\",\"type\":\"uint256\"}],\"name\":\"createAndRevert\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"remove\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"removeAndException\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"removeAndRevert\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"newBalance\",\"type\":\"uint256\"}],\"name\":\"update\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"newBalance\",\"type\":\"uint256\"}],\"name\":\"updateAndException\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"newBalance\",\"type\":\"uint256\"}],\"name\":\"updateAndRevert\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"}]"

// TestcontractBin is the compiled bytecode used for deploying new contracts.
var TestcontractBin = "0x608060405234801561001057600080fd5b5033600090815260208190526040902060649055610208806100336000396000f3fe608060405234801561001057600080fd5b506004361061009e5760003560e01c8063c2ce0ef711610066578063c2ce0ef71461010a578063c53e5ae314610127578063cb946a0714610127578063d592ed1f14610144578063f64c050d1461010a5761009e565b806327e235e3146100a3578063660cc200146100db578063780900dc146100e557806382ab890a146100e5578063a7f4377914610102575b600080fd5b6100c9600480360360208110156100b957600080fd5b50356001600160a01b031661014c565b60408051918252519081900360200190f35b6100e361015e565b005b6100e3600480360360208110156100fb57600080fd5b5035610170565b6100e3610182565b6100e36004803603602081101561012057600080fd5b5035610194565b6100e36004803603602081101561013d57600080fd5b50356101a8565b6100e36101bd565b60006020819052908152604090205481565b33600090815260208190526040812055fe5b33600090815260208190526040902055565b33600090815260208190526040812055565b336000908152602081905260409020819055fe5b33600090815260208190526040812082905580fd5b33600090815260208190526040812081905580fdfea2646970667358221220c40698b47133056d15d4a84d769c07a1f24008b50347105be970e1f0191ca44f64736f6c63430007020033"

// DeployTestcontract deploys a new Ethereum contract, binding an instance of Testcontract to it.
func DeployTestcontract(auth *bind.TransactOpts, backend bind.ContractBackend) (libcommon.Address, types.Transaction, *Testcontract, error) {
	parsed, err := abi.JSON(strings.NewReader(TestcontractABI))
	if err != nil {
		return libcommon.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(TestcontractBin), backend)
	if err != nil {
		return libcommon.Address{}, nil, nil, err
	}
	return address, tx, &Testcontract{TestcontractCaller: TestcontractCaller{contract: contract}, TestcontractTransactor: TestcontractTransactor{contract: contract}, TestcontractFilterer: TestcontractFilterer{contract: contract}}, nil
}

// Testcontract is an auto generated Go binding around an Ethereum contract.
type Testcontract struct {
	TestcontractCaller     // Read-only binding to the contract
	TestcontractTransactor // Write-only binding to the contract
	TestcontractFilterer   // Log filterer for contract events
}

// TestcontractCaller is an auto generated read-only Go binding around an Ethereum contract.
type TestcontractCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// TestcontractTransactor is an auto generated write-only Go binding around an Ethereum contract.
type TestcontractTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// TestcontractFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type TestcontractFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// TestcontractSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type TestcontractSession struct {
	Contract     *Testcontract     // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// TestcontractCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type TestcontractCallerSession struct {
	Contract *TestcontractCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts       // Call options to use throughout this session
}

// TestcontractTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type TestcontractTransactorSession struct {
	Contract     *TestcontractTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts       // Transaction auth options to use throughout this session
}

// TestcontractRaw is an auto generated low-level Go binding around an Ethereum contract.
type TestcontractRaw struct {
	Contract *Testcontract // Generic contract binding to access the raw methods on
}

// TestcontractCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type TestcontractCallerRaw struct {
	Contract *TestcontractCaller // Generic read-only contract binding to access the raw methods on
}

// TestcontractTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type TestcontractTransactorRaw struct {
	Contract *TestcontractTransactor // Generic write-only contract binding to access the raw methods on
}

// NewTestcontract creates a new instance of Testcontract, bound to a specific deployed contract.
func NewTestcontract(address libcommon.Address, backend bind.ContractBackend) (*Testcontract, error) {
	contract, err := bindTestcontract(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Testcontract{TestcontractCaller: TestcontractCaller{contract: contract}, TestcontractTransactor: TestcontractTransactor{contract: contract}, TestcontractFilterer: TestcontractFilterer{contract: contract}}, nil
}

// NewTestcontractCaller creates a new read-only instance of Testcontract, bound to a specific deployed contract.
func NewTestcontractCaller(address libcommon.Address, caller bind.ContractCaller) (*TestcontractCaller, error) {
	contract, err := bindTestcontract(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &TestcontractCaller{contract: contract}, nil
}

// NewTestcontractTransactor creates a new write-only instance of Testcontract, bound to a specific deployed contract.
func NewTestcontractTransactor(address libcommon.Address, transactor bind.ContractTransactor) (*TestcontractTransactor, error) {
	contract, err := bindTestcontract(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &TestcontractTransactor{contract: contract}, nil
}

// NewTestcontractFilterer creates a new log filterer instance of Testcontract, bound to a specific deployed contract.
func NewTestcontractFilterer(address libcommon.Address, filterer bind.ContractFilterer) (*TestcontractFilterer, error) {
	contract, err := bindTestcontract(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &TestcontractFilterer{contract: contract}, nil
}

// bindTestcontract binds a generic wrapper to an already deployed contract.
func bindTestcontract(address libcommon.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(TestcontractABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Testcontract *TestcontractRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Testcontract.Contract.TestcontractCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Testcontract *TestcontractRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _Testcontract.Contract.TestcontractTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Testcontract *TestcontractRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _Testcontract.Contract.TestcontractTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Testcontract *TestcontractCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Testcontract.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Testcontract *TestcontractTransactorRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _Testcontract.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Testcontract *TestcontractTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _Testcontract.Contract.contract.Transact(opts, method, params...)
}

// Balances is a free data retrieval call binding the contract method 0x27e235e3.
//
// Solidity: function balances(address ) view returns(uint256)
func (_Testcontract *TestcontractCaller) Balances(opts *bind.CallOpts, arg0 libcommon.Address) (*big.Int, error) {
	var out []interface{}
	err := _Testcontract.contract.Call(opts, &out, "balances", arg0)

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// Balances is a free data retrieval call binding the contract method 0x27e235e3.
//
// Solidity: function balances(address ) view returns(uint256)
func (_Testcontract *TestcontractSession) Balances(arg0 libcommon.Address) (*big.Int, error) {
	return _Testcontract.Contract.Balances(&_Testcontract.CallOpts, arg0)
}

// Balances is a free data retrieval call binding the contract method 0x27e235e3.
//
// Solidity: function balances(address ) view returns(uint256)
func (_Testcontract *TestcontractCallerSession) Balances(arg0 libcommon.Address) (*big.Int, error) {
	return _Testcontract.Contract.Balances(&_Testcontract.CallOpts, arg0)
}

// Create is a paid mutator transaction binding the contract method 0x780900dc.
//
// Solidity: function create(uint256 newBalance) returns()
func (_Testcontract *TestcontractTransactor) Create(opts *bind.TransactOpts, newBalance *big.Int) (types.Transaction, error) {
	return _Testcontract.contract.Transact(opts, "create", newBalance)
}

// Create is a paid mutator transaction binding the contract method 0x780900dc.
//
// Solidity: function create(uint256 newBalance) returns()
func (_Testcontract *TestcontractSession) Create(newBalance *big.Int) (types.Transaction, error) {
	return _Testcontract.Contract.Create(&_Testcontract.TransactOpts, newBalance)
}

// Create is a paid mutator transaction binding the contract method 0x780900dc.
//
// Solidity: function create(uint256 newBalance) returns()
func (_Testcontract *TestcontractTransactorSession) Create(newBalance *big.Int) (types.Transaction, error) {
	return _Testcontract.Contract.Create(&_Testcontract.TransactOpts, newBalance)
}

// CreateAndException is a paid mutator transaction binding the contract method 0xc2ce0ef7.
//
// Solidity: function createAndException(uint256 newBalance) returns()
func (_Testcontract *TestcontractTransactor) CreateAndException(opts *bind.TransactOpts, newBalance *big.Int) (types.Transaction, error) {
	return _Testcontract.contract.Transact(opts, "createAndException", newBalance)
}

// CreateAndException is a paid mutator transaction binding the contract method 0xc2ce0ef7.
//
// Solidity: function createAndException(uint256 newBalance) returns()
func (_Testcontract *TestcontractSession) CreateAndException(newBalance *big.Int) (types.Transaction, error) {
	return _Testcontract.Contract.CreateAndException(&_Testcontract.TransactOpts, newBalance)
}

// CreateAndException is a paid mutator transaction binding the contract method 0xc2ce0ef7.
//
// Solidity: function createAndException(uint256 newBalance) returns()
func (_Testcontract *TestcontractTransactorSession) CreateAndException(newBalance *big.Int) (types.Transaction, error) {
	return _Testcontract.Contract.CreateAndException(&_Testcontract.TransactOpts, newBalance)
}

// CreateAndRevert is a paid mutator transaction binding the contract method 0xc53e5ae3.
//
// Solidity: function createAndRevert(uint256 newBalance) returns()
func (_Testcontract *TestcontractTransactor) CreateAndRevert(opts *bind.TransactOpts, newBalance *big.Int) (types.Transaction, error) {
	return _Testcontract.contract.Transact(opts, "createAndRevert", newBalance)
}

// CreateAndRevert is a paid mutator transaction binding the contract method 0xc53e5ae3.
//
// Solidity: function createAndRevert(uint256 newBalance) returns()
func (_Testcontract *TestcontractSession) CreateAndRevert(newBalance *big.Int) (types.Transaction, error) {
	return _Testcontract.Contract.CreateAndRevert(&_Testcontract.TransactOpts, newBalance)
}

// CreateAndRevert is a paid mutator transaction binding the contract method 0xc53e5ae3.
//
// Solidity: function createAndRevert(uint256 newBalance) returns()
func (_Testcontract *TestcontractTransactorSession) CreateAndRevert(newBalance *big.Int) (types.Transaction, error) {
	return _Testcontract.Contract.CreateAndRevert(&_Testcontract.TransactOpts, newBalance)
}

// Remove is a paid mutator transaction binding the contract method 0xa7f43779.
//
// Solidity: function remove() returns()
func (_Testcontract *TestcontractTransactor) Remove(opts *bind.TransactOpts) (types.Transaction, error) {
	return _Testcontract.contract.Transact(opts, "remove")
}

// Remove is a paid mutator transaction binding the contract method 0xa7f43779.
//
// Solidity: function remove() returns()
func (_Testcontract *TestcontractSession) Remove() (types.Transaction, error) {
	return _Testcontract.Contract.Remove(&_Testcontract.TransactOpts)
}

// Remove is a paid mutator transaction binding the contract method 0xa7f43779.
//
// Solidity: function remove() returns()
func (_Testcontract *TestcontractTransactorSession) Remove() (types.Transaction, error) {
	return _Testcontract.Contract.Remove(&_Testcontract.TransactOpts)
}

// RemoveAndException is a paid mutator transaction binding the contract method 0x660cc200.
//
// Solidity: function removeAndException() returns()
func (_Testcontract *TestcontractTransactor) RemoveAndException(opts *bind.TransactOpts) (types.Transaction, error) {
	return _Testcontract.contract.Transact(opts, "removeAndException")
}

// RemoveAndException is a paid mutator transaction binding the contract method 0x660cc200.
//
// Solidity: function removeAndException() returns()
func (_Testcontract *TestcontractSession) RemoveAndException() (types.Transaction, error) {
	return _Testcontract.Contract.RemoveAndException(&_Testcontract.TransactOpts)
}

// RemoveAndException is a paid mutator transaction binding the contract method 0x660cc200.
//
// Solidity: function removeAndException() returns()
func (_Testcontract *TestcontractTransactorSession) RemoveAndException() (types.Transaction, error) {
	return _Testcontract.Contract.RemoveAndException(&_Testcontract.TransactOpts)
}

// RemoveAndRevert is a paid mutator transaction binding the contract method 0xd592ed1f.
//
// Solidity: function removeAndRevert() returns()
func (_Testcontract *TestcontractTransactor) RemoveAndRevert(opts *bind.TransactOpts) (types.Transaction, error) {
	return _Testcontract.contract.Transact(opts, "removeAndRevert")
}

// RemoveAndRevert is a paid mutator transaction binding the contract method 0xd592ed1f.
//
// Solidity: function removeAndRevert() returns()
func (_Testcontract *TestcontractSession) RemoveAndRevert() (types.Transaction, error) {
	return _Testcontract.Contract.RemoveAndRevert(&_Testcontract.TransactOpts)
}

// RemoveAndRevert is a paid mutator transaction binding the contract method 0xd592ed1f.
//
// Solidity: function removeAndRevert() returns()
func (_Testcontract *TestcontractTransactorSession) RemoveAndRevert() (types.Transaction, error) {
	return _Testcontract.Contract.RemoveAndRevert(&_Testcontract.TransactOpts)
}

// Update is a paid mutator transaction binding the contract method 0x82ab890a.
//
// Solidity: function update(uint256 newBalance) returns()
func (_Testcontract *TestcontractTransactor) Update(opts *bind.TransactOpts, newBalance *big.Int) (types.Transaction, error) {
	return _Testcontract.contract.Transact(opts, "update", newBalance)
}

// Update is a paid mutator transaction binding the contract method 0x82ab890a.
//
// Solidity: function update(uint256 newBalance) returns()
func (_Testcontract *TestcontractSession) Update(newBalance *big.Int) (types.Transaction, error) {
	return _Testcontract.Contract.Update(&_Testcontract.TransactOpts, newBalance)
}

// Update is a paid mutator transaction binding the contract method 0x82ab890a.
//
// Solidity: function update(uint256 newBalance) returns()
func (_Testcontract *TestcontractTransactorSession) Update(newBalance *big.Int) (types.Transaction, error) {
	return _Testcontract.Contract.Update(&_Testcontract.TransactOpts, newBalance)
}

// UpdateAndException is a paid mutator transaction binding the contract method 0xf64c050d.
//
// Solidity: function updateAndException(uint256 newBalance) returns()
func (_Testcontract *TestcontractTransactor) UpdateAndException(opts *bind.TransactOpts, newBalance *big.Int) (types.Transaction, error) {
	return _Testcontract.contract.Transact(opts, "updateAndException", newBalance)
}

// UpdateAndException is a paid mutator transaction binding the contract method 0xf64c050d.
//
// Solidity: function updateAndException(uint256 newBalance) returns()
func (_Testcontract *TestcontractSession) UpdateAndException(newBalance *big.Int) (types.Transaction, error) {
	return _Testcontract.Contract.UpdateAndException(&_Testcontract.TransactOpts, newBalance)
}

// UpdateAndException is a paid mutator transaction binding the contract method 0xf64c050d.
//
// Solidity: function updateAndException(uint256 newBalance) returns()
func (_Testcontract *TestcontractTransactorSession) UpdateAndException(newBalance *big.Int) (types.Transaction, error) {
	return _Testcontract.Contract.UpdateAndException(&_Testcontract.TransactOpts, newBalance)
}

// UpdateAndRevert is a paid mutator transaction binding the contract method 0xcb946a07.
//
// Solidity: function updateAndRevert(uint256 newBalance) returns()
func (_Testcontract *TestcontractTransactor) UpdateAndRevert(opts *bind.TransactOpts, newBalance *big.Int) (types.Transaction, error) {
	return _Testcontract.contract.Transact(opts, "updateAndRevert", newBalance)
}

// UpdateAndRevert is a paid mutator transaction binding the contract method 0xcb946a07.
//
// Solidity: function updateAndRevert(uint256 newBalance) returns()
func (_Testcontract *TestcontractSession) UpdateAndRevert(newBalance *big.Int) (types.Transaction, error) {
	return _Testcontract.Contract.UpdateAndRevert(&_Testcontract.TransactOpts, newBalance)
}

// UpdateAndRevert is a paid mutator transaction binding the contract method 0xcb946a07.
//
// Solidity: function updateAndRevert(uint256 newBalance) returns()
func (_Testcontract *TestcontractTransactorSession) UpdateAndRevert(newBalance *big.Int) (types.Transaction, error) {
	return _Testcontract.Contract.UpdateAndRevert(&_Testcontract.TransactOpts, newBalance)
}
