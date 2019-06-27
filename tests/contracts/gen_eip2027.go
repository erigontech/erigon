// Code generated - DO NOT EDIT.
// This file is a generated binding and any manual changes will be lost.

package contracts

import (
	"math/big"
	"strings"

	ethereum "github.com/ledgerwatch/turbo-geth"
	"github.com/ledgerwatch/turbo-geth/accounts/abi"
	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/event"
)

// Reference imports to suppress errors if they are not otherwise used.
var (
	_ = big.NewInt
	_ = strings.NewReader
	_ = ethereum.NotFound
	_ = abi.U256
	_ = bind.Bind
	_ = common.Big1
	_ = types.BloomLookup
	_ = event.NewSubscription
)

// Eip2027ABI is the input ABI used to generate the binding from.
const Eip2027ABI = "[{\"constant\":true,\"inputs\":[{\"name\":\"\",\"type\":\"address\"}],\"name\":\"balances\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"newBalance\",\"type\":\"uint256\"}],\"name\":\"update\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"constant\":false,\"inputs\":[],\"name\":\"remove\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"constant\":false,\"inputs\":[],\"name\":\"create\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"}]"

// Eip2027Bin is the compiled bytecode used for deploying new contracts.
const Eip2027Bin = `608060405234801561001057600080fd5b50610208806100206000396000f3fe608060405234801561001057600080fd5b506004361061004c5760003560e01c806327e235e31461005157806382ab890a146100a9578063a7f43779146100d7578063efc81a8c146100e1575b600080fd5b6100936004803603602081101561006757600080fd5b81019080803573ffffffffffffffffffffffffffffffffffffffff1690602001909291905050506100eb565b6040518082815260200191505060405180910390f35b6100d5600480360360208110156100bf57600080fd5b8101908080359060200190929190505050610103565b005b6100df610149565b005b6100e961018d565b005b60006020528060005260406000206000915090505481565b806000803373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019081526020016000208190555050565b6000803373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060009055565b60016000803373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019081526020016000208190555056fea265627a7a72305820c1517e1404142ff30939a7520083f658bc6459eb3b5382e03af91d670e49897264736f6c63430005090032`

// DeployEip2027 deploys a new Ethereum contract, binding an instance of Eip2027 to it.
func DeployEip2027(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *Eip2027, error) {
	parsed, err := abi.JSON(strings.NewReader(Eip2027ABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(Eip2027Bin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &Eip2027{Eip2027Caller: Eip2027Caller{contract: contract}, Eip2027Transactor: Eip2027Transactor{contract: contract}, Eip2027Filterer: Eip2027Filterer{contract: contract}}, nil
}

// Eip2027 is an auto generated Go binding around an Ethereum contract.
type Eip2027 struct {
	Eip2027Caller     // Read-only binding to the contract
	Eip2027Transactor // Write-only binding to the contract
	Eip2027Filterer   // Log filterer for contract events
}

// Eip2027Caller is an auto generated read-only Go binding around an Ethereum contract.
type Eip2027Caller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// Eip2027Transactor is an auto generated write-only Go binding around an Ethereum contract.
type Eip2027Transactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// Eip2027Filterer is an auto generated log filtering Go binding around an Ethereum contract events.
type Eip2027Filterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// Eip2027Session is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type Eip2027Session struct {
	Contract     *Eip2027          // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// Eip2027CallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type Eip2027CallerSession struct {
	Contract *Eip2027Caller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts  // Call options to use throughout this session
}

// Eip2027TransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type Eip2027TransactorSession struct {
	Contract     *Eip2027Transactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts  // Transaction auth options to use throughout this session
}

// Eip2027Raw is an auto generated low-level Go binding around an Ethereum contract.
type Eip2027Raw struct {
	Contract *Eip2027 // Generic contract binding to access the raw methods on
}

// Eip2027CallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type Eip2027CallerRaw struct {
	Contract *Eip2027Caller // Generic read-only contract binding to access the raw methods on
}

// Eip2027TransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type Eip2027TransactorRaw struct {
	Contract *Eip2027Transactor // Generic write-only contract binding to access the raw methods on
}

// NewEip2027 creates a new instance of Eip2027, bound to a specific deployed contract.
func NewEip2027(address common.Address, backend bind.ContractBackend) (*Eip2027, error) {
	contract, err := bindEip2027(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Eip2027{Eip2027Caller: Eip2027Caller{contract: contract}, Eip2027Transactor: Eip2027Transactor{contract: contract}, Eip2027Filterer: Eip2027Filterer{contract: contract}}, nil
}

// NewEip2027Caller creates a new read-only instance of Eip2027, bound to a specific deployed contract.
func NewEip2027Caller(address common.Address, caller bind.ContractCaller) (*Eip2027Caller, error) {
	contract, err := bindEip2027(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &Eip2027Caller{contract: contract}, nil
}

// NewEip2027Transactor creates a new write-only instance of Eip2027, bound to a specific deployed contract.
func NewEip2027Transactor(address common.Address, transactor bind.ContractTransactor) (*Eip2027Transactor, error) {
	contract, err := bindEip2027(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &Eip2027Transactor{contract: contract}, nil
}

// NewEip2027Filterer creates a new log filterer instance of Eip2027, bound to a specific deployed contract.
func NewEip2027Filterer(address common.Address, filterer bind.ContractFilterer) (*Eip2027Filterer, error) {
	contract, err := bindEip2027(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &Eip2027Filterer{contract: contract}, nil
}

// bindEip2027 binds a generic wrapper to an already deployed contract.
func bindEip2027(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(Eip2027ABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Eip2027 *Eip2027Raw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Eip2027.Contract.Eip2027Caller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Eip2027 *Eip2027Raw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Eip2027.Contract.Eip2027Transactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Eip2027 *Eip2027Raw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Eip2027.Contract.Eip2027Transactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Eip2027 *Eip2027CallerRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Eip2027.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Eip2027 *Eip2027TransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Eip2027.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Eip2027 *Eip2027TransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Eip2027.Contract.contract.Transact(opts, method, params...)
}

// Balances is a free data retrieval call binding the contract method 0x27e235e3.
//
// Solidity: function balances(address ) constant returns(uint256)
func (_Eip2027 *Eip2027Caller) Balances(opts *bind.CallOpts, arg0 common.Address) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _Eip2027.contract.Call(opts, out, "balances", arg0)
	return *ret0, err
}

// Balances is a free data retrieval call binding the contract method 0x27e235e3.
//
// Solidity: function balances(address ) constant returns(uint256)
func (_Eip2027 *Eip2027Session) Balances(arg0 common.Address) (*big.Int, error) {
	return _Eip2027.Contract.Balances(&_Eip2027.CallOpts, arg0)
}

// Balances is a free data retrieval call binding the contract method 0x27e235e3.
//
// Solidity: function balances(address ) constant returns(uint256)
func (_Eip2027 *Eip2027CallerSession) Balances(arg0 common.Address) (*big.Int, error) {
	return _Eip2027.Contract.Balances(&_Eip2027.CallOpts, arg0)
}

// Create is a paid mutator transaction binding the contract method 0xefc81a8c.
//
// Solidity: function create() returns()
func (_Eip2027 *Eip2027Transactor) Create(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Eip2027.contract.Transact(opts, "create")
}

// Create is a paid mutator transaction binding the contract method 0xefc81a8c.
//
// Solidity: function create() returns()
func (_Eip2027 *Eip2027Session) Create() (*types.Transaction, error) {
	return _Eip2027.Contract.Create(&_Eip2027.TransactOpts)
}

// Create is a paid mutator transaction binding the contract method 0xefc81a8c.
//
// Solidity: function create() returns()
func (_Eip2027 *Eip2027TransactorSession) Create() (*types.Transaction, error) {
	return _Eip2027.Contract.Create(&_Eip2027.TransactOpts)
}

// Remove is a paid mutator transaction binding the contract method 0xa7f43779.
//
// Solidity: function remove() returns()
func (_Eip2027 *Eip2027Transactor) Remove(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Eip2027.contract.Transact(opts, "remove")
}

// Remove is a paid mutator transaction binding the contract method 0xa7f43779.
//
// Solidity: function remove() returns()
func (_Eip2027 *Eip2027Session) Remove() (*types.Transaction, error) {
	return _Eip2027.Contract.Remove(&_Eip2027.TransactOpts)
}

// Remove is a paid mutator transaction binding the contract method 0xa7f43779.
//
// Solidity: function remove() returns()
func (_Eip2027 *Eip2027TransactorSession) Remove() (*types.Transaction, error) {
	return _Eip2027.Contract.Remove(&_Eip2027.TransactOpts)
}

// Update is a paid mutator transaction binding the contract method 0x82ab890a.
//
// Solidity: function update(uint256 newBalance) returns()
func (_Eip2027 *Eip2027Transactor) Update(opts *bind.TransactOpts, newBalance *big.Int) (*types.Transaction, error) {
	return _Eip2027.contract.Transact(opts, "update", newBalance)
}

// Update is a paid mutator transaction binding the contract method 0x82ab890a.
//
// Solidity: function update(uint256 newBalance) returns()
func (_Eip2027 *Eip2027Session) Update(newBalance *big.Int) (*types.Transaction, error) {
	return _Eip2027.Contract.Update(&_Eip2027.TransactOpts, newBalance)
}

// Update is a paid mutator transaction binding the contract method 0x82ab890a.
//
// Solidity: function update(uint256 newBalance) returns()
func (_Eip2027 *Eip2027TransactorSession) Update(newBalance *big.Int) (*types.Transaction, error) {
	return _Eip2027.Contract.Update(&_Eip2027.TransactOpts, newBalance)
}
