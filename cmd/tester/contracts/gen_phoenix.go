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

// PhoenixABI is the input ABI used to generate the binding from.
const PhoenixABI = "[{\"inputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"constructor\"},{\"inputs\":[],\"name\":\"die\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"store\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"stateMutability\":\"payable\",\"type\":\"receive\"}]"

// PhoenixBin is the compiled bytecode used for deploying new contracts.
var PhoenixBin = "0x6080604052348015600f57600080fd5b5060af8061001e6000396000f3fe60806040526004361060295760003560e01c806335f46994146034578063975057e714604857602f565b36602f57005b600080fd5b348015603f57600080fd5b506046605a565b005b348015605357600080fd5b506046605e565b6000ff5b6000805481526001602081905260408220819055815401905556fea2646970667358221220906cbf0d83672df8db0547ebbed7a74a4704d3ba29d7c8f191cc34ba4b1e635264736f6c63430006020033"

// DeployPhoenix deploys a new Ethereum contract, binding an instance of Phoenix to it.
func DeployPhoenix(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *Phoenix, error) {
	parsed, err := abi.JSON(strings.NewReader(PhoenixABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(PhoenixBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &Phoenix{PhoenixCaller: PhoenixCaller{contract: contract}, PhoenixTransactor: PhoenixTransactor{contract: contract}, PhoenixFilterer: PhoenixFilterer{contract: contract}}, nil
}

// Phoenix is an auto generated Go binding around an Ethereum contract.
type Phoenix struct {
	PhoenixCaller     // Read-only binding to the contract
	PhoenixTransactor // Write-only binding to the contract
	PhoenixFilterer   // Log filterer for contract events
}

// PhoenixCaller is an auto generated read-only Go binding around an Ethereum contract.
type PhoenixCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// PhoenixTransactor is an auto generated write-only Go binding around an Ethereum contract.
type PhoenixTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// PhoenixFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type PhoenixFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// PhoenixSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type PhoenixSession struct {
	Contract     *Phoenix          // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// PhoenixCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type PhoenixCallerSession struct {
	Contract *PhoenixCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts  // Call options to use throughout this session
}

// PhoenixTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type PhoenixTransactorSession struct {
	Contract     *PhoenixTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts  // Transaction auth options to use throughout this session
}

// PhoenixRaw is an auto generated low-level Go binding around an Ethereum contract.
type PhoenixRaw struct {
	Contract *Phoenix // Generic contract binding to access the raw methods on
}

// PhoenixCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type PhoenixCallerRaw struct {
	Contract *PhoenixCaller // Generic read-only contract binding to access the raw methods on
}

// PhoenixTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type PhoenixTransactorRaw struct {
	Contract *PhoenixTransactor // Generic write-only contract binding to access the raw methods on
}

// NewPhoenix creates a new instance of Phoenix, bound to a specific deployed contract.
func NewPhoenix(address common.Address, backend bind.ContractBackend) (*Phoenix, error) {
	contract, err := bindPhoenix(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Phoenix{PhoenixCaller: PhoenixCaller{contract: contract}, PhoenixTransactor: PhoenixTransactor{contract: contract}, PhoenixFilterer: PhoenixFilterer{contract: contract}}, nil
}

// NewPhoenixCaller creates a new read-only instance of Phoenix, bound to a specific deployed contract.
func NewPhoenixCaller(address common.Address, caller bind.ContractCaller) (*PhoenixCaller, error) {
	contract, err := bindPhoenix(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &PhoenixCaller{contract: contract}, nil
}

// NewPhoenixTransactor creates a new write-only instance of Phoenix, bound to a specific deployed contract.
func NewPhoenixTransactor(address common.Address, transactor bind.ContractTransactor) (*PhoenixTransactor, error) {
	contract, err := bindPhoenix(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &PhoenixTransactor{contract: contract}, nil
}

// NewPhoenixFilterer creates a new log filterer instance of Phoenix, bound to a specific deployed contract.
func NewPhoenixFilterer(address common.Address, filterer bind.ContractFilterer) (*PhoenixFilterer, error) {
	contract, err := bindPhoenix(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &PhoenixFilterer{contract: contract}, nil
}

// bindPhoenix binds a generic wrapper to an already deployed contract.
func bindPhoenix(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(PhoenixABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Phoenix *PhoenixRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Phoenix.Contract.PhoenixCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Phoenix *PhoenixRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Phoenix.Contract.PhoenixTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Phoenix *PhoenixRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Phoenix.Contract.PhoenixTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Phoenix *PhoenixCallerRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Phoenix.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Phoenix *PhoenixTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Phoenix.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Phoenix *PhoenixTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Phoenix.Contract.contract.Transact(opts, method, params...)
}

// Die is a paid mutator transaction binding the contract method 0x35f46994.
//
// Solidity: function die() returns()
func (_Phoenix *PhoenixTransactor) Die(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Phoenix.contract.Transact(opts, "die")
}

// Die is a paid mutator transaction binding the contract method 0x35f46994.
//
// Solidity: function die() returns()
func (_Phoenix *PhoenixSession) Die() (*types.Transaction, error) {
	return _Phoenix.Contract.Die(&_Phoenix.TransactOpts)
}

// Die is a paid mutator transaction binding the contract method 0x35f46994.
//
// Solidity: function die() returns()
func (_Phoenix *PhoenixTransactorSession) Die() (*types.Transaction, error) {
	return _Phoenix.Contract.Die(&_Phoenix.TransactOpts)
}

// Store is a paid mutator transaction binding the contract method 0x975057e7.
//
// Solidity: function store() returns()
func (_Phoenix *PhoenixTransactor) Store(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Phoenix.contract.Transact(opts, "store")
}

// Store is a paid mutator transaction binding the contract method 0x975057e7.
//
// Solidity: function store() returns()
func (_Phoenix *PhoenixSession) Store() (*types.Transaction, error) {
	return _Phoenix.Contract.Store(&_Phoenix.TransactOpts)
}

// Store is a paid mutator transaction binding the contract method 0x975057e7.
//
// Solidity: function store() returns()
func (_Phoenix *PhoenixTransactorSession) Store() (*types.Transaction, error) {
	return _Phoenix.Contract.Store(&_Phoenix.TransactOpts)
}
