// Code generated - DO NOT EDIT.
// This file is a generated binding and any manual changes will be lost.

package bindtest

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
	_ = bind.Bind
	_ = common.Big1
	_ = types.BloomLookup
	_ = event.NewSubscription
)

// CallbackParamABI is the input ABI used to generate the binding from.
const CallbackParamABI = "[{\"constant\":false,\"inputs\":[{\"name\":\"callback\",\"type\":\"function\"}],\"name\":\"test\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"}]"

// CallbackParamFuncSigs maps the 4-byte function signature to its string representation.
var CallbackParamFuncSigs = map[string]string{
	"d7a5aba2": "test(function)",
}

// CallbackParamBin is the compiled bytecode used for deploying new contracts.
var CallbackParamBin = "0x608060405234801561001057600080fd5b5061015e806100206000396000f3fe60806040526004361061003b576000357c010000000000000000000000000000000000000000000000000000000090048063d7a5aba214610040575b600080fd5b34801561004c57600080fd5b506100be6004803603602081101561006357600080fd5b810190808035806c0100000000000000000000000090049068010000000000000000900463ffffffff1677ffffffffffffffffffffffffffffffffffffffffffffffff169091602001919093929190939291905050506100c0565b005b818160016040518263ffffffff167c010000000000000000000000000000000000000000000000000000000002815260040180828152602001915050600060405180830381600087803b15801561011657600080fd5b505af115801561012a573d6000803e3d6000fd5b50505050505056fea165627a7a7230582062f87455ff84be90896dbb0c4e4ddb505c600d23089f8e80a512548440d7e2580029"

// DeployCallbackParam deploys a new Ethereum contract, binding an instance of CallbackParam to it.
func DeployCallbackParam(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *CallbackParam, error) {
	parsed, err := abi.JSON(strings.NewReader(CallbackParamABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(CallbackParamBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &CallbackParam{CallbackParamCaller: CallbackParamCaller{contract: contract}, CallbackParamTransactor: CallbackParamTransactor{contract: contract}, CallbackParamFilterer: CallbackParamFilterer{contract: contract}}, nil
}

// CallbackParam is an auto generated Go binding around an Ethereum contract.
type CallbackParam struct {
	CallbackParamCaller     // Read-only binding to the contract
	CallbackParamTransactor // Write-only binding to the contract
	CallbackParamFilterer   // Log filterer for contract events
}

// CallbackParamCaller is an auto generated read-only Go binding around an Ethereum contract.
type CallbackParamCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// CallbackParamTransactor is an auto generated write-only Go binding around an Ethereum contract.
type CallbackParamTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// CallbackParamFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type CallbackParamFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// CallbackParamSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type CallbackParamSession struct {
	Contract     *CallbackParam    // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// CallbackParamCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type CallbackParamCallerSession struct {
	Contract *CallbackParamCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts        // Call options to use throughout this session
}

// CallbackParamTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type CallbackParamTransactorSession struct {
	Contract     *CallbackParamTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts        // Transaction auth options to use throughout this session
}

// CallbackParamRaw is an auto generated low-level Go binding around an Ethereum contract.
type CallbackParamRaw struct {
	Contract *CallbackParam // Generic contract binding to access the raw methods on
}

// CallbackParamCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type CallbackParamCallerRaw struct {
	Contract *CallbackParamCaller // Generic read-only contract binding to access the raw methods on
}

// CallbackParamTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type CallbackParamTransactorRaw struct {
	Contract *CallbackParamTransactor // Generic write-only contract binding to access the raw methods on
}

// NewCallbackParam creates a new instance of CallbackParam, bound to a specific deployed contract.
func NewCallbackParam(address common.Address, backend bind.ContractBackend) (*CallbackParam, error) {
	contract, err := bindCallbackParam(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &CallbackParam{CallbackParamCaller: CallbackParamCaller{contract: contract}, CallbackParamTransactor: CallbackParamTransactor{contract: contract}, CallbackParamFilterer: CallbackParamFilterer{contract: contract}}, nil
}

// NewCallbackParamCaller creates a new read-only instance of CallbackParam, bound to a specific deployed contract.
func NewCallbackParamCaller(address common.Address, caller bind.ContractCaller) (*CallbackParamCaller, error) {
	contract, err := bindCallbackParam(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &CallbackParamCaller{contract: contract}, nil
}

// NewCallbackParamTransactor creates a new write-only instance of CallbackParam, bound to a specific deployed contract.
func NewCallbackParamTransactor(address common.Address, transactor bind.ContractTransactor) (*CallbackParamTransactor, error) {
	contract, err := bindCallbackParam(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &CallbackParamTransactor{contract: contract}, nil
}

// NewCallbackParamFilterer creates a new log filterer instance of CallbackParam, bound to a specific deployed contract.
func NewCallbackParamFilterer(address common.Address, filterer bind.ContractFilterer) (*CallbackParamFilterer, error) {
	contract, err := bindCallbackParam(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &CallbackParamFilterer{contract: contract}, nil
}

// bindCallbackParam binds a generic wrapper to an already deployed contract.
func bindCallbackParam(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(CallbackParamABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_CallbackParam *CallbackParamRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _CallbackParam.Contract.CallbackParamCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_CallbackParam *CallbackParamRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _CallbackParam.Contract.CallbackParamTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_CallbackParam *CallbackParamRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _CallbackParam.Contract.CallbackParamTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_CallbackParam *CallbackParamCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _CallbackParam.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_CallbackParam *CallbackParamTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _CallbackParam.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_CallbackParam *CallbackParamTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _CallbackParam.Contract.contract.Transact(opts, method, params...)
}

// Test is a paid mutator transaction binding the contract method 0xd7a5aba2.
//
// Solidity: function test(function callback) returns()
func (_CallbackParam *CallbackParamTransactor) Test(opts *bind.TransactOpts, callback [24]byte) (*types.Transaction, error) {
	return _CallbackParam.contract.Transact(opts, "test", callback)
}

// Test is a paid mutator transaction binding the contract method 0xd7a5aba2.
//
// Solidity: function test(function callback) returns()
func (_CallbackParam *CallbackParamSession) Test(callback [24]byte) (*types.Transaction, error) {
	return _CallbackParam.Contract.Test(&_CallbackParam.TransactOpts, callback)
}

// Test is a paid mutator transaction binding the contract method 0xd7a5aba2.
//
// Solidity: function test(function callback) returns()
func (_CallbackParam *CallbackParamTransactorSession) Test(callback [24]byte) (*types.Transaction, error) {
	return _CallbackParam.Contract.Test(&_CallbackParam.TransactOpts, callback)
}
