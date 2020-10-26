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

// InputCheckerABI is the input ABI used to generate the binding from.
const InputCheckerABI = "[{\"type\":\"function\",\"name\":\"noInput\",\"constant\":true,\"inputs\":[],\"outputs\":[]},{\"type\":\"function\",\"name\":\"namedInput\",\"constant\":true,\"inputs\":[{\"name\":\"str\",\"type\":\"string\"}],\"outputs\":[]},{\"type\":\"function\",\"name\":\"anonInput\",\"constant\":true,\"inputs\":[{\"name\":\"\",\"type\":\"string\"}],\"outputs\":[]},{\"type\":\"function\",\"name\":\"namedInputs\",\"constant\":true,\"inputs\":[{\"name\":\"str1\",\"type\":\"string\"},{\"name\":\"str2\",\"type\":\"string\"}],\"outputs\":[]},{\"type\":\"function\",\"name\":\"anonInputs\",\"constant\":true,\"inputs\":[{\"name\":\"\",\"type\":\"string\"},{\"name\":\"\",\"type\":\"string\"}],\"outputs\":[]},{\"type\":\"function\",\"name\":\"mixedInputs\",\"constant\":true,\"inputs\":[{\"name\":\"\",\"type\":\"string\"},{\"name\":\"str\",\"type\":\"string\"}],\"outputs\":[]}]"

// InputChecker is an auto generated Go binding around an Ethereum contract.
type InputChecker struct {
	InputCheckerCaller     // Read-only binding to the contract
	InputCheckerTransactor // Write-only binding to the contract
	InputCheckerFilterer   // Log filterer for contract events
}

// InputCheckerCaller is an auto generated read-only Go binding around an Ethereum contract.
type InputCheckerCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// InputCheckerTransactor is an auto generated write-only Go binding around an Ethereum contract.
type InputCheckerTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// InputCheckerFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type InputCheckerFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// InputCheckerSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type InputCheckerSession struct {
	Contract     *InputChecker     // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// InputCheckerCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type InputCheckerCallerSession struct {
	Contract *InputCheckerCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts       // Call options to use throughout this session
}

// InputCheckerTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type InputCheckerTransactorSession struct {
	Contract     *InputCheckerTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts       // Transaction auth options to use throughout this session
}

// InputCheckerRaw is an auto generated low-level Go binding around an Ethereum contract.
type InputCheckerRaw struct {
	Contract *InputChecker // Generic contract binding to access the raw methods on
}

// InputCheckerCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type InputCheckerCallerRaw struct {
	Contract *InputCheckerCaller // Generic read-only contract binding to access the raw methods on
}

// InputCheckerTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type InputCheckerTransactorRaw struct {
	Contract *InputCheckerTransactor // Generic write-only contract binding to access the raw methods on
}

// NewInputChecker creates a new instance of InputChecker, bound to a specific deployed contract.
func NewInputChecker(address common.Address, backend bind.ContractBackend) (*InputChecker, error) {
	contract, err := bindInputChecker(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &InputChecker{InputCheckerCaller: InputCheckerCaller{contract: contract}, InputCheckerTransactor: InputCheckerTransactor{contract: contract}, InputCheckerFilterer: InputCheckerFilterer{contract: contract}}, nil
}

// NewInputCheckerCaller creates a new read-only instance of InputChecker, bound to a specific deployed contract.
func NewInputCheckerCaller(address common.Address, caller bind.ContractCaller) (*InputCheckerCaller, error) {
	contract, err := bindInputChecker(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &InputCheckerCaller{contract: contract}, nil
}

// NewInputCheckerTransactor creates a new write-only instance of InputChecker, bound to a specific deployed contract.
func NewInputCheckerTransactor(address common.Address, transactor bind.ContractTransactor) (*InputCheckerTransactor, error) {
	contract, err := bindInputChecker(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &InputCheckerTransactor{contract: contract}, nil
}

// NewInputCheckerFilterer creates a new log filterer instance of InputChecker, bound to a specific deployed contract.
func NewInputCheckerFilterer(address common.Address, filterer bind.ContractFilterer) (*InputCheckerFilterer, error) {
	contract, err := bindInputChecker(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &InputCheckerFilterer{contract: contract}, nil
}

// bindInputChecker binds a generic wrapper to an already deployed contract.
func bindInputChecker(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(InputCheckerABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_InputChecker *InputCheckerRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _InputChecker.Contract.InputCheckerCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_InputChecker *InputCheckerRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _InputChecker.Contract.InputCheckerTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_InputChecker *InputCheckerRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _InputChecker.Contract.InputCheckerTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_InputChecker *InputCheckerCallerRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _InputChecker.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_InputChecker *InputCheckerTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _InputChecker.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_InputChecker *InputCheckerTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _InputChecker.Contract.contract.Transact(opts, method, params...)
}

// AnonInput is a free data retrieval call binding the contract method 0x3e708e82.
//
// Solidity: function anonInput(string ) returns()
func (_InputChecker *InputCheckerCaller) AnonInput(opts *bind.CallOpts, arg0 string) error {
	var ()
	out := &[]interface{}{}
	err := _InputChecker.contract.Call(opts, out, "anonInput", arg0)
	return err
}

// AnonInput is a free data retrieval call binding the contract method 0x3e708e82.
//
// Solidity: function anonInput(string ) returns()
func (_InputChecker *InputCheckerSession) AnonInput(arg0 string) error {
	return _InputChecker.Contract.AnonInput(&_InputChecker.CallOpts, arg0)
}

// AnonInput is a free data retrieval call binding the contract method 0x3e708e82.
//
// Solidity: function anonInput(string ) returns()
func (_InputChecker *InputCheckerCallerSession) AnonInput(arg0 string) error {
	return _InputChecker.Contract.AnonInput(&_InputChecker.CallOpts, arg0)
}

// AnonInputs is a free data retrieval call binding the contract method 0x28160527.
//
// Solidity: function anonInputs(string , string ) returns()
func (_InputChecker *InputCheckerCaller) AnonInputs(opts *bind.CallOpts, arg0 string, arg1 string) error {
	var ()
	out := &[]interface{}{}
	err := _InputChecker.contract.Call(opts, out, "anonInputs", arg0, arg1)
	return err
}

// AnonInputs is a free data retrieval call binding the contract method 0x28160527.
//
// Solidity: function anonInputs(string , string ) returns()
func (_InputChecker *InputCheckerSession) AnonInputs(arg0 string, arg1 string) error {
	return _InputChecker.Contract.AnonInputs(&_InputChecker.CallOpts, arg0, arg1)
}

// AnonInputs is a free data retrieval call binding the contract method 0x28160527.
//
// Solidity: function anonInputs(string , string ) returns()
func (_InputChecker *InputCheckerCallerSession) AnonInputs(arg0 string, arg1 string) error {
	return _InputChecker.Contract.AnonInputs(&_InputChecker.CallOpts, arg0, arg1)
}

// MixedInputs is a free data retrieval call binding the contract method 0xc689ebdc.
//
// Solidity: function mixedInputs(string , string str) returns()
func (_InputChecker *InputCheckerCaller) MixedInputs(opts *bind.CallOpts, arg0 string, str string) error {
	var ()
	out := &[]interface{}{}
	err := _InputChecker.contract.Call(opts, out, "mixedInputs", arg0, str)
	return err
}

// MixedInputs is a free data retrieval call binding the contract method 0xc689ebdc.
//
// Solidity: function mixedInputs(string , string str) returns()
func (_InputChecker *InputCheckerSession) MixedInputs(arg0 string, str string) error {
	return _InputChecker.Contract.MixedInputs(&_InputChecker.CallOpts, arg0, str)
}

// MixedInputs is a free data retrieval call binding the contract method 0xc689ebdc.
//
// Solidity: function mixedInputs(string , string str) returns()
func (_InputChecker *InputCheckerCallerSession) MixedInputs(arg0 string, str string) error {
	return _InputChecker.Contract.MixedInputs(&_InputChecker.CallOpts, arg0, str)
}

// NamedInput is a free data retrieval call binding the contract method 0x0d402005.
//
// Solidity: function namedInput(string str) returns()
func (_InputChecker *InputCheckerCaller) NamedInput(opts *bind.CallOpts, str string) error {
	var ()
	out := &[]interface{}{}
	err := _InputChecker.contract.Call(opts, out, "namedInput", str)
	return err
}

// NamedInput is a free data retrieval call binding the contract method 0x0d402005.
//
// Solidity: function namedInput(string str) returns()
func (_InputChecker *InputCheckerSession) NamedInput(str string) error {
	return _InputChecker.Contract.NamedInput(&_InputChecker.CallOpts, str)
}

// NamedInput is a free data retrieval call binding the contract method 0x0d402005.
//
// Solidity: function namedInput(string str) returns()
func (_InputChecker *InputCheckerCallerSession) NamedInput(str string) error {
	return _InputChecker.Contract.NamedInput(&_InputChecker.CallOpts, str)
}

// NamedInputs is a free data retrieval call binding the contract method 0x63c796ed.
//
// Solidity: function namedInputs(string str1, string str2) returns()
func (_InputChecker *InputCheckerCaller) NamedInputs(opts *bind.CallOpts, str1 string, str2 string) error {
	var ()
	out := &[]interface{}{}
	err := _InputChecker.contract.Call(opts, out, "namedInputs", str1, str2)
	return err
}

// NamedInputs is a free data retrieval call binding the contract method 0x63c796ed.
//
// Solidity: function namedInputs(string str1, string str2) returns()
func (_InputChecker *InputCheckerSession) NamedInputs(str1 string, str2 string) error {
	return _InputChecker.Contract.NamedInputs(&_InputChecker.CallOpts, str1, str2)
}

// NamedInputs is a free data retrieval call binding the contract method 0x63c796ed.
//
// Solidity: function namedInputs(string str1, string str2) returns()
func (_InputChecker *InputCheckerCallerSession) NamedInputs(str1 string, str2 string) error {
	return _InputChecker.Contract.NamedInputs(&_InputChecker.CallOpts, str1, str2)
}

// NoInput is a free data retrieval call binding the contract method 0x53539029.
//
// Solidity: function noInput() returns()
func (_InputChecker *InputCheckerCaller) NoInput(opts *bind.CallOpts) error {
	var ()
	out := &[]interface{}{}
	err := _InputChecker.contract.Call(opts, out, "noInput")
	return err
}

// NoInput is a free data retrieval call binding the contract method 0x53539029.
//
// Solidity: function noInput() returns()
func (_InputChecker *InputCheckerSession) NoInput() error {
	return _InputChecker.Contract.NoInput(&_InputChecker.CallOpts)
}

// NoInput is a free data retrieval call binding the contract method 0x53539029.
//
// Solidity: function noInput() returns()
func (_InputChecker *InputCheckerCallerSession) NoInput() error {
	return _InputChecker.Contract.NoInput(&_InputChecker.CallOpts)
}
