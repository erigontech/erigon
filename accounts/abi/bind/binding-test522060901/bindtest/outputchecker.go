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

// OutputCheckerABI is the input ABI used to generate the binding from.
const OutputCheckerABI = "[{\"type\":\"function\",\"name\":\"noOutput\",\"constant\":true,\"inputs\":[],\"outputs\":[]},{\"type\":\"function\",\"name\":\"namedOutput\",\"constant\":true,\"inputs\":[],\"outputs\":[{\"name\":\"str\",\"type\":\"string\"}]},{\"type\":\"function\",\"name\":\"anonOutput\",\"constant\":true,\"inputs\":[],\"outputs\":[{\"name\":\"\",\"type\":\"string\"}]},{\"type\":\"function\",\"name\":\"namedOutputs\",\"constant\":true,\"inputs\":[],\"outputs\":[{\"name\":\"str1\",\"type\":\"string\"},{\"name\":\"str2\",\"type\":\"string\"}]},{\"type\":\"function\",\"name\":\"collidingOutputs\",\"constant\":true,\"inputs\":[],\"outputs\":[{\"name\":\"str\",\"type\":\"string\"},{\"name\":\"Str\",\"type\":\"string\"}]},{\"type\":\"function\",\"name\":\"anonOutputs\",\"constant\":true,\"inputs\":[],\"outputs\":[{\"name\":\"\",\"type\":\"string\"},{\"name\":\"\",\"type\":\"string\"}]},{\"type\":\"function\",\"name\":\"mixedOutputs\",\"constant\":true,\"inputs\":[],\"outputs\":[{\"name\":\"\",\"type\":\"string\"},{\"name\":\"str\",\"type\":\"string\"}]}]"

// OutputChecker is an auto generated Go binding around an Ethereum contract.
type OutputChecker struct {
	OutputCheckerCaller     // Read-only binding to the contract
	OutputCheckerTransactor // Write-only binding to the contract
	OutputCheckerFilterer   // Log filterer for contract events
}

// OutputCheckerCaller is an auto generated read-only Go binding around an Ethereum contract.
type OutputCheckerCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// OutputCheckerTransactor is an auto generated write-only Go binding around an Ethereum contract.
type OutputCheckerTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// OutputCheckerFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type OutputCheckerFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// OutputCheckerSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type OutputCheckerSession struct {
	Contract     *OutputChecker    // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// OutputCheckerCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type OutputCheckerCallerSession struct {
	Contract *OutputCheckerCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts        // Call options to use throughout this session
}

// OutputCheckerTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type OutputCheckerTransactorSession struct {
	Contract     *OutputCheckerTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts        // Transaction auth options to use throughout this session
}

// OutputCheckerRaw is an auto generated low-level Go binding around an Ethereum contract.
type OutputCheckerRaw struct {
	Contract *OutputChecker // Generic contract binding to access the raw methods on
}

// OutputCheckerCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type OutputCheckerCallerRaw struct {
	Contract *OutputCheckerCaller // Generic read-only contract binding to access the raw methods on
}

// OutputCheckerTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type OutputCheckerTransactorRaw struct {
	Contract *OutputCheckerTransactor // Generic write-only contract binding to access the raw methods on
}

// NewOutputChecker creates a new instance of OutputChecker, bound to a specific deployed contract.
func NewOutputChecker(address common.Address, backend bind.ContractBackend) (*OutputChecker, error) {
	contract, err := bindOutputChecker(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &OutputChecker{OutputCheckerCaller: OutputCheckerCaller{contract: contract}, OutputCheckerTransactor: OutputCheckerTransactor{contract: contract}, OutputCheckerFilterer: OutputCheckerFilterer{contract: contract}}, nil
}

// NewOutputCheckerCaller creates a new read-only instance of OutputChecker, bound to a specific deployed contract.
func NewOutputCheckerCaller(address common.Address, caller bind.ContractCaller) (*OutputCheckerCaller, error) {
	contract, err := bindOutputChecker(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &OutputCheckerCaller{contract: contract}, nil
}

// NewOutputCheckerTransactor creates a new write-only instance of OutputChecker, bound to a specific deployed contract.
func NewOutputCheckerTransactor(address common.Address, transactor bind.ContractTransactor) (*OutputCheckerTransactor, error) {
	contract, err := bindOutputChecker(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &OutputCheckerTransactor{contract: contract}, nil
}

// NewOutputCheckerFilterer creates a new log filterer instance of OutputChecker, bound to a specific deployed contract.
func NewOutputCheckerFilterer(address common.Address, filterer bind.ContractFilterer) (*OutputCheckerFilterer, error) {
	contract, err := bindOutputChecker(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &OutputCheckerFilterer{contract: contract}, nil
}

// bindOutputChecker binds a generic wrapper to an already deployed contract.
func bindOutputChecker(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(OutputCheckerABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_OutputChecker *OutputCheckerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _OutputChecker.Contract.OutputCheckerCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_OutputChecker *OutputCheckerRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _OutputChecker.Contract.OutputCheckerTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_OutputChecker *OutputCheckerRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _OutputChecker.Contract.OutputCheckerTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_OutputChecker *OutputCheckerCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _OutputChecker.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_OutputChecker *OutputCheckerTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _OutputChecker.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_OutputChecker *OutputCheckerTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _OutputChecker.Contract.contract.Transact(opts, method, params...)
}

// AnonOutput is a free data retrieval call binding the contract method 0x008bda05.
//
// Solidity: function anonOutput() returns(string)
func (_OutputChecker *OutputCheckerCaller) AnonOutput(opts *bind.CallOpts) (string, error) {
	var out []interface{}
	err := _OutputChecker.contract.Call(opts, &out, "anonOutput")

	if err != nil {
		return *new(string), err
	}

	out0 := *abi.ConvertType(out[0], new(string)).(*string)

	return out0, err

}

// AnonOutput is a free data retrieval call binding the contract method 0x008bda05.
//
// Solidity: function anonOutput() returns(string)
func (_OutputChecker *OutputCheckerSession) AnonOutput() (string, error) {
	return _OutputChecker.Contract.AnonOutput(&_OutputChecker.CallOpts)
}

// AnonOutput is a free data retrieval call binding the contract method 0x008bda05.
//
// Solidity: function anonOutput() returns(string)
func (_OutputChecker *OutputCheckerCallerSession) AnonOutput() (string, error) {
	return _OutputChecker.Contract.AnonOutput(&_OutputChecker.CallOpts)
}

// AnonOutputs is a free data retrieval call binding the contract method 0x3c401115.
//
// Solidity: function anonOutputs() returns(string, string)
func (_OutputChecker *OutputCheckerCaller) AnonOutputs(opts *bind.CallOpts) (string, string, error) {
	var out []interface{}
	err := _OutputChecker.contract.Call(opts, &out, "anonOutputs")

	if err != nil {
		return *new(string), *new(string), err
	}

	out0 := *abi.ConvertType(out[0], new(string)).(*string)
	out1 := *abi.ConvertType(out[1], new(string)).(*string)

	return out0, out1, err

}

// AnonOutputs is a free data retrieval call binding the contract method 0x3c401115.
//
// Solidity: function anonOutputs() returns(string, string)
func (_OutputChecker *OutputCheckerSession) AnonOutputs() (string, string, error) {
	return _OutputChecker.Contract.AnonOutputs(&_OutputChecker.CallOpts)
}

// AnonOutputs is a free data retrieval call binding the contract method 0x3c401115.
//
// Solidity: function anonOutputs() returns(string, string)
func (_OutputChecker *OutputCheckerCallerSession) AnonOutputs() (string, string, error) {
	return _OutputChecker.Contract.AnonOutputs(&_OutputChecker.CallOpts)
}

// CollidingOutputs is a free data retrieval call binding the contract method 0xeccbc1ee.
//
// Solidity: function collidingOutputs() returns(string str, string Str)
func (_OutputChecker *OutputCheckerCaller) CollidingOutputs(opts *bind.CallOpts) (string, string, error) {
	var out []interface{}
	err := _OutputChecker.contract.Call(opts, &out, "collidingOutputs")

	if err != nil {
		return *new(string), *new(string), err
	}

	out0 := *abi.ConvertType(out[0], new(string)).(*string)
	out1 := *abi.ConvertType(out[1], new(string)).(*string)

	return out0, out1, err

}

// CollidingOutputs is a free data retrieval call binding the contract method 0xeccbc1ee.
//
// Solidity: function collidingOutputs() returns(string str, string Str)
func (_OutputChecker *OutputCheckerSession) CollidingOutputs() (string, string, error) {
	return _OutputChecker.Contract.CollidingOutputs(&_OutputChecker.CallOpts)
}

// CollidingOutputs is a free data retrieval call binding the contract method 0xeccbc1ee.
//
// Solidity: function collidingOutputs() returns(string str, string Str)
func (_OutputChecker *OutputCheckerCallerSession) CollidingOutputs() (string, string, error) {
	return _OutputChecker.Contract.CollidingOutputs(&_OutputChecker.CallOpts)
}

// MixedOutputs is a free data retrieval call binding the contract method 0x21b77b44.
//
// Solidity: function mixedOutputs() returns(string, string str)
func (_OutputChecker *OutputCheckerCaller) MixedOutputs(opts *bind.CallOpts) (string, string, error) {
	var out []interface{}
	err := _OutputChecker.contract.Call(opts, &out, "mixedOutputs")

	if err != nil {
		return *new(string), *new(string), err
	}

	out0 := *abi.ConvertType(out[0], new(string)).(*string)
	out1 := *abi.ConvertType(out[1], new(string)).(*string)

	return out0, out1, err

}

// MixedOutputs is a free data retrieval call binding the contract method 0x21b77b44.
//
// Solidity: function mixedOutputs() returns(string, string str)
func (_OutputChecker *OutputCheckerSession) MixedOutputs() (string, string, error) {
	return _OutputChecker.Contract.MixedOutputs(&_OutputChecker.CallOpts)
}

// MixedOutputs is a free data retrieval call binding the contract method 0x21b77b44.
//
// Solidity: function mixedOutputs() returns(string, string str)
func (_OutputChecker *OutputCheckerCallerSession) MixedOutputs() (string, string, error) {
	return _OutputChecker.Contract.MixedOutputs(&_OutputChecker.CallOpts)
}

// NamedOutput is a free data retrieval call binding the contract method 0x5e632bd5.
//
// Solidity: function namedOutput() returns(string str)
func (_OutputChecker *OutputCheckerCaller) NamedOutput(opts *bind.CallOpts) (string, error) {
	var out []interface{}
	err := _OutputChecker.contract.Call(opts, &out, "namedOutput")

	if err != nil {
		return *new(string), err
	}

	out0 := *abi.ConvertType(out[0], new(string)).(*string)

	return out0, err

}

// NamedOutput is a free data retrieval call binding the contract method 0x5e632bd5.
//
// Solidity: function namedOutput() returns(string str)
func (_OutputChecker *OutputCheckerSession) NamedOutput() (string, error) {
	return _OutputChecker.Contract.NamedOutput(&_OutputChecker.CallOpts)
}

// NamedOutput is a free data retrieval call binding the contract method 0x5e632bd5.
//
// Solidity: function namedOutput() returns(string str)
func (_OutputChecker *OutputCheckerCallerSession) NamedOutput() (string, error) {
	return _OutputChecker.Contract.NamedOutput(&_OutputChecker.CallOpts)
}

// NamedOutputs is a free data retrieval call binding the contract method 0x7970a189.
//
// Solidity: function namedOutputs() returns(string str1, string str2)
func (_OutputChecker *OutputCheckerCaller) NamedOutputs(opts *bind.CallOpts) (struct {
	Str1 string
	Str2 string
}, error) {
	var out []interface{}
	err := _OutputChecker.contract.Call(opts, &out, "namedOutputs")

	outstruct := new(struct {
		Str1 string
		Str2 string
	})
	if err != nil {
		return *outstruct, err
	}

	outstruct.Str1 = *abi.ConvertType(out[0], new(string)).(*string)
	outstruct.Str2 = *abi.ConvertType(out[1], new(string)).(*string)

	return *outstruct, err

}

// NamedOutputs is a free data retrieval call binding the contract method 0x7970a189.
//
// Solidity: function namedOutputs() returns(string str1, string str2)
func (_OutputChecker *OutputCheckerSession) NamedOutputs() (struct {
	Str1 string
	Str2 string
}, error) {
	return _OutputChecker.Contract.NamedOutputs(&_OutputChecker.CallOpts)
}

// NamedOutputs is a free data retrieval call binding the contract method 0x7970a189.
//
// Solidity: function namedOutputs() returns(string str1, string str2)
func (_OutputChecker *OutputCheckerCallerSession) NamedOutputs() (struct {
	Str1 string
	Str2 string
}, error) {
	return _OutputChecker.Contract.NamedOutputs(&_OutputChecker.CallOpts)
}

// NoOutput is a free data retrieval call binding the contract method 0x625f0306.
//
// Solidity: function noOutput() returns()
func (_OutputChecker *OutputCheckerCaller) NoOutput(opts *bind.CallOpts) error {
	var out []interface{}
	err := _OutputChecker.contract.Call(opts, &out, "noOutput")

	if err != nil {
		return err
	}

	return err

}

// NoOutput is a free data retrieval call binding the contract method 0x625f0306.
//
// Solidity: function noOutput() returns()
func (_OutputChecker *OutputCheckerSession) NoOutput() error {
	return _OutputChecker.Contract.NoOutput(&_OutputChecker.CallOpts)
}

// NoOutput is a free data retrieval call binding the contract method 0x625f0306.
//
// Solidity: function noOutput() returns()
func (_OutputChecker *OutputCheckerCallerSession) NoOutput() error {
	return _OutputChecker.Contract.NoOutput(&_OutputChecker.CallOpts)
}
