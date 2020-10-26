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

// CallFromABI is the input ABI used to generate the binding from.
const CallFromABI = "[{\"constant\":true,\"inputs\":[],\"name\":\"callFrom\",\"outputs\":[{\"name\":\"\",\"type\":\"address\"}],\"payable\":false,\"type\":\"function\"}]"

// CallFromBin is the compiled bytecode used for deploying new contracts.
var CallFromBin = "0x6060604052346000575b6086806100176000396000f300606060405263ffffffff60e060020a60003504166349f8e98281146022575b6000565b34600057602c6055565b6040805173ffffffffffffffffffffffffffffffffffffffff9092168252519081900360200190f35b335b905600a165627a7a72305820aef6b7685c0fa24ba6027e4870404a57df701473fe4107741805c19f5138417c0029"

// DeployCallFrom deploys a new Ethereum contract, binding an instance of CallFrom to it.
func DeployCallFrom(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *CallFrom, error) {
	parsed, err := abi.JSON(strings.NewReader(CallFromABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(CallFromBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &CallFrom{CallFromCaller: CallFromCaller{contract: contract}, CallFromTransactor: CallFromTransactor{contract: contract}, CallFromFilterer: CallFromFilterer{contract: contract}}, nil
}

// CallFrom is an auto generated Go binding around an Ethereum contract.
type CallFrom struct {
	CallFromCaller     // Read-only binding to the contract
	CallFromTransactor // Write-only binding to the contract
	CallFromFilterer   // Log filterer for contract events
}

// CallFromCaller is an auto generated read-only Go binding around an Ethereum contract.
type CallFromCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// CallFromTransactor is an auto generated write-only Go binding around an Ethereum contract.
type CallFromTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// CallFromFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type CallFromFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// CallFromSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type CallFromSession struct {
	Contract     *CallFrom         // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// CallFromCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type CallFromCallerSession struct {
	Contract *CallFromCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts   // Call options to use throughout this session
}

// CallFromTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type CallFromTransactorSession struct {
	Contract     *CallFromTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts   // Transaction auth options to use throughout this session
}

// CallFromRaw is an auto generated low-level Go binding around an Ethereum contract.
type CallFromRaw struct {
	Contract *CallFrom // Generic contract binding to access the raw methods on
}

// CallFromCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type CallFromCallerRaw struct {
	Contract *CallFromCaller // Generic read-only contract binding to access the raw methods on
}

// CallFromTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type CallFromTransactorRaw struct {
	Contract *CallFromTransactor // Generic write-only contract binding to access the raw methods on
}

// NewCallFrom creates a new instance of CallFrom, bound to a specific deployed contract.
func NewCallFrom(address common.Address, backend bind.ContractBackend) (*CallFrom, error) {
	contract, err := bindCallFrom(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &CallFrom{CallFromCaller: CallFromCaller{contract: contract}, CallFromTransactor: CallFromTransactor{contract: contract}, CallFromFilterer: CallFromFilterer{contract: contract}}, nil
}

// NewCallFromCaller creates a new read-only instance of CallFrom, bound to a specific deployed contract.
func NewCallFromCaller(address common.Address, caller bind.ContractCaller) (*CallFromCaller, error) {
	contract, err := bindCallFrom(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &CallFromCaller{contract: contract}, nil
}

// NewCallFromTransactor creates a new write-only instance of CallFrom, bound to a specific deployed contract.
func NewCallFromTransactor(address common.Address, transactor bind.ContractTransactor) (*CallFromTransactor, error) {
	contract, err := bindCallFrom(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &CallFromTransactor{contract: contract}, nil
}

// NewCallFromFilterer creates a new log filterer instance of CallFrom, bound to a specific deployed contract.
func NewCallFromFilterer(address common.Address, filterer bind.ContractFilterer) (*CallFromFilterer, error) {
	contract, err := bindCallFrom(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &CallFromFilterer{contract: contract}, nil
}

// bindCallFrom binds a generic wrapper to an already deployed contract.
func bindCallFrom(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(CallFromABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_CallFrom *CallFromRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _CallFrom.Contract.CallFromCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_CallFrom *CallFromRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _CallFrom.Contract.CallFromTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_CallFrom *CallFromRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _CallFrom.Contract.CallFromTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_CallFrom *CallFromCallerRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _CallFrom.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_CallFrom *CallFromTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _CallFrom.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_CallFrom *CallFromTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _CallFrom.Contract.contract.Transact(opts, method, params...)
}

// CallFrom is a free data retrieval call binding the contract method 0x49f8e982.
//
// Solidity: function callFrom() returns(address)
func (_CallFrom *CallFromCaller) CallFrom(opts *bind.CallOpts) (common.Address, error) {
	var (
		ret0 = new(common.Address)
	)
	out := ret0
	err := _CallFrom.contract.Call(opts, out, "callFrom")
	return *ret0, err
}

// CallFrom is a free data retrieval call binding the contract method 0x49f8e982.
//
// Solidity: function callFrom() returns(address)
func (_CallFrom *CallFromSession) CallFrom() (common.Address, error) {
	return _CallFrom.Contract.CallFrom(&_CallFrom.CallOpts)
}

// CallFrom is a free data retrieval call binding the contract method 0x49f8e982.
//
// Solidity: function callFrom() returns(address)
func (_CallFrom *CallFromCallerSession) CallFrom() (common.Address, error) {
	return _CallFrom.Contract.CallFrom(&_CallFrom.CallOpts)
}
