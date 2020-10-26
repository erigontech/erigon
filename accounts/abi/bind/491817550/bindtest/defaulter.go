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

// DefaulterABI is the input ABI used to generate the binding from.
const DefaulterABI = "[{\"constant\":true,\"inputs\":[],\"name\":\"caller\",\"outputs\":[{\"name\":\"\",\"type\":\"address\"}],\"type\":\"function\"}]"

// DefaulterBin is the compiled bytecode used for deploying new contracts.
var DefaulterBin = "0x6060604052606a8060106000396000f360606040523615601d5760e060020a6000350463fc9c8d3981146040575b605e6000805473ffffffffffffffffffffffffffffffffffffffff191633179055565b606060005473ffffffffffffffffffffffffffffffffffffffff1681565b005b6060908152602090f3"

// DeployDefaulter deploys a new Ethereum contract, binding an instance of Defaulter to it.
func DeployDefaulter(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *Defaulter, error) {
	parsed, err := abi.JSON(strings.NewReader(DefaulterABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(DefaulterBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &Defaulter{DefaulterCaller: DefaulterCaller{contract: contract}, DefaulterTransactor: DefaulterTransactor{contract: contract}, DefaulterFilterer: DefaulterFilterer{contract: contract}}, nil
}

// Defaulter is an auto generated Go binding around an Ethereum contract.
type Defaulter struct {
	DefaulterCaller     // Read-only binding to the contract
	DefaulterTransactor // Write-only binding to the contract
	DefaulterFilterer   // Log filterer for contract events
}

// DefaulterCaller is an auto generated read-only Go binding around an Ethereum contract.
type DefaulterCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// DefaulterTransactor is an auto generated write-only Go binding around an Ethereum contract.
type DefaulterTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// DefaulterFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type DefaulterFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// DefaulterSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type DefaulterSession struct {
	Contract     *Defaulter        // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// DefaulterCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type DefaulterCallerSession struct {
	Contract *DefaulterCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts    // Call options to use throughout this session
}

// DefaulterTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type DefaulterTransactorSession struct {
	Contract     *DefaulterTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts    // Transaction auth options to use throughout this session
}

// DefaulterRaw is an auto generated low-level Go binding around an Ethereum contract.
type DefaulterRaw struct {
	Contract *Defaulter // Generic contract binding to access the raw methods on
}

// DefaulterCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type DefaulterCallerRaw struct {
	Contract *DefaulterCaller // Generic read-only contract binding to access the raw methods on
}

// DefaulterTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type DefaulterTransactorRaw struct {
	Contract *DefaulterTransactor // Generic write-only contract binding to access the raw methods on
}

// NewDefaulter creates a new instance of Defaulter, bound to a specific deployed contract.
func NewDefaulter(address common.Address, backend bind.ContractBackend) (*Defaulter, error) {
	contract, err := bindDefaulter(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Defaulter{DefaulterCaller: DefaulterCaller{contract: contract}, DefaulterTransactor: DefaulterTransactor{contract: contract}, DefaulterFilterer: DefaulterFilterer{contract: contract}}, nil
}

// NewDefaulterCaller creates a new read-only instance of Defaulter, bound to a specific deployed contract.
func NewDefaulterCaller(address common.Address, caller bind.ContractCaller) (*DefaulterCaller, error) {
	contract, err := bindDefaulter(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &DefaulterCaller{contract: contract}, nil
}

// NewDefaulterTransactor creates a new write-only instance of Defaulter, bound to a specific deployed contract.
func NewDefaulterTransactor(address common.Address, transactor bind.ContractTransactor) (*DefaulterTransactor, error) {
	contract, err := bindDefaulter(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &DefaulterTransactor{contract: contract}, nil
}

// NewDefaulterFilterer creates a new log filterer instance of Defaulter, bound to a specific deployed contract.
func NewDefaulterFilterer(address common.Address, filterer bind.ContractFilterer) (*DefaulterFilterer, error) {
	contract, err := bindDefaulter(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &DefaulterFilterer{contract: contract}, nil
}

// bindDefaulter binds a generic wrapper to an already deployed contract.
func bindDefaulter(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(DefaulterABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Defaulter *DefaulterRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Defaulter.Contract.DefaulterCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Defaulter *DefaulterRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Defaulter.Contract.DefaulterTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Defaulter *DefaulterRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Defaulter.Contract.DefaulterTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Defaulter *DefaulterCallerRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Defaulter.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Defaulter *DefaulterTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Defaulter.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Defaulter *DefaulterTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Defaulter.Contract.contract.Transact(opts, method, params...)
}

// Caller is a free data retrieval call binding the contract method 0xfc9c8d39.
//
// Solidity: function caller() returns(address)
func (_Defaulter *DefaulterCaller) Caller(opts *bind.CallOpts) (common.Address, error) {
	var (
		ret0 = new(common.Address)
	)
	out := ret0
	err := _Defaulter.contract.Call(opts, out, "caller")
	return *ret0, err
}

// Caller is a free data retrieval call binding the contract method 0xfc9c8d39.
//
// Solidity: function caller() returns(address)
func (_Defaulter *DefaulterSession) Caller() (common.Address, error) {
	return _Defaulter.Contract.Caller(&_Defaulter.CallOpts)
}

// Caller is a free data retrieval call binding the contract method 0xfc9c8d39.
//
// Solidity: function caller() returns(address)
func (_Defaulter *DefaulterCallerSession) Caller() (common.Address, error) {
	return _Defaulter.Contract.Caller(&_Defaulter.CallOpts)
}
