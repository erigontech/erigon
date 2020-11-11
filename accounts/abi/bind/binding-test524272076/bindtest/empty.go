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

// EmptyABI is the input ABI used to generate the binding from.
const EmptyABI = "[]"

// EmptyBin is the compiled bytecode used for deploying new contracts.
var EmptyBin = "0x606060405260068060106000396000f3606060405200"

// DeployEmpty deploys a new Ethereum contract, binding an instance of Empty to it.
func DeployEmpty(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *Empty, error) {
	parsed, err := abi.JSON(strings.NewReader(EmptyABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(EmptyBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &Empty{EmptyCaller: EmptyCaller{contract: contract}, EmptyTransactor: EmptyTransactor{contract: contract}, EmptyFilterer: EmptyFilterer{contract: contract}}, nil
}

// Empty is an auto generated Go binding around an Ethereum contract.
type Empty struct {
	EmptyCaller     // Read-only binding to the contract
	EmptyTransactor // Write-only binding to the contract
	EmptyFilterer   // Log filterer for contract events
}

// EmptyCaller is an auto generated read-only Go binding around an Ethereum contract.
type EmptyCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// EmptyTransactor is an auto generated write-only Go binding around an Ethereum contract.
type EmptyTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// EmptyFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type EmptyFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// EmptySession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type EmptySession struct {
	Contract     *Empty            // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// EmptyCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type EmptyCallerSession struct {
	Contract *EmptyCaller  // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts // Call options to use throughout this session
}

// EmptyTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type EmptyTransactorSession struct {
	Contract     *EmptyTransactor  // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// EmptyRaw is an auto generated low-level Go binding around an Ethereum contract.
type EmptyRaw struct {
	Contract *Empty // Generic contract binding to access the raw methods on
}

// EmptyCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type EmptyCallerRaw struct {
	Contract *EmptyCaller // Generic read-only contract binding to access the raw methods on
}

// EmptyTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type EmptyTransactorRaw struct {
	Contract *EmptyTransactor // Generic write-only contract binding to access the raw methods on
}

// NewEmpty creates a new instance of Empty, bound to a specific deployed contract.
func NewEmpty(address common.Address, backend bind.ContractBackend) (*Empty, error) {
	contract, err := bindEmpty(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Empty{EmptyCaller: EmptyCaller{contract: contract}, EmptyTransactor: EmptyTransactor{contract: contract}, EmptyFilterer: EmptyFilterer{contract: contract}}, nil
}

// NewEmptyCaller creates a new read-only instance of Empty, bound to a specific deployed contract.
func NewEmptyCaller(address common.Address, caller bind.ContractCaller) (*EmptyCaller, error) {
	contract, err := bindEmpty(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &EmptyCaller{contract: contract}, nil
}

// NewEmptyTransactor creates a new write-only instance of Empty, bound to a specific deployed contract.
func NewEmptyTransactor(address common.Address, transactor bind.ContractTransactor) (*EmptyTransactor, error) {
	contract, err := bindEmpty(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &EmptyTransactor{contract: contract}, nil
}

// NewEmptyFilterer creates a new log filterer instance of Empty, bound to a specific deployed contract.
func NewEmptyFilterer(address common.Address, filterer bind.ContractFilterer) (*EmptyFilterer, error) {
	contract, err := bindEmpty(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &EmptyFilterer{contract: contract}, nil
}

// bindEmpty binds a generic wrapper to an already deployed contract.
func bindEmpty(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(EmptyABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Empty *EmptyRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Empty.Contract.EmptyCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Empty *EmptyRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Empty.Contract.EmptyTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Empty *EmptyRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Empty.Contract.EmptyTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Empty *EmptyCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Empty.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Empty *EmptyTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Empty.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Empty *EmptyTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Empty.Contract.contract.Transact(opts, method, params...)
}
