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

// NonExistentABI is the input ABI used to generate the binding from.
const NonExistentABI = "[{\"constant\":true,\"inputs\":[],\"name\":\"String\",\"outputs\":[{\"name\":\"\",\"type\":\"string\"}],\"type\":\"function\"}]"

// NonExistentBin is the compiled bytecode used for deploying new contracts.
var NonExistentBin = "0x6060604052609f8060106000396000f3606060405260e060020a6000350463f97a60058114601a575b005b600060605260c0604052600d60809081527f4920646f6e27742065786973740000000000000000000000000000000000000060a052602060c0908152600d60e081905281906101009060a09080838184600060046012f15050815172ffffffffffffffffffffffffffffffffffffff1916909152505060405161012081900392509050f3"

// DeployNonExistent deploys a new Ethereum contract, binding an instance of NonExistent to it.
func DeployNonExistent(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *NonExistent, error) {
	parsed, err := abi.JSON(strings.NewReader(NonExistentABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(NonExistentBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &NonExistent{NonExistentCaller: NonExistentCaller{contract: contract}, NonExistentTransactor: NonExistentTransactor{contract: contract}, NonExistentFilterer: NonExistentFilterer{contract: contract}}, nil
}

// NonExistent is an auto generated Go binding around an Ethereum contract.
type NonExistent struct {
	NonExistentCaller     // Read-only binding to the contract
	NonExistentTransactor // Write-only binding to the contract
	NonExistentFilterer   // Log filterer for contract events
}

// NonExistentCaller is an auto generated read-only Go binding around an Ethereum contract.
type NonExistentCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// NonExistentTransactor is an auto generated write-only Go binding around an Ethereum contract.
type NonExistentTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// NonExistentFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type NonExistentFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// NonExistentSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type NonExistentSession struct {
	Contract     *NonExistent      // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// NonExistentCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type NonExistentCallerSession struct {
	Contract *NonExistentCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts      // Call options to use throughout this session
}

// NonExistentTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type NonExistentTransactorSession struct {
	Contract     *NonExistentTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts      // Transaction auth options to use throughout this session
}

// NonExistentRaw is an auto generated low-level Go binding around an Ethereum contract.
type NonExistentRaw struct {
	Contract *NonExistent // Generic contract binding to access the raw methods on
}

// NonExistentCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type NonExistentCallerRaw struct {
	Contract *NonExistentCaller // Generic read-only contract binding to access the raw methods on
}

// NonExistentTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type NonExistentTransactorRaw struct {
	Contract *NonExistentTransactor // Generic write-only contract binding to access the raw methods on
}

// NewNonExistent creates a new instance of NonExistent, bound to a specific deployed contract.
func NewNonExistent(address common.Address, backend bind.ContractBackend) (*NonExistent, error) {
	contract, err := bindNonExistent(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &NonExistent{NonExistentCaller: NonExistentCaller{contract: contract}, NonExistentTransactor: NonExistentTransactor{contract: contract}, NonExistentFilterer: NonExistentFilterer{contract: contract}}, nil
}

// NewNonExistentCaller creates a new read-only instance of NonExistent, bound to a specific deployed contract.
func NewNonExistentCaller(address common.Address, caller bind.ContractCaller) (*NonExistentCaller, error) {
	contract, err := bindNonExistent(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &NonExistentCaller{contract: contract}, nil
}

// NewNonExistentTransactor creates a new write-only instance of NonExistent, bound to a specific deployed contract.
func NewNonExistentTransactor(address common.Address, transactor bind.ContractTransactor) (*NonExistentTransactor, error) {
	contract, err := bindNonExistent(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &NonExistentTransactor{contract: contract}, nil
}

// NewNonExistentFilterer creates a new log filterer instance of NonExistent, bound to a specific deployed contract.
func NewNonExistentFilterer(address common.Address, filterer bind.ContractFilterer) (*NonExistentFilterer, error) {
	contract, err := bindNonExistent(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &NonExistentFilterer{contract: contract}, nil
}

// bindNonExistent binds a generic wrapper to an already deployed contract.
func bindNonExistent(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(NonExistentABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_NonExistent *NonExistentRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _NonExistent.Contract.NonExistentCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_NonExistent *NonExistentRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _NonExistent.Contract.NonExistentTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_NonExistent *NonExistentRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _NonExistent.Contract.NonExistentTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_NonExistent *NonExistentCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _NonExistent.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_NonExistent *NonExistentTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _NonExistent.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_NonExistent *NonExistentTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _NonExistent.Contract.contract.Transact(opts, method, params...)
}

// String is a free data retrieval call binding the contract method 0xf97a6005.
//
// Solidity: function String() returns(string)
func (_NonExistent *NonExistentCaller) String(opts *bind.CallOpts) (string, error) {
	var out []interface{}
	err := _NonExistent.contract.Call(opts, &out, "String")

	if err != nil {
		return *new(string), err
	}

	out0 := *abi.ConvertType(out[0], new(string)).(*string)

	return out0, err

}

// String is a free data retrieval call binding the contract method 0xf97a6005.
//
// Solidity: function String() returns(string)
func (_NonExistent *NonExistentSession) String() (string, error) {
	return _NonExistent.Contract.String(&_NonExistent.CallOpts)
}

// String is a free data retrieval call binding the contract method 0xf97a6005.
//
// Solidity: function String() returns(string)
func (_NonExistent *NonExistentCallerSession) String() (string, error) {
	return _NonExistent.Contract.String(&_NonExistent.CallOpts)
}
