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

// FunkyGasPatternABI is the input ABI used to generate the binding from.
const FunkyGasPatternABI = "[{\"constant\":false,\"inputs\":[{\"name\":\"value\",\"type\":\"string\"}],\"name\":\"SetField\",\"outputs\":[],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"field\",\"outputs\":[{\"name\":\"\",\"type\":\"string\"}],\"type\":\"function\"}]"

// FunkyGasPatternBin is the compiled bytecode used for deploying new contracts.
var FunkyGasPatternBin = "0x606060405261021c806100126000396000f3606060405260e060020a600035046323fcf32a81146100265780634f28bf0e1461007b575b005b6040805160206004803580820135601f8101849004840285018401909552848452610024949193602493909291840191908190840183828082843750949650505050505050620186a05a101561014e57610002565b6100db60008054604080516020601f600260001961010060018816150201909516949094049384018190048102820181019092528281529291908301828280156102145780601f106101e957610100808354040283529160200191610214565b60405180806020018281038252838181518152602001915080519060200190808383829060006004602084601f0104600302600f01f150905090810190601f16801561013b5780820380516001836020036101000a031916815260200191505b509250505060405180910390f35b505050565b8060006000509080519060200190828054600181600116156101000203166002900490600052602060002090601f016020900481019282601f106101b557805160ff19168380011785555b506101499291505b808211156101e557600081556001016101a1565b82800160010185558215610199579182015b828111156101995782518260005055916020019190600101906101c7565b5090565b820191906000526020600020905b8154815290600101906020018083116101f757829003601f168201915b50505050508156"

// DeployFunkyGasPattern deploys a new Ethereum contract, binding an instance of FunkyGasPattern to it.
func DeployFunkyGasPattern(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *FunkyGasPattern, error) {
	parsed, err := abi.JSON(strings.NewReader(FunkyGasPatternABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(FunkyGasPatternBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &FunkyGasPattern{FunkyGasPatternCaller: FunkyGasPatternCaller{contract: contract}, FunkyGasPatternTransactor: FunkyGasPatternTransactor{contract: contract}, FunkyGasPatternFilterer: FunkyGasPatternFilterer{contract: contract}}, nil
}

// FunkyGasPattern is an auto generated Go binding around an Ethereum contract.
type FunkyGasPattern struct {
	FunkyGasPatternCaller     // Read-only binding to the contract
	FunkyGasPatternTransactor // Write-only binding to the contract
	FunkyGasPatternFilterer   // Log filterer for contract events
}

// FunkyGasPatternCaller is an auto generated read-only Go binding around an Ethereum contract.
type FunkyGasPatternCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// FunkyGasPatternTransactor is an auto generated write-only Go binding around an Ethereum contract.
type FunkyGasPatternTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// FunkyGasPatternFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type FunkyGasPatternFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// FunkyGasPatternSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type FunkyGasPatternSession struct {
	Contract     *FunkyGasPattern  // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// FunkyGasPatternCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type FunkyGasPatternCallerSession struct {
	Contract *FunkyGasPatternCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts          // Call options to use throughout this session
}

// FunkyGasPatternTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type FunkyGasPatternTransactorSession struct {
	Contract     *FunkyGasPatternTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts          // Transaction auth options to use throughout this session
}

// FunkyGasPatternRaw is an auto generated low-level Go binding around an Ethereum contract.
type FunkyGasPatternRaw struct {
	Contract *FunkyGasPattern // Generic contract binding to access the raw methods on
}

// FunkyGasPatternCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type FunkyGasPatternCallerRaw struct {
	Contract *FunkyGasPatternCaller // Generic read-only contract binding to access the raw methods on
}

// FunkyGasPatternTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type FunkyGasPatternTransactorRaw struct {
	Contract *FunkyGasPatternTransactor // Generic write-only contract binding to access the raw methods on
}

// NewFunkyGasPattern creates a new instance of FunkyGasPattern, bound to a specific deployed contract.
func NewFunkyGasPattern(address common.Address, backend bind.ContractBackend) (*FunkyGasPattern, error) {
	contract, err := bindFunkyGasPattern(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &FunkyGasPattern{FunkyGasPatternCaller: FunkyGasPatternCaller{contract: contract}, FunkyGasPatternTransactor: FunkyGasPatternTransactor{contract: contract}, FunkyGasPatternFilterer: FunkyGasPatternFilterer{contract: contract}}, nil
}

// NewFunkyGasPatternCaller creates a new read-only instance of FunkyGasPattern, bound to a specific deployed contract.
func NewFunkyGasPatternCaller(address common.Address, caller bind.ContractCaller) (*FunkyGasPatternCaller, error) {
	contract, err := bindFunkyGasPattern(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &FunkyGasPatternCaller{contract: contract}, nil
}

// NewFunkyGasPatternTransactor creates a new write-only instance of FunkyGasPattern, bound to a specific deployed contract.
func NewFunkyGasPatternTransactor(address common.Address, transactor bind.ContractTransactor) (*FunkyGasPatternTransactor, error) {
	contract, err := bindFunkyGasPattern(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &FunkyGasPatternTransactor{contract: contract}, nil
}

// NewFunkyGasPatternFilterer creates a new log filterer instance of FunkyGasPattern, bound to a specific deployed contract.
func NewFunkyGasPatternFilterer(address common.Address, filterer bind.ContractFilterer) (*FunkyGasPatternFilterer, error) {
	contract, err := bindFunkyGasPattern(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &FunkyGasPatternFilterer{contract: contract}, nil
}

// bindFunkyGasPattern binds a generic wrapper to an already deployed contract.
func bindFunkyGasPattern(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(FunkyGasPatternABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_FunkyGasPattern *FunkyGasPatternRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _FunkyGasPattern.Contract.FunkyGasPatternCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_FunkyGasPattern *FunkyGasPatternRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _FunkyGasPattern.Contract.FunkyGasPatternTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_FunkyGasPattern *FunkyGasPatternRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _FunkyGasPattern.Contract.FunkyGasPatternTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_FunkyGasPattern *FunkyGasPatternCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _FunkyGasPattern.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_FunkyGasPattern *FunkyGasPatternTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _FunkyGasPattern.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_FunkyGasPattern *FunkyGasPatternTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _FunkyGasPattern.Contract.contract.Transact(opts, method, params...)
}

// Field is a free data retrieval call binding the contract method 0x4f28bf0e.
//
// Solidity: function field() returns(string)
func (_FunkyGasPattern *FunkyGasPatternCaller) Field(opts *bind.CallOpts) (string, error) {
	var out []interface{}
	err := _FunkyGasPattern.contract.Call(opts, &out, "field")

	if err != nil {
		return *new(string), err
	}

	out0 := *abi.ConvertType(out[0], new(string)).(*string)

	return out0, err

}

// Field is a free data retrieval call binding the contract method 0x4f28bf0e.
//
// Solidity: function field() returns(string)
func (_FunkyGasPattern *FunkyGasPatternSession) Field() (string, error) {
	return _FunkyGasPattern.Contract.Field(&_FunkyGasPattern.CallOpts)
}

// Field is a free data retrieval call binding the contract method 0x4f28bf0e.
//
// Solidity: function field() returns(string)
func (_FunkyGasPattern *FunkyGasPatternCallerSession) Field() (string, error) {
	return _FunkyGasPattern.Contract.Field(&_FunkyGasPattern.CallOpts)
}

// SetField is a paid mutator transaction binding the contract method 0x23fcf32a.
//
// Solidity: function SetField(string value) returns()
func (_FunkyGasPattern *FunkyGasPatternTransactor) SetField(opts *bind.TransactOpts, value string) (*types.Transaction, error) {
	return _FunkyGasPattern.contract.Transact(opts, "SetField", value)
}

// SetField is a paid mutator transaction binding the contract method 0x23fcf32a.
//
// Solidity: function SetField(string value) returns()
func (_FunkyGasPattern *FunkyGasPatternSession) SetField(value string) (*types.Transaction, error) {
	return _FunkyGasPattern.Contract.SetField(&_FunkyGasPattern.TransactOpts, value)
}

// SetField is a paid mutator transaction binding the contract method 0x23fcf32a.
//
// Solidity: function SetField(string value) returns()
func (_FunkyGasPattern *FunkyGasPatternTransactorSession) SetField(value string) (*types.Transaction, error) {
	return _FunkyGasPattern.Contract.SetField(&_FunkyGasPattern.TransactOpts, value)
}
