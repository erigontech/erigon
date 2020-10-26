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

// SlicerABI is the input ABI used to generate the binding from.
const SlicerABI = "[{\"constant\":true,\"inputs\":[{\"name\":\"input\",\"type\":\"address[]\"}],\"name\":\"echoAddresses\",\"outputs\":[{\"name\":\"output\",\"type\":\"address[]\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[{\"name\":\"input\",\"type\":\"uint24[23]\"}],\"name\":\"echoFancyInts\",\"outputs\":[{\"name\":\"output\",\"type\":\"uint24[23]\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[{\"name\":\"input\",\"type\":\"int256[]\"}],\"name\":\"echoInts\",\"outputs\":[{\"name\":\"output\",\"type\":\"int256[]\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[{\"name\":\"input\",\"type\":\"bool[]\"}],\"name\":\"echoBools\",\"outputs\":[{\"name\":\"output\",\"type\":\"bool[]\"}],\"type\":\"function\"}]"

// SlicerBin is the compiled bytecode used for deploying new contracts.
var SlicerBin = "0x606060405261015c806100126000396000f3606060405260e060020a6000350463be1127a3811461003c578063d88becc014610092578063e15a3db71461003c578063f637e5891461003c575b005b604080516020600480358082013583810285810185019096528085526100ee959294602494909392850192829185019084908082843750949650505050505050604080516020810190915260009052805b919050565b604080516102e0818101909252610138916004916102e491839060179083908390808284375090955050505050506102e0604051908101604052806017905b60008152602001906001900390816100d15790505081905061008d565b60405180806020018281038252838181518152602001915080519060200190602002808383829060006004602084601f0104600f02600301f1509050019250505060405180910390f35b60405180826102e0808381846000600461015cf15090500191505060405180910390f3"

// DeploySlicer deploys a new Ethereum contract, binding an instance of Slicer to it.
func DeploySlicer(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *Slicer, error) {
	parsed, err := abi.JSON(strings.NewReader(SlicerABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(SlicerBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &Slicer{SlicerCaller: SlicerCaller{contract: contract}, SlicerTransactor: SlicerTransactor{contract: contract}, SlicerFilterer: SlicerFilterer{contract: contract}}, nil
}

// Slicer is an auto generated Go binding around an Ethereum contract.
type Slicer struct {
	SlicerCaller     // Read-only binding to the contract
	SlicerTransactor // Write-only binding to the contract
	SlicerFilterer   // Log filterer for contract events
}

// SlicerCaller is an auto generated read-only Go binding around an Ethereum contract.
type SlicerCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// SlicerTransactor is an auto generated write-only Go binding around an Ethereum contract.
type SlicerTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// SlicerFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type SlicerFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// SlicerSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type SlicerSession struct {
	Contract     *Slicer           // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// SlicerCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type SlicerCallerSession struct {
	Contract *SlicerCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts // Call options to use throughout this session
}

// SlicerTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type SlicerTransactorSession struct {
	Contract     *SlicerTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// SlicerRaw is an auto generated low-level Go binding around an Ethereum contract.
type SlicerRaw struct {
	Contract *Slicer // Generic contract binding to access the raw methods on
}

// SlicerCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type SlicerCallerRaw struct {
	Contract *SlicerCaller // Generic read-only contract binding to access the raw methods on
}

// SlicerTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type SlicerTransactorRaw struct {
	Contract *SlicerTransactor // Generic write-only contract binding to access the raw methods on
}

// NewSlicer creates a new instance of Slicer, bound to a specific deployed contract.
func NewSlicer(address common.Address, backend bind.ContractBackend) (*Slicer, error) {
	contract, err := bindSlicer(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Slicer{SlicerCaller: SlicerCaller{contract: contract}, SlicerTransactor: SlicerTransactor{contract: contract}, SlicerFilterer: SlicerFilterer{contract: contract}}, nil
}

// NewSlicerCaller creates a new read-only instance of Slicer, bound to a specific deployed contract.
func NewSlicerCaller(address common.Address, caller bind.ContractCaller) (*SlicerCaller, error) {
	contract, err := bindSlicer(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &SlicerCaller{contract: contract}, nil
}

// NewSlicerTransactor creates a new write-only instance of Slicer, bound to a specific deployed contract.
func NewSlicerTransactor(address common.Address, transactor bind.ContractTransactor) (*SlicerTransactor, error) {
	contract, err := bindSlicer(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &SlicerTransactor{contract: contract}, nil
}

// NewSlicerFilterer creates a new log filterer instance of Slicer, bound to a specific deployed contract.
func NewSlicerFilterer(address common.Address, filterer bind.ContractFilterer) (*SlicerFilterer, error) {
	contract, err := bindSlicer(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &SlicerFilterer{contract: contract}, nil
}

// bindSlicer binds a generic wrapper to an already deployed contract.
func bindSlicer(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(SlicerABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Slicer *SlicerRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Slicer.Contract.SlicerCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Slicer *SlicerRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Slicer.Contract.SlicerTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Slicer *SlicerRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Slicer.Contract.SlicerTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Slicer *SlicerCallerRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Slicer.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Slicer *SlicerTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Slicer.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Slicer *SlicerTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Slicer.Contract.contract.Transact(opts, method, params...)
}

// EchoAddresses is a free data retrieval call binding the contract method 0xbe1127a3.
//
// Solidity: function echoAddresses(address[] input) returns(address[] output)
func (_Slicer *SlicerCaller) EchoAddresses(opts *bind.CallOpts, input []common.Address) ([]common.Address, error) {
	var (
		ret0 = new([]common.Address)
	)
	out := ret0
	err := _Slicer.contract.Call(opts, out, "echoAddresses", input)
	return *ret0, err
}

// EchoAddresses is a free data retrieval call binding the contract method 0xbe1127a3.
//
// Solidity: function echoAddresses(address[] input) returns(address[] output)
func (_Slicer *SlicerSession) EchoAddresses(input []common.Address) ([]common.Address, error) {
	return _Slicer.Contract.EchoAddresses(&_Slicer.CallOpts, input)
}

// EchoAddresses is a free data retrieval call binding the contract method 0xbe1127a3.
//
// Solidity: function echoAddresses(address[] input) returns(address[] output)
func (_Slicer *SlicerCallerSession) EchoAddresses(input []common.Address) ([]common.Address, error) {
	return _Slicer.Contract.EchoAddresses(&_Slicer.CallOpts, input)
}

// EchoBools is a free data retrieval call binding the contract method 0xf637e589.
//
// Solidity: function echoBools(bool[] input) returns(bool[] output)
func (_Slicer *SlicerCaller) EchoBools(opts *bind.CallOpts, input []bool) ([]bool, error) {
	var (
		ret0 = new([]bool)
	)
	out := ret0
	err := _Slicer.contract.Call(opts, out, "echoBools", input)
	return *ret0, err
}

// EchoBools is a free data retrieval call binding the contract method 0xf637e589.
//
// Solidity: function echoBools(bool[] input) returns(bool[] output)
func (_Slicer *SlicerSession) EchoBools(input []bool) ([]bool, error) {
	return _Slicer.Contract.EchoBools(&_Slicer.CallOpts, input)
}

// EchoBools is a free data retrieval call binding the contract method 0xf637e589.
//
// Solidity: function echoBools(bool[] input) returns(bool[] output)
func (_Slicer *SlicerCallerSession) EchoBools(input []bool) ([]bool, error) {
	return _Slicer.Contract.EchoBools(&_Slicer.CallOpts, input)
}

// EchoFancyInts is a free data retrieval call binding the contract method 0xd88becc0.
//
// Solidity: function echoFancyInts(uint24[23] input) returns(uint24[23] output)
func (_Slicer *SlicerCaller) EchoFancyInts(opts *bind.CallOpts, input [23]*big.Int) ([23]*big.Int, error) {
	var (
		ret0 = new([23]*big.Int)
	)
	out := ret0
	err := _Slicer.contract.Call(opts, out, "echoFancyInts", input)
	return *ret0, err
}

// EchoFancyInts is a free data retrieval call binding the contract method 0xd88becc0.
//
// Solidity: function echoFancyInts(uint24[23] input) returns(uint24[23] output)
func (_Slicer *SlicerSession) EchoFancyInts(input [23]*big.Int) ([23]*big.Int, error) {
	return _Slicer.Contract.EchoFancyInts(&_Slicer.CallOpts, input)
}

// EchoFancyInts is a free data retrieval call binding the contract method 0xd88becc0.
//
// Solidity: function echoFancyInts(uint24[23] input) returns(uint24[23] output)
func (_Slicer *SlicerCallerSession) EchoFancyInts(input [23]*big.Int) ([23]*big.Int, error) {
	return _Slicer.Contract.EchoFancyInts(&_Slicer.CallOpts, input)
}

// EchoInts is a free data retrieval call binding the contract method 0xe15a3db7.
//
// Solidity: function echoInts(int256[] input) returns(int256[] output)
func (_Slicer *SlicerCaller) EchoInts(opts *bind.CallOpts, input []*big.Int) ([]*big.Int, error) {
	var (
		ret0 = new([]*big.Int)
	)
	out := ret0
	err := _Slicer.contract.Call(opts, out, "echoInts", input)
	return *ret0, err
}

// EchoInts is a free data retrieval call binding the contract method 0xe15a3db7.
//
// Solidity: function echoInts(int256[] input) returns(int256[] output)
func (_Slicer *SlicerSession) EchoInts(input []*big.Int) ([]*big.Int, error) {
	return _Slicer.Contract.EchoInts(&_Slicer.CallOpts, input)
}

// EchoInts is a free data retrieval call binding the contract method 0xe15a3db7.
//
// Solidity: function echoInts(int256[] input) returns(int256[] output)
func (_Slicer *SlicerCallerSession) EchoInts(input []*big.Int) ([]*big.Int, error) {
	return _Slicer.Contract.EchoInts(&_Slicer.CallOpts, input)
}
