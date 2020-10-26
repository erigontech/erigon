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

// GetterABI is the input ABI used to generate the binding from.
const GetterABI = "[{\"constant\":true,\"inputs\":[],\"name\":\"getter\",\"outputs\":[{\"name\":\"\",\"type\":\"string\"},{\"name\":\"\",\"type\":\"int256\"},{\"name\":\"\",\"type\":\"bytes32\"}],\"type\":\"function\"}]"

// GetterBin is the compiled bytecode used for deploying new contracts.
var GetterBin = "0x606060405260dc8060106000396000f3606060405260e060020a6000350463993a04b78114601a575b005b600060605260c0604052600260809081527f486900000000000000000000000000000000000000000000000000000000000060a05260017fc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47060e0829052610100819052606060c0908152600261012081905281906101409060a09080838184600060046012f1505081517fffff000000000000000000000000000000000000000000000000000000000000169091525050604051610160819003945092505050f3"

// DeployGetter deploys a new Ethereum contract, binding an instance of Getter to it.
func DeployGetter(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *Getter, error) {
	parsed, err := abi.JSON(strings.NewReader(GetterABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(GetterBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &Getter{GetterCaller: GetterCaller{contract: contract}, GetterTransactor: GetterTransactor{contract: contract}, GetterFilterer: GetterFilterer{contract: contract}}, nil
}

// Getter is an auto generated Go binding around an Ethereum contract.
type Getter struct {
	GetterCaller     // Read-only binding to the contract
	GetterTransactor // Write-only binding to the contract
	GetterFilterer   // Log filterer for contract events
}

// GetterCaller is an auto generated read-only Go binding around an Ethereum contract.
type GetterCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// GetterTransactor is an auto generated write-only Go binding around an Ethereum contract.
type GetterTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// GetterFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type GetterFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// GetterSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type GetterSession struct {
	Contract     *Getter           // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// GetterCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type GetterCallerSession struct {
	Contract *GetterCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts // Call options to use throughout this session
}

// GetterTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type GetterTransactorSession struct {
	Contract     *GetterTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// GetterRaw is an auto generated low-level Go binding around an Ethereum contract.
type GetterRaw struct {
	Contract *Getter // Generic contract binding to access the raw methods on
}

// GetterCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type GetterCallerRaw struct {
	Contract *GetterCaller // Generic read-only contract binding to access the raw methods on
}

// GetterTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type GetterTransactorRaw struct {
	Contract *GetterTransactor // Generic write-only contract binding to access the raw methods on
}

// NewGetter creates a new instance of Getter, bound to a specific deployed contract.
func NewGetter(address common.Address, backend bind.ContractBackend) (*Getter, error) {
	contract, err := bindGetter(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Getter{GetterCaller: GetterCaller{contract: contract}, GetterTransactor: GetterTransactor{contract: contract}, GetterFilterer: GetterFilterer{contract: contract}}, nil
}

// NewGetterCaller creates a new read-only instance of Getter, bound to a specific deployed contract.
func NewGetterCaller(address common.Address, caller bind.ContractCaller) (*GetterCaller, error) {
	contract, err := bindGetter(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &GetterCaller{contract: contract}, nil
}

// NewGetterTransactor creates a new write-only instance of Getter, bound to a specific deployed contract.
func NewGetterTransactor(address common.Address, transactor bind.ContractTransactor) (*GetterTransactor, error) {
	contract, err := bindGetter(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &GetterTransactor{contract: contract}, nil
}

// NewGetterFilterer creates a new log filterer instance of Getter, bound to a specific deployed contract.
func NewGetterFilterer(address common.Address, filterer bind.ContractFilterer) (*GetterFilterer, error) {
	contract, err := bindGetter(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &GetterFilterer{contract: contract}, nil
}

// bindGetter binds a generic wrapper to an already deployed contract.
func bindGetter(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(GetterABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Getter *GetterRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Getter.Contract.GetterCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Getter *GetterRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Getter.Contract.GetterTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Getter *GetterRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Getter.Contract.GetterTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Getter *GetterCallerRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Getter.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Getter *GetterTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Getter.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Getter *GetterTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Getter.Contract.contract.Transact(opts, method, params...)
}

// Getter is a free data retrieval call binding the contract method 0x993a04b7.
//
// Solidity: function getter() returns(string, int256, bytes32)
func (_Getter *GetterCaller) Getter(opts *bind.CallOpts) (string, *big.Int, [32]byte, error) {
	var (
		ret0 = new(string)
		ret1 = new(*big.Int)
		ret2 = new([32]byte)
	)
	out := &[]interface{}{
		ret0,
		ret1,
		ret2,
	}
	err := _Getter.contract.Call(opts, out, "getter")
	return *ret0, *ret1, *ret2, err
}

// Getter is a free data retrieval call binding the contract method 0x993a04b7.
//
// Solidity: function getter() returns(string, int256, bytes32)
func (_Getter *GetterSession) Getter() (string, *big.Int, [32]byte, error) {
	return _Getter.Contract.Getter(&_Getter.CallOpts)
}

// Getter is a free data retrieval call binding the contract method 0x993a04b7.
//
// Solidity: function getter() returns(string, int256, bytes32)
func (_Getter *GetterCallerSession) Getter() (string, *big.Int, [32]byte, error) {
	return _Getter.Contract.Getter(&_Getter.CallOpts)
}
