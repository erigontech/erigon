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

// PureAndViewABI is the input ABI used to generate the binding from.
const PureAndViewABI = "[{\"inputs\":[],\"name\":\"PureFunc\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"pure\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"ViewFunc\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"}]"

// PureAndViewBin is the compiled bytecode used for deploying new contracts.
var PureAndViewBin = "0x608060405234801561001057600080fd5b5060b68061001f6000396000f3fe6080604052348015600f57600080fd5b506004361060325760003560e01c806376b5686a146037578063bb38c66c146053575b600080fd5b603d606f565b6040518082815260200191505060405180910390f35b60596077565b6040518082815260200191505060405180910390f35b600043905090565b6000602a90509056fea2646970667358221220d158c2ab7fdfce366a7998ec79ab84edd43b9815630bbaede2c760ea77f29f7f64736f6c63430006000033"

// DeployPureAndView deploys a new Ethereum contract, binding an instance of PureAndView to it.
func DeployPureAndView(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *PureAndView, error) {
	parsed, err := abi.JSON(strings.NewReader(PureAndViewABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(PureAndViewBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &PureAndView{PureAndViewCaller: PureAndViewCaller{contract: contract}, PureAndViewTransactor: PureAndViewTransactor{contract: contract}, PureAndViewFilterer: PureAndViewFilterer{contract: contract}}, nil
}

// PureAndView is an auto generated Go binding around an Ethereum contract.
type PureAndView struct {
	PureAndViewCaller     // Read-only binding to the contract
	PureAndViewTransactor // Write-only binding to the contract
	PureAndViewFilterer   // Log filterer for contract events
}

// PureAndViewCaller is an auto generated read-only Go binding around an Ethereum contract.
type PureAndViewCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// PureAndViewTransactor is an auto generated write-only Go binding around an Ethereum contract.
type PureAndViewTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// PureAndViewFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type PureAndViewFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// PureAndViewSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type PureAndViewSession struct {
	Contract     *PureAndView      // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// PureAndViewCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type PureAndViewCallerSession struct {
	Contract *PureAndViewCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts      // Call options to use throughout this session
}

// PureAndViewTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type PureAndViewTransactorSession struct {
	Contract     *PureAndViewTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts      // Transaction auth options to use throughout this session
}

// PureAndViewRaw is an auto generated low-level Go binding around an Ethereum contract.
type PureAndViewRaw struct {
	Contract *PureAndView // Generic contract binding to access the raw methods on
}

// PureAndViewCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type PureAndViewCallerRaw struct {
	Contract *PureAndViewCaller // Generic read-only contract binding to access the raw methods on
}

// PureAndViewTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type PureAndViewTransactorRaw struct {
	Contract *PureAndViewTransactor // Generic write-only contract binding to access the raw methods on
}

// NewPureAndView creates a new instance of PureAndView, bound to a specific deployed contract.
func NewPureAndView(address common.Address, backend bind.ContractBackend) (*PureAndView, error) {
	contract, err := bindPureAndView(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &PureAndView{PureAndViewCaller: PureAndViewCaller{contract: contract}, PureAndViewTransactor: PureAndViewTransactor{contract: contract}, PureAndViewFilterer: PureAndViewFilterer{contract: contract}}, nil
}

// NewPureAndViewCaller creates a new read-only instance of PureAndView, bound to a specific deployed contract.
func NewPureAndViewCaller(address common.Address, caller bind.ContractCaller) (*PureAndViewCaller, error) {
	contract, err := bindPureAndView(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &PureAndViewCaller{contract: contract}, nil
}

// NewPureAndViewTransactor creates a new write-only instance of PureAndView, bound to a specific deployed contract.
func NewPureAndViewTransactor(address common.Address, transactor bind.ContractTransactor) (*PureAndViewTransactor, error) {
	contract, err := bindPureAndView(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &PureAndViewTransactor{contract: contract}, nil
}

// NewPureAndViewFilterer creates a new log filterer instance of PureAndView, bound to a specific deployed contract.
func NewPureAndViewFilterer(address common.Address, filterer bind.ContractFilterer) (*PureAndViewFilterer, error) {
	contract, err := bindPureAndView(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &PureAndViewFilterer{contract: contract}, nil
}

// bindPureAndView binds a generic wrapper to an already deployed contract.
func bindPureAndView(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(PureAndViewABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_PureAndView *PureAndViewRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _PureAndView.Contract.PureAndViewCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_PureAndView *PureAndViewRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _PureAndView.Contract.PureAndViewTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_PureAndView *PureAndViewRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _PureAndView.Contract.PureAndViewTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_PureAndView *PureAndViewCallerRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _PureAndView.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_PureAndView *PureAndViewTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _PureAndView.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_PureAndView *PureAndViewTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _PureAndView.Contract.contract.Transact(opts, method, params...)
}

// PureFunc is a free data retrieval call binding the contract method 0xbb38c66c.
//
// Solidity: function PureFunc() pure returns(uint256)
func (_PureAndView *PureAndViewCaller) PureFunc(opts *bind.CallOpts) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _PureAndView.contract.Call(opts, out, "PureFunc")
	return *ret0, err
}

// PureFunc is a free data retrieval call binding the contract method 0xbb38c66c.
//
// Solidity: function PureFunc() pure returns(uint256)
func (_PureAndView *PureAndViewSession) PureFunc() (*big.Int, error) {
	return _PureAndView.Contract.PureFunc(&_PureAndView.CallOpts)
}

// PureFunc is a free data retrieval call binding the contract method 0xbb38c66c.
//
// Solidity: function PureFunc() pure returns(uint256)
func (_PureAndView *PureAndViewCallerSession) PureFunc() (*big.Int, error) {
	return _PureAndView.Contract.PureFunc(&_PureAndView.CallOpts)
}

// ViewFunc is a free data retrieval call binding the contract method 0x76b5686a.
//
// Solidity: function ViewFunc() view returns(uint256)
func (_PureAndView *PureAndViewCaller) ViewFunc(opts *bind.CallOpts) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _PureAndView.contract.Call(opts, out, "ViewFunc")
	return *ret0, err
}

// ViewFunc is a free data retrieval call binding the contract method 0x76b5686a.
//
// Solidity: function ViewFunc() view returns(uint256)
func (_PureAndView *PureAndViewSession) ViewFunc() (*big.Int, error) {
	return _PureAndView.Contract.ViewFunc(&_PureAndView.CallOpts)
}

// ViewFunc is a free data retrieval call binding the contract method 0x76b5686a.
//
// Solidity: function ViewFunc() view returns(uint256)
func (_PureAndView *PureAndViewCallerSession) ViewFunc() (*big.Int, error) {
	return _PureAndView.Contract.ViewFunc(&_PureAndView.CallOpts)
}
