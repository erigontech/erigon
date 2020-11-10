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

// IdentifierCollisionABI is the input ABI used to generate the binding from.
const IdentifierCollisionABI = "[{\"constant\":true,\"inputs\":[],\"name\":\"MyVar\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"_myVar\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"}]"

// IdentifierCollisionBin is the compiled bytecode used for deploying new contracts.
var IdentifierCollisionBin = "0x60806040523480156100115760006000fd5b50610017565b60c3806100256000396000f3fe608060405234801560105760006000fd5b506004361060365760003560e01c806301ad4d8714603c5780634ef1f0ad146058576036565b60006000fd5b60426074565b6040518082815260200191505060405180910390f35b605e607d565b6040518082815260200191505060405180910390f35b60006000505481565b60006000600050549050608b565b9056fea265627a7a7231582067c8d84688b01c4754ba40a2a871cede94ea1f28b5981593ab2a45b46ac43af664736f6c634300050c0032"

// DeployIdentifierCollision deploys a new Ethereum contract, binding an instance of IdentifierCollision to it.
func DeployIdentifierCollision(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *IdentifierCollision, error) {
	parsed, err := abi.JSON(strings.NewReader(IdentifierCollisionABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(IdentifierCollisionBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &IdentifierCollision{IdentifierCollisionCaller: IdentifierCollisionCaller{contract: contract}, IdentifierCollisionTransactor: IdentifierCollisionTransactor{contract: contract}, IdentifierCollisionFilterer: IdentifierCollisionFilterer{contract: contract}}, nil
}

// IdentifierCollision is an auto generated Go binding around an Ethereum contract.
type IdentifierCollision struct {
	IdentifierCollisionCaller     // Read-only binding to the contract
	IdentifierCollisionTransactor // Write-only binding to the contract
	IdentifierCollisionFilterer   // Log filterer for contract events
}

// IdentifierCollisionCaller is an auto generated read-only Go binding around an Ethereum contract.
type IdentifierCollisionCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// IdentifierCollisionTransactor is an auto generated write-only Go binding around an Ethereum contract.
type IdentifierCollisionTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// IdentifierCollisionFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type IdentifierCollisionFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// IdentifierCollisionSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type IdentifierCollisionSession struct {
	Contract     *IdentifierCollision // Generic contract binding to set the session for
	CallOpts     bind.CallOpts        // Call options to use throughout this session
	TransactOpts bind.TransactOpts    // Transaction auth options to use throughout this session
}

// IdentifierCollisionCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type IdentifierCollisionCallerSession struct {
	Contract *IdentifierCollisionCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts              // Call options to use throughout this session
}

// IdentifierCollisionTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type IdentifierCollisionTransactorSession struct {
	Contract     *IdentifierCollisionTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts              // Transaction auth options to use throughout this session
}

// IdentifierCollisionRaw is an auto generated low-level Go binding around an Ethereum contract.
type IdentifierCollisionRaw struct {
	Contract *IdentifierCollision // Generic contract binding to access the raw methods on
}

// IdentifierCollisionCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type IdentifierCollisionCallerRaw struct {
	Contract *IdentifierCollisionCaller // Generic read-only contract binding to access the raw methods on
}

// IdentifierCollisionTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type IdentifierCollisionTransactorRaw struct {
	Contract *IdentifierCollisionTransactor // Generic write-only contract binding to access the raw methods on
}

// NewIdentifierCollision creates a new instance of IdentifierCollision, bound to a specific deployed contract.
func NewIdentifierCollision(address common.Address, backend bind.ContractBackend) (*IdentifierCollision, error) {
	contract, err := bindIdentifierCollision(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &IdentifierCollision{IdentifierCollisionCaller: IdentifierCollisionCaller{contract: contract}, IdentifierCollisionTransactor: IdentifierCollisionTransactor{contract: contract}, IdentifierCollisionFilterer: IdentifierCollisionFilterer{contract: contract}}, nil
}

// NewIdentifierCollisionCaller creates a new read-only instance of IdentifierCollision, bound to a specific deployed contract.
func NewIdentifierCollisionCaller(address common.Address, caller bind.ContractCaller) (*IdentifierCollisionCaller, error) {
	contract, err := bindIdentifierCollision(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &IdentifierCollisionCaller{contract: contract}, nil
}

// NewIdentifierCollisionTransactor creates a new write-only instance of IdentifierCollision, bound to a specific deployed contract.
func NewIdentifierCollisionTransactor(address common.Address, transactor bind.ContractTransactor) (*IdentifierCollisionTransactor, error) {
	contract, err := bindIdentifierCollision(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &IdentifierCollisionTransactor{contract: contract}, nil
}

// NewIdentifierCollisionFilterer creates a new log filterer instance of IdentifierCollision, bound to a specific deployed contract.
func NewIdentifierCollisionFilterer(address common.Address, filterer bind.ContractFilterer) (*IdentifierCollisionFilterer, error) {
	contract, err := bindIdentifierCollision(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &IdentifierCollisionFilterer{contract: contract}, nil
}

// bindIdentifierCollision binds a generic wrapper to an already deployed contract.
func bindIdentifierCollision(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(IdentifierCollisionABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_IdentifierCollision *IdentifierCollisionRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _IdentifierCollision.Contract.IdentifierCollisionCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_IdentifierCollision *IdentifierCollisionRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _IdentifierCollision.Contract.IdentifierCollisionTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_IdentifierCollision *IdentifierCollisionRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _IdentifierCollision.Contract.IdentifierCollisionTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_IdentifierCollision *IdentifierCollisionCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _IdentifierCollision.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_IdentifierCollision *IdentifierCollisionTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _IdentifierCollision.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_IdentifierCollision *IdentifierCollisionTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _IdentifierCollision.Contract.contract.Transact(opts, method, params...)
}

// MyVar is a free data retrieval call binding the contract method 0x4ef1f0ad.
//
// Solidity: function MyVar() view returns(uint256)
func (_IdentifierCollision *IdentifierCollisionCaller) MyVar(opts *bind.CallOpts) (*big.Int, error) {
	var out []interface{}
	err := _IdentifierCollision.contract.Call(opts, &out, "MyVar")

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// MyVar is a free data retrieval call binding the contract method 0x4ef1f0ad.
//
// Solidity: function MyVar() view returns(uint256)
func (_IdentifierCollision *IdentifierCollisionSession) MyVar() (*big.Int, error) {
	return _IdentifierCollision.Contract.MyVar(&_IdentifierCollision.CallOpts)
}

// MyVar is a free data retrieval call binding the contract method 0x4ef1f0ad.
//
// Solidity: function MyVar() view returns(uint256)
func (_IdentifierCollision *IdentifierCollisionCallerSession) MyVar() (*big.Int, error) {
	return _IdentifierCollision.Contract.MyVar(&_IdentifierCollision.CallOpts)
}

// PubVar is a free data retrieval call binding the contract method 0x01ad4d87.
//
// Solidity: function _myVar() view returns(uint256)
func (_IdentifierCollision *IdentifierCollisionCaller) PubVar(opts *bind.CallOpts) (*big.Int, error) {
	var out []interface{}
	err := _IdentifierCollision.contract.Call(opts, &out, "_myVar")

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// PubVar is a free data retrieval call binding the contract method 0x01ad4d87.
//
// Solidity: function _myVar() view returns(uint256)
func (_IdentifierCollision *IdentifierCollisionSession) PubVar() (*big.Int, error) {
	return _IdentifierCollision.Contract.PubVar(&_IdentifierCollision.CallOpts)
}

// PubVar is a free data retrieval call binding the contract method 0x01ad4d87.
//
// Solidity: function _myVar() view returns(uint256)
func (_IdentifierCollision *IdentifierCollisionCallerSession) PubVar() (*big.Int, error) {
	return _IdentifierCollision.Contract.PubVar(&_IdentifierCollision.CallOpts)
}
