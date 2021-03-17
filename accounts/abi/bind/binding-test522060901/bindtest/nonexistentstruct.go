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

// NonExistentStructABI is the input ABI used to generate the binding from.
const NonExistentStructABI = "[{\"inputs\":[],\"name\":\"Struct\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"a\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"b\",\"type\":\"uint256\"}],\"stateMutability\":\"pure\",\"type\":\"function\"}]"

// NonExistentStructBin is the compiled bytecode used for deploying new contracts.
var NonExistentStructBin = "0x6080604052348015600f57600080fd5b5060888061001e6000396000f3fe6080604052348015600f57600080fd5b506004361060285760003560e01c8063d5f6622514602d575b600080fd5b6033604c565b6040805192835260208301919091528051918290030190f35b600a809156fea264697066735822beefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeef64736f6c6343decafe0033"

// DeployNonExistentStruct deploys a new Ethereum contract, binding an instance of NonExistentStruct to it.
func DeployNonExistentStruct(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *NonExistentStruct, error) {
	parsed, err := abi.JSON(strings.NewReader(NonExistentStructABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(NonExistentStructBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &NonExistentStruct{NonExistentStructCaller: NonExistentStructCaller{contract: contract}, NonExistentStructTransactor: NonExistentStructTransactor{contract: contract}, NonExistentStructFilterer: NonExistentStructFilterer{contract: contract}}, nil
}

// NonExistentStruct is an auto generated Go binding around an Ethereum contract.
type NonExistentStruct struct {
	NonExistentStructCaller     // Read-only binding to the contract
	NonExistentStructTransactor // Write-only binding to the contract
	NonExistentStructFilterer   // Log filterer for contract events
}

// NonExistentStructCaller is an auto generated read-only Go binding around an Ethereum contract.
type NonExistentStructCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// NonExistentStructTransactor is an auto generated write-only Go binding around an Ethereum contract.
type NonExistentStructTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// NonExistentStructFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type NonExistentStructFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// NonExistentStructSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type NonExistentStructSession struct {
	Contract     *NonExistentStruct // Generic contract binding to set the session for
	CallOpts     bind.CallOpts      // Call options to use throughout this session
	TransactOpts bind.TransactOpts  // Transaction auth options to use throughout this session
}

// NonExistentStructCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type NonExistentStructCallerSession struct {
	Contract *NonExistentStructCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts            // Call options to use throughout this session
}

// NonExistentStructTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type NonExistentStructTransactorSession struct {
	Contract     *NonExistentStructTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts            // Transaction auth options to use throughout this session
}

// NonExistentStructRaw is an auto generated low-level Go binding around an Ethereum contract.
type NonExistentStructRaw struct {
	Contract *NonExistentStruct // Generic contract binding to access the raw methods on
}

// NonExistentStructCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type NonExistentStructCallerRaw struct {
	Contract *NonExistentStructCaller // Generic read-only contract binding to access the raw methods on
}

// NonExistentStructTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type NonExistentStructTransactorRaw struct {
	Contract *NonExistentStructTransactor // Generic write-only contract binding to access the raw methods on
}

// NewNonExistentStruct creates a new instance of NonExistentStruct, bound to a specific deployed contract.
func NewNonExistentStruct(address common.Address, backend bind.ContractBackend) (*NonExistentStruct, error) {
	contract, err := bindNonExistentStruct(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &NonExistentStruct{NonExistentStructCaller: NonExistentStructCaller{contract: contract}, NonExistentStructTransactor: NonExistentStructTransactor{contract: contract}, NonExistentStructFilterer: NonExistentStructFilterer{contract: contract}}, nil
}

// NewNonExistentStructCaller creates a new read-only instance of NonExistentStruct, bound to a specific deployed contract.
func NewNonExistentStructCaller(address common.Address, caller bind.ContractCaller) (*NonExistentStructCaller, error) {
	contract, err := bindNonExistentStruct(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &NonExistentStructCaller{contract: contract}, nil
}

// NewNonExistentStructTransactor creates a new write-only instance of NonExistentStruct, bound to a specific deployed contract.
func NewNonExistentStructTransactor(address common.Address, transactor bind.ContractTransactor) (*NonExistentStructTransactor, error) {
	contract, err := bindNonExistentStruct(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &NonExistentStructTransactor{contract: contract}, nil
}

// NewNonExistentStructFilterer creates a new log filterer instance of NonExistentStruct, bound to a specific deployed contract.
func NewNonExistentStructFilterer(address common.Address, filterer bind.ContractFilterer) (*NonExistentStructFilterer, error) {
	contract, err := bindNonExistentStruct(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &NonExistentStructFilterer{contract: contract}, nil
}

// bindNonExistentStruct binds a generic wrapper to an already deployed contract.
func bindNonExistentStruct(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(NonExistentStructABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_NonExistentStruct *NonExistentStructRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _NonExistentStruct.Contract.NonExistentStructCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_NonExistentStruct *NonExistentStructRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _NonExistentStruct.Contract.NonExistentStructTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_NonExistentStruct *NonExistentStructRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _NonExistentStruct.Contract.NonExistentStructTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_NonExistentStruct *NonExistentStructCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _NonExistentStruct.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_NonExistentStruct *NonExistentStructTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _NonExistentStruct.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_NonExistentStruct *NonExistentStructTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _NonExistentStruct.Contract.contract.Transact(opts, method, params...)
}

// Struct is a free data retrieval call binding the contract method 0xd5f66225.
//
// Solidity: function Struct() pure returns(uint256 a, uint256 b)
func (_NonExistentStruct *NonExistentStructCaller) Struct(opts *bind.CallOpts) (struct {
	A *big.Int
	B *big.Int
}, error) {
	var out []interface{}
	err := _NonExistentStruct.contract.Call(opts, &out, "Struct")

	outstruct := new(struct {
		A *big.Int
		B *big.Int
	})
	if err != nil {
		return *outstruct, err
	}

	outstruct.A = *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)
	outstruct.B = *abi.ConvertType(out[1], new(*big.Int)).(**big.Int)

	return *outstruct, err

}

// Struct is a free data retrieval call binding the contract method 0xd5f66225.
//
// Solidity: function Struct() pure returns(uint256 a, uint256 b)
func (_NonExistentStruct *NonExistentStructSession) Struct() (struct {
	A *big.Int
	B *big.Int
}, error) {
	return _NonExistentStruct.Contract.Struct(&_NonExistentStruct.CallOpts)
}

// Struct is a free data retrieval call binding the contract method 0xd5f66225.
//
// Solidity: function Struct() pure returns(uint256 a, uint256 b)
func (_NonExistentStruct *NonExistentStructCallerSession) Struct() (struct {
	A *big.Int
	B *big.Int
}, error) {
	return _NonExistentStruct.Contract.Struct(&_NonExistentStruct.CallOpts)
}
