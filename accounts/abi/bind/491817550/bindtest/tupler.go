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

// TuplerABI is the input ABI used to generate the binding from.
const TuplerABI = "[{\"constant\":true,\"inputs\":[],\"name\":\"tuple\",\"outputs\":[{\"name\":\"a\",\"type\":\"string\"},{\"name\":\"b\",\"type\":\"int256\"},{\"name\":\"c\",\"type\":\"bytes32\"}],\"type\":\"function\"}]"

// TuplerBin is the compiled bytecode used for deploying new contracts.
var TuplerBin = "0x606060405260dc8060106000396000f3606060405260e060020a60003504633175aae28114601a575b005b600060605260c0604052600260809081527f486900000000000000000000000000000000000000000000000000000000000060a05260017fc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47060e0829052610100819052606060c0908152600261012081905281906101409060a09080838184600060046012f1505081517fffff000000000000000000000000000000000000000000000000000000000000169091525050604051610160819003945092505050f3"

// DeployTupler deploys a new Ethereum contract, binding an instance of Tupler to it.
func DeployTupler(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *Tupler, error) {
	parsed, err := abi.JSON(strings.NewReader(TuplerABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(TuplerBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &Tupler{TuplerCaller: TuplerCaller{contract: contract}, TuplerTransactor: TuplerTransactor{contract: contract}, TuplerFilterer: TuplerFilterer{contract: contract}}, nil
}

// Tupler is an auto generated Go binding around an Ethereum contract.
type Tupler struct {
	TuplerCaller     // Read-only binding to the contract
	TuplerTransactor // Write-only binding to the contract
	TuplerFilterer   // Log filterer for contract events
}

// TuplerCaller is an auto generated read-only Go binding around an Ethereum contract.
type TuplerCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// TuplerTransactor is an auto generated write-only Go binding around an Ethereum contract.
type TuplerTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// TuplerFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type TuplerFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// TuplerSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type TuplerSession struct {
	Contract     *Tupler           // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// TuplerCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type TuplerCallerSession struct {
	Contract *TuplerCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts // Call options to use throughout this session
}

// TuplerTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type TuplerTransactorSession struct {
	Contract     *TuplerTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// TuplerRaw is an auto generated low-level Go binding around an Ethereum contract.
type TuplerRaw struct {
	Contract *Tupler // Generic contract binding to access the raw methods on
}

// TuplerCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type TuplerCallerRaw struct {
	Contract *TuplerCaller // Generic read-only contract binding to access the raw methods on
}

// TuplerTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type TuplerTransactorRaw struct {
	Contract *TuplerTransactor // Generic write-only contract binding to access the raw methods on
}

// NewTupler creates a new instance of Tupler, bound to a specific deployed contract.
func NewTupler(address common.Address, backend bind.ContractBackend) (*Tupler, error) {
	contract, err := bindTupler(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Tupler{TuplerCaller: TuplerCaller{contract: contract}, TuplerTransactor: TuplerTransactor{contract: contract}, TuplerFilterer: TuplerFilterer{contract: contract}}, nil
}

// NewTuplerCaller creates a new read-only instance of Tupler, bound to a specific deployed contract.
func NewTuplerCaller(address common.Address, caller bind.ContractCaller) (*TuplerCaller, error) {
	contract, err := bindTupler(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &TuplerCaller{contract: contract}, nil
}

// NewTuplerTransactor creates a new write-only instance of Tupler, bound to a specific deployed contract.
func NewTuplerTransactor(address common.Address, transactor bind.ContractTransactor) (*TuplerTransactor, error) {
	contract, err := bindTupler(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &TuplerTransactor{contract: contract}, nil
}

// NewTuplerFilterer creates a new log filterer instance of Tupler, bound to a specific deployed contract.
func NewTuplerFilterer(address common.Address, filterer bind.ContractFilterer) (*TuplerFilterer, error) {
	contract, err := bindTupler(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &TuplerFilterer{contract: contract}, nil
}

// bindTupler binds a generic wrapper to an already deployed contract.
func bindTupler(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(TuplerABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Tupler *TuplerRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Tupler.Contract.TuplerCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Tupler *TuplerRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Tupler.Contract.TuplerTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Tupler *TuplerRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Tupler.Contract.TuplerTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Tupler *TuplerCallerRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Tupler.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Tupler *TuplerTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Tupler.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Tupler *TuplerTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Tupler.Contract.contract.Transact(opts, method, params...)
}

// Tuple is a free data retrieval call binding the contract method 0x3175aae2.
//
// Solidity: function tuple() returns(string a, int256 b, bytes32 c)
func (_Tupler *TuplerCaller) Tuple(opts *bind.CallOpts) (struct {
	A string
	B *big.Int
	C [32]byte
}, error) {
	ret := new(struct {
		A string
		B *big.Int
		C [32]byte
	})
	out := ret
	err := _Tupler.contract.Call(opts, out, "tuple")
	return *ret, err
}

// Tuple is a free data retrieval call binding the contract method 0x3175aae2.
//
// Solidity: function tuple() returns(string a, int256 b, bytes32 c)
func (_Tupler *TuplerSession) Tuple() (struct {
	A string
	B *big.Int
	C [32]byte
}, error) {
	return _Tupler.Contract.Tuple(&_Tupler.CallOpts)
}

// Tuple is a free data retrieval call binding the contract method 0x3175aae2.
//
// Solidity: function tuple() returns(string a, int256 b, bytes32 c)
func (_Tupler *TuplerCallerSession) Tuple() (struct {
	A string
	B *big.Int
	C [32]byte
}, error) {
	return _Tupler.Contract.Tuple(&_Tupler.CallOpts)
}
