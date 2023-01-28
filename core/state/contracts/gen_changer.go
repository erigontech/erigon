// Code generated - DO NOT EDIT.
// This file is a generated binding and any manual changes will be lost.

package contracts

import (
	"math/big"
	"strings"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	ethereum "github.com/ledgerwatch/erigon"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/event"
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

// ChangerABI is the input ABI used to generate the binding from.
const ChangerABI = "[{\"inputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"constructor\"},{\"inputs\":[],\"name\":\"change\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"}]"

// ChangerBin is the compiled bytecode used for deploying new contracts.
var ChangerBin = "0x6080604052348015600f57600080fd5b50607e8061001e6000396000f3fe6080604052348015600f57600080fd5b506004361060285760003560e01c80632ee79ded14602d575b600080fd5b60336035565b005b600160008190556002908190556003905556fea264697066735822122055759a7d66bd94e58f9e8393e991422147044bd5fddc39451c4ef60edbcfa29264736f6c63430007020033"

// DeployChanger deploys a new Ethereum contract, binding an instance of Changer to it.
func DeployChanger(auth *bind.TransactOpts, backend bind.ContractBackend) (libcommon.Address, types.Transaction, *Changer, error) {
	parsed, err := abi.JSON(strings.NewReader(ChangerABI))
	if err != nil {
		return libcommon.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(ChangerBin), backend)
	if err != nil {
		return libcommon.Address{}, nil, nil, err
	}
	return address, tx, &Changer{ChangerCaller: ChangerCaller{contract: contract}, ChangerTransactor: ChangerTransactor{contract: contract}, ChangerFilterer: ChangerFilterer{contract: contract}}, nil
}

// Changer is an auto generated Go binding around an Ethereum contract.
type Changer struct {
	ChangerCaller     // Read-only binding to the contract
	ChangerTransactor // Write-only binding to the contract
	ChangerFilterer   // Log filterer for contract events
}

// ChangerCaller is an auto generated read-only Go binding around an Ethereum contract.
type ChangerCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ChangerTransactor is an auto generated write-only Go binding around an Ethereum contract.
type ChangerTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ChangerFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type ChangerFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ChangerSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type ChangerSession struct {
	Contract     *Changer          // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// ChangerCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type ChangerCallerSession struct {
	Contract *ChangerCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts  // Call options to use throughout this session
}

// ChangerTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type ChangerTransactorSession struct {
	Contract     *ChangerTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts  // Transaction auth options to use throughout this session
}

// ChangerRaw is an auto generated low-level Go binding around an Ethereum contract.
type ChangerRaw struct {
	Contract *Changer // Generic contract binding to access the raw methods on
}

// ChangerCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type ChangerCallerRaw struct {
	Contract *ChangerCaller // Generic read-only contract binding to access the raw methods on
}

// ChangerTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type ChangerTransactorRaw struct {
	Contract *ChangerTransactor // Generic write-only contract binding to access the raw methods on
}

// NewChanger creates a new instance of Changer, bound to a specific deployed contract.
func NewChanger(address libcommon.Address, backend bind.ContractBackend) (*Changer, error) {
	contract, err := bindChanger(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Changer{ChangerCaller: ChangerCaller{contract: contract}, ChangerTransactor: ChangerTransactor{contract: contract}, ChangerFilterer: ChangerFilterer{contract: contract}}, nil
}

// NewChangerCaller creates a new read-only instance of Changer, bound to a specific deployed contract.
func NewChangerCaller(address libcommon.Address, caller bind.ContractCaller) (*ChangerCaller, error) {
	contract, err := bindChanger(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &ChangerCaller{contract: contract}, nil
}

// NewChangerTransactor creates a new write-only instance of Changer, bound to a specific deployed contract.
func NewChangerTransactor(address libcommon.Address, transactor bind.ContractTransactor) (*ChangerTransactor, error) {
	contract, err := bindChanger(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &ChangerTransactor{contract: contract}, nil
}

// NewChangerFilterer creates a new log filterer instance of Changer, bound to a specific deployed contract.
func NewChangerFilterer(address libcommon.Address, filterer bind.ContractFilterer) (*ChangerFilterer, error) {
	contract, err := bindChanger(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &ChangerFilterer{contract: contract}, nil
}

// bindChanger binds a generic wrapper to an already deployed contract.
func bindChanger(address libcommon.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(ChangerABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Changer *ChangerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Changer.Contract.ChangerCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Changer *ChangerRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _Changer.Contract.ChangerTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Changer *ChangerRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _Changer.Contract.ChangerTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Changer *ChangerCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Changer.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Changer *ChangerTransactorRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _Changer.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Changer *ChangerTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _Changer.Contract.contract.Transact(opts, method, params...)
}

// Change is a paid mutator transaction binding the contract method 0x2ee79ded.
//
// Solidity: function change() returns()
func (_Changer *ChangerTransactor) Change(opts *bind.TransactOpts) (types.Transaction, error) {
	return _Changer.contract.Transact(opts, "change")
}

// Change is a paid mutator transaction binding the contract method 0x2ee79ded.
//
// Solidity: function change() returns()
func (_Changer *ChangerSession) Change() (types.Transaction, error) {
	return _Changer.Contract.Change(&_Changer.TransactOpts)
}

// Change is a paid mutator transaction binding the contract method 0x2ee79ded.
//
// Solidity: function change() returns()
func (_Changer *ChangerTransactorSession) Change() (types.Transaction, error) {
	return _Changer.Contract.Change(&_Changer.TransactOpts)
}
