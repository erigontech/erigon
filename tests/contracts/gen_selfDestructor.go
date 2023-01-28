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

// SelfDestructorABI is the input ABI used to generate the binding from.
const SelfDestructorABI = "[{\"inputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"constructor\"},{\"inputs\":[],\"name\":\"selfDestruct\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"}]"

// SelfDestructorBin is the compiled bytecode used for deploying new contracts.
var SelfDestructorBin = "0x6080604052348015600f57600080fd5b50600160005560708060226000396000f3fe6080604052348015600f57600080fd5b506004361060285760003560e01c80639cb8a26a14602d575b600080fd5b60336035565b005b600080fffea2646970667358221220a74afcbd1431019f7f78bcea3f22970f82560adb72829f56b28140de356bf95164736f6c63430007020033"

// DeploySelfDestructor deploys a new Ethereum contract, binding an instance of SelfDestructor to it.
func DeploySelfDestructor(auth *bind.TransactOpts, backend bind.ContractBackend) (libcommon.Address, types.Transaction, *SelfDestructor, error) {
	parsed, err := abi.JSON(strings.NewReader(SelfDestructorABI))
	if err != nil {
		return libcommon.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(SelfDestructorBin), backend)
	if err != nil {
		return libcommon.Address{}, nil, nil, err
	}
	return address, tx, &SelfDestructor{SelfDestructorCaller: SelfDestructorCaller{contract: contract}, SelfDestructorTransactor: SelfDestructorTransactor{contract: contract}, SelfDestructorFilterer: SelfDestructorFilterer{contract: contract}}, nil
}

// SelfDestructor is an auto generated Go binding around an Ethereum contract.
type SelfDestructor struct {
	SelfDestructorCaller     // Read-only binding to the contract
	SelfDestructorTransactor // Write-only binding to the contract
	SelfDestructorFilterer   // Log filterer for contract events
}

// SelfDestructorCaller is an auto generated read-only Go binding around an Ethereum contract.
type SelfDestructorCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// SelfDestructorTransactor is an auto generated write-only Go binding around an Ethereum contract.
type SelfDestructorTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// SelfDestructorFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type SelfDestructorFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// SelfDestructorSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type SelfDestructorSession struct {
	Contract     *SelfDestructor   // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// SelfDestructorCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type SelfDestructorCallerSession struct {
	Contract *SelfDestructorCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts         // Call options to use throughout this session
}

// SelfDestructorTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type SelfDestructorTransactorSession struct {
	Contract     *SelfDestructorTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts         // Transaction auth options to use throughout this session
}

// SelfDestructorRaw is an auto generated low-level Go binding around an Ethereum contract.
type SelfDestructorRaw struct {
	Contract *SelfDestructor // Generic contract binding to access the raw methods on
}

// SelfDestructorCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type SelfDestructorCallerRaw struct {
	Contract *SelfDestructorCaller // Generic read-only contract binding to access the raw methods on
}

// SelfDestructorTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type SelfDestructorTransactorRaw struct {
	Contract *SelfDestructorTransactor // Generic write-only contract binding to access the raw methods on
}

// NewSelfDestructor creates a new instance of SelfDestructor, bound to a specific deployed contract.
func NewSelfDestructor(address libcommon.Address, backend bind.ContractBackend) (*SelfDestructor, error) {
	contract, err := bindSelfDestructor(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &SelfDestructor{SelfDestructorCaller: SelfDestructorCaller{contract: contract}, SelfDestructorTransactor: SelfDestructorTransactor{contract: contract}, SelfDestructorFilterer: SelfDestructorFilterer{contract: contract}}, nil
}

// NewSelfDestructorCaller creates a new read-only instance of SelfDestructor, bound to a specific deployed contract.
func NewSelfDestructorCaller(address libcommon.Address, caller bind.ContractCaller) (*SelfDestructorCaller, error) {
	contract, err := bindSelfDestructor(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &SelfDestructorCaller{contract: contract}, nil
}

// NewSelfDestructorTransactor creates a new write-only instance of SelfDestructor, bound to a specific deployed contract.
func NewSelfDestructorTransactor(address libcommon.Address, transactor bind.ContractTransactor) (*SelfDestructorTransactor, error) {
	contract, err := bindSelfDestructor(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &SelfDestructorTransactor{contract: contract}, nil
}

// NewSelfDestructorFilterer creates a new log filterer instance of SelfDestructor, bound to a specific deployed contract.
func NewSelfDestructorFilterer(address libcommon.Address, filterer bind.ContractFilterer) (*SelfDestructorFilterer, error) {
	contract, err := bindSelfDestructor(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &SelfDestructorFilterer{contract: contract}, nil
}

// bindSelfDestructor binds a generic wrapper to an already deployed contract.
func bindSelfDestructor(address libcommon.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(SelfDestructorABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_SelfDestructor *SelfDestructorRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _SelfDestructor.Contract.SelfDestructorCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_SelfDestructor *SelfDestructorRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _SelfDestructor.Contract.SelfDestructorTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_SelfDestructor *SelfDestructorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _SelfDestructor.Contract.SelfDestructorTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_SelfDestructor *SelfDestructorCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _SelfDestructor.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_SelfDestructor *SelfDestructorTransactorRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _SelfDestructor.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_SelfDestructor *SelfDestructorTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _SelfDestructor.Contract.contract.Transact(opts, method, params...)
}

// SelfDestruct is a paid mutator transaction binding the contract method 0x9cb8a26a.
//
// Solidity: function selfDestruct() returns()
func (_SelfDestructor *SelfDestructorTransactor) SelfDestruct(opts *bind.TransactOpts) (types.Transaction, error) {
	return _SelfDestructor.contract.Transact(opts, "selfDestruct")
}

// SelfDestruct is a paid mutator transaction binding the contract method 0x9cb8a26a.
//
// Solidity: function selfDestruct() returns()
func (_SelfDestructor *SelfDestructorSession) SelfDestruct() (types.Transaction, error) {
	return _SelfDestructor.Contract.SelfDestruct(&_SelfDestructor.TransactOpts)
}

// SelfDestruct is a paid mutator transaction binding the contract method 0x9cb8a26a.
//
// Solidity: function selfDestruct() returns()
func (_SelfDestructor *SelfDestructorTransactorSession) SelfDestruct() (types.Transaction, error) {
	return _SelfDestructor.Contract.SelfDestruct(&_SelfDestructor.TransactOpts)
}
