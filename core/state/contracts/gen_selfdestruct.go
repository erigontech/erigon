// Code generated - DO NOT EDIT.
// This file is a generated binding and any manual changes will be lost.

package contracts

import (
	"math/big"
	"strings"

	ethereum "github.com/ledgerwatch/turbo-geth"
	"github.com/ledgerwatch/turbo-geth/accounts/abi"
	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/math"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/event"
)

// Reference imports to suppress errors if they are not otherwise used.
var (
	_ = big.NewInt
	_ = strings.NewReader
	_ = ethereum.NotFound
	_ = math.U256
	_ = bind.Bind
	_ = common.Big1
	_ = types.BloomLookup
	_ = event.NewSubscription
)

// SelfdestructABI is the input ABI used to generate the binding from.
const SelfdestructABI = "[{\"constant\":false,\"inputs\":[],\"name\":\"destruct\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"constant\":false,\"inputs\":[],\"name\":\"change\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"constructor\"},{\"payable\":true,\"stateMutability\":\"payable\",\"type\":\"fallback\"}]"

// SelfdestructBin is the compiled bytecode used for deploying new contracts.
const SelfdestructBin = `608060405234801561001057600080fd5b50640100000000600055600260018190556003905560ba806100336000396000f3fe60806040526004361060485763ffffffff7c01000000000000000000000000000000000000000000000000000000006000350416632b68b9c68114604a5780632ee79ded14605c575b005b348015605557600080fd5b506048606e565b348015606757600080fd5b5060486071565b30ff5b60008054600190810190915580548101815560028054909101905556fea165627a7a72305820e49fa5f7c67f3950748bcd6231418ed1b93be742c71840fdc6014656d2e56d8b0029`

// DeploySelfdestruct deploys a new Ethereum contract, binding an instance of Selfdestruct to it.
func DeploySelfdestruct(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *Selfdestruct, error) {
	parsed, err := abi.JSON(strings.NewReader(SelfdestructABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(SelfdestructBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &Selfdestruct{SelfdestructCaller: SelfdestructCaller{contract: contract}, SelfdestructTransactor: SelfdestructTransactor{contract: contract}, SelfdestructFilterer: SelfdestructFilterer{contract: contract}}, nil
}

// Selfdestruct is an auto generated Go binding around an Ethereum contract.
type Selfdestruct struct {
	SelfdestructCaller     // Read-only binding to the contract
	SelfdestructTransactor // Write-only binding to the contract
	SelfdestructFilterer   // Log filterer for contract events
}

// SelfdestructCaller is an auto generated read-only Go binding around an Ethereum contract.
type SelfdestructCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// SelfdestructTransactor is an auto generated write-only Go binding around an Ethereum contract.
type SelfdestructTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// SelfdestructFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type SelfdestructFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// SelfdestructSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type SelfdestructSession struct {
	Contract     *Selfdestruct     // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// SelfdestructCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type SelfdestructCallerSession struct {
	Contract *SelfdestructCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts       // Call options to use throughout this session
}

// SelfdestructTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type SelfdestructTransactorSession struct {
	Contract     *SelfdestructTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts       // Transaction auth options to use throughout this session
}

// SelfdestructRaw is an auto generated low-level Go binding around an Ethereum contract.
type SelfdestructRaw struct {
	Contract *Selfdestruct // Generic contract binding to access the raw methods on
}

// SelfdestructCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type SelfdestructCallerRaw struct {
	Contract *SelfdestructCaller // Generic read-only contract binding to access the raw methods on
}

// SelfdestructTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type SelfdestructTransactorRaw struct {
	Contract *SelfdestructTransactor // Generic write-only contract binding to access the raw methods on
}

// NewSelfdestruct creates a new instance of Selfdestruct, bound to a specific deployed contract.
func NewSelfdestruct(address common.Address, backend bind.ContractBackend) (*Selfdestruct, error) {
	contract, err := bindSelfdestruct(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Selfdestruct{SelfdestructCaller: SelfdestructCaller{contract: contract}, SelfdestructTransactor: SelfdestructTransactor{contract: contract}, SelfdestructFilterer: SelfdestructFilterer{contract: contract}}, nil
}

// NewSelfdestructCaller creates a new read-only instance of Selfdestruct, bound to a specific deployed contract.
func NewSelfdestructCaller(address common.Address, caller bind.ContractCaller) (*SelfdestructCaller, error) {
	contract, err := bindSelfdestruct(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &SelfdestructCaller{contract: contract}, nil
}

// NewSelfdestructTransactor creates a new write-only instance of Selfdestruct, bound to a specific deployed contract.
func NewSelfdestructTransactor(address common.Address, transactor bind.ContractTransactor) (*SelfdestructTransactor, error) {
	contract, err := bindSelfdestruct(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &SelfdestructTransactor{contract: contract}, nil
}

// NewSelfdestructFilterer creates a new log filterer instance of Selfdestruct, bound to a specific deployed contract.
func NewSelfdestructFilterer(address common.Address, filterer bind.ContractFilterer) (*SelfdestructFilterer, error) {
	contract, err := bindSelfdestruct(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &SelfdestructFilterer{contract: contract}, nil
}

// bindSelfdestruct binds a generic wrapper to an already deployed contract.
func bindSelfdestruct(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(SelfdestructABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Selfdestruct *SelfdestructRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Selfdestruct.Contract.SelfdestructCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Selfdestruct *SelfdestructRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Selfdestruct.Contract.SelfdestructTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Selfdestruct *SelfdestructRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Selfdestruct.Contract.SelfdestructTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Selfdestruct *SelfdestructCallerRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Selfdestruct.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Selfdestruct *SelfdestructTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Selfdestruct.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Selfdestruct *SelfdestructTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Selfdestruct.Contract.contract.Transact(opts, method, params...)
}

// Change is a paid mutator transaction binding the contract method 0x2ee79ded.
//
// Solidity: function change() returns()
func (_Selfdestruct *SelfdestructTransactor) Change(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Selfdestruct.contract.Transact(opts, "change")
}

// Change is a paid mutator transaction binding the contract method 0x2ee79ded.
//
// Solidity: function change() returns()
func (_Selfdestruct *SelfdestructSession) Change() (*types.Transaction, error) {
	return _Selfdestruct.Contract.Change(&_Selfdestruct.TransactOpts)
}

// Change is a paid mutator transaction binding the contract method 0x2ee79ded.
//
// Solidity: function change() returns()
func (_Selfdestruct *SelfdestructTransactorSession) Change() (*types.Transaction, error) {
	return _Selfdestruct.Contract.Change(&_Selfdestruct.TransactOpts)
}

// Destruct is a paid mutator transaction binding the contract method 0x2b68b9c6.
//
// Solidity: function destruct() returns()
func (_Selfdestruct *SelfdestructTransactor) Destruct(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Selfdestruct.contract.Transact(opts, "destruct")
}

// Destruct is a paid mutator transaction binding the contract method 0x2b68b9c6.
//
// Solidity: function destruct() returns()
func (_Selfdestruct *SelfdestructSession) Destruct() (*types.Transaction, error) {
	return _Selfdestruct.Contract.Destruct(&_Selfdestruct.TransactOpts)
}

// Destruct is a paid mutator transaction binding the contract method 0x2b68b9c6.
//
// Solidity: function destruct() returns()
func (_Selfdestruct *SelfdestructTransactorSession) Destruct() (*types.Transaction, error) {
	return _Selfdestruct.Contract.Destruct(&_Selfdestruct.TransactOpts)
}
