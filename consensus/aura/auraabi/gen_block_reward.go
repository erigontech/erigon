// Code generated - DO NOT EDIT.
// This file is a generated binding and any manual changes will be lost.

package auraabi

import (
	"math/big"
	"strings"

	ethereum "github.com/ledgerwatch/erigon"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/event"
)

// Reference imports to suppress errors if they are not otherwise used.
var (
	_ = big.NewInt
	_ = strings.NewReader
	_ = ethereum.NotFound
	_ = bind.Bind
	_ = libcommon.Big1
	_ = types.BloomLookup
	_ = event.NewSubscription
)

// BlockRewardABI is the input ABI used to generate the binding from.
const BlockRewardABI = "[{\"constant\":false,\"inputs\":[{\"name\":\"benefactors\",\"type\":\"address[]\"},{\"name\":\"kind\",\"type\":\"uint16[]\"}],\"name\":\"reward\",\"outputs\":[{\"name\":\"\",\"type\":\"address[]\"},{\"name\":\"\",\"type\":\"uint256[]\"}],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"}]"

// BlockReward is an auto generated Go binding around an Ethereum contract.
type BlockReward struct {
	BlockRewardCaller     // Read-only binding to the contract
	BlockRewardTransactor // Write-only binding to the contract
	BlockRewardFilterer   // Log filterer for contract events
}

// BlockRewardCaller is an auto generated read-only Go binding around an Ethereum contract.
type BlockRewardCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// BlockRewardTransactor is an auto generated write-only Go binding around an Ethereum contract.
type BlockRewardTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// BlockRewardFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type BlockRewardFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// BlockRewardSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type BlockRewardSession struct {
	Contract     *BlockReward      // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// BlockRewardCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type BlockRewardCallerSession struct {
	Contract *BlockRewardCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts      // Call options to use throughout this session
}

// BlockRewardTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type BlockRewardTransactorSession struct {
	Contract     *BlockRewardTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts      // Transaction auth options to use throughout this session
}

// BlockRewardRaw is an auto generated low-level Go binding around an Ethereum contract.
type BlockRewardRaw struct {
	Contract *BlockReward // Generic contract binding to access the raw methods on
}

// BlockRewardCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type BlockRewardCallerRaw struct {
	Contract *BlockRewardCaller // Generic read-only contract binding to access the raw methods on
}

// BlockRewardTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type BlockRewardTransactorRaw struct {
	Contract *BlockRewardTransactor // Generic write-only contract binding to access the raw methods on
}

// NewBlockReward creates a new instance of BlockReward, bound to a specific deployed contract.
func NewBlockReward(address libcommon.Address, backend bind.ContractBackend) (*BlockReward, error) {
	contract, err := bindBlockReward(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &BlockReward{BlockRewardCaller: BlockRewardCaller{contract: contract}, BlockRewardTransactor: BlockRewardTransactor{contract: contract}, BlockRewardFilterer: BlockRewardFilterer{contract: contract}}, nil
}

// NewBlockRewardCaller creates a new read-only instance of BlockReward, bound to a specific deployed contract.
func NewBlockRewardCaller(address libcommon.Address, caller bind.ContractCaller) (*BlockRewardCaller, error) {
	contract, err := bindBlockReward(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &BlockRewardCaller{contract: contract}, nil
}

// NewBlockRewardTransactor creates a new write-only instance of BlockReward, bound to a specific deployed contract.
func NewBlockRewardTransactor(address libcommon.Address, transactor bind.ContractTransactor) (*BlockRewardTransactor, error) {
	contract, err := bindBlockReward(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &BlockRewardTransactor{contract: contract}, nil
}

// NewBlockRewardFilterer creates a new log filterer instance of BlockReward, bound to a specific deployed contract.
func NewBlockRewardFilterer(address libcommon.Address, filterer bind.ContractFilterer) (*BlockRewardFilterer, error) {
	contract, err := bindBlockReward(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &BlockRewardFilterer{contract: contract}, nil
}

// bindBlockReward binds a generic wrapper to an already deployed contract.
func bindBlockReward(address libcommon.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(BlockRewardABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_BlockReward *BlockRewardRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _BlockReward.Contract.BlockRewardCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_BlockReward *BlockRewardRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _BlockReward.Contract.BlockRewardTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_BlockReward *BlockRewardRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _BlockReward.Contract.BlockRewardTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_BlockReward *BlockRewardCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _BlockReward.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_BlockReward *BlockRewardTransactorRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _BlockReward.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_BlockReward *BlockRewardTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _BlockReward.Contract.contract.Transact(opts, method, params...)
}

// Reward is a paid mutator transaction binding the contract method 0xf91c2898.
//
// Solidity: function reward(address[] benefactors, uint16[] kind) returns(address[], uint256[])
func (_BlockReward *BlockRewardTransactor) Reward(opts *bind.TransactOpts, benefactors []libcommon.Address, kind []uint16) (types.Transaction, error) {
	return _BlockReward.contract.Transact(opts, "reward", benefactors, kind)
}

// Reward is a paid mutator transaction binding the contract method 0xf91c2898.
//
// Solidity: function reward(address[] benefactors, uint16[] kind) returns(address[], uint256[])
func (_BlockReward *BlockRewardSession) Reward(benefactors []libcommon.Address, kind []uint16) (types.Transaction, error) {
	return _BlockReward.Contract.Reward(&_BlockReward.TransactOpts, benefactors, kind)
}

// Reward is a paid mutator transaction binding the contract method 0xf91c2898.
//
// Solidity: function reward(address[] benefactors, uint16[] kind) returns(address[], uint256[])
func (_BlockReward *BlockRewardTransactorSession) Reward(benefactors []libcommon.Address, kind []uint16) (types.Transaction, error) {
	return _BlockReward.Contract.Reward(&_BlockReward.TransactOpts, benefactors, kind)
}
