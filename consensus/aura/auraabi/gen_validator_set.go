// Code generated - DO NOT EDIT.
// This file is a generated binding and any manual changes will be lost.

package auraabi

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

// ValidatorSetABI is the input ABI used to generate the binding from.
const ValidatorSetABI = "[{\"constant\":false,\"inputs\":[],\"name\":\"finalizeChange\",\"outputs\":[],\"payable\":false,\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"getValidators\",\"outputs\":[{\"name\":\"validators\",\"type\":\"address[]\"}],\"payable\":false,\"type\":\"function\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"name\":\"_parent_hash\",\"type\":\"bytes32\"},{\"indexed\":false,\"name\":\"_new_set\",\"type\":\"address[]\"}],\"name\":\"InitiateChange\",\"type\":\"event\"},{\"constant\":true,\"inputs\":[],\"name\":\"emitInitiateChangeCallable\",\"outputs\":[{\"name\":\"\",\"type\":\"bool\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":false,\"inputs\":[],\"name\":\"emitInitiateChange\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[{\"name\":\"_reportingValidator\",\"type\":\"address\"},{\"name\":\"_maliciousValidator\",\"type\":\"address\"},{\"name\":\"_blockNumber\",\"type\":\"uint256\"}],\"name\":\"shouldValidatorReport\",\"outputs\":[{\"name\":\"\",\"type\":\"bool\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"}]"

// ValidatorSet is an auto generated Go binding around an Ethereum contract.
type ValidatorSet struct {
	ValidatorSetCaller     // Read-only binding to the contract
	ValidatorSetTransactor // Write-only binding to the contract
	ValidatorSetFilterer   // Log filterer for contract events
}

// ValidatorSetCaller is an auto generated read-only Go binding around an Ethereum contract.
type ValidatorSetCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ValidatorSetTransactor is an auto generated write-only Go binding around an Ethereum contract.
type ValidatorSetTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ValidatorSetFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type ValidatorSetFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ValidatorSetSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type ValidatorSetSession struct {
	Contract     *ValidatorSet     // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// ValidatorSetCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type ValidatorSetCallerSession struct {
	Contract *ValidatorSetCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts       // Call options to use throughout this session
}

// ValidatorSetTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type ValidatorSetTransactorSession struct {
	Contract     *ValidatorSetTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts       // Transaction auth options to use throughout this session
}

// ValidatorSetRaw is an auto generated low-level Go binding around an Ethereum contract.
type ValidatorSetRaw struct {
	Contract *ValidatorSet // Generic contract binding to access the raw methods on
}

// ValidatorSetCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type ValidatorSetCallerRaw struct {
	Contract *ValidatorSetCaller // Generic read-only contract binding to access the raw methods on
}

// ValidatorSetTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type ValidatorSetTransactorRaw struct {
	Contract *ValidatorSetTransactor // Generic write-only contract binding to access the raw methods on
}

// NewValidatorSet creates a new instance of ValidatorSet, bound to a specific deployed contract.
func NewValidatorSet(address libcommon.Address, backend bind.ContractBackend) (*ValidatorSet, error) {
	contract, err := bindValidatorSet(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &ValidatorSet{ValidatorSetCaller: ValidatorSetCaller{contract: contract}, ValidatorSetTransactor: ValidatorSetTransactor{contract: contract}, ValidatorSetFilterer: ValidatorSetFilterer{contract: contract}}, nil
}

// NewValidatorSetCaller creates a new read-only instance of ValidatorSet, bound to a specific deployed contract.
func NewValidatorSetCaller(address libcommon.Address, caller bind.ContractCaller) (*ValidatorSetCaller, error) {
	contract, err := bindValidatorSet(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &ValidatorSetCaller{contract: contract}, nil
}

// NewValidatorSetTransactor creates a new write-only instance of ValidatorSet, bound to a specific deployed contract.
func NewValidatorSetTransactor(address libcommon.Address, transactor bind.ContractTransactor) (*ValidatorSetTransactor, error) {
	contract, err := bindValidatorSet(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &ValidatorSetTransactor{contract: contract}, nil
}

// NewValidatorSetFilterer creates a new log filterer instance of ValidatorSet, bound to a specific deployed contract.
func NewValidatorSetFilterer(address libcommon.Address, filterer bind.ContractFilterer) (*ValidatorSetFilterer, error) {
	contract, err := bindValidatorSet(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &ValidatorSetFilterer{contract: contract}, nil
}

// bindValidatorSet binds a generic wrapper to an already deployed contract.
func bindValidatorSet(address libcommon.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(ValidatorSetABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_ValidatorSet *ValidatorSetRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _ValidatorSet.Contract.ValidatorSetCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_ValidatorSet *ValidatorSetRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _ValidatorSet.Contract.ValidatorSetTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_ValidatorSet *ValidatorSetRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _ValidatorSet.Contract.ValidatorSetTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_ValidatorSet *ValidatorSetCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _ValidatorSet.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_ValidatorSet *ValidatorSetTransactorRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _ValidatorSet.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_ValidatorSet *ValidatorSetTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _ValidatorSet.Contract.contract.Transact(opts, method, params...)
}

// EmitInitiateChangeCallable is a free data retrieval call binding the contract method 0x3d3b5458.
//
// Solidity: function emitInitiateChangeCallable() view returns(bool)
func (_ValidatorSet *ValidatorSetCaller) EmitInitiateChangeCallable(opts *bind.CallOpts) (bool, error) {
	var out []interface{}
	err := _ValidatorSet.contract.Call(opts, &out, "emitInitiateChangeCallable")

	if err != nil {
		return *new(bool), err
	}

	out0 := *abi.ConvertType(out[0], new(bool)).(*bool)

	return out0, err

}

// EmitInitiateChangeCallable is a free data retrieval call binding the contract method 0x3d3b5458.
//
// Solidity: function emitInitiateChangeCallable() view returns(bool)
func (_ValidatorSet *ValidatorSetSession) EmitInitiateChangeCallable() (bool, error) {
	return _ValidatorSet.Contract.EmitInitiateChangeCallable(&_ValidatorSet.CallOpts)
}

// EmitInitiateChangeCallable is a free data retrieval call binding the contract method 0x3d3b5458.
//
// Solidity: function emitInitiateChangeCallable() view returns(bool)
func (_ValidatorSet *ValidatorSetCallerSession) EmitInitiateChangeCallable() (bool, error) {
	return _ValidatorSet.Contract.EmitInitiateChangeCallable(&_ValidatorSet.CallOpts)
}

// GetValidators is a free data retrieval call binding the contract method 0xb7ab4db5.
//
// Solidity: function getValidators() returns(address[] validators)
func (_ValidatorSet *ValidatorSetCaller) GetValidators(opts *bind.CallOpts) ([]libcommon.Address, error) {
	var out []interface{}
	err := _ValidatorSet.contract.Call(opts, &out, "getValidators")

	if err != nil {
		return *new([]libcommon.Address), err
	}

	out0 := *abi.ConvertType(out[0], new([]libcommon.Address)).(*[]libcommon.Address)

	return out0, err

}

// GetValidators is a free data retrieval call binding the contract method 0xb7ab4db5.
//
// Solidity: function getValidators() returns(address[] validators)
func (_ValidatorSet *ValidatorSetSession) GetValidators() ([]libcommon.Address, error) {
	return _ValidatorSet.Contract.GetValidators(&_ValidatorSet.CallOpts)
}

// GetValidators is a free data retrieval call binding the contract method 0xb7ab4db5.
//
// Solidity: function getValidators() returns(address[] validators)
func (_ValidatorSet *ValidatorSetCallerSession) GetValidators() ([]libcommon.Address, error) {
	return _ValidatorSet.Contract.GetValidators(&_ValidatorSet.CallOpts)
}

// ShouldValidatorReport is a free data retrieval call binding the contract method 0xcbd2d528.
//
// Solidity: function shouldValidatorReport(address _reportingValidator, address _maliciousValidator, uint256 _blockNumber) view returns(bool)
func (_ValidatorSet *ValidatorSetCaller) ShouldValidatorReport(opts *bind.CallOpts, _reportingValidator libcommon.Address, _maliciousValidator libcommon.Address, _blockNumber *big.Int) (bool, error) {
	var out []interface{}
	err := _ValidatorSet.contract.Call(opts, &out, "shouldValidatorReport", _reportingValidator, _maliciousValidator, _blockNumber)

	if err != nil {
		return *new(bool), err
	}

	out0 := *abi.ConvertType(out[0], new(bool)).(*bool)

	return out0, err

}

// ShouldValidatorReport is a free data retrieval call binding the contract method 0xcbd2d528.
//
// Solidity: function shouldValidatorReport(address _reportingValidator, address _maliciousValidator, uint256 _blockNumber) view returns(bool)
func (_ValidatorSet *ValidatorSetSession) ShouldValidatorReport(_reportingValidator libcommon.Address, _maliciousValidator libcommon.Address, _blockNumber *big.Int) (bool, error) {
	return _ValidatorSet.Contract.ShouldValidatorReport(&_ValidatorSet.CallOpts, _reportingValidator, _maliciousValidator, _blockNumber)
}

// ShouldValidatorReport is a free data retrieval call binding the contract method 0xcbd2d528.
//
// Solidity: function shouldValidatorReport(address _reportingValidator, address _maliciousValidator, uint256 _blockNumber) view returns(bool)
func (_ValidatorSet *ValidatorSetCallerSession) ShouldValidatorReport(_reportingValidator libcommon.Address, _maliciousValidator libcommon.Address, _blockNumber *big.Int) (bool, error) {
	return _ValidatorSet.Contract.ShouldValidatorReport(&_ValidatorSet.CallOpts, _reportingValidator, _maliciousValidator, _blockNumber)
}

// EmitInitiateChange is a paid mutator transaction binding the contract method 0x93b4e25e.
//
// Solidity: function emitInitiateChange() returns()
func (_ValidatorSet *ValidatorSetTransactor) EmitInitiateChange(opts *bind.TransactOpts) (types.Transaction, error) {
	return _ValidatorSet.contract.Transact(opts, "emitInitiateChange")
}

// EmitInitiateChange is a paid mutator transaction binding the contract method 0x93b4e25e.
//
// Solidity: function emitInitiateChange() returns()
func (_ValidatorSet *ValidatorSetSession) EmitInitiateChange() (types.Transaction, error) {
	return _ValidatorSet.Contract.EmitInitiateChange(&_ValidatorSet.TransactOpts)
}

// EmitInitiateChange is a paid mutator transaction binding the contract method 0x93b4e25e.
//
// Solidity: function emitInitiateChange() returns()
func (_ValidatorSet *ValidatorSetTransactorSession) EmitInitiateChange() (types.Transaction, error) {
	return _ValidatorSet.Contract.EmitInitiateChange(&_ValidatorSet.TransactOpts)
}

// FinalizeChange is a paid mutator transaction binding the contract method 0x75286211.
//
// Solidity: function finalizeChange() returns()
func (_ValidatorSet *ValidatorSetTransactor) FinalizeChange(opts *bind.TransactOpts) (types.Transaction, error) {
	return _ValidatorSet.contract.Transact(opts, "finalizeChange")
}

// FinalizeChange is a paid mutator transaction binding the contract method 0x75286211.
//
// Solidity: function finalizeChange() returns()
func (_ValidatorSet *ValidatorSetSession) FinalizeChange() (types.Transaction, error) {
	return _ValidatorSet.Contract.FinalizeChange(&_ValidatorSet.TransactOpts)
}

// FinalizeChange is a paid mutator transaction binding the contract method 0x75286211.
//
// Solidity: function finalizeChange() returns()
func (_ValidatorSet *ValidatorSetTransactorSession) FinalizeChange() (types.Transaction, error) {
	return _ValidatorSet.Contract.FinalizeChange(&_ValidatorSet.TransactOpts)
}

// ValidatorSetInitiateChangeIterator is returned from FilterInitiateChange and is used to iterate over the raw logs and unpacked data for InitiateChange events raised by the ValidatorSet contract.
type ValidatorSetInitiateChangeIterator struct {
	Event *ValidatorSetInitiateChange // Event containing the contract specifics and raw log

	contract *bind.BoundContract // Generic contract to use for unpacking event data
	event    string              // Event name to use for unpacking event data

	logs chan types.Log        // Log channel receiving the found contract events
	sub  ethereum.Subscription // Subscription for errors, completion and termination
	done bool                  // Whether the subscription completed delivering logs
	fail error                 // Occurred error to stop iteration
}

// Next advances the iterator to the subsequent event, returning whether there
// are any more events found. In case of a retrieval or parsing error, false is
// returned and Error() can be queried for the exact failure.
func (it *ValidatorSetInitiateChangeIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(ValidatorSetInitiateChange)
			if err := it.contract.UnpackLog(it.Event, it.event, log); err != nil {
				it.fail = err
				return false
			}
			it.Event.Raw = log
			return true

		default:
			return false
		}
	}
	// Iterator still in progress, wait for either a data or an error event
	select {
	case log := <-it.logs:
		it.Event = new(ValidatorSetInitiateChange)
		if err := it.contract.UnpackLog(it.Event, it.event, log); err != nil {
			it.fail = err
			return false
		}
		it.Event.Raw = log
		return true

	case err := <-it.sub.Err():
		it.done = true
		it.fail = err
		return it.Next()
	}
}

// Error returns any retrieval or parsing error occurred during filtering.
func (it *ValidatorSetInitiateChangeIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *ValidatorSetInitiateChangeIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// ValidatorSetInitiateChange represents a InitiateChange event raised by the ValidatorSet contract.
type ValidatorSetInitiateChange struct {
	ParentHash [32]byte
	NewSet     []libcommon.Address
	Raw        types.Log // Blockchain specific contextual infos
}

// FilterInitiateChange is a free log retrieval operation binding the contract event 0x55252fa6eee4741b4e24a74a70e9c11fd2c2281df8d6ea13126ff845f7825c89.
//
// Solidity: event InitiateChange(bytes32 indexed _parent_hash, address[] _new_set)
func (_ValidatorSet *ValidatorSetFilterer) FilterInitiateChange(opts *bind.FilterOpts, _parent_hash [][32]byte) (*ValidatorSetInitiateChangeIterator, error) {

	var _parent_hashRule []interface{}
	for _, _parent_hashItem := range _parent_hash {
		_parent_hashRule = append(_parent_hashRule, _parent_hashItem)
	}

	logs, sub, err := _ValidatorSet.contract.FilterLogs(opts, "InitiateChange", _parent_hashRule)
	if err != nil {
		return nil, err
	}
	return &ValidatorSetInitiateChangeIterator{contract: _ValidatorSet.contract, event: "InitiateChange", logs: logs, sub: sub}, nil
}

// WatchInitiateChange is a free log subscription operation binding the contract event 0x55252fa6eee4741b4e24a74a70e9c11fd2c2281df8d6ea13126ff845f7825c89.
//
// Solidity: event InitiateChange(bytes32 indexed _parent_hash, address[] _new_set)
func (_ValidatorSet *ValidatorSetFilterer) WatchInitiateChange(opts *bind.WatchOpts, sink chan<- *ValidatorSetInitiateChange, _parent_hash [][32]byte) (event.Subscription, error) {

	var _parent_hashRule []interface{}
	for _, _parent_hashItem := range _parent_hash {
		_parent_hashRule = append(_parent_hashRule, _parent_hashItem)
	}

	logs, sub, err := _ValidatorSet.contract.WatchLogs(opts, "InitiateChange", _parent_hashRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(ValidatorSetInitiateChange)
				if err := _ValidatorSet.contract.UnpackLog(event, "InitiateChange", log); err != nil {
					return err
				}
				event.Raw = log

				select {
				case sink <- event:
				case err := <-sub.Err():
					return err
				case <-quit:
					return nil
				}
			case err := <-sub.Err():
				return err
			case <-quit:
				return nil
			}
		}
	}), nil
}

// ParseInitiateChange is a log parse operation binding the contract event 0x55252fa6eee4741b4e24a74a70e9c11fd2c2281df8d6ea13126ff845f7825c89.
//
// Solidity: event InitiateChange(bytes32 indexed _parent_hash, address[] _new_set)
func (_ValidatorSet *ValidatorSetFilterer) ParseInitiateChange(log types.Log) (*ValidatorSetInitiateChange, error) {
	event := new(ValidatorSetInitiateChange)
	if err := _ValidatorSet.contract.UnpackLog(event, "InitiateChange", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}
