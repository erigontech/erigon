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
	_ = libcommon.Big1
	_ = types.BloomLookup
	_ = event.NewSubscription
)

// SubscriptionABI is the input ABI used to generate the binding from.
const SubscriptionABI = "[{\"anonymous\":false,\"inputs\":[],\"name\":\"SubscriptionEvent\",\"type\":\"event\"},{\"stateMutability\":\"nonpayable\",\"type\":\"fallback\"}]"

// SubscriptionBin is the compiled bytecode used for deploying new contracts.
var SubscriptionBin = "0x6080604052348015600f57600080fd5b50607180601d6000396000f3fe6080604052348015600f57600080fd5b506040517f67abc7edb0ab50964ef0e90541d39366b9c69f6f714520f2ff4570059ee8ad8090600090a100fea264697066735822122045a70478ef4f6a283c0e153ad72ec6731dc9ee2e1c191c7334b74dea21a92eaf64736f6c634300080c0033"

// DeploySubscription deploys a new Ethereum contract, binding an instance of Subscription to it.
func DeploySubscription(auth *bind.TransactOpts, backend bind.ContractBackend) (libcommon.Address, types.Transaction, *Subscription, error) {
	parsed, err := abi.JSON(strings.NewReader(SubscriptionABI))
	if err != nil {
		return libcommon.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(SubscriptionBin), backend)
	if err != nil {
		return libcommon.Address{}, nil, nil, err
	}
	return address, tx, &Subscription{SubscriptionCaller: SubscriptionCaller{contract: contract}, SubscriptionTransactor: SubscriptionTransactor{contract: contract}, SubscriptionFilterer: SubscriptionFilterer{contract: contract}}, nil
}

// Subscription is an auto generated Go binding around an Ethereum contract.
type Subscription struct {
	SubscriptionCaller     // Read-only binding to the contract
	SubscriptionTransactor // Write-only binding to the contract
	SubscriptionFilterer   // Log filterer for contract events
}

// SubscriptionCaller is an auto generated read-only Go binding around an Ethereum contract.
type SubscriptionCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// SubscriptionTransactor is an auto generated write-only Go binding around an Ethereum contract.
type SubscriptionTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// SubscriptionFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type SubscriptionFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// SubscriptionSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type SubscriptionSession struct {
	Contract     *Subscription     // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// SubscriptionCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type SubscriptionCallerSession struct {
	Contract *SubscriptionCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts       // Call options to use throughout this session
}

// SubscriptionTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type SubscriptionTransactorSession struct {
	Contract     *SubscriptionTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts       // Transaction auth options to use throughout this session
}

// SubscriptionRaw is an auto generated low-level Go binding around an Ethereum contract.
type SubscriptionRaw struct {
	Contract *Subscription // Generic contract binding to access the raw methods on
}

// SubscriptionCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type SubscriptionCallerRaw struct {
	Contract *SubscriptionCaller // Generic read-only contract binding to access the raw methods on
}

// SubscriptionTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type SubscriptionTransactorRaw struct {
	Contract *SubscriptionTransactor // Generic write-only contract binding to access the raw methods on
}

// NewSubscription creates a new instance of Subscription, bound to a specific deployed contract.
func NewSubscription(address libcommon.Address, backend bind.ContractBackend) (*Subscription, error) {
	contract, err := bindSubscription(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Subscription{SubscriptionCaller: SubscriptionCaller{contract: contract}, SubscriptionTransactor: SubscriptionTransactor{contract: contract}, SubscriptionFilterer: SubscriptionFilterer{contract: contract}}, nil
}

// NewSubscriptionCaller creates a new read-only instance of Subscription, bound to a specific deployed contract.
func NewSubscriptionCaller(address libcommon.Address, caller bind.ContractCaller) (*SubscriptionCaller, error) {
	contract, err := bindSubscription(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &SubscriptionCaller{contract: contract}, nil
}

// NewSubscriptionTransactor creates a new write-only instance of Subscription, bound to a specific deployed contract.
func NewSubscriptionTransactor(address libcommon.Address, transactor bind.ContractTransactor) (*SubscriptionTransactor, error) {
	contract, err := bindSubscription(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &SubscriptionTransactor{contract: contract}, nil
}

// NewSubscriptionFilterer creates a new log filterer instance of Subscription, bound to a specific deployed contract.
func NewSubscriptionFilterer(address libcommon.Address, filterer bind.ContractFilterer) (*SubscriptionFilterer, error) {
	contract, err := bindSubscription(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &SubscriptionFilterer{contract: contract}, nil
}

// bindSubscription binds a generic wrapper to an already deployed contract.
func bindSubscription(address libcommon.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(SubscriptionABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Subscription *SubscriptionRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Subscription.Contract.SubscriptionCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Subscription *SubscriptionRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _Subscription.Contract.SubscriptionTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Subscription *SubscriptionRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _Subscription.Contract.SubscriptionTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Subscription *SubscriptionCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Subscription.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Subscription *SubscriptionTransactorRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _Subscription.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Subscription *SubscriptionTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _Subscription.Contract.contract.Transact(opts, method, params...)
}

// Fallback is a paid mutator transaction binding the contract fallback function.
//
// Solidity: fallback() returns()
func (_Subscription *SubscriptionTransactor) Fallback(opts *bind.TransactOpts, calldata []byte) (types.Transaction, error) {
	return _Subscription.contract.RawTransact(opts, calldata)
}

// Fallback is a paid mutator transaction binding the contract fallback function.
//
// Solidity: fallback() returns()
func (_Subscription *SubscriptionSession) Fallback(calldata []byte) (types.Transaction, error) {
	return _Subscription.Contract.Fallback(&_Subscription.TransactOpts, calldata)
}

// Fallback is a paid mutator transaction binding the contract fallback function.
//
// Solidity: fallback() returns()
func (_Subscription *SubscriptionTransactorSession) Fallback(calldata []byte) (types.Transaction, error) {
	return _Subscription.Contract.Fallback(&_Subscription.TransactOpts, calldata)
}

// SubscriptionSubscriptionEventIterator is returned from FilterSubscriptionEvent and is used to iterate over the raw logs and unpacked data for SubscriptionEvent events raised by the Subscription contract.
type SubscriptionSubscriptionEventIterator struct {
	Event *SubscriptionSubscriptionEvent // Event containing the contract specifics and raw log

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
func (it *SubscriptionSubscriptionEventIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(SubscriptionSubscriptionEvent)
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
		it.Event = new(SubscriptionSubscriptionEvent)
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
func (it *SubscriptionSubscriptionEventIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *SubscriptionSubscriptionEventIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// SubscriptionSubscriptionEvent represents a SubscriptionEvent event raised by the Subscription contract.
type SubscriptionSubscriptionEvent struct {
	Raw types.Log // Blockchain specific contextual infos
}

// FilterSubscriptionEvent is a free log retrieval operation binding the contract event 0x67abc7edb0ab50964ef0e90541d39366b9c69f6f714520f2ff4570059ee8ad80.
//
// Solidity: event SubscriptionEvent()
func (_Subscription *SubscriptionFilterer) FilterSubscriptionEvent(opts *bind.FilterOpts) (*SubscriptionSubscriptionEventIterator, error) {

	logs, sub, err := _Subscription.contract.FilterLogs(opts, "SubscriptionEvent")
	if err != nil {
		return nil, err
	}
	return &SubscriptionSubscriptionEventIterator{contract: _Subscription.contract, event: "SubscriptionEvent", logs: logs, sub: sub}, nil
}

// WatchSubscriptionEvent is a free log subscription operation binding the contract event 0x67abc7edb0ab50964ef0e90541d39366b9c69f6f714520f2ff4570059ee8ad80.
//
// Solidity: event SubscriptionEvent()
func (_Subscription *SubscriptionFilterer) WatchSubscriptionEvent(opts *bind.WatchOpts, sink chan<- *SubscriptionSubscriptionEvent) (event.Subscription, error) {

	logs, sub, err := _Subscription.contract.WatchLogs(opts, "SubscriptionEvent")
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(SubscriptionSubscriptionEvent)
				if err := _Subscription.contract.UnpackLog(event, "SubscriptionEvent", log); err != nil {
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

// ParseSubscriptionEvent is a log parse operation binding the contract event 0x67abc7edb0ab50964ef0e90541d39366b9c69f6f714520f2ff4570059ee8ad80.
//
// Solidity: event SubscriptionEvent()
func (_Subscription *SubscriptionFilterer) ParseSubscriptionEvent(log types.Log) (*SubscriptionSubscriptionEvent, error) {
	event := new(SubscriptionSubscriptionEvent)
	if err := _Subscription.contract.UnpackLog(event, "SubscriptionEvent", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}
