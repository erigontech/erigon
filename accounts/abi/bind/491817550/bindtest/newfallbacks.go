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

// NewFallbacksABI is the input ABI used to generate the binding from.
const NewFallbacksABI = "[{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"internalType\":\"bytes\",\"name\":\"data\",\"type\":\"bytes\"}],\"name\":\"Fallback\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"internalType\":\"address\",\"name\":\"addr\",\"type\":\"address\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"value\",\"type\":\"uint256\"}],\"name\":\"Received\",\"type\":\"event\"},{\"stateMutability\":\"nonpayable\",\"type\":\"fallback\"},{\"stateMutability\":\"payable\",\"type\":\"receive\"}]"

// NewFallbacksBin is the compiled bytecode used for deploying new contracts.
var NewFallbacksBin = "0x60806040523480156100115760006000fd5b50610017565b61016e806100266000396000f3fe60806040526004361061000d575b36610081575b7f88a5966d370b9919b20f3e2c13ff65706f196a4e32cc2c12bf57088f885258743334604051808373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020018281526020019250505060405180910390a15b005b34801561008e5760006000fd5b505b606036600082377f9043988963722edecc2099c75b0af0ff76af14ffca42ed6bce059a20a2a9f986816040518080602001828103825283818151815260200191508051906020019080838360005b838110156100fa5780820151818401525b6020810190506100de565b50505050905090810190601f1680156101275780820380516001836020036101000a031916815260200191505b509250505060405180910390a1505b00fea26469706673582212205643ca37f40c2b352dc541f42e9e6720de065de756324b7fcc9fb1d67eda4a7d64736f6c63430006040033"

// DeployNewFallbacks deploys a new Ethereum contract, binding an instance of NewFallbacks to it.
func DeployNewFallbacks(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *NewFallbacks, error) {
	parsed, err := abi.JSON(strings.NewReader(NewFallbacksABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(NewFallbacksBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &NewFallbacks{NewFallbacksCaller: NewFallbacksCaller{contract: contract}, NewFallbacksTransactor: NewFallbacksTransactor{contract: contract}, NewFallbacksFilterer: NewFallbacksFilterer{contract: contract}}, nil
}

// NewFallbacks is an auto generated Go binding around an Ethereum contract.
type NewFallbacks struct {
	NewFallbacksCaller     // Read-only binding to the contract
	NewFallbacksTransactor // Write-only binding to the contract
	NewFallbacksFilterer   // Log filterer for contract events
}

// NewFallbacksCaller is an auto generated read-only Go binding around an Ethereum contract.
type NewFallbacksCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// NewFallbacksTransactor is an auto generated write-only Go binding around an Ethereum contract.
type NewFallbacksTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// NewFallbacksFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type NewFallbacksFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// NewFallbacksSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type NewFallbacksSession struct {
	Contract     *NewFallbacks     // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// NewFallbacksCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type NewFallbacksCallerSession struct {
	Contract *NewFallbacksCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts       // Call options to use throughout this session
}

// NewFallbacksTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type NewFallbacksTransactorSession struct {
	Contract     *NewFallbacksTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts       // Transaction auth options to use throughout this session
}

// NewFallbacksRaw is an auto generated low-level Go binding around an Ethereum contract.
type NewFallbacksRaw struct {
	Contract *NewFallbacks // Generic contract binding to access the raw methods on
}

// NewFallbacksCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type NewFallbacksCallerRaw struct {
	Contract *NewFallbacksCaller // Generic read-only contract binding to access the raw methods on
}

// NewFallbacksTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type NewFallbacksTransactorRaw struct {
	Contract *NewFallbacksTransactor // Generic write-only contract binding to access the raw methods on
}

// NewNewFallbacks creates a new instance of NewFallbacks, bound to a specific deployed contract.
func NewNewFallbacks(address common.Address, backend bind.ContractBackend) (*NewFallbacks, error) {
	contract, err := bindNewFallbacks(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &NewFallbacks{NewFallbacksCaller: NewFallbacksCaller{contract: contract}, NewFallbacksTransactor: NewFallbacksTransactor{contract: contract}, NewFallbacksFilterer: NewFallbacksFilterer{contract: contract}}, nil
}

// NewNewFallbacksCaller creates a new read-only instance of NewFallbacks, bound to a specific deployed contract.
func NewNewFallbacksCaller(address common.Address, caller bind.ContractCaller) (*NewFallbacksCaller, error) {
	contract, err := bindNewFallbacks(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &NewFallbacksCaller{contract: contract}, nil
}

// NewNewFallbacksTransactor creates a new write-only instance of NewFallbacks, bound to a specific deployed contract.
func NewNewFallbacksTransactor(address common.Address, transactor bind.ContractTransactor) (*NewFallbacksTransactor, error) {
	contract, err := bindNewFallbacks(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &NewFallbacksTransactor{contract: contract}, nil
}

// NewNewFallbacksFilterer creates a new log filterer instance of NewFallbacks, bound to a specific deployed contract.
func NewNewFallbacksFilterer(address common.Address, filterer bind.ContractFilterer) (*NewFallbacksFilterer, error) {
	contract, err := bindNewFallbacks(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &NewFallbacksFilterer{contract: contract}, nil
}

// bindNewFallbacks binds a generic wrapper to an already deployed contract.
func bindNewFallbacks(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(NewFallbacksABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_NewFallbacks *NewFallbacksRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _NewFallbacks.Contract.NewFallbacksCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_NewFallbacks *NewFallbacksRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _NewFallbacks.Contract.NewFallbacksTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_NewFallbacks *NewFallbacksRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _NewFallbacks.Contract.NewFallbacksTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_NewFallbacks *NewFallbacksCallerRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _NewFallbacks.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_NewFallbacks *NewFallbacksTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _NewFallbacks.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_NewFallbacks *NewFallbacksTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _NewFallbacks.Contract.contract.Transact(opts, method, params...)
}

// Fallback is a paid mutator transaction binding the contract fallback function.
//
// Solidity: fallback() returns()
func (_NewFallbacks *NewFallbacksTransactor) Fallback(opts *bind.TransactOpts, calldata []byte) (*types.Transaction, error) {
	return _NewFallbacks.contract.RawTransact(opts, calldata)
}

// Fallback is a paid mutator transaction binding the contract fallback function.
//
// Solidity: fallback() returns()
func (_NewFallbacks *NewFallbacksSession) Fallback(calldata []byte) (*types.Transaction, error) {
	return _NewFallbacks.Contract.Fallback(&_NewFallbacks.TransactOpts, calldata)
}

// Fallback is a paid mutator transaction binding the contract fallback function.
//
// Solidity: fallback() returns()
func (_NewFallbacks *NewFallbacksTransactorSession) Fallback(calldata []byte) (*types.Transaction, error) {
	return _NewFallbacks.Contract.Fallback(&_NewFallbacks.TransactOpts, calldata)
}

// Receive is a paid mutator transaction binding the contract receive function.
//
// Solidity: receive() payable returns()
func (_NewFallbacks *NewFallbacksTransactor) Receive(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _NewFallbacks.contract.RawTransact(opts, nil) // calldata is disallowed for receive function
}

// Receive is a paid mutator transaction binding the contract receive function.
//
// Solidity: receive() payable returns()
func (_NewFallbacks *NewFallbacksSession) Receive() (*types.Transaction, error) {
	return _NewFallbacks.Contract.Receive(&_NewFallbacks.TransactOpts)
}

// Receive is a paid mutator transaction binding the contract receive function.
//
// Solidity: receive() payable returns()
func (_NewFallbacks *NewFallbacksTransactorSession) Receive() (*types.Transaction, error) {
	return _NewFallbacks.Contract.Receive(&_NewFallbacks.TransactOpts)
}

// NewFallbacksFallbackIterator is returned from FilterFallback and is used to iterate over the raw logs and unpacked data for Fallback events raised by the NewFallbacks contract.
type NewFallbacksFallbackIterator struct {
	Event *NewFallbacksFallback // Event containing the contract specifics and raw log

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
func (it *NewFallbacksFallbackIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(NewFallbacksFallback)
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
		it.Event = new(NewFallbacksFallback)
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
func (it *NewFallbacksFallbackIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *NewFallbacksFallbackIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// NewFallbacksFallback represents a Fallback event raised by the NewFallbacks contract.
type NewFallbacksFallback struct {
	Data []byte
	Raw  types.Log // Blockchain specific contextual infos
}

// FilterFallback is a free log retrieval operation binding the contract event 0x9043988963722edecc2099c75b0af0ff76af14ffca42ed6bce059a20a2a9f986.
//
// Solidity: event Fallback(bytes data)
func (_NewFallbacks *NewFallbacksFilterer) FilterFallback(opts *bind.FilterOpts) (*NewFallbacksFallbackIterator, error) {

	logs, sub, err := _NewFallbacks.contract.FilterLogs(opts, "Fallback")
	if err != nil {
		return nil, err
	}
	return &NewFallbacksFallbackIterator{contract: _NewFallbacks.contract, event: "Fallback", logs: logs, sub: sub}, nil
}

// WatchFallback is a free log subscription operation binding the contract event 0x9043988963722edecc2099c75b0af0ff76af14ffca42ed6bce059a20a2a9f986.
//
// Solidity: event Fallback(bytes data)
func (_NewFallbacks *NewFallbacksFilterer) WatchFallback(opts *bind.WatchOpts, sink chan<- *NewFallbacksFallback) (event.Subscription, error) {

	logs, sub, err := _NewFallbacks.contract.WatchLogs(opts, "Fallback")
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(NewFallbacksFallback)
				if err := _NewFallbacks.contract.UnpackLog(event, "Fallback", log); err != nil {
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

// ParseFallback is a log parse operation binding the contract event 0x9043988963722edecc2099c75b0af0ff76af14ffca42ed6bce059a20a2a9f986.
//
// Solidity: event Fallback(bytes data)
func (_NewFallbacks *NewFallbacksFilterer) ParseFallback(log types.Log) (*NewFallbacksFallback, error) {
	event := new(NewFallbacksFallback)
	if err := _NewFallbacks.contract.UnpackLog(event, "Fallback", log); err != nil {
		return nil, err
	}
	return event, nil
}

// NewFallbacksReceivedIterator is returned from FilterReceived and is used to iterate over the raw logs and unpacked data for Received events raised by the NewFallbacks contract.
type NewFallbacksReceivedIterator struct {
	Event *NewFallbacksReceived // Event containing the contract specifics and raw log

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
func (it *NewFallbacksReceivedIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(NewFallbacksReceived)
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
		it.Event = new(NewFallbacksReceived)
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
func (it *NewFallbacksReceivedIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *NewFallbacksReceivedIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// NewFallbacksReceived represents a Received event raised by the NewFallbacks contract.
type NewFallbacksReceived struct {
	Addr  common.Address
	Value *big.Int
	Raw   types.Log // Blockchain specific contextual infos
}

// FilterReceived is a free log retrieval operation binding the contract event 0x88a5966d370b9919b20f3e2c13ff65706f196a4e32cc2c12bf57088f88525874.
//
// Solidity: event Received(address addr, uint256 value)
func (_NewFallbacks *NewFallbacksFilterer) FilterReceived(opts *bind.FilterOpts) (*NewFallbacksReceivedIterator, error) {

	logs, sub, err := _NewFallbacks.contract.FilterLogs(opts, "Received")
	if err != nil {
		return nil, err
	}
	return &NewFallbacksReceivedIterator{contract: _NewFallbacks.contract, event: "Received", logs: logs, sub: sub}, nil
}

// WatchReceived is a free log subscription operation binding the contract event 0x88a5966d370b9919b20f3e2c13ff65706f196a4e32cc2c12bf57088f88525874.
//
// Solidity: event Received(address addr, uint256 value)
func (_NewFallbacks *NewFallbacksFilterer) WatchReceived(opts *bind.WatchOpts, sink chan<- *NewFallbacksReceived) (event.Subscription, error) {

	logs, sub, err := _NewFallbacks.contract.WatchLogs(opts, "Received")
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(NewFallbacksReceived)
				if err := _NewFallbacks.contract.UnpackLog(event, "Received", log); err != nil {
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

// ParseReceived is a log parse operation binding the contract event 0x88a5966d370b9919b20f3e2c13ff65706f196a4e32cc2c12bf57088f88525874.
//
// Solidity: event Received(address addr, uint256 value)
func (_NewFallbacks *NewFallbacksFilterer) ParseReceived(log types.Log) (*NewFallbacksReceived, error) {
	event := new(NewFallbacksReceived)
	if err := _NewFallbacks.contract.UnpackLog(event, "Received", log); err != nil {
		return nil, err
	}
	return event, nil
}
