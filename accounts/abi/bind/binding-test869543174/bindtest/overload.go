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

// OverloadABI is the input ABI used to generate the binding from.
const OverloadABI = "[{\"constant\":false,\"inputs\":[{\"name\":\"i\",\"type\":\"uint256\"},{\"name\":\"j\",\"type\":\"uint256\"}],\"name\":\"foo\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"i\",\"type\":\"uint256\"}],\"name\":\"foo\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"name\":\"i\",\"type\":\"uint256\"}],\"name\":\"bar\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"name\":\"i\",\"type\":\"uint256\"},{\"indexed\":false,\"name\":\"j\",\"type\":\"uint256\"}],\"name\":\"bar\",\"type\":\"event\"}]"

// OverloadBin is the compiled bytecode used for deploying new contracts.
var OverloadBin = "0x608060405234801561001057600080fd5b50610153806100206000396000f3fe608060405234801561001057600080fd5b50600436106100365760003560e01c806304bc52f81461003b5780632fbebd3814610073575b600080fd5b6100716004803603604081101561005157600080fd5b8101908080359060200190929190803590602001909291905050506100a1565b005b61009f6004803603602081101561008957600080fd5b81019080803590602001909291905050506100e4565b005b7fae42e9514233792a47a1e4554624e83fe852228e1503f63cd383e8a431f4f46d8282604051808381526020018281526020019250505060405180910390a15050565b7f0423a1321222a0a8716c22b92fac42d85a45a612b696a461784d9fa537c81e5c816040518082815260200191505060405180910390a15056fea265627a7a72305820e22b049858b33291cbe67eeaece0c5f64333e439d27032ea8337d08b1de18fe864736f6c634300050a0032"

// DeployOverload deploys a new Ethereum contract, binding an instance of Overload to it.
func DeployOverload(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *Overload, error) {
	parsed, err := abi.JSON(strings.NewReader(OverloadABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(OverloadBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &Overload{OverloadCaller: OverloadCaller{contract: contract}, OverloadTransactor: OverloadTransactor{contract: contract}, OverloadFilterer: OverloadFilterer{contract: contract}}, nil
}

// Overload is an auto generated Go binding around an Ethereum contract.
type Overload struct {
	OverloadCaller     // Read-only binding to the contract
	OverloadTransactor // Write-only binding to the contract
	OverloadFilterer   // Log filterer for contract events
}

// OverloadCaller is an auto generated read-only Go binding around an Ethereum contract.
type OverloadCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// OverloadTransactor is an auto generated write-only Go binding around an Ethereum contract.
type OverloadTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// OverloadFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type OverloadFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// OverloadSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type OverloadSession struct {
	Contract     *Overload         // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// OverloadCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type OverloadCallerSession struct {
	Contract *OverloadCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts   // Call options to use throughout this session
}

// OverloadTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type OverloadTransactorSession struct {
	Contract     *OverloadTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts   // Transaction auth options to use throughout this session
}

// OverloadRaw is an auto generated low-level Go binding around an Ethereum contract.
type OverloadRaw struct {
	Contract *Overload // Generic contract binding to access the raw methods on
}

// OverloadCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type OverloadCallerRaw struct {
	Contract *OverloadCaller // Generic read-only contract binding to access the raw methods on
}

// OverloadTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type OverloadTransactorRaw struct {
	Contract *OverloadTransactor // Generic write-only contract binding to access the raw methods on
}

// NewOverload creates a new instance of Overload, bound to a specific deployed contract.
func NewOverload(address common.Address, backend bind.ContractBackend) (*Overload, error) {
	contract, err := bindOverload(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Overload{OverloadCaller: OverloadCaller{contract: contract}, OverloadTransactor: OverloadTransactor{contract: contract}, OverloadFilterer: OverloadFilterer{contract: contract}}, nil
}

// NewOverloadCaller creates a new read-only instance of Overload, bound to a specific deployed contract.
func NewOverloadCaller(address common.Address, caller bind.ContractCaller) (*OverloadCaller, error) {
	contract, err := bindOverload(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &OverloadCaller{contract: contract}, nil
}

// NewOverloadTransactor creates a new write-only instance of Overload, bound to a specific deployed contract.
func NewOverloadTransactor(address common.Address, transactor bind.ContractTransactor) (*OverloadTransactor, error) {
	contract, err := bindOverload(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &OverloadTransactor{contract: contract}, nil
}

// NewOverloadFilterer creates a new log filterer instance of Overload, bound to a specific deployed contract.
func NewOverloadFilterer(address common.Address, filterer bind.ContractFilterer) (*OverloadFilterer, error) {
	contract, err := bindOverload(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &OverloadFilterer{contract: contract}, nil
}

// bindOverload binds a generic wrapper to an already deployed contract.
func bindOverload(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(OverloadABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Overload *OverloadRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Overload.Contract.OverloadCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Overload *OverloadRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Overload.Contract.OverloadTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Overload *OverloadRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Overload.Contract.OverloadTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Overload *OverloadCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Overload.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Overload *OverloadTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Overload.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Overload *OverloadTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Overload.Contract.contract.Transact(opts, method, params...)
}

// Foo is a paid mutator transaction binding the contract method 0x04bc52f8.
//
// Solidity: function foo(uint256 i, uint256 j) returns()
func (_Overload *OverloadTransactor) Foo(opts *bind.TransactOpts, i *big.Int, j *big.Int) (*types.Transaction, error) {
	return _Overload.contract.Transact(opts, "foo", i, j)
}

// Foo is a paid mutator transaction binding the contract method 0x04bc52f8.
//
// Solidity: function foo(uint256 i, uint256 j) returns()
func (_Overload *OverloadSession) Foo(i *big.Int, j *big.Int) (*types.Transaction, error) {
	return _Overload.Contract.Foo(&_Overload.TransactOpts, i, j)
}

// Foo is a paid mutator transaction binding the contract method 0x04bc52f8.
//
// Solidity: function foo(uint256 i, uint256 j) returns()
func (_Overload *OverloadTransactorSession) Foo(i *big.Int, j *big.Int) (*types.Transaction, error) {
	return _Overload.Contract.Foo(&_Overload.TransactOpts, i, j)
}

// Foo0 is a paid mutator transaction binding the contract method 0x2fbebd38.
//
// Solidity: function foo(uint256 i) returns()
func (_Overload *OverloadTransactor) Foo0(opts *bind.TransactOpts, i *big.Int) (*types.Transaction, error) {
	return _Overload.contract.Transact(opts, "foo0", i)
}

// Foo0 is a paid mutator transaction binding the contract method 0x2fbebd38.
//
// Solidity: function foo(uint256 i) returns()
func (_Overload *OverloadSession) Foo0(i *big.Int) (*types.Transaction, error) {
	return _Overload.Contract.Foo0(&_Overload.TransactOpts, i)
}

// Foo0 is a paid mutator transaction binding the contract method 0x2fbebd38.
//
// Solidity: function foo(uint256 i) returns()
func (_Overload *OverloadTransactorSession) Foo0(i *big.Int) (*types.Transaction, error) {
	return _Overload.Contract.Foo0(&_Overload.TransactOpts, i)
}

// OverloadBarIterator is returned from FilterBar and is used to iterate over the raw logs and unpacked data for Bar events raised by the Overload contract.
type OverloadBarIterator struct {
	Event *OverloadBar // Event containing the contract specifics and raw log

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
func (it *OverloadBarIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(OverloadBar)
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
		it.Event = new(OverloadBar)
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
func (it *OverloadBarIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *OverloadBarIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// OverloadBar represents a Bar event raised by the Overload contract.
type OverloadBar struct {
	I   *big.Int
	Raw types.Log // Blockchain specific contextual infos
}

// FilterBar is a free log retrieval operation binding the contract event 0x0423a1321222a0a8716c22b92fac42d85a45a612b696a461784d9fa537c81e5c.
//
// Solidity: event bar(uint256 i)
func (_Overload *OverloadFilterer) FilterBar(opts *bind.FilterOpts) (*OverloadBarIterator, error) {

	logs, sub, err := _Overload.contract.FilterLogs(opts, "bar")
	if err != nil {
		return nil, err
	}
	return &OverloadBarIterator{contract: _Overload.contract, event: "bar", logs: logs, sub: sub}, nil
}

// WatchBar is a free log subscription operation binding the contract event 0x0423a1321222a0a8716c22b92fac42d85a45a612b696a461784d9fa537c81e5c.
//
// Solidity: event bar(uint256 i)
func (_Overload *OverloadFilterer) WatchBar(opts *bind.WatchOpts, sink chan<- *OverloadBar) (event.Subscription, error) {

	logs, sub, err := _Overload.contract.WatchLogs(opts, "bar")
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(OverloadBar)
				if err := _Overload.contract.UnpackLog(event, "bar", log); err != nil {
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

// ParseBar is a log parse operation binding the contract event 0x0423a1321222a0a8716c22b92fac42d85a45a612b696a461784d9fa537c81e5c.
//
// Solidity: event bar(uint256 i)
func (_Overload *OverloadFilterer) ParseBar(log types.Log) (*OverloadBar, error) {
	event := new(OverloadBar)
	if err := _Overload.contract.UnpackLog(event, "bar", log); err != nil {
		return nil, err
	}
	return event, nil
}

// OverloadBar0Iterator is returned from FilterBar0 and is used to iterate over the raw logs and unpacked data for Bar0 events raised by the Overload contract.
type OverloadBar0Iterator struct {
	Event *OverloadBar0 // Event containing the contract specifics and raw log

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
func (it *OverloadBar0Iterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(OverloadBar0)
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
		it.Event = new(OverloadBar0)
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
func (it *OverloadBar0Iterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *OverloadBar0Iterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// OverloadBar0 represents a Bar0 event raised by the Overload contract.
type OverloadBar0 struct {
	I   *big.Int
	J   *big.Int
	Raw types.Log // Blockchain specific contextual infos
}

// FilterBar0 is a free log retrieval operation binding the contract event 0xae42e9514233792a47a1e4554624e83fe852228e1503f63cd383e8a431f4f46d.
//
// Solidity: event bar(uint256 i, uint256 j)
func (_Overload *OverloadFilterer) FilterBar0(opts *bind.FilterOpts) (*OverloadBar0Iterator, error) {

	logs, sub, err := _Overload.contract.FilterLogs(opts, "bar0")
	if err != nil {
		return nil, err
	}
	return &OverloadBar0Iterator{contract: _Overload.contract, event: "bar0", logs: logs, sub: sub}, nil
}

// WatchBar0 is a free log subscription operation binding the contract event 0xae42e9514233792a47a1e4554624e83fe852228e1503f63cd383e8a431f4f46d.
//
// Solidity: event bar(uint256 i, uint256 j)
func (_Overload *OverloadFilterer) WatchBar0(opts *bind.WatchOpts, sink chan<- *OverloadBar0) (event.Subscription, error) {

	logs, sub, err := _Overload.contract.WatchLogs(opts, "bar0")
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(OverloadBar0)
				if err := _Overload.contract.UnpackLog(event, "bar0", log); err != nil {
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

// ParseBar0 is a log parse operation binding the contract event 0xae42e9514233792a47a1e4554624e83fe852228e1503f63cd383e8a431f4f46d.
//
// Solidity: event bar(uint256 i, uint256 j)
func (_Overload *OverloadFilterer) ParseBar0(log types.Log) (*OverloadBar0, error) {
	event := new(OverloadBar0)
	if err := _Overload.contract.UnpackLog(event, "bar0", log); err != nil {
		return nil, err
	}
	return event, nil
}
