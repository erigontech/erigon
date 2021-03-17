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

// EventCheckerABI is the input ABI used to generate the binding from.
const EventCheckerABI = "[{\"type\":\"event\",\"name\":\"empty\",\"inputs\":[]},{\"type\":\"event\",\"name\":\"indexed\",\"inputs\":[{\"name\":\"addr\",\"type\":\"address\",\"indexed\":true},{\"name\":\"num\",\"type\":\"int256\",\"indexed\":true}]},{\"type\":\"event\",\"name\":\"mixed\",\"inputs\":[{\"name\":\"addr\",\"type\":\"address\",\"indexed\":true},{\"name\":\"num\",\"type\":\"int256\"}]},{\"type\":\"event\",\"name\":\"anonymous\",\"anonymous\":true,\"inputs\":[]},{\"type\":\"event\",\"name\":\"dynamic\",\"inputs\":[{\"name\":\"idxStr\",\"type\":\"string\",\"indexed\":true},{\"name\":\"idxDat\",\"type\":\"bytes\",\"indexed\":true},{\"name\":\"str\",\"type\":\"string\"},{\"name\":\"dat\",\"type\":\"bytes\"}]},{\"type\":\"event\",\"name\":\"unnamed\",\"inputs\":[{\"name\":\"\",\"type\":\"uint256\",\"indexed\":true},{\"name\":\"\",\"type\":\"uint256\",\"indexed\":true}]}]"

// EventChecker is an auto generated Go binding around an Ethereum contract.
type EventChecker struct {
	EventCheckerCaller     // Read-only binding to the contract
	EventCheckerTransactor // Write-only binding to the contract
	EventCheckerFilterer   // Log filterer for contract events
}

// EventCheckerCaller is an auto generated read-only Go binding around an Ethereum contract.
type EventCheckerCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// EventCheckerTransactor is an auto generated write-only Go binding around an Ethereum contract.
type EventCheckerTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// EventCheckerFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type EventCheckerFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// EventCheckerSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type EventCheckerSession struct {
	Contract     *EventChecker     // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// EventCheckerCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type EventCheckerCallerSession struct {
	Contract *EventCheckerCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts       // Call options to use throughout this session
}

// EventCheckerTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type EventCheckerTransactorSession struct {
	Contract     *EventCheckerTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts       // Transaction auth options to use throughout this session
}

// EventCheckerRaw is an auto generated low-level Go binding around an Ethereum contract.
type EventCheckerRaw struct {
	Contract *EventChecker // Generic contract binding to access the raw methods on
}

// EventCheckerCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type EventCheckerCallerRaw struct {
	Contract *EventCheckerCaller // Generic read-only contract binding to access the raw methods on
}

// EventCheckerTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type EventCheckerTransactorRaw struct {
	Contract *EventCheckerTransactor // Generic write-only contract binding to access the raw methods on
}

// NewEventChecker creates a new instance of EventChecker, bound to a specific deployed contract.
func NewEventChecker(address common.Address, backend bind.ContractBackend) (*EventChecker, error) {
	contract, err := bindEventChecker(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &EventChecker{EventCheckerCaller: EventCheckerCaller{contract: contract}, EventCheckerTransactor: EventCheckerTransactor{contract: contract}, EventCheckerFilterer: EventCheckerFilterer{contract: contract}}, nil
}

// NewEventCheckerCaller creates a new read-only instance of EventChecker, bound to a specific deployed contract.
func NewEventCheckerCaller(address common.Address, caller bind.ContractCaller) (*EventCheckerCaller, error) {
	contract, err := bindEventChecker(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &EventCheckerCaller{contract: contract}, nil
}

// NewEventCheckerTransactor creates a new write-only instance of EventChecker, bound to a specific deployed contract.
func NewEventCheckerTransactor(address common.Address, transactor bind.ContractTransactor) (*EventCheckerTransactor, error) {
	contract, err := bindEventChecker(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &EventCheckerTransactor{contract: contract}, nil
}

// NewEventCheckerFilterer creates a new log filterer instance of EventChecker, bound to a specific deployed contract.
func NewEventCheckerFilterer(address common.Address, filterer bind.ContractFilterer) (*EventCheckerFilterer, error) {
	contract, err := bindEventChecker(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &EventCheckerFilterer{contract: contract}, nil
}

// bindEventChecker binds a generic wrapper to an already deployed contract.
func bindEventChecker(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(EventCheckerABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_EventChecker *EventCheckerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _EventChecker.Contract.EventCheckerCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_EventChecker *EventCheckerRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _EventChecker.Contract.EventCheckerTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_EventChecker *EventCheckerRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _EventChecker.Contract.EventCheckerTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_EventChecker *EventCheckerCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _EventChecker.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_EventChecker *EventCheckerTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _EventChecker.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_EventChecker *EventCheckerTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _EventChecker.Contract.contract.Transact(opts, method, params...)
}

// EventCheckerDynamicIterator is returned from FilterDynamic and is used to iterate over the raw logs and unpacked data for Dynamic events raised by the EventChecker contract.
type EventCheckerDynamicIterator struct {
	Event *EventCheckerDynamic // Event containing the contract specifics and raw log

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
func (it *EventCheckerDynamicIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(EventCheckerDynamic)
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
		it.Event = new(EventCheckerDynamic)
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
func (it *EventCheckerDynamicIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *EventCheckerDynamicIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// EventCheckerDynamic represents a Dynamic event raised by the EventChecker contract.
type EventCheckerDynamic struct {
	IdxStr common.Hash
	IdxDat common.Hash
	Str    string
	Dat    []byte
	Raw    types.Log // Blockchain specific contextual infos
}

// FilterDynamic is a free log retrieval operation binding the contract event 0x884557713ed39a810cef8832b329c1666edb477e908c90033208f086a00a1d8c.
//
// Solidity: event dynamic(string indexed idxStr, bytes indexed idxDat, string str, bytes dat)
func (_EventChecker *EventCheckerFilterer) FilterDynamic(opts *bind.FilterOpts, idxStr []string, idxDat [][]byte) (*EventCheckerDynamicIterator, error) {

	var idxStrRule []interface{}
	for _, idxStrItem := range idxStr {
		idxStrRule = append(idxStrRule, idxStrItem)
	}
	var idxDatRule []interface{}
	for _, idxDatItem := range idxDat {
		idxDatRule = append(idxDatRule, idxDatItem)
	}

	logs, sub, err := _EventChecker.contract.FilterLogs(opts, "dynamic", idxStrRule, idxDatRule)
	if err != nil {
		return nil, err
	}
	return &EventCheckerDynamicIterator{contract: _EventChecker.contract, event: "dynamic", logs: logs, sub: sub}, nil
}

// WatchDynamic is a free log subscription operation binding the contract event 0x884557713ed39a810cef8832b329c1666edb477e908c90033208f086a00a1d8c.
//
// Solidity: event dynamic(string indexed idxStr, bytes indexed idxDat, string str, bytes dat)
func (_EventChecker *EventCheckerFilterer) WatchDynamic(opts *bind.WatchOpts, sink chan<- *EventCheckerDynamic, idxStr []string, idxDat [][]byte) (event.Subscription, error) {

	var idxStrRule []interface{}
	for _, idxStrItem := range idxStr {
		idxStrRule = append(idxStrRule, idxStrItem)
	}
	var idxDatRule []interface{}
	for _, idxDatItem := range idxDat {
		idxDatRule = append(idxDatRule, idxDatItem)
	}

	logs, sub, err := _EventChecker.contract.WatchLogs(opts, "dynamic", idxStrRule, idxDatRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(EventCheckerDynamic)
				if err := _EventChecker.contract.UnpackLog(event, "dynamic", log); err != nil {
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

// ParseDynamic is a log parse operation binding the contract event 0x884557713ed39a810cef8832b329c1666edb477e908c90033208f086a00a1d8c.
//
// Solidity: event dynamic(string indexed idxStr, bytes indexed idxDat, string str, bytes dat)
func (_EventChecker *EventCheckerFilterer) ParseDynamic(log types.Log) (*EventCheckerDynamic, error) {
	event := new(EventCheckerDynamic)
	if err := _EventChecker.contract.UnpackLog(event, "dynamic", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}

// EventCheckerEmptyIterator is returned from FilterEmpty and is used to iterate over the raw logs and unpacked data for Empty events raised by the EventChecker contract.
type EventCheckerEmptyIterator struct {
	Event *EventCheckerEmpty // Event containing the contract specifics and raw log

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
func (it *EventCheckerEmptyIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(EventCheckerEmpty)
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
		it.Event = new(EventCheckerEmpty)
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
func (it *EventCheckerEmptyIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *EventCheckerEmptyIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// EventCheckerEmpty represents a Empty event raised by the EventChecker contract.
type EventCheckerEmpty struct {
	Raw types.Log // Blockchain specific contextual infos
}

// FilterEmpty is a free log retrieval operation binding the contract event 0xf2a75fe4d7cd25bbe769d566fbf5a20c67283479ab765ae5857c5ba963305b55.
//
// Solidity: event empty()
func (_EventChecker *EventCheckerFilterer) FilterEmpty(opts *bind.FilterOpts) (*EventCheckerEmptyIterator, error) {

	logs, sub, err := _EventChecker.contract.FilterLogs(opts, "empty")
	if err != nil {
		return nil, err
	}
	return &EventCheckerEmptyIterator{contract: _EventChecker.contract, event: "empty", logs: logs, sub: sub}, nil
}

// WatchEmpty is a free log subscription operation binding the contract event 0xf2a75fe4d7cd25bbe769d566fbf5a20c67283479ab765ae5857c5ba963305b55.
//
// Solidity: event empty()
func (_EventChecker *EventCheckerFilterer) WatchEmpty(opts *bind.WatchOpts, sink chan<- *EventCheckerEmpty) (event.Subscription, error) {

	logs, sub, err := _EventChecker.contract.WatchLogs(opts, "empty")
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(EventCheckerEmpty)
				if err := _EventChecker.contract.UnpackLog(event, "empty", log); err != nil {
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

// ParseEmpty is a log parse operation binding the contract event 0xf2a75fe4d7cd25bbe769d566fbf5a20c67283479ab765ae5857c5ba963305b55.
//
// Solidity: event empty()
func (_EventChecker *EventCheckerFilterer) ParseEmpty(log types.Log) (*EventCheckerEmpty, error) {
	event := new(EventCheckerEmpty)
	if err := _EventChecker.contract.UnpackLog(event, "empty", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}

// EventCheckerIndexedIterator is returned from FilterIndexed and is used to iterate over the raw logs and unpacked data for Indexed events raised by the EventChecker contract.
type EventCheckerIndexedIterator struct {
	Event *EventCheckerIndexed // Event containing the contract specifics and raw log

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
func (it *EventCheckerIndexedIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(EventCheckerIndexed)
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
		it.Event = new(EventCheckerIndexed)
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
func (it *EventCheckerIndexedIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *EventCheckerIndexedIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// EventCheckerIndexed represents a Indexed event raised by the EventChecker contract.
type EventCheckerIndexed struct {
	Addr common.Address
	Num  *big.Int
	Raw  types.Log // Blockchain specific contextual infos
}

// FilterIndexed is a free log retrieval operation binding the contract event 0x68f08c93f2bc784352cdd6cec6d8155cd183e8d593cec16c4e6e28c376d0a757.
//
// Solidity: event indexed(address indexed addr, int256 indexed num)
func (_EventChecker *EventCheckerFilterer) FilterIndexed(opts *bind.FilterOpts, addr []common.Address, num []*big.Int) (*EventCheckerIndexedIterator, error) {

	var addrRule []interface{}
	for _, addrItem := range addr {
		addrRule = append(addrRule, addrItem)
	}
	var numRule []interface{}
	for _, numItem := range num {
		numRule = append(numRule, numItem)
	}

	logs, sub, err := _EventChecker.contract.FilterLogs(opts, "indexed", addrRule, numRule)
	if err != nil {
		return nil, err
	}
	return &EventCheckerIndexedIterator{contract: _EventChecker.contract, event: "indexed", logs: logs, sub: sub}, nil
}

// WatchIndexed is a free log subscription operation binding the contract event 0x68f08c93f2bc784352cdd6cec6d8155cd183e8d593cec16c4e6e28c376d0a757.
//
// Solidity: event indexed(address indexed addr, int256 indexed num)
func (_EventChecker *EventCheckerFilterer) WatchIndexed(opts *bind.WatchOpts, sink chan<- *EventCheckerIndexed, addr []common.Address, num []*big.Int) (event.Subscription, error) {

	var addrRule []interface{}
	for _, addrItem := range addr {
		addrRule = append(addrRule, addrItem)
	}
	var numRule []interface{}
	for _, numItem := range num {
		numRule = append(numRule, numItem)
	}

	logs, sub, err := _EventChecker.contract.WatchLogs(opts, "indexed", addrRule, numRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(EventCheckerIndexed)
				if err := _EventChecker.contract.UnpackLog(event, "indexed", log); err != nil {
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

// ParseIndexed is a log parse operation binding the contract event 0x68f08c93f2bc784352cdd6cec6d8155cd183e8d593cec16c4e6e28c376d0a757.
//
// Solidity: event indexed(address indexed addr, int256 indexed num)
func (_EventChecker *EventCheckerFilterer) ParseIndexed(log types.Log) (*EventCheckerIndexed, error) {
	event := new(EventCheckerIndexed)
	if err := _EventChecker.contract.UnpackLog(event, "indexed", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}

// EventCheckerMixedIterator is returned from FilterMixed and is used to iterate over the raw logs and unpacked data for Mixed events raised by the EventChecker contract.
type EventCheckerMixedIterator struct {
	Event *EventCheckerMixed // Event containing the contract specifics and raw log

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
func (it *EventCheckerMixedIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(EventCheckerMixed)
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
		it.Event = new(EventCheckerMixed)
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
func (it *EventCheckerMixedIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *EventCheckerMixedIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// EventCheckerMixed represents a Mixed event raised by the EventChecker contract.
type EventCheckerMixed struct {
	Addr common.Address
	Num  *big.Int
	Raw  types.Log // Blockchain specific contextual infos
}

// FilterMixed is a free log retrieval operation binding the contract event 0xfd60ab978c670ea276c6de7291142f38390018998199f6cd242c4337b8003406.
//
// Solidity: event mixed(address indexed addr, int256 num)
func (_EventChecker *EventCheckerFilterer) FilterMixed(opts *bind.FilterOpts, addr []common.Address) (*EventCheckerMixedIterator, error) {

	var addrRule []interface{}
	for _, addrItem := range addr {
		addrRule = append(addrRule, addrItem)
	}

	logs, sub, err := _EventChecker.contract.FilterLogs(opts, "mixed", addrRule)
	if err != nil {
		return nil, err
	}
	return &EventCheckerMixedIterator{contract: _EventChecker.contract, event: "mixed", logs: logs, sub: sub}, nil
}

// WatchMixed is a free log subscription operation binding the contract event 0xfd60ab978c670ea276c6de7291142f38390018998199f6cd242c4337b8003406.
//
// Solidity: event mixed(address indexed addr, int256 num)
func (_EventChecker *EventCheckerFilterer) WatchMixed(opts *bind.WatchOpts, sink chan<- *EventCheckerMixed, addr []common.Address) (event.Subscription, error) {

	var addrRule []interface{}
	for _, addrItem := range addr {
		addrRule = append(addrRule, addrItem)
	}

	logs, sub, err := _EventChecker.contract.WatchLogs(opts, "mixed", addrRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(EventCheckerMixed)
				if err := _EventChecker.contract.UnpackLog(event, "mixed", log); err != nil {
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

// ParseMixed is a log parse operation binding the contract event 0xfd60ab978c670ea276c6de7291142f38390018998199f6cd242c4337b8003406.
//
// Solidity: event mixed(address indexed addr, int256 num)
func (_EventChecker *EventCheckerFilterer) ParseMixed(log types.Log) (*EventCheckerMixed, error) {
	event := new(EventCheckerMixed)
	if err := _EventChecker.contract.UnpackLog(event, "mixed", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}

// EventCheckerUnnamedIterator is returned from FilterUnnamed and is used to iterate over the raw logs and unpacked data for Unnamed events raised by the EventChecker contract.
type EventCheckerUnnamedIterator struct {
	Event *EventCheckerUnnamed // Event containing the contract specifics and raw log

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
func (it *EventCheckerUnnamedIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(EventCheckerUnnamed)
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
		it.Event = new(EventCheckerUnnamed)
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
func (it *EventCheckerUnnamedIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *EventCheckerUnnamedIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// EventCheckerUnnamed represents a Unnamed event raised by the EventChecker contract.
type EventCheckerUnnamed struct {
	Arg0 *big.Int
	Arg1 *big.Int
	Raw  types.Log // Blockchain specific contextual infos
}

// FilterUnnamed is a free log retrieval operation binding the contract event 0x91f37301972b2ec7db5b931c909e4fbe7ecfd001b4255dd3c2df14821afc7d39.
//
// Solidity: event unnamed(uint256 indexed arg0, uint256 indexed arg1)
func (_EventChecker *EventCheckerFilterer) FilterUnnamed(opts *bind.FilterOpts, arg0 []*big.Int, arg1 []*big.Int) (*EventCheckerUnnamedIterator, error) {

	var arg0Rule []interface{}
	for _, arg0Item := range arg0 {
		arg0Rule = append(arg0Rule, arg0Item)
	}
	var arg1Rule []interface{}
	for _, arg1Item := range arg1 {
		arg1Rule = append(arg1Rule, arg1Item)
	}

	logs, sub, err := _EventChecker.contract.FilterLogs(opts, "unnamed", arg0Rule, arg1Rule)
	if err != nil {
		return nil, err
	}
	return &EventCheckerUnnamedIterator{contract: _EventChecker.contract, event: "unnamed", logs: logs, sub: sub}, nil
}

// WatchUnnamed is a free log subscription operation binding the contract event 0x91f37301972b2ec7db5b931c909e4fbe7ecfd001b4255dd3c2df14821afc7d39.
//
// Solidity: event unnamed(uint256 indexed arg0, uint256 indexed arg1)
func (_EventChecker *EventCheckerFilterer) WatchUnnamed(opts *bind.WatchOpts, sink chan<- *EventCheckerUnnamed, arg0 []*big.Int, arg1 []*big.Int) (event.Subscription, error) {

	var arg0Rule []interface{}
	for _, arg0Item := range arg0 {
		arg0Rule = append(arg0Rule, arg0Item)
	}
	var arg1Rule []interface{}
	for _, arg1Item := range arg1 {
		arg1Rule = append(arg1Rule, arg1Item)
	}

	logs, sub, err := _EventChecker.contract.WatchLogs(opts, "unnamed", arg0Rule, arg1Rule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(EventCheckerUnnamed)
				if err := _EventChecker.contract.UnpackLog(event, "unnamed", log); err != nil {
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

// ParseUnnamed is a log parse operation binding the contract event 0x91f37301972b2ec7db5b931c909e4fbe7ecfd001b4255dd3c2df14821afc7d39.
//
// Solidity: event unnamed(uint256 indexed arg0, uint256 indexed arg1)
func (_EventChecker *EventCheckerFilterer) ParseUnnamed(log types.Log) (*EventCheckerUnnamed, error) {
	event := new(EventCheckerUnnamed)
	if err := _EventChecker.contract.UnpackLog(event, "unnamed", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}
