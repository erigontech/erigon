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

// EventerABI is the input ABI used to generate the binding from.
const EventerABI = "[{\"constant\":false,\"inputs\":[{\"name\":\"str\",\"type\":\"string\"},{\"name\":\"blob\",\"type\":\"bytes\"}],\"name\":\"raiseDynamicEvent\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"addr\",\"type\":\"address\"},{\"name\":\"id\",\"type\":\"bytes32\"},{\"name\":\"flag\",\"type\":\"bool\"},{\"name\":\"value\",\"type\":\"uint256\"}],\"name\":\"raiseSimpleEvent\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"blob\",\"type\":\"bytes24\"}],\"name\":\"raiseFixedBytesEvent\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"number\",\"type\":\"uint256\"},{\"name\":\"short\",\"type\":\"int16\"},{\"name\":\"long\",\"type\":\"uint32\"}],\"name\":\"raiseNodataEvent\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"name\":\"Addr\",\"type\":\"address\"},{\"indexed\":true,\"name\":\"Id\",\"type\":\"bytes32\"},{\"indexed\":true,\"name\":\"Flag\",\"type\":\"bool\"},{\"indexed\":false,\"name\":\"Value\",\"type\":\"uint256\"}],\"name\":\"SimpleEvent\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"name\":\"Number\",\"type\":\"uint256\"},{\"indexed\":true,\"name\":\"Short\",\"type\":\"int16\"},{\"indexed\":true,\"name\":\"Long\",\"type\":\"uint32\"}],\"name\":\"NodataEvent\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"name\":\"IndexedString\",\"type\":\"string\"},{\"indexed\":true,\"name\":\"IndexedBytes\",\"type\":\"bytes\"},{\"indexed\":false,\"name\":\"NonIndexedString\",\"type\":\"string\"},{\"indexed\":false,\"name\":\"NonIndexedBytes\",\"type\":\"bytes\"}],\"name\":\"DynamicEvent\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"name\":\"IndexedBytes\",\"type\":\"bytes24\"},{\"indexed\":false,\"name\":\"NonIndexedBytes\",\"type\":\"bytes24\"}],\"name\":\"FixedBytesEvent\",\"type\":\"event\"}]"

// EventerBin is the compiled bytecode used for deploying new contracts.
var EventerBin = "0x608060405234801561001057600080fd5b5061043f806100206000396000f3006080604052600436106100615763ffffffff7c0100000000000000000000000000000000000000000000000000000000600035041663528300ff8114610066578063630c31e2146100ff5780636cc6b94014610138578063c7d116dd1461015b575b600080fd5b34801561007257600080fd5b506040805160206004803580820135601f81018490048402850184019095528484526100fd94369492936024939284019190819084018382808284375050604080516020601f89358b018035918201839004830284018301909452808352979a9998810197919650918201945092508291508401838280828437509497506101829650505050505050565b005b34801561010b57600080fd5b506100fd73ffffffffffffffffffffffffffffffffffffffff60043516602435604435151560643561033c565b34801561014457600080fd5b506100fd67ffffffffffffffff1960043516610394565b34801561016757600080fd5b506100fd60043560243560010b63ffffffff604435166103d6565b806040518082805190602001908083835b602083106101b25780518252601f199092019160209182019101610193565b51815160209384036101000a6000190180199092169116179052604051919093018190038120875190955087945090928392508401908083835b6020831061020b5780518252601f1990920191602091820191016101ec565b6001836020036101000a03801982511681845116808217855250505050505090500191505060405180910390207f3281fd4f5e152dd3385df49104a3f633706e21c9e80672e88d3bcddf33101f008484604051808060200180602001838103835285818151815260200191508051906020019080838360005b8381101561029c578181015183820152602001610284565b50505050905090810190601f1680156102c95780820380516001836020036101000a031916815260200191505b50838103825284518152845160209182019186019080838360005b838110156102fc5781810151838201526020016102e4565b50505050905090810190601f1680156103295780820380516001836020036101000a031916815260200191505b5094505050505060405180910390a35050565b60408051828152905183151591859173ffffffffffffffffffffffffffffffffffffffff8816917f1f097de4289df643bd9c11011cc61367aa12983405c021056e706eb5ba1250c8919081900360200190a450505050565b6040805167ffffffffffffffff19831680825291517fcdc4c1b1aed5524ffb4198d7a5839a34712baef5fa06884fac7559f4a5854e0a9181900360200190a250565b8063ffffffff168260010b847f3ca7f3a77e5e6e15e781850bc82e32adfa378a2a609370db24b4d0fae10da2c960405160405180910390a45050505600a165627a7a72305820468b5843bf653145bd924b323c64ef035d3dd922c170644b44d61aa666ea6eee0029"

// DeployEventer deploys a new Ethereum contract, binding an instance of Eventer to it.
func DeployEventer(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *Eventer, error) {
	parsed, err := abi.JSON(strings.NewReader(EventerABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(EventerBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &Eventer{EventerCaller: EventerCaller{contract: contract}, EventerTransactor: EventerTransactor{contract: contract}, EventerFilterer: EventerFilterer{contract: contract}}, nil
}

// Eventer is an auto generated Go binding around an Ethereum contract.
type Eventer struct {
	EventerCaller     // Read-only binding to the contract
	EventerTransactor // Write-only binding to the contract
	EventerFilterer   // Log filterer for contract events
}

// EventerCaller is an auto generated read-only Go binding around an Ethereum contract.
type EventerCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// EventerTransactor is an auto generated write-only Go binding around an Ethereum contract.
type EventerTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// EventerFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type EventerFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// EventerSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type EventerSession struct {
	Contract     *Eventer          // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// EventerCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type EventerCallerSession struct {
	Contract *EventerCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts  // Call options to use throughout this session
}

// EventerTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type EventerTransactorSession struct {
	Contract     *EventerTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts  // Transaction auth options to use throughout this session
}

// EventerRaw is an auto generated low-level Go binding around an Ethereum contract.
type EventerRaw struct {
	Contract *Eventer // Generic contract binding to access the raw methods on
}

// EventerCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type EventerCallerRaw struct {
	Contract *EventerCaller // Generic read-only contract binding to access the raw methods on
}

// EventerTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type EventerTransactorRaw struct {
	Contract *EventerTransactor // Generic write-only contract binding to access the raw methods on
}

// NewEventer creates a new instance of Eventer, bound to a specific deployed contract.
func NewEventer(address common.Address, backend bind.ContractBackend) (*Eventer, error) {
	contract, err := bindEventer(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Eventer{EventerCaller: EventerCaller{contract: contract}, EventerTransactor: EventerTransactor{contract: contract}, EventerFilterer: EventerFilterer{contract: contract}}, nil
}

// NewEventerCaller creates a new read-only instance of Eventer, bound to a specific deployed contract.
func NewEventerCaller(address common.Address, caller bind.ContractCaller) (*EventerCaller, error) {
	contract, err := bindEventer(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &EventerCaller{contract: contract}, nil
}

// NewEventerTransactor creates a new write-only instance of Eventer, bound to a specific deployed contract.
func NewEventerTransactor(address common.Address, transactor bind.ContractTransactor) (*EventerTransactor, error) {
	contract, err := bindEventer(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &EventerTransactor{contract: contract}, nil
}

// NewEventerFilterer creates a new log filterer instance of Eventer, bound to a specific deployed contract.
func NewEventerFilterer(address common.Address, filterer bind.ContractFilterer) (*EventerFilterer, error) {
	contract, err := bindEventer(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &EventerFilterer{contract: contract}, nil
}

// bindEventer binds a generic wrapper to an already deployed contract.
func bindEventer(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(EventerABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Eventer *EventerRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Eventer.Contract.EventerCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Eventer *EventerRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Eventer.Contract.EventerTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Eventer *EventerRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Eventer.Contract.EventerTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Eventer *EventerCallerRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Eventer.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Eventer *EventerTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Eventer.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Eventer *EventerTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Eventer.Contract.contract.Transact(opts, method, params...)
}

// RaiseDynamicEvent is a paid mutator transaction binding the contract method 0x528300ff.
//
// Solidity: function raiseDynamicEvent(string str, bytes blob) returns()
func (_Eventer *EventerTransactor) RaiseDynamicEvent(opts *bind.TransactOpts, str string, blob []byte) (*types.Transaction, error) {
	return _Eventer.contract.Transact(opts, "raiseDynamicEvent", str, blob)
}

// RaiseDynamicEvent is a paid mutator transaction binding the contract method 0x528300ff.
//
// Solidity: function raiseDynamicEvent(string str, bytes blob) returns()
func (_Eventer *EventerSession) RaiseDynamicEvent(str string, blob []byte) (*types.Transaction, error) {
	return _Eventer.Contract.RaiseDynamicEvent(&_Eventer.TransactOpts, str, blob)
}

// RaiseDynamicEvent is a paid mutator transaction binding the contract method 0x528300ff.
//
// Solidity: function raiseDynamicEvent(string str, bytes blob) returns()
func (_Eventer *EventerTransactorSession) RaiseDynamicEvent(str string, blob []byte) (*types.Transaction, error) {
	return _Eventer.Contract.RaiseDynamicEvent(&_Eventer.TransactOpts, str, blob)
}

// RaiseFixedBytesEvent is a paid mutator transaction binding the contract method 0x6cc6b940.
//
// Solidity: function raiseFixedBytesEvent(bytes24 blob) returns()
func (_Eventer *EventerTransactor) RaiseFixedBytesEvent(opts *bind.TransactOpts, blob [24]byte) (*types.Transaction, error) {
	return _Eventer.contract.Transact(opts, "raiseFixedBytesEvent", blob)
}

// RaiseFixedBytesEvent is a paid mutator transaction binding the contract method 0x6cc6b940.
//
// Solidity: function raiseFixedBytesEvent(bytes24 blob) returns()
func (_Eventer *EventerSession) RaiseFixedBytesEvent(blob [24]byte) (*types.Transaction, error) {
	return _Eventer.Contract.RaiseFixedBytesEvent(&_Eventer.TransactOpts, blob)
}

// RaiseFixedBytesEvent is a paid mutator transaction binding the contract method 0x6cc6b940.
//
// Solidity: function raiseFixedBytesEvent(bytes24 blob) returns()
func (_Eventer *EventerTransactorSession) RaiseFixedBytesEvent(blob [24]byte) (*types.Transaction, error) {
	return _Eventer.Contract.RaiseFixedBytesEvent(&_Eventer.TransactOpts, blob)
}

// RaiseNodataEvent is a paid mutator transaction binding the contract method 0xc7d116dd.
//
// Solidity: function raiseNodataEvent(uint256 number, int16 short, uint32 long) returns()
func (_Eventer *EventerTransactor) RaiseNodataEvent(opts *bind.TransactOpts, number *big.Int, short int16, long uint32) (*types.Transaction, error) {
	return _Eventer.contract.Transact(opts, "raiseNodataEvent", number, short, long)
}

// RaiseNodataEvent is a paid mutator transaction binding the contract method 0xc7d116dd.
//
// Solidity: function raiseNodataEvent(uint256 number, int16 short, uint32 long) returns()
func (_Eventer *EventerSession) RaiseNodataEvent(number *big.Int, short int16, long uint32) (*types.Transaction, error) {
	return _Eventer.Contract.RaiseNodataEvent(&_Eventer.TransactOpts, number, short, long)
}

// RaiseNodataEvent is a paid mutator transaction binding the contract method 0xc7d116dd.
//
// Solidity: function raiseNodataEvent(uint256 number, int16 short, uint32 long) returns()
func (_Eventer *EventerTransactorSession) RaiseNodataEvent(number *big.Int, short int16, long uint32) (*types.Transaction, error) {
	return _Eventer.Contract.RaiseNodataEvent(&_Eventer.TransactOpts, number, short, long)
}

// RaiseSimpleEvent is a paid mutator transaction binding the contract method 0x630c31e2.
//
// Solidity: function raiseSimpleEvent(address addr, bytes32 id, bool flag, uint256 value) returns()
func (_Eventer *EventerTransactor) RaiseSimpleEvent(opts *bind.TransactOpts, addr common.Address, id [32]byte, flag bool, value *big.Int) (*types.Transaction, error) {
	return _Eventer.contract.Transact(opts, "raiseSimpleEvent", addr, id, flag, value)
}

// RaiseSimpleEvent is a paid mutator transaction binding the contract method 0x630c31e2.
//
// Solidity: function raiseSimpleEvent(address addr, bytes32 id, bool flag, uint256 value) returns()
func (_Eventer *EventerSession) RaiseSimpleEvent(addr common.Address, id [32]byte, flag bool, value *big.Int) (*types.Transaction, error) {
	return _Eventer.Contract.RaiseSimpleEvent(&_Eventer.TransactOpts, addr, id, flag, value)
}

// RaiseSimpleEvent is a paid mutator transaction binding the contract method 0x630c31e2.
//
// Solidity: function raiseSimpleEvent(address addr, bytes32 id, bool flag, uint256 value) returns()
func (_Eventer *EventerTransactorSession) RaiseSimpleEvent(addr common.Address, id [32]byte, flag bool, value *big.Int) (*types.Transaction, error) {
	return _Eventer.Contract.RaiseSimpleEvent(&_Eventer.TransactOpts, addr, id, flag, value)
}

// EventerDynamicEventIterator is returned from FilterDynamicEvent and is used to iterate over the raw logs and unpacked data for DynamicEvent events raised by the Eventer contract.
type EventerDynamicEventIterator struct {
	Event *EventerDynamicEvent // Event containing the contract specifics and raw log

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
func (it *EventerDynamicEventIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(EventerDynamicEvent)
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
		it.Event = new(EventerDynamicEvent)
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
func (it *EventerDynamicEventIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *EventerDynamicEventIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// EventerDynamicEvent represents a DynamicEvent event raised by the Eventer contract.
type EventerDynamicEvent struct {
	IndexedString    common.Hash
	IndexedBytes     common.Hash
	NonIndexedString string
	NonIndexedBytes  []byte
	Raw              types.Log // Blockchain specific contextual infos
}

// FilterDynamicEvent is a free log retrieval operation binding the contract event 0x3281fd4f5e152dd3385df49104a3f633706e21c9e80672e88d3bcddf33101f00.
//
// Solidity: event DynamicEvent(string indexed IndexedString, bytes indexed IndexedBytes, string NonIndexedString, bytes NonIndexedBytes)
func (_Eventer *EventerFilterer) FilterDynamicEvent(opts *bind.FilterOpts, IndexedString []string, IndexedBytes [][]byte) (*EventerDynamicEventIterator, error) {

	var IndexedStringRule []interface{}
	for _, IndexedStringItem := range IndexedString {
		IndexedStringRule = append(IndexedStringRule, IndexedStringItem)
	}
	var IndexedBytesRule []interface{}
	for _, IndexedBytesItem := range IndexedBytes {
		IndexedBytesRule = append(IndexedBytesRule, IndexedBytesItem)
	}

	logs, sub, err := _Eventer.contract.FilterLogs(opts, "DynamicEvent", IndexedStringRule, IndexedBytesRule)
	if err != nil {
		return nil, err
	}
	return &EventerDynamicEventIterator{contract: _Eventer.contract, event: "DynamicEvent", logs: logs, sub: sub}, nil
}

// WatchDynamicEvent is a free log subscription operation binding the contract event 0x3281fd4f5e152dd3385df49104a3f633706e21c9e80672e88d3bcddf33101f00.
//
// Solidity: event DynamicEvent(string indexed IndexedString, bytes indexed IndexedBytes, string NonIndexedString, bytes NonIndexedBytes)
func (_Eventer *EventerFilterer) WatchDynamicEvent(opts *bind.WatchOpts, sink chan<- *EventerDynamicEvent, IndexedString []string, IndexedBytes [][]byte) (event.Subscription, error) {

	var IndexedStringRule []interface{}
	for _, IndexedStringItem := range IndexedString {
		IndexedStringRule = append(IndexedStringRule, IndexedStringItem)
	}
	var IndexedBytesRule []interface{}
	for _, IndexedBytesItem := range IndexedBytes {
		IndexedBytesRule = append(IndexedBytesRule, IndexedBytesItem)
	}

	logs, sub, err := _Eventer.contract.WatchLogs(opts, "DynamicEvent", IndexedStringRule, IndexedBytesRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(EventerDynamicEvent)
				if err := _Eventer.contract.UnpackLog(event, "DynamicEvent", log); err != nil {
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

// ParseDynamicEvent is a log parse operation binding the contract event 0x3281fd4f5e152dd3385df49104a3f633706e21c9e80672e88d3bcddf33101f00.
//
// Solidity: event DynamicEvent(string indexed IndexedString, bytes indexed IndexedBytes, string NonIndexedString, bytes NonIndexedBytes)
func (_Eventer *EventerFilterer) ParseDynamicEvent(log types.Log) (*EventerDynamicEvent, error) {
	event := new(EventerDynamicEvent)
	if err := _Eventer.contract.UnpackLog(event, "DynamicEvent", log); err != nil {
		return nil, err
	}
	return event, nil
}

// EventerFixedBytesEventIterator is returned from FilterFixedBytesEvent and is used to iterate over the raw logs and unpacked data for FixedBytesEvent events raised by the Eventer contract.
type EventerFixedBytesEventIterator struct {
	Event *EventerFixedBytesEvent // Event containing the contract specifics and raw log

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
func (it *EventerFixedBytesEventIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(EventerFixedBytesEvent)
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
		it.Event = new(EventerFixedBytesEvent)
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
func (it *EventerFixedBytesEventIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *EventerFixedBytesEventIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// EventerFixedBytesEvent represents a FixedBytesEvent event raised by the Eventer contract.
type EventerFixedBytesEvent struct {
	IndexedBytes    [24]byte
	NonIndexedBytes [24]byte
	Raw             types.Log // Blockchain specific contextual infos
}

// FilterFixedBytesEvent is a free log retrieval operation binding the contract event 0xcdc4c1b1aed5524ffb4198d7a5839a34712baef5fa06884fac7559f4a5854e0a.
//
// Solidity: event FixedBytesEvent(bytes24 indexed IndexedBytes, bytes24 NonIndexedBytes)
func (_Eventer *EventerFilterer) FilterFixedBytesEvent(opts *bind.FilterOpts, IndexedBytes [][24]byte) (*EventerFixedBytesEventIterator, error) {

	var IndexedBytesRule []interface{}
	for _, IndexedBytesItem := range IndexedBytes {
		IndexedBytesRule = append(IndexedBytesRule, IndexedBytesItem)
	}

	logs, sub, err := _Eventer.contract.FilterLogs(opts, "FixedBytesEvent", IndexedBytesRule)
	if err != nil {
		return nil, err
	}
	return &EventerFixedBytesEventIterator{contract: _Eventer.contract, event: "FixedBytesEvent", logs: logs, sub: sub}, nil
}

// WatchFixedBytesEvent is a free log subscription operation binding the contract event 0xcdc4c1b1aed5524ffb4198d7a5839a34712baef5fa06884fac7559f4a5854e0a.
//
// Solidity: event FixedBytesEvent(bytes24 indexed IndexedBytes, bytes24 NonIndexedBytes)
func (_Eventer *EventerFilterer) WatchFixedBytesEvent(opts *bind.WatchOpts, sink chan<- *EventerFixedBytesEvent, IndexedBytes [][24]byte) (event.Subscription, error) {

	var IndexedBytesRule []interface{}
	for _, IndexedBytesItem := range IndexedBytes {
		IndexedBytesRule = append(IndexedBytesRule, IndexedBytesItem)
	}

	logs, sub, err := _Eventer.contract.WatchLogs(opts, "FixedBytesEvent", IndexedBytesRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(EventerFixedBytesEvent)
				if err := _Eventer.contract.UnpackLog(event, "FixedBytesEvent", log); err != nil {
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

// ParseFixedBytesEvent is a log parse operation binding the contract event 0xcdc4c1b1aed5524ffb4198d7a5839a34712baef5fa06884fac7559f4a5854e0a.
//
// Solidity: event FixedBytesEvent(bytes24 indexed IndexedBytes, bytes24 NonIndexedBytes)
func (_Eventer *EventerFilterer) ParseFixedBytesEvent(log types.Log) (*EventerFixedBytesEvent, error) {
	event := new(EventerFixedBytesEvent)
	if err := _Eventer.contract.UnpackLog(event, "FixedBytesEvent", log); err != nil {
		return nil, err
	}
	return event, nil
}

// EventerNodataEventIterator is returned from FilterNodataEvent and is used to iterate over the raw logs and unpacked data for NodataEvent events raised by the Eventer contract.
type EventerNodataEventIterator struct {
	Event *EventerNodataEvent // Event containing the contract specifics and raw log

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
func (it *EventerNodataEventIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(EventerNodataEvent)
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
		it.Event = new(EventerNodataEvent)
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
func (it *EventerNodataEventIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *EventerNodataEventIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// EventerNodataEvent represents a NodataEvent event raised by the Eventer contract.
type EventerNodataEvent struct {
	Number *big.Int
	Short  int16
	Long   uint32
	Raw    types.Log // Blockchain specific contextual infos
}

// FilterNodataEvent is a free log retrieval operation binding the contract event 0x3ca7f3a77e5e6e15e781850bc82e32adfa378a2a609370db24b4d0fae10da2c9.
//
// Solidity: event NodataEvent(uint256 indexed Number, int16 indexed Short, uint32 indexed Long)
func (_Eventer *EventerFilterer) FilterNodataEvent(opts *bind.FilterOpts, Number []*big.Int, Short []int16, Long []uint32) (*EventerNodataEventIterator, error) {

	var NumberRule []interface{}
	for _, NumberItem := range Number {
		NumberRule = append(NumberRule, NumberItem)
	}
	var ShortRule []interface{}
	for _, ShortItem := range Short {
		ShortRule = append(ShortRule, ShortItem)
	}
	var LongRule []interface{}
	for _, LongItem := range Long {
		LongRule = append(LongRule, LongItem)
	}

	logs, sub, err := _Eventer.contract.FilterLogs(opts, "NodataEvent", NumberRule, ShortRule, LongRule)
	if err != nil {
		return nil, err
	}
	return &EventerNodataEventIterator{contract: _Eventer.contract, event: "NodataEvent", logs: logs, sub: sub}, nil
}

// WatchNodataEvent is a free log subscription operation binding the contract event 0x3ca7f3a77e5e6e15e781850bc82e32adfa378a2a609370db24b4d0fae10da2c9.
//
// Solidity: event NodataEvent(uint256 indexed Number, int16 indexed Short, uint32 indexed Long)
func (_Eventer *EventerFilterer) WatchNodataEvent(opts *bind.WatchOpts, sink chan<- *EventerNodataEvent, Number []*big.Int, Short []int16, Long []uint32) (event.Subscription, error) {

	var NumberRule []interface{}
	for _, NumberItem := range Number {
		NumberRule = append(NumberRule, NumberItem)
	}
	var ShortRule []interface{}
	for _, ShortItem := range Short {
		ShortRule = append(ShortRule, ShortItem)
	}
	var LongRule []interface{}
	for _, LongItem := range Long {
		LongRule = append(LongRule, LongItem)
	}

	logs, sub, err := _Eventer.contract.WatchLogs(opts, "NodataEvent", NumberRule, ShortRule, LongRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(EventerNodataEvent)
				if err := _Eventer.contract.UnpackLog(event, "NodataEvent", log); err != nil {
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

// ParseNodataEvent is a log parse operation binding the contract event 0x3ca7f3a77e5e6e15e781850bc82e32adfa378a2a609370db24b4d0fae10da2c9.
//
// Solidity: event NodataEvent(uint256 indexed Number, int16 indexed Short, uint32 indexed Long)
func (_Eventer *EventerFilterer) ParseNodataEvent(log types.Log) (*EventerNodataEvent, error) {
	event := new(EventerNodataEvent)
	if err := _Eventer.contract.UnpackLog(event, "NodataEvent", log); err != nil {
		return nil, err
	}
	return event, nil
}

// EventerSimpleEventIterator is returned from FilterSimpleEvent and is used to iterate over the raw logs and unpacked data for SimpleEvent events raised by the Eventer contract.
type EventerSimpleEventIterator struct {
	Event *EventerSimpleEvent // Event containing the contract specifics and raw log

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
func (it *EventerSimpleEventIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(EventerSimpleEvent)
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
		it.Event = new(EventerSimpleEvent)
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
func (it *EventerSimpleEventIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *EventerSimpleEventIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// EventerSimpleEvent represents a SimpleEvent event raised by the Eventer contract.
type EventerSimpleEvent struct {
	Addr  common.Address
	Id    [32]byte
	Flag  bool
	Value *big.Int
	Raw   types.Log // Blockchain specific contextual infos
}

// FilterSimpleEvent is a free log retrieval operation binding the contract event 0x1f097de4289df643bd9c11011cc61367aa12983405c021056e706eb5ba1250c8.
//
// Solidity: event SimpleEvent(address indexed Addr, bytes32 indexed Id, bool indexed Flag, uint256 Value)
func (_Eventer *EventerFilterer) FilterSimpleEvent(opts *bind.FilterOpts, Addr []common.Address, Id [][32]byte, Flag []bool) (*EventerSimpleEventIterator, error) {

	var AddrRule []interface{}
	for _, AddrItem := range Addr {
		AddrRule = append(AddrRule, AddrItem)
	}
	var IdRule []interface{}
	for _, IdItem := range Id {
		IdRule = append(IdRule, IdItem)
	}
	var FlagRule []interface{}
	for _, FlagItem := range Flag {
		FlagRule = append(FlagRule, FlagItem)
	}

	logs, sub, err := _Eventer.contract.FilterLogs(opts, "SimpleEvent", AddrRule, IdRule, FlagRule)
	if err != nil {
		return nil, err
	}
	return &EventerSimpleEventIterator{contract: _Eventer.contract, event: "SimpleEvent", logs: logs, sub: sub}, nil
}

// WatchSimpleEvent is a free log subscription operation binding the contract event 0x1f097de4289df643bd9c11011cc61367aa12983405c021056e706eb5ba1250c8.
//
// Solidity: event SimpleEvent(address indexed Addr, bytes32 indexed Id, bool indexed Flag, uint256 Value)
func (_Eventer *EventerFilterer) WatchSimpleEvent(opts *bind.WatchOpts, sink chan<- *EventerSimpleEvent, Addr []common.Address, Id [][32]byte, Flag []bool) (event.Subscription, error) {

	var AddrRule []interface{}
	for _, AddrItem := range Addr {
		AddrRule = append(AddrRule, AddrItem)
	}
	var IdRule []interface{}
	for _, IdItem := range Id {
		IdRule = append(IdRule, IdItem)
	}
	var FlagRule []interface{}
	for _, FlagItem := range Flag {
		FlagRule = append(FlagRule, FlagItem)
	}

	logs, sub, err := _Eventer.contract.WatchLogs(opts, "SimpleEvent", AddrRule, IdRule, FlagRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(EventerSimpleEvent)
				if err := _Eventer.contract.UnpackLog(event, "SimpleEvent", log); err != nil {
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

// ParseSimpleEvent is a log parse operation binding the contract event 0x1f097de4289df643bd9c11011cc61367aa12983405c021056e706eb5ba1250c8.
//
// Solidity: event SimpleEvent(address indexed Addr, bytes32 indexed Id, bool indexed Flag, uint256 Value)
func (_Eventer *EventerFilterer) ParseSimpleEvent(log types.Log) (*EventerSimpleEvent, error) {
	event := new(EventerSimpleEvent)
	if err := _Eventer.contract.UnpackLog(event, "SimpleEvent", log); err != nil {
		return nil, err
	}
	return event, nil
}
