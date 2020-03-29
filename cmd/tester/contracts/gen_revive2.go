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
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/event"
)

// Reference imports to suppress errors if they are not otherwise used.
var (
	_ = big.NewInt
	_ = strings.NewReader
	_ = ethereum.NotFound
	_ = abi.U256
	_ = bind.Bind
	_ = common.Big1
	_ = types.BloomLookup
	_ = event.NewSubscription
)

// Revive2ABI is the input ABI used to generate the binding from.
const Revive2ABI = "[{\"inputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"constructor\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"internalType\":\"contractPhoenix\",\"name\":\"d\",\"type\":\"address\"}],\"name\":\"DeployEvent\",\"type\":\"event\"},{\"inputs\":[{\"internalType\":\"bytes32\",\"name\":\"salt\",\"type\":\"bytes32\"}],\"name\":\"deploy\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"}]"

// Revive2Bin is the compiled bytecode used for deploying new contracts.
var Revive2Bin = "0x608060405234801561001057600080fd5b50610288806100206000396000f3fe608060405234801561001057600080fd5b506004361061002b5760003560e01c80632b85ba3814610030575b600080fd5b61004d6004803603602081101561004657600080fd5b503561004f565b005b60008160405161005e906100c3565b8190604051809103906000f590508015801561007e573d6000803e3d6000fd5b50604080516001600160a01b038316815290519192507f68f6a0f063c25c6678c443b9a484086f15ba8f91f60218695d32a5251f2050eb919081900360200190a15050565b610182806100d18339019056fe608060405234801561001057600080fd5b50610162806100206000396000f3fe6080604052600436106100385760003560e01c806335f4699414610044578063975057e71461005b578063d09de08a146100705761003f565b3661003f57005b600080fd5b34801561005057600080fd5b50610059610085565b005b34801561006757600080fd5b50610059610089565b34801561007c57600080fd5b506100596100a5565b6000ff5b6000805481526002602052604081206001908190558154019055565b60005460015411156100e85760405162461bcd60e51b81526004018080602001828103825260248152602001806101096024913960400191505060405180910390fd5b60018054600090815260026020526040902080548201905580548101905556fe74727920746f20696e6372656d656e74206e6f7420637265617465642073746f72616765a2646970667358221220774e0afd99cd3bb44defa924b3b87c03e421791c7c5fd3cdf3b97b18443aa96064736f6c63430006040033a26469706673582212201603c25bc443bcb37306df8612afbe42c2b1d7714ccbc2488268ec63ece3f4eb64736f6c63430006040033"

// DeployRevive2 deploys a new Ethereum contract, binding an instance of Revive2 to it.
func DeployRevive2(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *Revive2, error) {
	parsed, err := abi.JSON(strings.NewReader(Revive2ABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(Revive2Bin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &Revive2{Revive2Caller: Revive2Caller{contract: contract}, Revive2Transactor: Revive2Transactor{contract: contract}, Revive2Filterer: Revive2Filterer{contract: contract}}, nil
}

// Revive2 is an auto generated Go binding around an Ethereum contract.
type Revive2 struct {
	Revive2Caller     // Read-only binding to the contract
	Revive2Transactor // Write-only binding to the contract
	Revive2Filterer   // Log filterer for contract events
}

// Revive2Caller is an auto generated read-only Go binding around an Ethereum contract.
type Revive2Caller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// Revive2Transactor is an auto generated write-only Go binding around an Ethereum contract.
type Revive2Transactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// Revive2Filterer is an auto generated log filtering Go binding around an Ethereum contract events.
type Revive2Filterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// Revive2Session is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type Revive2Session struct {
	Contract     *Revive2          // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// Revive2CallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type Revive2CallerSession struct {
	Contract *Revive2Caller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts  // Call options to use throughout this session
}

// Revive2TransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type Revive2TransactorSession struct {
	Contract     *Revive2Transactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts  // Transaction auth options to use throughout this session
}

// Revive2Raw is an auto generated low-level Go binding around an Ethereum contract.
type Revive2Raw struct {
	Contract *Revive2 // Generic contract binding to access the raw methods on
}

// Revive2CallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type Revive2CallerRaw struct {
	Contract *Revive2Caller // Generic read-only contract binding to access the raw methods on
}

// Revive2TransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type Revive2TransactorRaw struct {
	Contract *Revive2Transactor // Generic write-only contract binding to access the raw methods on
}

// NewRevive2 creates a new instance of Revive2, bound to a specific deployed contract.
func NewRevive2(address common.Address, backend bind.ContractBackend) (*Revive2, error) {
	contract, err := bindRevive2(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Revive2{Revive2Caller: Revive2Caller{contract: contract}, Revive2Transactor: Revive2Transactor{contract: contract}, Revive2Filterer: Revive2Filterer{contract: contract}}, nil
}

// NewRevive2Caller creates a new read-only instance of Revive2, bound to a specific deployed contract.
func NewRevive2Caller(address common.Address, caller bind.ContractCaller) (*Revive2Caller, error) {
	contract, err := bindRevive2(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &Revive2Caller{contract: contract}, nil
}

// NewRevive2Transactor creates a new write-only instance of Revive2, bound to a specific deployed contract.
func NewRevive2Transactor(address common.Address, transactor bind.ContractTransactor) (*Revive2Transactor, error) {
	contract, err := bindRevive2(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &Revive2Transactor{contract: contract}, nil
}

// NewRevive2Filterer creates a new log filterer instance of Revive2, bound to a specific deployed contract.
func NewRevive2Filterer(address common.Address, filterer bind.ContractFilterer) (*Revive2Filterer, error) {
	contract, err := bindRevive2(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &Revive2Filterer{contract: contract}, nil
}

// bindRevive2 binds a generic wrapper to an already deployed contract.
func bindRevive2(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(Revive2ABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Revive2 *Revive2Raw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Revive2.Contract.Revive2Caller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Revive2 *Revive2Raw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Revive2.Contract.Revive2Transactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Revive2 *Revive2Raw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Revive2.Contract.Revive2Transactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Revive2 *Revive2CallerRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Revive2.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Revive2 *Revive2TransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Revive2.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Revive2 *Revive2TransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Revive2.Contract.contract.Transact(opts, method, params...)
}

// Deploy is a paid mutator transaction binding the contract method 0x2b85ba38.
//
// Solidity: function deploy(bytes32 salt) returns()
func (_Revive2 *Revive2Transactor) Deploy(opts *bind.TransactOpts, salt [32]byte) (*types.Transaction, error) {
	return _Revive2.contract.Transact(opts, "deploy", salt)
}

// Deploy is a paid mutator transaction binding the contract method 0x2b85ba38.
//
// Solidity: function deploy(bytes32 salt) returns()
func (_Revive2 *Revive2Session) Deploy(salt [32]byte) (*types.Transaction, error) {
	return _Revive2.Contract.Deploy(&_Revive2.TransactOpts, salt)
}

// Deploy is a paid mutator transaction binding the contract method 0x2b85ba38.
//
// Solidity: function deploy(bytes32 salt) returns()
func (_Revive2 *Revive2TransactorSession) Deploy(salt [32]byte) (*types.Transaction, error) {
	return _Revive2.Contract.Deploy(&_Revive2.TransactOpts, salt)
}

// Revive2DeployEventIterator is returned from FilterDeployEvent and is used to iterate over the raw logs and unpacked data for DeployEvent events raised by the Revive2 contract.
type Revive2DeployEventIterator struct {
	Event *Revive2DeployEvent // Event containing the contract specifics and raw log

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
func (it *Revive2DeployEventIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(Revive2DeployEvent)
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
		it.Event = new(Revive2DeployEvent)
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
func (it *Revive2DeployEventIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *Revive2DeployEventIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// Revive2DeployEvent represents a DeployEvent event raised by the Revive2 contract.
type Revive2DeployEvent struct {
	D   common.Address
	Raw types.Log // Blockchain specific contextual infos
}

// FilterDeployEvent is a free log retrieval operation binding the contract event 0x68f6a0f063c25c6678c443b9a484086f15ba8f91f60218695d32a5251f2050eb.
//
// Solidity: event DeployEvent(address d)
func (_Revive2 *Revive2Filterer) FilterDeployEvent(opts *bind.FilterOpts) (*Revive2DeployEventIterator, error) {

	logs, sub, err := _Revive2.contract.FilterLogs(opts, "DeployEvent")
	if err != nil {
		return nil, err
	}
	return &Revive2DeployEventIterator{contract: _Revive2.contract, event: "DeployEvent", logs: logs, sub: sub}, nil
}

// WatchDeployEvent is a free log subscription operation binding the contract event 0x68f6a0f063c25c6678c443b9a484086f15ba8f91f60218695d32a5251f2050eb.
//
// Solidity: event DeployEvent(address d)
func (_Revive2 *Revive2Filterer) WatchDeployEvent(opts *bind.WatchOpts, sink chan<- *Revive2DeployEvent) (event.Subscription, error) {

	logs, sub, err := _Revive2.contract.WatchLogs(opts, "DeployEvent")
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(Revive2DeployEvent)
				if err := _Revive2.contract.UnpackLog(event, "DeployEvent", log); err != nil {
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

// ParseDeployEvent is a log parse operation binding the contract event 0x68f6a0f063c25c6678c443b9a484086f15ba8f91f60218695d32a5251f2050eb.
//
// Solidity: event DeployEvent(address d)
func (_Revive2 *Revive2Filterer) ParseDeployEvent(log types.Log) (*Revive2DeployEvent, error) {
	event := new(Revive2DeployEvent)
	if err := _Revive2.contract.UnpackLog(event, "DeployEvent", log); err != nil {
		return nil, err
	}
	return event, nil
}
