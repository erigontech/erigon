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

// ReviveABI is the input ABI used to generate the binding from.
const ReviveABI = "[{\"inputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"constructor\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"internalType\":\"address\",\"name\":\"d\",\"type\":\"address\"}],\"name\":\"DeployEvent\",\"type\":\"event\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"salt\",\"type\":\"uint256\"}],\"name\":\"deploy\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"}]"

// ReviveBin is the compiled bytecode used for deploying new contracts.
var ReviveBin = "0x6080604052348015600f57600080fd5b5060f88061001e6000396000f3fe6080604052348015600f57600080fd5b506004361060285760003560e01c8063a5e3875114602d575b600080fd5b604760048036036020811015604157600080fd5b50356049565b005b6040805180820190915260138082527260424355603060005360ff60015360026000f360681b60208301908152600091849183f5604080516001600160a01b038316815290519192507f68f6a0f063c25c6678c443b9a484086f15ba8f91f60218695d32a5251f2050eb919081900360200190a150505056fea2646970667358221220855dd55ebe1fcb1f01e25d5255c5c7a31d14ea2be941590bf8e916d24abad06164736f6c63430006060033"

// DeployRevive deploys a new Ethereum contract, binding an instance of Revive to it.
func DeployRevive(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *Revive, error) {
	parsed, err := abi.JSON(strings.NewReader(ReviveABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(ReviveBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &Revive{ReviveCaller: ReviveCaller{contract: contract}, ReviveTransactor: ReviveTransactor{contract: contract}, ReviveFilterer: ReviveFilterer{contract: contract}}, nil
}

// Revive is an auto generated Go binding around an Ethereum contract.
type Revive struct {
	ReviveCaller     // Read-only binding to the contract
	ReviveTransactor // Write-only binding to the contract
	ReviveFilterer   // Log filterer for contract events
}

// ReviveCaller is an auto generated read-only Go binding around an Ethereum contract.
type ReviveCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ReviveTransactor is an auto generated write-only Go binding around an Ethereum contract.
type ReviveTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ReviveFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type ReviveFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ReviveSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type ReviveSession struct {
	Contract     *Revive           // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// ReviveCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type ReviveCallerSession struct {
	Contract *ReviveCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts // Call options to use throughout this session
}

// ReviveTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type ReviveTransactorSession struct {
	Contract     *ReviveTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// ReviveRaw is an auto generated low-level Go binding around an Ethereum contract.
type ReviveRaw struct {
	Contract *Revive // Generic contract binding to access the raw methods on
}

// ReviveCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type ReviveCallerRaw struct {
	Contract *ReviveCaller // Generic read-only contract binding to access the raw methods on
}

// ReviveTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type ReviveTransactorRaw struct {
	Contract *ReviveTransactor // Generic write-only contract binding to access the raw methods on
}

// NewRevive creates a new instance of Revive, bound to a specific deployed contract.
func NewRevive(address common.Address, backend bind.ContractBackend) (*Revive, error) {
	contract, err := bindRevive(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Revive{ReviveCaller: ReviveCaller{contract: contract}, ReviveTransactor: ReviveTransactor{contract: contract}, ReviveFilterer: ReviveFilterer{contract: contract}}, nil
}

// NewReviveCaller creates a new read-only instance of Revive, bound to a specific deployed contract.
func NewReviveCaller(address common.Address, caller bind.ContractCaller) (*ReviveCaller, error) {
	contract, err := bindRevive(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &ReviveCaller{contract: contract}, nil
}

// NewReviveTransactor creates a new write-only instance of Revive, bound to a specific deployed contract.
func NewReviveTransactor(address common.Address, transactor bind.ContractTransactor) (*ReviveTransactor, error) {
	contract, err := bindRevive(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &ReviveTransactor{contract: contract}, nil
}

// NewReviveFilterer creates a new log filterer instance of Revive, bound to a specific deployed contract.
func NewReviveFilterer(address common.Address, filterer bind.ContractFilterer) (*ReviveFilterer, error) {
	contract, err := bindRevive(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &ReviveFilterer{contract: contract}, nil
}

// bindRevive binds a generic wrapper to an already deployed contract.
func bindRevive(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(ReviveABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Revive *ReviveRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Revive.Contract.ReviveCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Revive *ReviveRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Revive.Contract.ReviveTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Revive *ReviveRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Revive.Contract.ReviveTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Revive *ReviveCallerRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Revive.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Revive *ReviveTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Revive.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Revive *ReviveTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Revive.Contract.contract.Transact(opts, method, params...)
}

// Deploy is a paid mutator transaction binding the contract method 0xa5e38751.
//
// Solidity: function deploy(uint256 salt) returns()
func (_Revive *ReviveTransactor) Deploy(opts *bind.TransactOpts, salt *big.Int) (*types.Transaction, error) {
	return _Revive.contract.Transact(opts, "deploy", salt)
}

// Deploy is a paid mutator transaction binding the contract method 0xa5e38751.
//
// Solidity: function deploy(uint256 salt) returns()
func (_Revive *ReviveSession) Deploy(salt *big.Int) (*types.Transaction, error) {
	return _Revive.Contract.Deploy(&_Revive.TransactOpts, salt)
}

// Deploy is a paid mutator transaction binding the contract method 0xa5e38751.
//
// Solidity: function deploy(uint256 salt) returns()
func (_Revive *ReviveTransactorSession) Deploy(salt *big.Int) (*types.Transaction, error) {
	return _Revive.Contract.Deploy(&_Revive.TransactOpts, salt)
}

// ReviveDeployEventIterator is returned from FilterDeployEvent and is used to iterate over the raw logs and unpacked data for DeployEvent events raised by the Revive contract.
type ReviveDeployEventIterator struct {
	Event *ReviveDeployEvent // Event containing the contract specifics and raw log

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
func (it *ReviveDeployEventIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(ReviveDeployEvent)
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
		it.Event = new(ReviveDeployEvent)
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
func (it *ReviveDeployEventIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *ReviveDeployEventIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// ReviveDeployEvent represents a DeployEvent event raised by the Revive contract.
type ReviveDeployEvent struct {
	D   common.Address
	Raw types.Log // Blockchain specific contextual infos
}

// FilterDeployEvent is a free log retrieval operation binding the contract event 0x68f6a0f063c25c6678c443b9a484086f15ba8f91f60218695d32a5251f2050eb.
//
// Solidity: event DeployEvent(address d)
func (_Revive *ReviveFilterer) FilterDeployEvent(opts *bind.FilterOpts) (*ReviveDeployEventIterator, error) {

	logs, sub, err := _Revive.contract.FilterLogs(opts, "DeployEvent")
	if err != nil {
		return nil, err
	}
	return &ReviveDeployEventIterator{contract: _Revive.contract, event: "DeployEvent", logs: logs, sub: sub}, nil
}

// WatchDeployEvent is a free log subscription operation binding the contract event 0x68f6a0f063c25c6678c443b9a484086f15ba8f91f60218695d32a5251f2050eb.
//
// Solidity: event DeployEvent(address d)
func (_Revive *ReviveFilterer) WatchDeployEvent(opts *bind.WatchOpts, sink chan<- *ReviveDeployEvent) (event.Subscription, error) {

	logs, sub, err := _Revive.contract.WatchLogs(opts, "DeployEvent")
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(ReviveDeployEvent)
				if err := _Revive.contract.UnpackLog(event, "DeployEvent", log); err != nil {
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
func (_Revive *ReviveFilterer) ParseDeployEvent(log types.Log) (*ReviveDeployEvent, error) {
	event := new(ReviveDeployEvent)
	if err := _Revive.contract.UnpackLog(event, "DeployEvent", log); err != nil {
		return nil, err
	}
	return event, nil
}
