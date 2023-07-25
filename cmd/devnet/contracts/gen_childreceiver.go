// Code generated - DO NOT EDIT.
// This file is a generated binding and any manual changes will be lost.

package contracts

import (
	"fmt"
	"math/big"
	"reflect"
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

// ChildReceiverABI is the input ABI used to generate the binding from.
const ChildReceiverABI = "[{\"inputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"constructor\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"internalType\":\"address\",\"name\":\"_source\",\"type\":\"address\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"_amount\",\"type\":\"uint256\"}],\"name\":\"received\",\"type\":\"event\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"},{\"internalType\":\"bytes\",\"name\":\"data\",\"type\":\"bytes\"}],\"name\":\"onStateReceive\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"name\":\"senders\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"}]"

// ChildReceiverBin is the compiled bytecode used for deploying new contracts.
var ChildReceiverBin = "0x608060405234801561001057600080fd5b5061029c806100206000396000f3fe608060405234801561001057600080fd5b50600436106100365760003560e01c806326c53bea1461003b578063982fb9d814610050575b600080fd5b61004e61004936600461015b565b610082565b005b61007061005e3660046101ef565b60006020819052908152604090205481565b60405190815260200160405180910390f35b33611001146100c85760405162461bcd60e51b815260206004820152600e60248201526d24b73b30b634b21039b2b73232b960911b604482015260640160405180910390fd5b6000806100d783850185610213565b6001600160a01b03821660009081526020819052604090205491935091506100ff828261023f565b6001600160a01b038416600081815260208181526040918290209390935580519182529181018490527ff11e547d796cc64acdf758e7cee90439494fd886a19159454aa61e473fdbafef910160405180910390a1505050505050565b60008060006040848603121561017057600080fd5b83359250602084013567ffffffffffffffff8082111561018f57600080fd5b818601915086601f8301126101a357600080fd5b8135818111156101b257600080fd5b8760208285010111156101c457600080fd5b6020830194508093505050509250925092565b6001600160a01b03811681146101ec57600080fd5b50565b60006020828403121561020157600080fd5b813561020c816101d7565b9392505050565b6000806040838503121561022657600080fd5b8235610231816101d7565b946020939093013593505050565b8082018082111561026057634e487b7160e01b600052601160045260246000fd5b9291505056fea2646970667358221220bb3a513950ddc3581a83b932be35476871cfca25f2faf93bb137e0f50d8c5ad864736f6c63430008140033"

// DeployChildReceiver deploys a new Ethereum contract, binding an instance of ChildReceiver to it.
func DeployChildReceiver(auth *bind.TransactOpts, backend bind.ContractBackend) (libcommon.Address, types.Transaction, *ChildReceiver, error) {
	parsed, err := abi.JSON(strings.NewReader(ChildReceiverABI))
	if err != nil {
		return libcommon.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, libcommon.FromHex(ChildReceiverBin), backend)
	if err != nil {
		return libcommon.Address{}, nil, nil, err
	}
	return address, tx, &ChildReceiver{ChildReceiverCaller: ChildReceiverCaller{contract: contract}, ChildReceiverTransactor: ChildReceiverTransactor{contract: contract}, ChildReceiverFilterer: ChildReceiverFilterer{contract: contract}}, nil
}

// ChildReceiver is an auto generated Go binding around an Ethereum contract.
type ChildReceiver struct {
	ChildReceiverCaller     // Read-only binding to the contract
	ChildReceiverTransactor // Write-only binding to the contract
	ChildReceiverFilterer   // Log filterer for contract events
}

// ChildReceiverCaller is an auto generated read-only Go binding around an Ethereum contract.
type ChildReceiverCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ChildReceiverTransactor is an auto generated write-only Go binding around an Ethereum contract.
type ChildReceiverTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ChildReceiverFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type ChildReceiverFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ChildReceiverSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type ChildReceiverSession struct {
	Contract     *ChildReceiver    // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// ChildReceiverCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type ChildReceiverCallerSession struct {
	Contract *ChildReceiverCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts        // Call options to use throughout this session
}

// ChildReceiverTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type ChildReceiverTransactorSession struct {
	Contract     *ChildReceiverTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts        // Transaction auth options to use throughout this session
}

// ChildReceiverRaw is an auto generated low-level Go binding around an Ethereum contract.
type ChildReceiverRaw struct {
	Contract *ChildReceiver // Generic contract binding to access the raw methods on
}

// ChildReceiverCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type ChildReceiverCallerRaw struct {
	Contract *ChildReceiverCaller // Generic read-only contract binding to access the raw methods on
}

// ChildReceiverTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type ChildReceiverTransactorRaw struct {
	Contract *ChildReceiverTransactor // Generic write-only contract binding to access the raw methods on
}

// NewChildReceiver creates a new instance of ChildReceiver, bound to a specific deployed contract.
func NewChildReceiver(address libcommon.Address, backend bind.ContractBackend) (*ChildReceiver, error) {
	contract, err := bindChildReceiver(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &ChildReceiver{ChildReceiverCaller: ChildReceiverCaller{contract: contract}, ChildReceiverTransactor: ChildReceiverTransactor{contract: contract}, ChildReceiverFilterer: ChildReceiverFilterer{contract: contract}}, nil
}

// NewChildReceiverCaller creates a new read-only instance of ChildReceiver, bound to a specific deployed contract.
func NewChildReceiverCaller(address libcommon.Address, caller bind.ContractCaller) (*ChildReceiverCaller, error) {
	contract, err := bindChildReceiver(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &ChildReceiverCaller{contract: contract}, nil
}

// NewChildReceiverTransactor creates a new write-only instance of ChildReceiver, bound to a specific deployed contract.
func NewChildReceiverTransactor(address libcommon.Address, transactor bind.ContractTransactor) (*ChildReceiverTransactor, error) {
	contract, err := bindChildReceiver(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &ChildReceiverTransactor{contract: contract}, nil
}

// NewChildReceiverFilterer creates a new log filterer instance of ChildReceiver, bound to a specific deployed contract.
func NewChildReceiverFilterer(address libcommon.Address, filterer bind.ContractFilterer) (*ChildReceiverFilterer, error) {
	contract, err := bindChildReceiver(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &ChildReceiverFilterer{contract: contract}, nil
}

// bindChildReceiver binds a generic wrapper to an already deployed contract.
func bindChildReceiver(address libcommon.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(ChildReceiverABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_ChildReceiver *ChildReceiverRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _ChildReceiver.Contract.ChildReceiverCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_ChildReceiver *ChildReceiverRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _ChildReceiver.Contract.ChildReceiverTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_ChildReceiver *ChildReceiverRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _ChildReceiver.Contract.ChildReceiverTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_ChildReceiver *ChildReceiverCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _ChildReceiver.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_ChildReceiver *ChildReceiverTransactorRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _ChildReceiver.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_ChildReceiver *ChildReceiverTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _ChildReceiver.Contract.contract.Transact(opts, method, params...)
}

// Senders is a free data retrieval call binding the contract method 0x982fb9d8.
//
// Solidity: function senders(address ) view returns(uint256)
func (_ChildReceiver *ChildReceiverCaller) Senders(opts *bind.CallOpts, arg0 libcommon.Address) (*big.Int, error) {
	var out []interface{}
	err := _ChildReceiver.contract.Call(opts, &out, "senders", arg0)

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// Senders is a free data retrieval call binding the contract method 0x982fb9d8.
//
// Solidity: function senders(address ) view returns(uint256)
func (_ChildReceiver *ChildReceiverSession) Senders(arg0 libcommon.Address) (*big.Int, error) {
	return _ChildReceiver.Contract.Senders(&_ChildReceiver.CallOpts, arg0)
}

// Senders is a free data retrieval call binding the contract method 0x982fb9d8.
//
// Solidity: function senders(address ) view returns(uint256)
func (_ChildReceiver *ChildReceiverCallerSession) Senders(arg0 libcommon.Address) (*big.Int, error) {
	return _ChildReceiver.Contract.Senders(&_ChildReceiver.CallOpts, arg0)
}

// OnStateReceive is a paid mutator transaction binding the contract method 0x26c53bea.
//
// Solidity: function onStateReceive(uint256 , bytes data) returns()
func (_ChildReceiver *ChildReceiverTransactor) OnStateReceive(opts *bind.TransactOpts, arg0 *big.Int, data []byte) (types.Transaction, error) {
	return _ChildReceiver.contract.Transact(opts, "onStateReceive", arg0, data)
}

// OnStateReceive is a paid mutator transaction binding the contract method 0x26c53bea.
//
// Solidity: function onStateReceive(uint256 , bytes data) returns()
func (_ChildReceiver *ChildReceiverSession) OnStateReceive(arg0 *big.Int, data []byte) (types.Transaction, error) {
	return _ChildReceiver.Contract.OnStateReceive(&_ChildReceiver.TransactOpts, arg0, data)
}

// OnStateReceive is a paid mutator transaction binding the contract method 0x26c53bea.
//
// Solidity: function onStateReceive(uint256 , bytes data) returns()
func (_ChildReceiver *ChildReceiverTransactorSession) OnStateReceive(arg0 *big.Int, data []byte) (types.Transaction, error) {
	return _ChildReceiver.Contract.OnStateReceive(&_ChildReceiver.TransactOpts, arg0, data)
}

// OnStateReceiveParams is an auto generated read-only Go binding of transcaction calldata params
type OnStateReceiveParams struct {
	Param_arg0 *big.Int
	Param_data []byte
}

// Parse OnStateReceive method from calldata of a transaction
//
// Solidity: function onStateReceive(uint256 , bytes data) returns()
func ParseOnStateReceive(calldata []byte) (*OnStateReceiveParams, error) {
	if len(calldata) <= 4 {
		return nil, fmt.Errorf("invalid calldata input")
	}

	_abi, err := abi.JSON(strings.NewReader(ChildReceiverABI))
	if err != nil {
		return nil, fmt.Errorf("failed to get abi of registry metadata: %w", err)
	}

	out, err := _abi.Methods["onStateReceive"].Inputs.Unpack(calldata[4:])
	if err != nil {
		return nil, fmt.Errorf("failed to unpack onStateReceive params data: %w", err)
	}

	var paramsResult = new(OnStateReceiveParams)
	value := reflect.ValueOf(paramsResult).Elem()

	if value.NumField() != len(out) {
		return nil, fmt.Errorf("failed to match calldata with param field number")
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)
	out1 := *abi.ConvertType(out[1], new([]byte)).(*[]byte)

	return &OnStateReceiveParams{
		Param_arg0: out0, Param_data: out1,
	}, nil
}

// ChildReceiverReceivedIterator is returned from FilterReceived and is used to iterate over the raw logs and unpacked data for Received events raised by the ChildReceiver contract.
type ChildReceiverReceivedIterator struct {
	Event *ChildReceiverReceived // Event containing the contract specifics and raw log

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
func (it *ChildReceiverReceivedIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(ChildReceiverReceived)
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
		it.Event = new(ChildReceiverReceived)
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
func (it *ChildReceiverReceivedIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *ChildReceiverReceivedIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// ChildReceiverReceived represents a Received event raised by the ChildReceiver contract.
type ChildReceiverReceived struct {
	Source libcommon.Address
	Amount *big.Int
	Raw    types.Log // Blockchain specific contextual infos
}

// FilterReceived is a free log retrieval operation binding the contract event 0xf11e547d796cc64acdf758e7cee90439494fd886a19159454aa61e473fdbafef.
//
// Solidity: event received(address _source, uint256 _amount)
func (_ChildReceiver *ChildReceiverFilterer) FilterReceived(opts *bind.FilterOpts) (*ChildReceiverReceivedIterator, error) {

	logs, sub, err := _ChildReceiver.contract.FilterLogs(opts, "received")
	if err != nil {
		return nil, err
	}
	return &ChildReceiverReceivedIterator{contract: _ChildReceiver.contract, event: "received", logs: logs, sub: sub}, nil
}

// WatchReceived is a free log subscription operation binding the contract event 0xf11e547d796cc64acdf758e7cee90439494fd886a19159454aa61e473fdbafef.
//
// Solidity: event received(address _source, uint256 _amount)
func (_ChildReceiver *ChildReceiverFilterer) WatchReceived(opts *bind.WatchOpts, sink chan<- *ChildReceiverReceived) (event.Subscription, error) {

	logs, sub, err := _ChildReceiver.contract.WatchLogs(opts, "received")
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(ChildReceiverReceived)
				if err := _ChildReceiver.contract.UnpackLog(event, "received", log); err != nil {
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

// ParseReceived is a log parse operation binding the contract event 0xf11e547d796cc64acdf758e7cee90439494fd886a19159454aa61e473fdbafef.
//
// Solidity: event received(address _source, uint256 _amount)
func (_ChildReceiver *ChildReceiverFilterer) ParseReceived(log types.Log) (*ChildReceiverReceived, error) {
	event := new(ChildReceiverReceived)
	if err := _ChildReceiver.contract.UnpackLog(event, "received", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}
