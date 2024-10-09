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

// FaucetABI is the input ABI used to generate the binding from.
const FaucetABI = "[{\"inputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"constructor\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"internalType\":\"address\",\"name\":\"_source\",\"type\":\"address\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"_amount\",\"type\":\"uint256\"}],\"name\":\"received\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"internalType\":\"address\",\"name\":\"_destination\",\"type\":\"address\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"_amount\",\"type\":\"uint256\"}],\"name\":\"sent\",\"type\":\"event\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"name\":\"destinations\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"addresspayable\",\"name\":\"_destination\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"_requested\",\"type\":\"uint256\"}],\"name\":\"send\",\"outputs\":[],\"stateMutability\":\"payable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"name\":\"sources\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"stateMutability\":\"payable\",\"type\":\"receive\"}]"

// FaucetBin is the compiled bytecode used for deploying new contracts.
var FaucetBin = "0x608060405234801561001057600080fd5b506102ea806100206000396000f3fe6080604052600436106100385760003560e01c806359c02c37146100a0578063b750bdde146100df578063d0679d341461010c57600080fd5b3661009b57336000908152602081905260408120805434929061005c908490610225565b9091555050604080513381523460208201527ff11e547d796cc64acdf758e7cee90439494fd886a19159454aa61e473fdbafef910160405180910390a1005b600080fd5b3480156100ac57600080fd5b506100cd6100bb366004610264565b60016020526000908152604090205481565b60405190815260200160405180910390f35b3480156100eb57600080fd5b506100cd6100fa366004610264565b60006020819052908152604090205481565b61011f61011a366004610288565b610121565b005b4760000361012d575050565b600081471115610176575060405181906001600160a01b0384169082156108fc029083906000818181858888f19350505050158015610170573d6000803e3d6000fd5b506101b1565b5060405147906001600160a01b0384169082156108fc029083906000818181858888f193505050501580156101af573d6000803e3d6000fd5b505b6001600160a01b038316600090815260016020526040812080548392906101d9908490610225565b9091555050604080516001600160a01b0385168152602081018390527f3bcb2e664d8f57273201bc888e82d6549f8308a52a9fcd7702b2ea8387f769a9910160405180910390a1505050565b8082018082111561024657634e487b7160e01b600052601160045260246000fd5b92915050565b6001600160a01b038116811461026157600080fd5b50565b60006020828403121561027657600080fd5b81356102818161024c565b9392505050565b6000806040838503121561029b57600080fd5b82356102a68161024c565b94602093909301359350505056fea2646970667358221220ac81b3f12efbe2860b7a5f00b56a253c6661d9ad6df22e642da3546e0015e9d664736f6c63430008140033"

// DeployFaucet deploys a new Ethereum contract, binding an instance of Faucet to it.
func DeployFaucet(auth *bind.TransactOpts, backend bind.ContractBackend) (libcommon.Address, types.Transaction, *Faucet, error) {
	parsed, err := abi.JSON(strings.NewReader(FaucetABI))
	if err != nil {
		return libcommon.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, libcommon.FromHex(FaucetBin), backend)
	if err != nil {
		return libcommon.Address{}, nil, nil, err
	}
	return address, tx, &Faucet{FaucetCaller: FaucetCaller{contract: contract}, FaucetTransactor: FaucetTransactor{contract: contract}, FaucetFilterer: FaucetFilterer{contract: contract}}, nil
}

// Faucet is an auto generated Go binding around an Ethereum contract.
type Faucet struct {
	FaucetCaller     // Read-only binding to the contract
	FaucetTransactor // Write-only binding to the contract
	FaucetFilterer   // Log filterer for contract events
}

// FaucetCaller is an auto generated read-only Go binding around an Ethereum contract.
type FaucetCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// FaucetTransactor is an auto generated write-only Go binding around an Ethereum contract.
type FaucetTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// FaucetFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type FaucetFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// FaucetSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type FaucetSession struct {
	Contract     *Faucet           // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// FaucetCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type FaucetCallerSession struct {
	Contract *FaucetCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts // Call options to use throughout this session
}

// FaucetTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type FaucetTransactorSession struct {
	Contract     *FaucetTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// FaucetRaw is an auto generated low-level Go binding around an Ethereum contract.
type FaucetRaw struct {
	Contract *Faucet // Generic contract binding to access the raw methods on
}

// FaucetCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type FaucetCallerRaw struct {
	Contract *FaucetCaller // Generic read-only contract binding to access the raw methods on
}

// FaucetTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type FaucetTransactorRaw struct {
	Contract *FaucetTransactor // Generic write-only contract binding to access the raw methods on
}

// NewFaucet creates a new instance of Faucet, bound to a specific deployed contract.
func NewFaucet(address libcommon.Address, backend bind.ContractBackend) (*Faucet, error) {
	contract, err := bindFaucet(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Faucet{FaucetCaller: FaucetCaller{contract: contract}, FaucetTransactor: FaucetTransactor{contract: contract}, FaucetFilterer: FaucetFilterer{contract: contract}}, nil
}

// NewFaucetCaller creates a new read-only instance of Faucet, bound to a specific deployed contract.
func NewFaucetCaller(address libcommon.Address, caller bind.ContractCaller) (*FaucetCaller, error) {
	contract, err := bindFaucet(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &FaucetCaller{contract: contract}, nil
}

// NewFaucetTransactor creates a new write-only instance of Faucet, bound to a specific deployed contract.
func NewFaucetTransactor(address libcommon.Address, transactor bind.ContractTransactor) (*FaucetTransactor, error) {
	contract, err := bindFaucet(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &FaucetTransactor{contract: contract}, nil
}

// NewFaucetFilterer creates a new log filterer instance of Faucet, bound to a specific deployed contract.
func NewFaucetFilterer(address libcommon.Address, filterer bind.ContractFilterer) (*FaucetFilterer, error) {
	contract, err := bindFaucet(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &FaucetFilterer{contract: contract}, nil
}

// bindFaucet binds a generic wrapper to an already deployed contract.
func bindFaucet(address libcommon.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(FaucetABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Faucet *FaucetRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Faucet.Contract.FaucetCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Faucet *FaucetRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _Faucet.Contract.FaucetTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Faucet *FaucetRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _Faucet.Contract.FaucetTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Faucet *FaucetCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Faucet.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Faucet *FaucetTransactorRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _Faucet.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Faucet *FaucetTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _Faucet.Contract.contract.Transact(opts, method, params...)
}

// Destinations is a free data retrieval call binding the contract method 0x59c02c37.
//
// Solidity: function destinations(address ) view returns(uint256)
func (_Faucet *FaucetCaller) Destinations(opts *bind.CallOpts, arg0 libcommon.Address) (*big.Int, error) {
	var out []interface{}
	err := _Faucet.contract.Call(opts, &out, "destinations", arg0)

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// Destinations is a free data retrieval call binding the contract method 0x59c02c37.
//
// Solidity: function destinations(address ) view returns(uint256)
func (_Faucet *FaucetSession) Destinations(arg0 libcommon.Address) (*big.Int, error) {
	return _Faucet.Contract.Destinations(&_Faucet.CallOpts, arg0)
}

// Destinations is a free data retrieval call binding the contract method 0x59c02c37.
//
// Solidity: function destinations(address ) view returns(uint256)
func (_Faucet *FaucetCallerSession) Destinations(arg0 libcommon.Address) (*big.Int, error) {
	return _Faucet.Contract.Destinations(&_Faucet.CallOpts, arg0)
}

// Sources is a free data retrieval call binding the contract method 0xb750bdde.
//
// Solidity: function sources(address ) view returns(uint256)
func (_Faucet *FaucetCaller) Sources(opts *bind.CallOpts, arg0 libcommon.Address) (*big.Int, error) {
	var out []interface{}
	err := _Faucet.contract.Call(opts, &out, "sources", arg0)

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// Sources is a free data retrieval call binding the contract method 0xb750bdde.
//
// Solidity: function sources(address ) view returns(uint256)
func (_Faucet *FaucetSession) Sources(arg0 libcommon.Address) (*big.Int, error) {
	return _Faucet.Contract.Sources(&_Faucet.CallOpts, arg0)
}

// Sources is a free data retrieval call binding the contract method 0xb750bdde.
//
// Solidity: function sources(address ) view returns(uint256)
func (_Faucet *FaucetCallerSession) Sources(arg0 libcommon.Address) (*big.Int, error) {
	return _Faucet.Contract.Sources(&_Faucet.CallOpts, arg0)
}

// Send is a paid mutator transaction binding the contract method 0xd0679d34.
//
// Solidity: function send(address _destination, uint256 _requested) payable returns()
func (_Faucet *FaucetTransactor) Send(opts *bind.TransactOpts, _destination libcommon.Address, _requested *big.Int) (types.Transaction, error) {
	return _Faucet.contract.Transact(opts, "send", _destination, _requested)
}

// Send is a paid mutator transaction binding the contract method 0xd0679d34.
//
// Solidity: function send(address _destination, uint256 _requested) payable returns()
func (_Faucet *FaucetSession) Send(_destination libcommon.Address, _requested *big.Int) (types.Transaction, error) {
	return _Faucet.Contract.Send(&_Faucet.TransactOpts, _destination, _requested)
}

// Send is a paid mutator transaction binding the contract method 0xd0679d34.
//
// Solidity: function send(address _destination, uint256 _requested) payable returns()
func (_Faucet *FaucetTransactorSession) Send(_destination libcommon.Address, _requested *big.Int) (types.Transaction, error) {
	return _Faucet.Contract.Send(&_Faucet.TransactOpts, _destination, _requested)
}

// SendParams is an auto generated read-only Go binding of transcaction calldata params
type SendParams struct {
	Param__destination libcommon.Address
	Param__requested   *big.Int
}

// Parse Send method from calldata of a transaction
//
// Solidity: function send(address _destination, uint256 _requested) payable returns()
func ParseSend(calldata []byte) (*SendParams, error) {
	if len(calldata) <= 4 {
		return nil, fmt.Errorf("invalid calldata input")
	}

	_abi, err := abi.JSON(strings.NewReader(FaucetABI))
	if err != nil {
		return nil, fmt.Errorf("failed to get abi of registry metadata: %w", err)
	}

	out, err := _abi.Methods["send"].Inputs.Unpack(calldata[4:])
	if err != nil {
		return nil, fmt.Errorf("failed to unpack send params data: %w", err)
	}

	var paramsResult = new(SendParams)
	value := reflect.ValueOf(paramsResult).Elem()

	if value.NumField() != len(out) {
		return nil, fmt.Errorf("failed to match calldata with param field number")
	}

	out0 := *abi.ConvertType(out[0], new(libcommon.Address)).(*libcommon.Address)
	out1 := *abi.ConvertType(out[1], new(*big.Int)).(**big.Int)

	return &SendParams{
		Param__destination: out0, Param__requested: out1,
	}, nil
}

// Receive is a paid mutator transaction binding the contract receive function.
//
// Solidity: receive() payable returns()
func (_Faucet *FaucetTransactor) Receive(opts *bind.TransactOpts) (types.Transaction, error) {
	return _Faucet.contract.RawTransact(opts, nil) // calldata is disallowed for receive function
}

// Receive is a paid mutator transaction binding the contract receive function.
//
// Solidity: receive() payable returns()
func (_Faucet *FaucetSession) Receive() (types.Transaction, error) {
	return _Faucet.Contract.Receive(&_Faucet.TransactOpts)
}

// Receive is a paid mutator transaction binding the contract receive function.
//
// Solidity: receive() payable returns()
func (_Faucet *FaucetTransactorSession) Receive() (types.Transaction, error) {
	return _Faucet.Contract.Receive(&_Faucet.TransactOpts)
}

// FaucetReceivedIterator is returned from FilterReceived and is used to iterate over the raw logs and unpacked data for Received events raised by the Faucet contract.
type FaucetReceivedIterator struct {
	Event *FaucetReceived // Event containing the contract specifics and raw log

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
func (it *FaucetReceivedIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(FaucetReceived)
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
		it.Event = new(FaucetReceived)
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
func (it *FaucetReceivedIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *FaucetReceivedIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// FaucetReceived represents a Received event raised by the Faucet contract.
type FaucetReceived struct {
	Source libcommon.Address
	Amount *big.Int
	Raw    types.Log // Blockchain specific contextual infos
}

// FilterReceived is a free log retrieval operation binding the contract event 0xf11e547d796cc64acdf758e7cee90439494fd886a19159454aa61e473fdbafef.
//
// Solidity: event received(address _source, uint256 _amount)
func (_Faucet *FaucetFilterer) FilterReceived(opts *bind.FilterOpts) (*FaucetReceivedIterator, error) {

	logs, sub, err := _Faucet.contract.FilterLogs(opts, "received")
	if err != nil {
		return nil, err
	}
	return &FaucetReceivedIterator{contract: _Faucet.contract, event: "received", logs: logs, sub: sub}, nil
}

// WatchReceived is a free log subscription operation binding the contract event 0xf11e547d796cc64acdf758e7cee90439494fd886a19159454aa61e473fdbafef.
//
// Solidity: event received(address _source, uint256 _amount)
func (_Faucet *FaucetFilterer) WatchReceived(opts *bind.WatchOpts, sink chan<- *FaucetReceived) (event.Subscription, error) {

	logs, sub, err := _Faucet.contract.WatchLogs(opts, "received")
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(FaucetReceived)
				if err := _Faucet.contract.UnpackLog(event, "received", log); err != nil {
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
func (_Faucet *FaucetFilterer) ParseReceived(log types.Log) (*FaucetReceived, error) {
	event := new(FaucetReceived)
	if err := _Faucet.contract.UnpackLog(event, "received", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}

// FaucetSentIterator is returned from FilterSent and is used to iterate over the raw logs and unpacked data for Sent events raised by the Faucet contract.
type FaucetSentIterator struct {
	Event *FaucetSent // Event containing the contract specifics and raw log

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
func (it *FaucetSentIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(FaucetSent)
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
		it.Event = new(FaucetSent)
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
func (it *FaucetSentIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *FaucetSentIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// FaucetSent represents a Sent event raised by the Faucet contract.
type FaucetSent struct {
	Destination libcommon.Address
	Amount      *big.Int
	Raw         types.Log // Blockchain specific contextual infos
}

// FilterSent is a free log retrieval operation binding the contract event 0x3bcb2e664d8f57273201bc888e82d6549f8308a52a9fcd7702b2ea8387f769a9.
//
// Solidity: event sent(address _destination, uint256 _amount)
func (_Faucet *FaucetFilterer) FilterSent(opts *bind.FilterOpts) (*FaucetSentIterator, error) {

	logs, sub, err := _Faucet.contract.FilterLogs(opts, "sent")
	if err != nil {
		return nil, err
	}
	return &FaucetSentIterator{contract: _Faucet.contract, event: "sent", logs: logs, sub: sub}, nil
}

// WatchSent is a free log subscription operation binding the contract event 0x3bcb2e664d8f57273201bc888e82d6549f8308a52a9fcd7702b2ea8387f769a9.
//
// Solidity: event sent(address _destination, uint256 _amount)
func (_Faucet *FaucetFilterer) WatchSent(opts *bind.WatchOpts, sink chan<- *FaucetSent) (event.Subscription, error) {

	logs, sub, err := _Faucet.contract.WatchLogs(opts, "sent")
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(FaucetSent)
				if err := _Faucet.contract.UnpackLog(event, "sent", log); err != nil {
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

// ParseSent is a log parse operation binding the contract event 0x3bcb2e664d8f57273201bc888e82d6549f8308a52a9fcd7702b2ea8387f769a9.
//
// Solidity: event sent(address _destination, uint256 _amount)
func (_Faucet *FaucetFilterer) ParseSent(log types.Log) (*FaucetSent, error) {
	event := new(FaucetSent)
	if err := _Faucet.contract.UnpackLog(event, "sent", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}
