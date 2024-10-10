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

// ChildSenderABI is the input ABI used to generate the binding from.
const ChildSenderABI = "[{\"inputs\":[{\"internalType\":\"address\",\"name\":\"childStateReceiver_\",\"type\":\"address\"}],\"stateMutability\":\"nonpayable\",\"type\":\"constructor\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"internalType\":\"bytes\",\"name\":\"message\",\"type\":\"bytes\"}],\"name\":\"MessageSent\",\"type\":\"event\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"amount\",\"type\":\"uint256\"}],\"name\":\"sendToRoot\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"name\":\"sent\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"}]"

// ChildSenderBin is the compiled bytecode used for deploying new contracts.
var ChildSenderBin = "0x608060405234801561001057600080fd5b506040516102b33803806102b383398101604081905261002f91610054565b600080546001600160a01b0319166001600160a01b0392909216919091179055610084565b60006020828403121561006657600080fd5b81516001600160a01b038116811461007d57600080fd5b9392505050565b610220806100936000396000f3fe608060405234801561001057600080fd5b50600436106100365760003560e01c80637bf786f81461003b5780638152e5021461006d575b600080fd5b61005b61004936600461012c565b60016020526000908152604090205481565b60405190815260200160405180910390f35b61008061007b36600461015c565b610082565b005b3360009081526001602052604090205461009c8282610175565b33600081815260016020908152604080832094909455905483516001600160a01b039091169181019190915291820152606081018390526100ee906080016040516020818303038152906040526100f2565b5050565b7f8c5261668696ce22758910d05bab8f186d6eb247ceac2af2e82c7dc17669b03681604051610121919061019c565b60405180910390a150565b60006020828403121561013e57600080fd5b81356001600160a01b038116811461015557600080fd5b9392505050565b60006020828403121561016e57600080fd5b5035919050565b8082018082111561019657634e487b7160e01b600052601160045260246000fd5b92915050565b600060208083528351808285015260005b818110156101c9578581018301518582016040015282016101ad565b506000604082860101526040601f19601f830116850101925050509291505056fea26469706673582212202b5e4ad44349bb7aa70272a65afd939d928b9e646835ef4b7e65acff3d07b21364736f6c63430008140033"

// DeployChildSender deploys a new Ethereum contract, binding an instance of ChildSender to it.
func DeployChildSender(auth *bind.TransactOpts, backend bind.ContractBackend, childStateReceiver_ libcommon.Address) (libcommon.Address, types.Transaction, *ChildSender, error) {
	parsed, err := abi.JSON(strings.NewReader(ChildSenderABI))
	if err != nil {
		return libcommon.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, libcommon.FromHex(ChildSenderBin), backend, childStateReceiver_)
	if err != nil {
		return libcommon.Address{}, nil, nil, err
	}
	return address, tx, &ChildSender{ChildSenderCaller: ChildSenderCaller{contract: contract}, ChildSenderTransactor: ChildSenderTransactor{contract: contract}, ChildSenderFilterer: ChildSenderFilterer{contract: contract}}, nil
}

// ChildSender is an auto generated Go binding around an Ethereum contract.
type ChildSender struct {
	ChildSenderCaller     // Read-only binding to the contract
	ChildSenderTransactor // Write-only binding to the contract
	ChildSenderFilterer   // Log filterer for contract events
}

// ChildSenderCaller is an auto generated read-only Go binding around an Ethereum contract.
type ChildSenderCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ChildSenderTransactor is an auto generated write-only Go binding around an Ethereum contract.
type ChildSenderTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ChildSenderFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type ChildSenderFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ChildSenderSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type ChildSenderSession struct {
	Contract     *ChildSender      // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// ChildSenderCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type ChildSenderCallerSession struct {
	Contract *ChildSenderCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts      // Call options to use throughout this session
}

// ChildSenderTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type ChildSenderTransactorSession struct {
	Contract     *ChildSenderTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts      // Transaction auth options to use throughout this session
}

// ChildSenderRaw is an auto generated low-level Go binding around an Ethereum contract.
type ChildSenderRaw struct {
	Contract *ChildSender // Generic contract binding to access the raw methods on
}

// ChildSenderCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type ChildSenderCallerRaw struct {
	Contract *ChildSenderCaller // Generic read-only contract binding to access the raw methods on
}

// ChildSenderTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type ChildSenderTransactorRaw struct {
	Contract *ChildSenderTransactor // Generic write-only contract binding to access the raw methods on
}

// NewChildSender creates a new instance of ChildSender, bound to a specific deployed contract.
func NewChildSender(address libcommon.Address, backend bind.ContractBackend) (*ChildSender, error) {
	contract, err := bindChildSender(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &ChildSender{ChildSenderCaller: ChildSenderCaller{contract: contract}, ChildSenderTransactor: ChildSenderTransactor{contract: contract}, ChildSenderFilterer: ChildSenderFilterer{contract: contract}}, nil
}

// NewChildSenderCaller creates a new read-only instance of ChildSender, bound to a specific deployed contract.
func NewChildSenderCaller(address libcommon.Address, caller bind.ContractCaller) (*ChildSenderCaller, error) {
	contract, err := bindChildSender(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &ChildSenderCaller{contract: contract}, nil
}

// NewChildSenderTransactor creates a new write-only instance of ChildSender, bound to a specific deployed contract.
func NewChildSenderTransactor(address libcommon.Address, transactor bind.ContractTransactor) (*ChildSenderTransactor, error) {
	contract, err := bindChildSender(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &ChildSenderTransactor{contract: contract}, nil
}

// NewChildSenderFilterer creates a new log filterer instance of ChildSender, bound to a specific deployed contract.
func NewChildSenderFilterer(address libcommon.Address, filterer bind.ContractFilterer) (*ChildSenderFilterer, error) {
	contract, err := bindChildSender(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &ChildSenderFilterer{contract: contract}, nil
}

// bindChildSender binds a generic wrapper to an already deployed contract.
func bindChildSender(address libcommon.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(ChildSenderABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_ChildSender *ChildSenderRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _ChildSender.Contract.ChildSenderCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_ChildSender *ChildSenderRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _ChildSender.Contract.ChildSenderTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_ChildSender *ChildSenderRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _ChildSender.Contract.ChildSenderTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_ChildSender *ChildSenderCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _ChildSender.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_ChildSender *ChildSenderTransactorRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _ChildSender.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_ChildSender *ChildSenderTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _ChildSender.Contract.contract.Transact(opts, method, params...)
}

// Sent is a free data retrieval call binding the contract method 0x7bf786f8.
//
// Solidity: function sent(address ) view returns(uint256)
func (_ChildSender *ChildSenderCaller) Sent(opts *bind.CallOpts, arg0 libcommon.Address) (*big.Int, error) {
	var out []interface{}
	err := _ChildSender.contract.Call(opts, &out, "sent", arg0)

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// Sent is a free data retrieval call binding the contract method 0x7bf786f8.
//
// Solidity: function sent(address ) view returns(uint256)
func (_ChildSender *ChildSenderSession) Sent(arg0 libcommon.Address) (*big.Int, error) {
	return _ChildSender.Contract.Sent(&_ChildSender.CallOpts, arg0)
}

// Sent is a free data retrieval call binding the contract method 0x7bf786f8.
//
// Solidity: function sent(address ) view returns(uint256)
func (_ChildSender *ChildSenderCallerSession) Sent(arg0 libcommon.Address) (*big.Int, error) {
	return _ChildSender.Contract.Sent(&_ChildSender.CallOpts, arg0)
}

// SendToRoot is a paid mutator transaction binding the contract method 0x8152e502.
//
// Solidity: function sendToRoot(uint256 amount) returns()
func (_ChildSender *ChildSenderTransactor) SendToRoot(opts *bind.TransactOpts, amount *big.Int) (types.Transaction, error) {
	return _ChildSender.contract.Transact(opts, "sendToRoot", amount)
}

// SendToRoot is a paid mutator transaction binding the contract method 0x8152e502.
//
// Solidity: function sendToRoot(uint256 amount) returns()
func (_ChildSender *ChildSenderSession) SendToRoot(amount *big.Int) (types.Transaction, error) {
	return _ChildSender.Contract.SendToRoot(&_ChildSender.TransactOpts, amount)
}

// SendToRoot is a paid mutator transaction binding the contract method 0x8152e502.
//
// Solidity: function sendToRoot(uint256 amount) returns()
func (_ChildSender *ChildSenderTransactorSession) SendToRoot(amount *big.Int) (types.Transaction, error) {
	return _ChildSender.Contract.SendToRoot(&_ChildSender.TransactOpts, amount)
}

// SendToRootParams is an auto generated read-only Go binding of transcaction calldata params
type SendToRootParams struct {
	Param_amount *big.Int
}

// Parse SendToRoot method from calldata of a transaction
//
// Solidity: function sendToRoot(uint256 amount) returns()
func ParseSendToRoot(calldata []byte) (*SendToRootParams, error) {
	if len(calldata) <= 4 {
		return nil, fmt.Errorf("invalid calldata input")
	}

	_abi, err := abi.JSON(strings.NewReader(ChildSenderABI))
	if err != nil {
		return nil, fmt.Errorf("failed to get abi of registry metadata: %w", err)
	}

	out, err := _abi.Methods["sendToRoot"].Inputs.Unpack(calldata[4:])
	if err != nil {
		return nil, fmt.Errorf("failed to unpack sendToRoot params data: %w", err)
	}

	var paramsResult = new(SendToRootParams)
	value := reflect.ValueOf(paramsResult).Elem()

	if value.NumField() != len(out) {
		return nil, fmt.Errorf("failed to match calldata with param field number")
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return &SendToRootParams{
		Param_amount: out0,
	}, nil
}

// ChildSenderMessageSentIterator is returned from FilterMessageSent and is used to iterate over the raw logs and unpacked data for MessageSent events raised by the ChildSender contract.
type ChildSenderMessageSentIterator struct {
	Event *ChildSenderMessageSent // Event containing the contract specifics and raw log

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
func (it *ChildSenderMessageSentIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(ChildSenderMessageSent)
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
		it.Event = new(ChildSenderMessageSent)
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
func (it *ChildSenderMessageSentIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *ChildSenderMessageSentIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// ChildSenderMessageSent represents a MessageSent event raised by the ChildSender contract.
type ChildSenderMessageSent struct {
	Message []byte
	Raw     types.Log // Blockchain specific contextual infos
}

// FilterMessageSent is a free log retrieval operation binding the contract event 0x8c5261668696ce22758910d05bab8f186d6eb247ceac2af2e82c7dc17669b036.
//
// Solidity: event MessageSent(bytes message)
func (_ChildSender *ChildSenderFilterer) FilterMessageSent(opts *bind.FilterOpts) (*ChildSenderMessageSentIterator, error) {

	logs, sub, err := _ChildSender.contract.FilterLogs(opts, "MessageSent")
	if err != nil {
		return nil, err
	}
	return &ChildSenderMessageSentIterator{contract: _ChildSender.contract, event: "MessageSent", logs: logs, sub: sub}, nil
}

// WatchMessageSent is a free log subscription operation binding the contract event 0x8c5261668696ce22758910d05bab8f186d6eb247ceac2af2e82c7dc17669b036.
//
// Solidity: event MessageSent(bytes message)
func (_ChildSender *ChildSenderFilterer) WatchMessageSent(opts *bind.WatchOpts, sink chan<- *ChildSenderMessageSent) (event.Subscription, error) {

	logs, sub, err := _ChildSender.contract.WatchLogs(opts, "MessageSent")
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(ChildSenderMessageSent)
				if err := _ChildSender.contract.UnpackLog(event, "MessageSent", log); err != nil {
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

// ParseMessageSent is a log parse operation binding the contract event 0x8c5261668696ce22758910d05bab8f186d6eb247ceac2af2e82c7dc17669b036.
//
// Solidity: event MessageSent(bytes message)
func (_ChildSender *ChildSenderFilterer) ParseMessageSent(log types.Log) (*ChildSenderMessageSent, error) {
	event := new(ChildSenderMessageSent)
	if err := _ChildSender.contract.UnpackLog(event, "MessageSent", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}
