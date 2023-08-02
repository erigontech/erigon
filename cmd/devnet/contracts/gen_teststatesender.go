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

// TestStateSenderABI is the input ABI used to generate the binding from.
const TestStateSenderABI = "[{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"user\",\"type\":\"address\"},{\"indexed\":true,\"internalType\":\"address\",\"name\":\"sender\",\"type\":\"address\"},{\"indexed\":true,\"internalType\":\"address\",\"name\":\"receiver\",\"type\":\"address\"}],\"name\":\"NewRegistration\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"user\",\"type\":\"address\"},{\"indexed\":true,\"internalType\":\"address\",\"name\":\"sender\",\"type\":\"address\"},{\"indexed\":true,\"internalType\":\"address\",\"name\":\"receiver\",\"type\":\"address\"}],\"name\":\"RegistrationUpdated\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"uint256\",\"name\":\"id\",\"type\":\"uint256\"},{\"indexed\":true,\"internalType\":\"address\",\"name\":\"contractAddress\",\"type\":\"address\"},{\"indexed\":false,\"internalType\":\"bytes\",\"name\":\"data\",\"type\":\"bytes\"}],\"name\":\"StateSynced\",\"type\":\"event\"},{\"inputs\":[],\"name\":\"counter\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"sender\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"receiver\",\"type\":\"address\"}],\"name\":\"register\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"name\":\"registrations\",\"outputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"receiver\",\"type\":\"address\"},{\"internalType\":\"bytes\",\"name\":\"data\",\"type\":\"bytes\"}],\"name\":\"syncState\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"}]"

// TestStateSenderBin is the compiled bytecode used for deploying new contracts.
var TestStateSenderBin = "0x608060405234801561001057600080fd5b50610366806100206000396000f3fe608060405234801561001057600080fd5b506004361061004c5760003560e01c806316f198311461005157806361bc221a14610066578063942e6bcf14610082578063aa677354146100c3575b600080fd5b61006461005f366004610202565b6100d6565b005b61006f60005481565b6040519081526020015b60405180910390f35b6100ab610090366004610285565b6001602052600090815260409020546001600160a01b031681565b6040516001600160a01b039091168152602001610079565b6100646100d13660046102a7565b610137565b8260005460016100e691906102da565b60008190556040516001600160a01b03861691907f103fed9db65eac19c4d870f49ab7520fe03b99f1838e5996caf47e9e43308392906101299087908790610301565b60405180910390a350505050565b6001600160a01b03818116600090815260016020526040902080546001600160a01b03191691841691821790556101a7576040516001600160a01b03808316919084169033907f3f4512aacd7a664fdb321a48e8340120d63253a91c6367a143abd19ecf68aedd90600090a45050565b6040516001600160a01b03808316919084169033907fc51cb1a93ec91e927852b3445875ec77b148271953e5c0b43698c968ad6fc47d90600090a45050565b80356001600160a01b03811681146101fd57600080fd5b919050565b60008060006040848603121561021757600080fd5b610220846101e6565b9250602084013567ffffffffffffffff8082111561023d57600080fd5b818601915086601f83011261025157600080fd5b81358181111561026057600080fd5b87602082850101111561027257600080fd5b6020830194508093505050509250925092565b60006020828403121561029757600080fd5b6102a0826101e6565b9392505050565b600080604083850312156102ba57600080fd5b6102c3836101e6565b91506102d1602084016101e6565b90509250929050565b808201808211156102fb57634e487b7160e01b600052601160045260246000fd5b92915050565b60208152816020820152818360408301376000818301604090810191909152601f909201601f1916010191905056fea2646970667358221220503899fb2efad396cb70e03842531a8cc17c120a711e076fcab0878258e1c2bf64736f6c63430008140033"

// DeployTestStateSender deploys a new Ethereum contract, binding an instance of TestStateSender to it.
func DeployTestStateSender(auth *bind.TransactOpts, backend bind.ContractBackend) (libcommon.Address, types.Transaction, *TestStateSender, error) {
	parsed, err := abi.JSON(strings.NewReader(TestStateSenderABI))
	if err != nil {
		return libcommon.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, libcommon.FromHex(TestStateSenderBin), backend)
	if err != nil {
		return libcommon.Address{}, nil, nil, err
	}
	return address, tx, &TestStateSender{TestStateSenderCaller: TestStateSenderCaller{contract: contract}, TestStateSenderTransactor: TestStateSenderTransactor{contract: contract}, TestStateSenderFilterer: TestStateSenderFilterer{contract: contract}}, nil
}

// TestStateSender is an auto generated Go binding around an Ethereum contract.
type TestStateSender struct {
	TestStateSenderCaller     // Read-only binding to the contract
	TestStateSenderTransactor // Write-only binding to the contract
	TestStateSenderFilterer   // Log filterer for contract events
}

// TestStateSenderCaller is an auto generated read-only Go binding around an Ethereum contract.
type TestStateSenderCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// TestStateSenderTransactor is an auto generated write-only Go binding around an Ethereum contract.
type TestStateSenderTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// TestStateSenderFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type TestStateSenderFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// TestStateSenderSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type TestStateSenderSession struct {
	Contract     *TestStateSender  // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// TestStateSenderCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type TestStateSenderCallerSession struct {
	Contract *TestStateSenderCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts          // Call options to use throughout this session
}

// TestStateSenderTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type TestStateSenderTransactorSession struct {
	Contract     *TestStateSenderTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts          // Transaction auth options to use throughout this session
}

// TestStateSenderRaw is an auto generated low-level Go binding around an Ethereum contract.
type TestStateSenderRaw struct {
	Contract *TestStateSender // Generic contract binding to access the raw methods on
}

// TestStateSenderCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type TestStateSenderCallerRaw struct {
	Contract *TestStateSenderCaller // Generic read-only contract binding to access the raw methods on
}

// TestStateSenderTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type TestStateSenderTransactorRaw struct {
	Contract *TestStateSenderTransactor // Generic write-only contract binding to access the raw methods on
}

// NewTestStateSender creates a new instance of TestStateSender, bound to a specific deployed contract.
func NewTestStateSender(address libcommon.Address, backend bind.ContractBackend) (*TestStateSender, error) {
	contract, err := bindTestStateSender(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &TestStateSender{TestStateSenderCaller: TestStateSenderCaller{contract: contract}, TestStateSenderTransactor: TestStateSenderTransactor{contract: contract}, TestStateSenderFilterer: TestStateSenderFilterer{contract: contract}}, nil
}

// NewTestStateSenderCaller creates a new read-only instance of TestStateSender, bound to a specific deployed contract.
func NewTestStateSenderCaller(address libcommon.Address, caller bind.ContractCaller) (*TestStateSenderCaller, error) {
	contract, err := bindTestStateSender(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &TestStateSenderCaller{contract: contract}, nil
}

// NewTestStateSenderTransactor creates a new write-only instance of TestStateSender, bound to a specific deployed contract.
func NewTestStateSenderTransactor(address libcommon.Address, transactor bind.ContractTransactor) (*TestStateSenderTransactor, error) {
	contract, err := bindTestStateSender(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &TestStateSenderTransactor{contract: contract}, nil
}

// NewTestStateSenderFilterer creates a new log filterer instance of TestStateSender, bound to a specific deployed contract.
func NewTestStateSenderFilterer(address libcommon.Address, filterer bind.ContractFilterer) (*TestStateSenderFilterer, error) {
	contract, err := bindTestStateSender(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &TestStateSenderFilterer{contract: contract}, nil
}

// bindTestStateSender binds a generic wrapper to an already deployed contract.
func bindTestStateSender(address libcommon.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(TestStateSenderABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_TestStateSender *TestStateSenderRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _TestStateSender.Contract.TestStateSenderCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_TestStateSender *TestStateSenderRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _TestStateSender.Contract.TestStateSenderTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_TestStateSender *TestStateSenderRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _TestStateSender.Contract.TestStateSenderTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_TestStateSender *TestStateSenderCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _TestStateSender.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_TestStateSender *TestStateSenderTransactorRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _TestStateSender.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_TestStateSender *TestStateSenderTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _TestStateSender.Contract.contract.Transact(opts, method, params...)
}

// Counter is a free data retrieval call binding the contract method 0x61bc221a.
//
// Solidity: function counter() view returns(uint256)
func (_TestStateSender *TestStateSenderCaller) Counter(opts *bind.CallOpts) (*big.Int, error) {
	var out []interface{}
	err := _TestStateSender.contract.Call(opts, &out, "counter")

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// Counter is a free data retrieval call binding the contract method 0x61bc221a.
//
// Solidity: function counter() view returns(uint256)
func (_TestStateSender *TestStateSenderSession) Counter() (*big.Int, error) {
	return _TestStateSender.Contract.Counter(&_TestStateSender.CallOpts)
}

// Counter is a free data retrieval call binding the contract method 0x61bc221a.
//
// Solidity: function counter() view returns(uint256)
func (_TestStateSender *TestStateSenderCallerSession) Counter() (*big.Int, error) {
	return _TestStateSender.Contract.Counter(&_TestStateSender.CallOpts)
}

// Registrations is a free data retrieval call binding the contract method 0x942e6bcf.
//
// Solidity: function registrations(address ) view returns(address)
func (_TestStateSender *TestStateSenderCaller) Registrations(opts *bind.CallOpts, arg0 libcommon.Address) (libcommon.Address, error) {
	var out []interface{}
	err := _TestStateSender.contract.Call(opts, &out, "registrations", arg0)

	if err != nil {
		return *new(libcommon.Address), err
	}

	out0 := *abi.ConvertType(out[0], new(libcommon.Address)).(*libcommon.Address)

	return out0, err

}

// Registrations is a free data retrieval call binding the contract method 0x942e6bcf.
//
// Solidity: function registrations(address ) view returns(address)
func (_TestStateSender *TestStateSenderSession) Registrations(arg0 libcommon.Address) (libcommon.Address, error) {
	return _TestStateSender.Contract.Registrations(&_TestStateSender.CallOpts, arg0)
}

// Registrations is a free data retrieval call binding the contract method 0x942e6bcf.
//
// Solidity: function registrations(address ) view returns(address)
func (_TestStateSender *TestStateSenderCallerSession) Registrations(arg0 libcommon.Address) (libcommon.Address, error) {
	return _TestStateSender.Contract.Registrations(&_TestStateSender.CallOpts, arg0)
}

// Register is a paid mutator transaction binding the contract method 0xaa677354.
//
// Solidity: function register(address sender, address receiver) returns()
func (_TestStateSender *TestStateSenderTransactor) Register(opts *bind.TransactOpts, sender libcommon.Address, receiver libcommon.Address) (types.Transaction, error) {
	return _TestStateSender.contract.Transact(opts, "register", sender, receiver)
}

// Register is a paid mutator transaction binding the contract method 0xaa677354.
//
// Solidity: function register(address sender, address receiver) returns()
func (_TestStateSender *TestStateSenderSession) Register(sender libcommon.Address, receiver libcommon.Address) (types.Transaction, error) {
	return _TestStateSender.Contract.Register(&_TestStateSender.TransactOpts, sender, receiver)
}

// Register is a paid mutator transaction binding the contract method 0xaa677354.
//
// Solidity: function register(address sender, address receiver) returns()
func (_TestStateSender *TestStateSenderTransactorSession) Register(sender libcommon.Address, receiver libcommon.Address) (types.Transaction, error) {
	return _TestStateSender.Contract.Register(&_TestStateSender.TransactOpts, sender, receiver)
}

// SyncState is a paid mutator transaction binding the contract method 0x16f19831.
//
// Solidity: function syncState(address receiver, bytes data) returns()
func (_TestStateSender *TestStateSenderTransactor) SyncState(opts *bind.TransactOpts, receiver libcommon.Address, data []byte) (types.Transaction, error) {
	return _TestStateSender.contract.Transact(opts, "syncState", receiver, data)
}

// SyncState is a paid mutator transaction binding the contract method 0x16f19831.
//
// Solidity: function syncState(address receiver, bytes data) returns()
func (_TestStateSender *TestStateSenderSession) SyncState(receiver libcommon.Address, data []byte) (types.Transaction, error) {
	return _TestStateSender.Contract.SyncState(&_TestStateSender.TransactOpts, receiver, data)
}

// SyncState is a paid mutator transaction binding the contract method 0x16f19831.
//
// Solidity: function syncState(address receiver, bytes data) returns()
func (_TestStateSender *TestStateSenderTransactorSession) SyncState(receiver libcommon.Address, data []byte) (types.Transaction, error) {
	return _TestStateSender.Contract.SyncState(&_TestStateSender.TransactOpts, receiver, data)
}

// RegisterParams is an auto generated read-only Go binding of transcaction calldata params
type RegisterParams struct {
	Param_sender   libcommon.Address
	Param_receiver libcommon.Address
}

// Parse Register method from calldata of a transaction
//
// Solidity: function register(address sender, address receiver) returns()
func ParseRegister(calldata []byte) (*RegisterParams, error) {
	if len(calldata) <= 4 {
		return nil, fmt.Errorf("invalid calldata input")
	}

	_abi, err := abi.JSON(strings.NewReader(TestStateSenderABI))
	if err != nil {
		return nil, fmt.Errorf("failed to get abi of registry metadata: %w", err)
	}

	out, err := _abi.Methods["register"].Inputs.Unpack(calldata[4:])
	if err != nil {
		return nil, fmt.Errorf("failed to unpack register params data: %w", err)
	}

	var paramsResult = new(RegisterParams)
	value := reflect.ValueOf(paramsResult).Elem()

	if value.NumField() != len(out) {
		return nil, fmt.Errorf("failed to match calldata with param field number")
	}

	out0 := *abi.ConvertType(out[0], new(libcommon.Address)).(*libcommon.Address)
	out1 := *abi.ConvertType(out[1], new(libcommon.Address)).(*libcommon.Address)

	return &RegisterParams{
		Param_sender: out0, Param_receiver: out1,
	}, nil
}

// SyncStateParams is an auto generated read-only Go binding of transcaction calldata params
type SyncStateParams struct {
	Param_receiver libcommon.Address
	Param_data     []byte
}

// Parse SyncState method from calldata of a transaction
//
// Solidity: function syncState(address receiver, bytes data) returns()
func ParseSyncState(calldata []byte) (*SyncStateParams, error) {
	if len(calldata) <= 4 {
		return nil, fmt.Errorf("invalid calldata input")
	}

	_abi, err := abi.JSON(strings.NewReader(TestStateSenderABI))
	if err != nil {
		return nil, fmt.Errorf("failed to get abi of registry metadata: %w", err)
	}

	out, err := _abi.Methods["syncState"].Inputs.Unpack(calldata[4:])
	if err != nil {
		return nil, fmt.Errorf("failed to unpack syncState params data: %w", err)
	}

	var paramsResult = new(SyncStateParams)
	value := reflect.ValueOf(paramsResult).Elem()

	if value.NumField() != len(out) {
		return nil, fmt.Errorf("failed to match calldata with param field number")
	}

	out0 := *abi.ConvertType(out[0], new(libcommon.Address)).(*libcommon.Address)
	out1 := *abi.ConvertType(out[1], new([]byte)).(*[]byte)

	return &SyncStateParams{
		Param_receiver: out0, Param_data: out1,
	}, nil
}

// TestStateSenderNewRegistrationIterator is returned from FilterNewRegistration and is used to iterate over the raw logs and unpacked data for NewRegistration events raised by the TestStateSender contract.
type TestStateSenderNewRegistrationIterator struct {
	Event *TestStateSenderNewRegistration // Event containing the contract specifics and raw log

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
func (it *TestStateSenderNewRegistrationIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(TestStateSenderNewRegistration)
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
		it.Event = new(TestStateSenderNewRegistration)
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
func (it *TestStateSenderNewRegistrationIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *TestStateSenderNewRegistrationIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// TestStateSenderNewRegistration represents a NewRegistration event raised by the TestStateSender contract.
type TestStateSenderNewRegistration struct {
	User     libcommon.Address
	Sender   libcommon.Address
	Receiver libcommon.Address
	Raw      types.Log // Blockchain specific contextual infos
}

// FilterNewRegistration is a free log retrieval operation binding the contract event 0x3f4512aacd7a664fdb321a48e8340120d63253a91c6367a143abd19ecf68aedd.
//
// Solidity: event NewRegistration(address indexed user, address indexed sender, address indexed receiver)
func (_TestStateSender *TestStateSenderFilterer) FilterNewRegistration(opts *bind.FilterOpts, user []libcommon.Address, sender []libcommon.Address, receiver []libcommon.Address) (*TestStateSenderNewRegistrationIterator, error) {

	var userRule []interface{}
	for _, userItem := range user {
		userRule = append(userRule, userItem)
	}
	var senderRule []interface{}
	for _, senderItem := range sender {
		senderRule = append(senderRule, senderItem)
	}
	var receiverRule []interface{}
	for _, receiverItem := range receiver {
		receiverRule = append(receiverRule, receiverItem)
	}

	logs, sub, err := _TestStateSender.contract.FilterLogs(opts, "NewRegistration", userRule, senderRule, receiverRule)
	if err != nil {
		return nil, err
	}
	return &TestStateSenderNewRegistrationIterator{contract: _TestStateSender.contract, event: "NewRegistration", logs: logs, sub: sub}, nil
}

// WatchNewRegistration is a free log subscription operation binding the contract event 0x3f4512aacd7a664fdb321a48e8340120d63253a91c6367a143abd19ecf68aedd.
//
// Solidity: event NewRegistration(address indexed user, address indexed sender, address indexed receiver)
func (_TestStateSender *TestStateSenderFilterer) WatchNewRegistration(opts *bind.WatchOpts, sink chan<- *TestStateSenderNewRegistration, user []libcommon.Address, sender []libcommon.Address, receiver []libcommon.Address) (event.Subscription, error) {

	var userRule []interface{}
	for _, userItem := range user {
		userRule = append(userRule, userItem)
	}
	var senderRule []interface{}
	for _, senderItem := range sender {
		senderRule = append(senderRule, senderItem)
	}
	var receiverRule []interface{}
	for _, receiverItem := range receiver {
		receiverRule = append(receiverRule, receiverItem)
	}

	logs, sub, err := _TestStateSender.contract.WatchLogs(opts, "NewRegistration", userRule, senderRule, receiverRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(TestStateSenderNewRegistration)
				if err := _TestStateSender.contract.UnpackLog(event, "NewRegistration", log); err != nil {
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

// ParseNewRegistration is a log parse operation binding the contract event 0x3f4512aacd7a664fdb321a48e8340120d63253a91c6367a143abd19ecf68aedd.
//
// Solidity: event NewRegistration(address indexed user, address indexed sender, address indexed receiver)
func (_TestStateSender *TestStateSenderFilterer) ParseNewRegistration(log types.Log) (*TestStateSenderNewRegistration, error) {
	event := new(TestStateSenderNewRegistration)
	if err := _TestStateSender.contract.UnpackLog(event, "NewRegistration", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}

// TestStateSenderRegistrationUpdatedIterator is returned from FilterRegistrationUpdated and is used to iterate over the raw logs and unpacked data for RegistrationUpdated events raised by the TestStateSender contract.
type TestStateSenderRegistrationUpdatedIterator struct {
	Event *TestStateSenderRegistrationUpdated // Event containing the contract specifics and raw log

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
func (it *TestStateSenderRegistrationUpdatedIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(TestStateSenderRegistrationUpdated)
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
		it.Event = new(TestStateSenderRegistrationUpdated)
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
func (it *TestStateSenderRegistrationUpdatedIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *TestStateSenderRegistrationUpdatedIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// TestStateSenderRegistrationUpdated represents a RegistrationUpdated event raised by the TestStateSender contract.
type TestStateSenderRegistrationUpdated struct {
	User     libcommon.Address
	Sender   libcommon.Address
	Receiver libcommon.Address
	Raw      types.Log // Blockchain specific contextual infos
}

// FilterRegistrationUpdated is a free log retrieval operation binding the contract event 0xc51cb1a93ec91e927852b3445875ec77b148271953e5c0b43698c968ad6fc47d.
//
// Solidity: event RegistrationUpdated(address indexed user, address indexed sender, address indexed receiver)
func (_TestStateSender *TestStateSenderFilterer) FilterRegistrationUpdated(opts *bind.FilterOpts, user []libcommon.Address, sender []libcommon.Address, receiver []libcommon.Address) (*TestStateSenderRegistrationUpdatedIterator, error) {

	var userRule []interface{}
	for _, userItem := range user {
		userRule = append(userRule, userItem)
	}
	var senderRule []interface{}
	for _, senderItem := range sender {
		senderRule = append(senderRule, senderItem)
	}
	var receiverRule []interface{}
	for _, receiverItem := range receiver {
		receiverRule = append(receiverRule, receiverItem)
	}

	logs, sub, err := _TestStateSender.contract.FilterLogs(opts, "RegistrationUpdated", userRule, senderRule, receiverRule)
	if err != nil {
		return nil, err
	}
	return &TestStateSenderRegistrationUpdatedIterator{contract: _TestStateSender.contract, event: "RegistrationUpdated", logs: logs, sub: sub}, nil
}

// WatchRegistrationUpdated is a free log subscription operation binding the contract event 0xc51cb1a93ec91e927852b3445875ec77b148271953e5c0b43698c968ad6fc47d.
//
// Solidity: event RegistrationUpdated(address indexed user, address indexed sender, address indexed receiver)
func (_TestStateSender *TestStateSenderFilterer) WatchRegistrationUpdated(opts *bind.WatchOpts, sink chan<- *TestStateSenderRegistrationUpdated, user []libcommon.Address, sender []libcommon.Address, receiver []libcommon.Address) (event.Subscription, error) {

	var userRule []interface{}
	for _, userItem := range user {
		userRule = append(userRule, userItem)
	}
	var senderRule []interface{}
	for _, senderItem := range sender {
		senderRule = append(senderRule, senderItem)
	}
	var receiverRule []interface{}
	for _, receiverItem := range receiver {
		receiverRule = append(receiverRule, receiverItem)
	}

	logs, sub, err := _TestStateSender.contract.WatchLogs(opts, "RegistrationUpdated", userRule, senderRule, receiverRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(TestStateSenderRegistrationUpdated)
				if err := _TestStateSender.contract.UnpackLog(event, "RegistrationUpdated", log); err != nil {
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

// ParseRegistrationUpdated is a log parse operation binding the contract event 0xc51cb1a93ec91e927852b3445875ec77b148271953e5c0b43698c968ad6fc47d.
//
// Solidity: event RegistrationUpdated(address indexed user, address indexed sender, address indexed receiver)
func (_TestStateSender *TestStateSenderFilterer) ParseRegistrationUpdated(log types.Log) (*TestStateSenderRegistrationUpdated, error) {
	event := new(TestStateSenderRegistrationUpdated)
	if err := _TestStateSender.contract.UnpackLog(event, "RegistrationUpdated", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}

// TestStateSenderStateSyncedIterator is returned from FilterStateSynced and is used to iterate over the raw logs and unpacked data for StateSynced events raised by the TestStateSender contract.
type TestStateSenderStateSyncedIterator struct {
	Event *TestStateSenderStateSynced // Event containing the contract specifics and raw log

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
func (it *TestStateSenderStateSyncedIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(TestStateSenderStateSynced)
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
		it.Event = new(TestStateSenderStateSynced)
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
func (it *TestStateSenderStateSyncedIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *TestStateSenderStateSyncedIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// TestStateSenderStateSynced represents a StateSynced event raised by the TestStateSender contract.
type TestStateSenderStateSynced struct {
	Id              *big.Int
	ContractAddress libcommon.Address
	Data            []byte
	Raw             types.Log // Blockchain specific contextual infos
}

// FilterStateSynced is a free log retrieval operation binding the contract event 0x103fed9db65eac19c4d870f49ab7520fe03b99f1838e5996caf47e9e43308392.
//
// Solidity: event StateSynced(uint256 indexed id, address indexed contractAddress, bytes data)
func (_TestStateSender *TestStateSenderFilterer) FilterStateSynced(opts *bind.FilterOpts, id []*big.Int, contractAddress []libcommon.Address) (*TestStateSenderStateSyncedIterator, error) {

	var idRule []interface{}
	for _, idItem := range id {
		idRule = append(idRule, idItem)
	}
	var contractAddressRule []interface{}
	for _, contractAddressItem := range contractAddress {
		contractAddressRule = append(contractAddressRule, contractAddressItem)
	}

	logs, sub, err := _TestStateSender.contract.FilterLogs(opts, "StateSynced", idRule, contractAddressRule)
	if err != nil {
		return nil, err
	}
	return &TestStateSenderStateSyncedIterator{contract: _TestStateSender.contract, event: "StateSynced", logs: logs, sub: sub}, nil
}

// WatchStateSynced is a free log subscription operation binding the contract event 0x103fed9db65eac19c4d870f49ab7520fe03b99f1838e5996caf47e9e43308392.
//
// Solidity: event StateSynced(uint256 indexed id, address indexed contractAddress, bytes data)
func (_TestStateSender *TestStateSenderFilterer) WatchStateSynced(opts *bind.WatchOpts, sink chan<- *TestStateSenderStateSynced, id []*big.Int, contractAddress []libcommon.Address) (event.Subscription, error) {

	var idRule []interface{}
	for _, idItem := range id {
		idRule = append(idRule, idItem)
	}
	var contractAddressRule []interface{}
	for _, contractAddressItem := range contractAddress {
		contractAddressRule = append(contractAddressRule, contractAddressItem)
	}

	logs, sub, err := _TestStateSender.contract.WatchLogs(opts, "StateSynced", idRule, contractAddressRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(TestStateSenderStateSynced)
				if err := _TestStateSender.contract.UnpackLog(event, "StateSynced", log); err != nil {
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

// ParseStateSynced is a log parse operation binding the contract event 0x103fed9db65eac19c4d870f49ab7520fe03b99f1838e5996caf47e9e43308392.
//
// Solidity: event StateSynced(uint256 indexed id, address indexed contractAddress, bytes data)
func (_TestStateSender *TestStateSenderFilterer) ParseStateSynced(log types.Log) (*TestStateSenderStateSynced, error) {
	event := new(TestStateSenderStateSynced)
	if err := _TestStateSender.contract.UnpackLog(event, "StateSynced", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}
