// Code generated - DO NOT EDIT.
// This file is a generated binding and any manual changes will be lost.

package contracts

import (
	"math/big"
	"strings"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	ethereum "github.com/ledgerwatch/erigon"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/event"
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

// PolyABI is the input ABI used to generate the binding from.
const PolyABI = "[{\"inputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"constructor\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"internalType\":\"address\",\"name\":\"d\",\"type\":\"address\"}],\"name\":\"DeployEvent\",\"type\":\"event\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"salt\",\"type\":\"uint256\"}],\"name\":\"deploy\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"salt\",\"type\":\"uint256\"}],\"name\":\"deployAndDestruct\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"}]"

// PolyBin is the compiled bytecode used for deploying new contracts.
var PolyBin = "0x608060405234801561001057600080fd5b506101d1806100206000396000f3fe608060405234801561001057600080fd5b50600436106100365760003560e01c80639debe9811461003b578063a5e387511461005a575b600080fd5b6100586004803603602081101561005157600080fd5b5035610077565b005b6100586004803603602081101561007057600080fd5b50356100fd565b6040805180820190915260138082527260606000534360015360ff60025360036000f360681b60208301908152600091849183f59050600080600080600085620186a0f150604080516001600160a01b038316815290517f68f6a0f063c25c6678c443b9a484086f15ba8f91f60218695d32a5251f2050eb9181900360200190a1505050565b6040805180820190915260138082527260606000534360015360ff60025360036000f360681b60208301908152600091849183f5604080516001600160a01b038316815290519192507f68f6a0f063c25c6678c443b9a484086f15ba8f91f60218695d32a5251f2050eb919081900360200190a150505056fea2646970667358221220c4436dde70fbebb14cf02477e4d8f270620c7f9f54b9b1a2e09b1edcc8c6db6764736f6c637827302e372e352d646576656c6f702e323032302e31322e392b636f6d6d69742e65623737656430380058"

// DeployPoly deploys a new Ethereum contract, binding an instance of Poly to it.
func DeployPoly(auth *bind.TransactOpts, backend bind.ContractBackend) (libcommon.Address, types.Transaction, *Poly, error) {
	parsed, err := abi.JSON(strings.NewReader(PolyABI))
	if err != nil {
		return libcommon.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(PolyBin), backend)
	if err != nil {
		return libcommon.Address{}, nil, nil, err
	}
	return address, tx, &Poly{PolyCaller: PolyCaller{contract: contract}, PolyTransactor: PolyTransactor{contract: contract}, PolyFilterer: PolyFilterer{contract: contract}}, nil
}

// Poly is an auto generated Go binding around an Ethereum contract.
type Poly struct {
	PolyCaller     // Read-only binding to the contract
	PolyTransactor // Write-only binding to the contract
	PolyFilterer   // Log filterer for contract events
}

// PolyCaller is an auto generated read-only Go binding around an Ethereum contract.
type PolyCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// PolyTransactor is an auto generated write-only Go binding around an Ethereum contract.
type PolyTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// PolyFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type PolyFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// PolySession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type PolySession struct {
	Contract     *Poly             // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// PolyCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type PolyCallerSession struct {
	Contract *PolyCaller   // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts // Call options to use throughout this session
}

// PolyTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type PolyTransactorSession struct {
	Contract     *PolyTransactor   // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// PolyRaw is an auto generated low-level Go binding around an Ethereum contract.
type PolyRaw struct {
	Contract *Poly // Generic contract binding to access the raw methods on
}

// PolyCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type PolyCallerRaw struct {
	Contract *PolyCaller // Generic read-only contract binding to access the raw methods on
}

// PolyTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type PolyTransactorRaw struct {
	Contract *PolyTransactor // Generic write-only contract binding to access the raw methods on
}

// NewPoly creates a new instance of Poly, bound to a specific deployed contract.
func NewPoly(address libcommon.Address, backend bind.ContractBackend) (*Poly, error) {
	contract, err := bindPoly(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Poly{PolyCaller: PolyCaller{contract: contract}, PolyTransactor: PolyTransactor{contract: contract}, PolyFilterer: PolyFilterer{contract: contract}}, nil
}

// NewPolyCaller creates a new read-only instance of Poly, bound to a specific deployed contract.
func NewPolyCaller(address libcommon.Address, caller bind.ContractCaller) (*PolyCaller, error) {
	contract, err := bindPoly(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &PolyCaller{contract: contract}, nil
}

// NewPolyTransactor creates a new write-only instance of Poly, bound to a specific deployed contract.
func NewPolyTransactor(address libcommon.Address, transactor bind.ContractTransactor) (*PolyTransactor, error) {
	contract, err := bindPoly(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &PolyTransactor{contract: contract}, nil
}

// NewPolyFilterer creates a new log filterer instance of Poly, bound to a specific deployed contract.
func NewPolyFilterer(address libcommon.Address, filterer bind.ContractFilterer) (*PolyFilterer, error) {
	contract, err := bindPoly(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &PolyFilterer{contract: contract}, nil
}

// bindPoly binds a generic wrapper to an already deployed contract.
func bindPoly(address libcommon.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(PolyABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Poly *PolyRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Poly.Contract.PolyCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Poly *PolyRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _Poly.Contract.PolyTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Poly *PolyRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _Poly.Contract.PolyTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Poly *PolyCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Poly.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Poly *PolyTransactorRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _Poly.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Poly *PolyTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _Poly.Contract.contract.Transact(opts, method, params...)
}

// Deploy is a paid mutator transaction binding the contract method 0xa5e38751.
//
// Solidity: function deploy(uint256 salt) returns()
func (_Poly *PolyTransactor) Deploy(opts *bind.TransactOpts, salt *big.Int) (types.Transaction, error) {
	return _Poly.contract.Transact(opts, "deploy", salt)
}

// Deploy is a paid mutator transaction binding the contract method 0xa5e38751.
//
// Solidity: function deploy(uint256 salt) returns()
func (_Poly *PolySession) Deploy(salt *big.Int) (types.Transaction, error) {
	return _Poly.Contract.Deploy(&_Poly.TransactOpts, salt)
}

// Deploy is a paid mutator transaction binding the contract method 0xa5e38751.
//
// Solidity: function deploy(uint256 salt) returns()
func (_Poly *PolyTransactorSession) Deploy(salt *big.Int) (types.Transaction, error) {
	return _Poly.Contract.Deploy(&_Poly.TransactOpts, salt)
}

// DeployAndDestruct is a paid mutator transaction binding the contract method 0x9debe981.
//
// Solidity: function deployAndDestruct(uint256 salt) returns()
func (_Poly *PolyTransactor) DeployAndDestruct(opts *bind.TransactOpts, salt *big.Int) (types.Transaction, error) {
	return _Poly.contract.Transact(opts, "deployAndDestruct", salt)
}

// DeployAndDestruct is a paid mutator transaction binding the contract method 0x9debe981.
//
// Solidity: function deployAndDestruct(uint256 salt) returns()
func (_Poly *PolySession) DeployAndDestruct(salt *big.Int) (types.Transaction, error) {
	return _Poly.Contract.DeployAndDestruct(&_Poly.TransactOpts, salt)
}

// DeployAndDestruct is a paid mutator transaction binding the contract method 0x9debe981.
//
// Solidity: function deployAndDestruct(uint256 salt) returns()
func (_Poly *PolyTransactorSession) DeployAndDestruct(salt *big.Int) (types.Transaction, error) {
	return _Poly.Contract.DeployAndDestruct(&_Poly.TransactOpts, salt)
}

// PolyDeployEventIterator is returned from FilterDeployEvent and is used to iterate over the raw logs and unpacked data for DeployEvent events raised by the Poly contract.
type PolyDeployEventIterator struct {
	Event *PolyDeployEvent // Event containing the contract specifics and raw log

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
func (it *PolyDeployEventIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(PolyDeployEvent)
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
		it.Event = new(PolyDeployEvent)
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
func (it *PolyDeployEventIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *PolyDeployEventIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// PolyDeployEvent represents a DeployEvent event raised by the Poly contract.
type PolyDeployEvent struct {
	D   libcommon.Address
	Raw types.Log // Blockchain specific contextual infos
}

// FilterDeployEvent is a free log retrieval operation binding the contract event 0x68f6a0f063c25c6678c443b9a484086f15ba8f91f60218695d32a5251f2050eb.
//
// Solidity: event DeployEvent(address d)
func (_Poly *PolyFilterer) FilterDeployEvent(opts *bind.FilterOpts) (*PolyDeployEventIterator, error) {

	logs, sub, err := _Poly.contract.FilterLogs(opts, "DeployEvent")
	if err != nil {
		return nil, err
	}
	return &PolyDeployEventIterator{contract: _Poly.contract, event: "DeployEvent", logs: logs, sub: sub}, nil
}

// WatchDeployEvent is a free log subscription operation binding the contract event 0x68f6a0f063c25c6678c443b9a484086f15ba8f91f60218695d32a5251f2050eb.
//
// Solidity: event DeployEvent(address d)
func (_Poly *PolyFilterer) WatchDeployEvent(opts *bind.WatchOpts, sink chan<- *PolyDeployEvent) (event.Subscription, error) {

	logs, sub, err := _Poly.contract.WatchLogs(opts, "DeployEvent")
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(PolyDeployEvent)
				if err := _Poly.contract.UnpackLog(event, "DeployEvent", log); err != nil {
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
func (_Poly *PolyFilterer) ParseDeployEvent(log types.Log) (*PolyDeployEvent, error) {
	event := new(PolyDeployEvent)
	if err := _Poly.contract.UnpackLog(event, "DeployEvent", log); err != nil {
		return nil, err
	}
	return event, nil
}
