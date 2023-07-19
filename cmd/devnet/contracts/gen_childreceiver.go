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
const ChildReceiverABI = "[{\"inputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"constructor\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"},{\"internalType\":\"bytes\",\"name\":\"data\",\"type\":\"bytes\"}],\"name\":\"onStateReceive\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"name\":\"received\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"}]"

// ChildReceiverBin is the compiled bytecode used for deploying new contracts.
var ChildReceiverBin = "0x608060405234801561000f575f80fd5b506102098061001d5f395ff3fe608060405234801561000f575f80fd5b5060043610610034575f3560e01c806326c53bea14610038578063df0cb9341461004d575b5f80fd5b61004b6100463660046100d6565b61007e565b005b61006c61005b366004610162565b5f6020819052908152604090205481565b60405190815260200160405180910390f35b5f8061008c83850185610184565b6001600160a01b0382165f9081526020819052604090205491935091506100b382826101ae565b6001600160a01b039093165f908152602081905260409020929092555050505050565b5f805f604084860312156100e8575f80fd5b83359250602084013567ffffffffffffffff80821115610106575f80fd5b818601915086601f830112610119575f80fd5b813581811115610127575f80fd5b876020828501011115610138575f80fd5b6020830194508093505050509250925092565b6001600160a01b038116811461015f575f80fd5b50565b5f60208284031215610172575f80fd5b813561017d8161014b565b9392505050565b5f8060408385031215610195575f80fd5b82356101a08161014b565b946020939093013593505050565b808201808211156101cd57634e487b7160e01b5f52601160045260245ffd5b9291505056fea2646970667358221220170a9b17116a5cc2ff0cb0d3b11024dec78d30cc87dbae17e31a0be0d58ef1ce64736f6c63430008140033"

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

// Received is a free data retrieval call binding the contract method 0xdf0cb934.
//
// Solidity: function received(address ) view returns(uint256)
func (_ChildReceiver *ChildReceiverCaller) Received(opts *bind.CallOpts, arg0 libcommon.Address) (*big.Int, error) {
	var out []interface{}
	err := _ChildReceiver.contract.Call(opts, &out, "received", arg0)

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// Received is a free data retrieval call binding the contract method 0xdf0cb934.
//
// Solidity: function received(address ) view returns(uint256)
func (_ChildReceiver *ChildReceiverSession) Received(arg0 libcommon.Address) (*big.Int, error) {
	return _ChildReceiver.Contract.Received(&_ChildReceiver.CallOpts, arg0)
}

// Received is a free data retrieval call binding the contract method 0xdf0cb934.
//
// Solidity: function received(address ) view returns(uint256)
func (_ChildReceiver *ChildReceiverCallerSession) Received(arg0 libcommon.Address) (*big.Int, error) {
	return _ChildReceiver.Contract.Received(&_ChildReceiver.CallOpts, arg0)
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
