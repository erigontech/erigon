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

// RootSenderABI is the input ABI used to generate the binding from.
const RootSenderABI = "[{\"inputs\":[{\"internalType\":\"address\",\"name\":\"stateSender_\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"childStateReceiver_\",\"type\":\"address\"}],\"stateMutability\":\"nonpayable\",\"type\":\"constructor\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"amount\",\"type\":\"uint256\"}],\"name\":\"sendToChild\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"name\":\"sent\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"}]"

// RootSenderBin is the compiled bytecode used for deploying new contracts.
var RootSenderBin = "0x608060405234801561001057600080fd5b506040516102fb3803806102fb83398101604081905261002f9161007c565b600080546001600160a01b039384166001600160a01b031991821617909155600180549290931691161790556100af565b80516001600160a01b038116811461007757600080fd5b919050565b6000806040838503121561008f57600080fd5b61009883610060565b91506100a660208401610060565b90509250929050565b61023d806100be6000396000f3fe608060405234801561001057600080fd5b50600436106100365760003560e01c8063513e29ff1461003b5780637bf786f814610050575b600080fd5b61004e610049366004610139565b610082565b005b61007061005e366004610152565b60026020526000908152604090205481565b60405190815260200160405180910390f35b3360009081526002602052604090205461009c8282610182565b33600081815260026020908152604080832094909455905460015484519283019390935281840186905283518083038501815260608301948590526316f1983160e01b9094526001600160a01b03908116936316f1983193610103939216916064016101a9565b600060405180830381600087803b15801561011d57600080fd5b505af1158015610131573d6000803e3d6000fd5b505050505050565b60006020828403121561014b57600080fd5b5035919050565b60006020828403121561016457600080fd5b81356001600160a01b038116811461017b57600080fd5b9392505050565b808201808211156101a357634e487b7160e01b600052601160045260246000fd5b92915050565b60018060a01b038316815260006020604081840152835180604085015260005b818110156101e5578581018301518582016060015282016101c9565b506000606082860101526060601f19601f83011685010192505050939250505056fea2646970667358221220fa5fa4e9dd64f8da1ad4844228b4671828b48d8de1f8d3f92ba0e5551ce1e47c64736f6c63430008140033"

// DeployRootSender deploys a new Ethereum contract, binding an instance of RootSender to it.
func DeployRootSender(auth *bind.TransactOpts, backend bind.ContractBackend, stateSender_ libcommon.Address, childStateReceiver_ libcommon.Address) (libcommon.Address, types.Transaction, *RootSender, error) {
	parsed, err := abi.JSON(strings.NewReader(RootSenderABI))
	if err != nil {
		return libcommon.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, libcommon.FromHex(RootSenderBin), backend, stateSender_, childStateReceiver_)
	if err != nil {
		return libcommon.Address{}, nil, nil, err
	}
	return address, tx, &RootSender{RootSenderCaller: RootSenderCaller{contract: contract}, RootSenderTransactor: RootSenderTransactor{contract: contract}, RootSenderFilterer: RootSenderFilterer{contract: contract}}, nil
}

// RootSender is an auto generated Go binding around an Ethereum contract.
type RootSender struct {
	RootSenderCaller     // Read-only binding to the contract
	RootSenderTransactor // Write-only binding to the contract
	RootSenderFilterer   // Log filterer for contract events
}

// RootSenderCaller is an auto generated read-only Go binding around an Ethereum contract.
type RootSenderCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// RootSenderTransactor is an auto generated write-only Go binding around an Ethereum contract.
type RootSenderTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// RootSenderFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type RootSenderFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// RootSenderSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type RootSenderSession struct {
	Contract     *RootSender       // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// RootSenderCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type RootSenderCallerSession struct {
	Contract *RootSenderCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts     // Call options to use throughout this session
}

// RootSenderTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type RootSenderTransactorSession struct {
	Contract     *RootSenderTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts     // Transaction auth options to use throughout this session
}

// RootSenderRaw is an auto generated low-level Go binding around an Ethereum contract.
type RootSenderRaw struct {
	Contract *RootSender // Generic contract binding to access the raw methods on
}

// RootSenderCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type RootSenderCallerRaw struct {
	Contract *RootSenderCaller // Generic read-only contract binding to access the raw methods on
}

// RootSenderTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type RootSenderTransactorRaw struct {
	Contract *RootSenderTransactor // Generic write-only contract binding to access the raw methods on
}

// NewRootSender creates a new instance of RootSender, bound to a specific deployed contract.
func NewRootSender(address libcommon.Address, backend bind.ContractBackend) (*RootSender, error) {
	contract, err := bindRootSender(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &RootSender{RootSenderCaller: RootSenderCaller{contract: contract}, RootSenderTransactor: RootSenderTransactor{contract: contract}, RootSenderFilterer: RootSenderFilterer{contract: contract}}, nil
}

// NewRootSenderCaller creates a new read-only instance of RootSender, bound to a specific deployed contract.
func NewRootSenderCaller(address libcommon.Address, caller bind.ContractCaller) (*RootSenderCaller, error) {
	contract, err := bindRootSender(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &RootSenderCaller{contract: contract}, nil
}

// NewRootSenderTransactor creates a new write-only instance of RootSender, bound to a specific deployed contract.
func NewRootSenderTransactor(address libcommon.Address, transactor bind.ContractTransactor) (*RootSenderTransactor, error) {
	contract, err := bindRootSender(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &RootSenderTransactor{contract: contract}, nil
}

// NewRootSenderFilterer creates a new log filterer instance of RootSender, bound to a specific deployed contract.
func NewRootSenderFilterer(address libcommon.Address, filterer bind.ContractFilterer) (*RootSenderFilterer, error) {
	contract, err := bindRootSender(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &RootSenderFilterer{contract: contract}, nil
}

// bindRootSender binds a generic wrapper to an already deployed contract.
func bindRootSender(address libcommon.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(RootSenderABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_RootSender *RootSenderRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _RootSender.Contract.RootSenderCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_RootSender *RootSenderRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _RootSender.Contract.RootSenderTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_RootSender *RootSenderRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _RootSender.Contract.RootSenderTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_RootSender *RootSenderCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _RootSender.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_RootSender *RootSenderTransactorRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _RootSender.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_RootSender *RootSenderTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _RootSender.Contract.contract.Transact(opts, method, params...)
}

// Sent is a free data retrieval call binding the contract method 0x7bf786f8.
//
// Solidity: function sent(address ) view returns(uint256)
func (_RootSender *RootSenderCaller) Sent(opts *bind.CallOpts, arg0 libcommon.Address) (*big.Int, error) {
	var out []interface{}
	err := _RootSender.contract.Call(opts, &out, "sent", arg0)

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// Sent is a free data retrieval call binding the contract method 0x7bf786f8.
//
// Solidity: function sent(address ) view returns(uint256)
func (_RootSender *RootSenderSession) Sent(arg0 libcommon.Address) (*big.Int, error) {
	return _RootSender.Contract.Sent(&_RootSender.CallOpts, arg0)
}

// Sent is a free data retrieval call binding the contract method 0x7bf786f8.
//
// Solidity: function sent(address ) view returns(uint256)
func (_RootSender *RootSenderCallerSession) Sent(arg0 libcommon.Address) (*big.Int, error) {
	return _RootSender.Contract.Sent(&_RootSender.CallOpts, arg0)
}

// SendToChild is a paid mutator transaction binding the contract method 0x513e29ff.
//
// Solidity: function sendToChild(uint256 amount) returns()
func (_RootSender *RootSenderTransactor) SendToChild(opts *bind.TransactOpts, amount *big.Int) (types.Transaction, error) {
	return _RootSender.contract.Transact(opts, "sendToChild", amount)
}

// SendToChild is a paid mutator transaction binding the contract method 0x513e29ff.
//
// Solidity: function sendToChild(uint256 amount) returns()
func (_RootSender *RootSenderSession) SendToChild(amount *big.Int) (types.Transaction, error) {
	return _RootSender.Contract.SendToChild(&_RootSender.TransactOpts, amount)
}

// SendToChild is a paid mutator transaction binding the contract method 0x513e29ff.
//
// Solidity: function sendToChild(uint256 amount) returns()
func (_RootSender *RootSenderTransactorSession) SendToChild(amount *big.Int) (types.Transaction, error) {
	return _RootSender.Contract.SendToChild(&_RootSender.TransactOpts, amount)
}

// SendToChildParams is an auto generated read-only Go binding of transcaction calldata params
type SendToChildParams struct {
	Param_amount *big.Int
}

// Parse SendToChild method from calldata of a transaction
//
// Solidity: function sendToChild(uint256 amount) returns()
func ParseSendToChild(calldata []byte) (*SendToChildParams, error) {
	if len(calldata) <= 4 {
		return nil, fmt.Errorf("invalid calldata input")
	}

	_abi, err := abi.JSON(strings.NewReader(RootSenderABI))
	if err != nil {
		return nil, fmt.Errorf("failed to get abi of registry metadata: %w", err)
	}

	out, err := _abi.Methods["sendToChild"].Inputs.Unpack(calldata[4:])
	if err != nil {
		return nil, fmt.Errorf("failed to unpack sendToChild params data: %w", err)
	}

	var paramsResult = new(SendToChildParams)
	value := reflect.ValueOf(paramsResult).Elem()

	if value.NumField() != len(out) {
		return nil, fmt.Errorf("failed to match calldata with param field number")
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return &SendToChildParams{
		Param_amount: out0,
	}, nil
}
