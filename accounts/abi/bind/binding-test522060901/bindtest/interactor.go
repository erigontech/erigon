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

// InteractorABI is the input ABI used to generate the binding from.
const InteractorABI = "[{\"constant\":true,\"inputs\":[],\"name\":\"transactString\",\"outputs\":[{\"name\":\"\",\"type\":\"string\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"deployString\",\"outputs\":[{\"name\":\"\",\"type\":\"string\"}],\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"str\",\"type\":\"string\"}],\"name\":\"transact\",\"outputs\":[],\"type\":\"function\"},{\"inputs\":[{\"name\":\"str\",\"type\":\"string\"}],\"type\":\"constructor\"}]"

// InteractorBin is the compiled bytecode used for deploying new contracts.
var InteractorBin = "0x6060604052604051610328380380610328833981016040528051018060006000509080519060200190828054600181600116156101000203166002900490600052602060002090601f016020900481019282601f10608d57805160ff19168380011785555b50607c9291505b8082111560ba57838155600101606b565b50505061026a806100be6000396000f35b828001600101855582156064579182015b828111156064578251826000505591602001919060010190609e565b509056606060405260e060020a60003504630d86a0e181146100315780636874e8091461008d578063d736c513146100ea575b005b610190600180546020600282841615610100026000190190921691909104601f810182900490910260809081016040526060828152929190828280156102295780601f106101fe57610100808354040283529160200191610229565b61019060008054602060026001831615610100026000190190921691909104601f810182900490910260809081016040526060828152929190828280156102295780601f106101fe57610100808354040283529160200191610229565b60206004803580820135601f81018490049093026080908101604052606084815261002f946024939192918401918190838280828437509496505050505050508060016000509080519060200190828054600181600116156101000203166002900490600052602060002090601f016020900481019282601f1061023157805160ff19168380011785555b506102619291505b808211156102665760008155830161017d565b60405180806020018281038252838181518152602001915080519060200190808383829060006004602084601f0104600f02600301f150905090810190601f1680156101f05780820380516001836020036101000a031916815260200191505b509250505060405180910390f35b820191906000526020600020905b81548152906001019060200180831161020c57829003601f168201915b505050505081565b82800160010185558215610175579182015b82811115610175578251826000505591602001919060010190610243565b505050565b509056"

// DeployInteractor deploys a new Ethereum contract, binding an instance of Interactor to it.
func DeployInteractor(auth *bind.TransactOpts, backend bind.ContractBackend, str string) (common.Address, *types.Transaction, *Interactor, error) {
	parsed, err := abi.JSON(strings.NewReader(InteractorABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(InteractorBin), backend, str)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &Interactor{InteractorCaller: InteractorCaller{contract: contract}, InteractorTransactor: InteractorTransactor{contract: contract}, InteractorFilterer: InteractorFilterer{contract: contract}}, nil
}

// Interactor is an auto generated Go binding around an Ethereum contract.
type Interactor struct {
	InteractorCaller     // Read-only binding to the contract
	InteractorTransactor // Write-only binding to the contract
	InteractorFilterer   // Log filterer for contract events
}

// InteractorCaller is an auto generated read-only Go binding around an Ethereum contract.
type InteractorCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// InteractorTransactor is an auto generated write-only Go binding around an Ethereum contract.
type InteractorTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// InteractorFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type InteractorFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// InteractorSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type InteractorSession struct {
	Contract     *Interactor       // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// InteractorCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type InteractorCallerSession struct {
	Contract *InteractorCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts     // Call options to use throughout this session
}

// InteractorTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type InteractorTransactorSession struct {
	Contract     *InteractorTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts     // Transaction auth options to use throughout this session
}

// InteractorRaw is an auto generated low-level Go binding around an Ethereum contract.
type InteractorRaw struct {
	Contract *Interactor // Generic contract binding to access the raw methods on
}

// InteractorCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type InteractorCallerRaw struct {
	Contract *InteractorCaller // Generic read-only contract binding to access the raw methods on
}

// InteractorTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type InteractorTransactorRaw struct {
	Contract *InteractorTransactor // Generic write-only contract binding to access the raw methods on
}

// NewInteractor creates a new instance of Interactor, bound to a specific deployed contract.
func NewInteractor(address common.Address, backend bind.ContractBackend) (*Interactor, error) {
	contract, err := bindInteractor(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Interactor{InteractorCaller: InteractorCaller{contract: contract}, InteractorTransactor: InteractorTransactor{contract: contract}, InteractorFilterer: InteractorFilterer{contract: contract}}, nil
}

// NewInteractorCaller creates a new read-only instance of Interactor, bound to a specific deployed contract.
func NewInteractorCaller(address common.Address, caller bind.ContractCaller) (*InteractorCaller, error) {
	contract, err := bindInteractor(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &InteractorCaller{contract: contract}, nil
}

// NewInteractorTransactor creates a new write-only instance of Interactor, bound to a specific deployed contract.
func NewInteractorTransactor(address common.Address, transactor bind.ContractTransactor) (*InteractorTransactor, error) {
	contract, err := bindInteractor(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &InteractorTransactor{contract: contract}, nil
}

// NewInteractorFilterer creates a new log filterer instance of Interactor, bound to a specific deployed contract.
func NewInteractorFilterer(address common.Address, filterer bind.ContractFilterer) (*InteractorFilterer, error) {
	contract, err := bindInteractor(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &InteractorFilterer{contract: contract}, nil
}

// bindInteractor binds a generic wrapper to an already deployed contract.
func bindInteractor(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(InteractorABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Interactor *InteractorRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Interactor.Contract.InteractorCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Interactor *InteractorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Interactor.Contract.InteractorTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Interactor *InteractorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Interactor.Contract.InteractorTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Interactor *InteractorCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Interactor.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Interactor *InteractorTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Interactor.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Interactor *InteractorTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Interactor.Contract.contract.Transact(opts, method, params...)
}

// DeployString is a free data retrieval call binding the contract method 0x6874e809.
//
// Solidity: function deployString() returns(string)
func (_Interactor *InteractorCaller) DeployString(opts *bind.CallOpts) (string, error) {
	var out []interface{}
	err := _Interactor.contract.Call(opts, &out, "deployString")

	if err != nil {
		return *new(string), err
	}

	out0 := *abi.ConvertType(out[0], new(string)).(*string)

	return out0, err

}

// DeployString is a free data retrieval call binding the contract method 0x6874e809.
//
// Solidity: function deployString() returns(string)
func (_Interactor *InteractorSession) DeployString() (string, error) {
	return _Interactor.Contract.DeployString(&_Interactor.CallOpts)
}

// DeployString is a free data retrieval call binding the contract method 0x6874e809.
//
// Solidity: function deployString() returns(string)
func (_Interactor *InteractorCallerSession) DeployString() (string, error) {
	return _Interactor.Contract.DeployString(&_Interactor.CallOpts)
}

// TransactString is a free data retrieval call binding the contract method 0x0d86a0e1.
//
// Solidity: function transactString() returns(string)
func (_Interactor *InteractorCaller) TransactString(opts *bind.CallOpts) (string, error) {
	var out []interface{}
	err := _Interactor.contract.Call(opts, &out, "transactString")

	if err != nil {
		return *new(string), err
	}

	out0 := *abi.ConvertType(out[0], new(string)).(*string)

	return out0, err

}

// TransactString is a free data retrieval call binding the contract method 0x0d86a0e1.
//
// Solidity: function transactString() returns(string)
func (_Interactor *InteractorSession) TransactString() (string, error) {
	return _Interactor.Contract.TransactString(&_Interactor.CallOpts)
}

// TransactString is a free data retrieval call binding the contract method 0x0d86a0e1.
//
// Solidity: function transactString() returns(string)
func (_Interactor *InteractorCallerSession) TransactString() (string, error) {
	return _Interactor.Contract.TransactString(&_Interactor.CallOpts)
}

// Transact is a paid mutator transaction binding the contract method 0xd736c513.
//
// Solidity: function transact(string str) returns()
func (_Interactor *InteractorTransactor) Transact(opts *bind.TransactOpts, str string) (*types.Transaction, error) {
	return _Interactor.contract.Transact(opts, "transact", str)
}

// Transact is a paid mutator transaction binding the contract method 0xd736c513.
//
// Solidity: function transact(string str) returns()
func (_Interactor *InteractorSession) Transact(str string) (*types.Transaction, error) {
	return _Interactor.Contract.Transact(&_Interactor.TransactOpts, str)
}

// Transact is a paid mutator transaction binding the contract method 0xd736c513.
//
// Solidity: function transact(string str) returns()
func (_Interactor *InteractorTransactorSession) Transact(str string) (*types.Transaction, error) {
	return _Interactor.Contract.Transact(&_Interactor.TransactOpts, str)
}
