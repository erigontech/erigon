// Code generated - DO NOT EDIT.
// This file is a generated binding and any manual changes will be lost.

package mockverifier

import (
	"errors"
	"math/big"
	"strings"

	ethereum "github.com/ledgerwatch/erigon"
	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/event"
)

// Reference imports to suppress errors if they are not otherwise used.
var (
	_ = errors.New
	_ = big.NewInt
	_ = strings.NewReader
	_ = ethereum.NotFound
	_ = bind.Bind
	_ = common.Big1
	_ = types.BloomLookup
	_ = event.NewSubscription
	_ = abi.ConvertType
)

// MockverifierMetaData contains all meta data concerning the Mockverifier contract.
var MockverifierMetaData = &bind.MetaData{
	ABI: "[{\"inputs\":[{\"internalType\":\"bytes\",\"name\":\"proof\",\"type\":\"bytes\"},{\"internalType\":\"uint256[1]\",\"name\":\"pubSignals\",\"type\":\"uint256[1]\"}],\"name\":\"verifyProof\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"view\",\"type\":\"function\"}]",
	Bin: "0x608060405234801561001057600080fd5b50610205806100206000396000f3fe608060405234801561001057600080fd5b506004361061002b5760003560e01c80638d8f8a5c14610030575b600080fd5b61004661003e366004610128565b600192915050565b604051901515815260200160405180910390f35b7f4e487b7100000000000000000000000000000000000000000000000000000000600052604160045260246000fd5b604051601f8201601f1916810167ffffffffffffffff811182821017156100b2576100b261005a565b604052919050565b600082601f8301126100cb57600080fd5b604051602080820182811067ffffffffffffffff821117156100ef576100ef61005a565b604052818482018681111561010357600080fd5b855b8181101561011c5780358352918301918301610105565b50929695505050505050565b6000806040838503121561013b57600080fd5b823567ffffffffffffffff8082111561015357600080fd5b818501915085601f83011261016757600080fd5b813560208282111561017b5761017b61005a565b61018d601f8301601f19168201610089565b925081835287818386010111156101a357600080fd5b818185018285013760008183850101528295506101c2888289016100ba565b945050505050925092905056fea26469706673582212204af7f3bda67f3a30e1891af134e290590c6063b5a163b85e57b0c1111e1463a364736f6c63430008110033",
}

// MockverifierABI is the input ABI used to generate the binding from.
// Deprecated: Use MockverifierMetaData.ABI instead.
var MockverifierABI = MockverifierMetaData.ABI

// MockverifierBin is the compiled bytecode used for deploying new contracts.
// Deprecated: Use MockverifierMetaData.Bin instead.
var MockverifierBin = MockverifierMetaData.Bin

// DeployMockverifier deploys a new Ethereum contract, binding an instance of Mockverifier to it.
func DeployMockverifier(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, types.Transaction, *Mockverifier, error) {
	parsed, err := MockverifierMetaData.GetAbi()
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	if parsed == nil {
		return common.Address{}, nil, nil, errors.New("GetABI returned nil")
	}

	address, tx, contract, err := bind.DeployContract(auth, *parsed, common.FromHex(MockverifierBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &Mockverifier{MockverifierCaller: MockverifierCaller{contract: contract}, MockverifierTransactor: MockverifierTransactor{contract: contract}, MockverifierFilterer: MockverifierFilterer{contract: contract}}, nil
}

// Mockverifier is an auto generated Go binding around an Ethereum contract.
type Mockverifier struct {
	MockverifierCaller     // Read-only binding to the contract
	MockverifierTransactor // Write-only binding to the contract
	MockverifierFilterer   // Log filterer for contract events
}

// MockverifierCaller is an auto generated read-only Go binding around an Ethereum contract.
type MockverifierCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// MockverifierTransactor is an auto generated write-only Go binding around an Ethereum contract.
type MockverifierTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// MockverifierFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type MockverifierFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// MockverifierSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type MockverifierSession struct {
	Contract     *Mockverifier     // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// MockverifierCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type MockverifierCallerSession struct {
	Contract *MockverifierCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts       // Call options to use throughout this session
}

// MockverifierTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type MockverifierTransactorSession struct {
	Contract     *MockverifierTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts       // Transaction auth options to use throughout this session
}

// MockverifierRaw is an auto generated low-level Go binding around an Ethereum contract.
type MockverifierRaw struct {
	Contract *Mockverifier // Generic contract binding to access the raw methods on
}

// MockverifierCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type MockverifierCallerRaw struct {
	Contract *MockverifierCaller // Generic read-only contract binding to access the raw methods on
}

// MockverifierTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type MockverifierTransactorRaw struct {
	Contract *MockverifierTransactor // Generic write-only contract binding to access the raw methods on
}

// NewMockverifier creates a new instance of Mockverifier, bound to a specific deployed contract.
func NewMockverifier(address common.Address, backend bind.ContractBackend) (*Mockverifier, error) {
	contract, err := bindMockverifier(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Mockverifier{MockverifierCaller: MockverifierCaller{contract: contract}, MockverifierTransactor: MockverifierTransactor{contract: contract}, MockverifierFilterer: MockverifierFilterer{contract: contract}}, nil
}

// NewMockverifierCaller creates a new read-only instance of Mockverifier, bound to a specific deployed contract.
func NewMockverifierCaller(address common.Address, caller bind.ContractCaller) (*MockverifierCaller, error) {
	contract, err := bindMockverifier(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &MockverifierCaller{contract: contract}, nil
}

// NewMockverifierTransactor creates a new write-only instance of Mockverifier, bound to a specific deployed contract.
func NewMockverifierTransactor(address common.Address, transactor bind.ContractTransactor) (*MockverifierTransactor, error) {
	contract, err := bindMockverifier(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &MockverifierTransactor{contract: contract}, nil
}

// NewMockverifierFilterer creates a new log filterer instance of Mockverifier, bound to a specific deployed contract.
func NewMockverifierFilterer(address common.Address, filterer bind.ContractFilterer) (*MockverifierFilterer, error) {
	contract, err := bindMockverifier(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &MockverifierFilterer{contract: contract}, nil
}

// bindMockverifier binds a generic wrapper to an already deployed contract.
func bindMockverifier(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := MockverifierMetaData.GetAbi()
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, *parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Mockverifier *MockverifierRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Mockverifier.Contract.MockverifierCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Mockverifier *MockverifierRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _Mockverifier.Contract.MockverifierTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Mockverifier *MockverifierRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _Mockverifier.Contract.MockverifierTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Mockverifier *MockverifierCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Mockverifier.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Mockverifier *MockverifierTransactorRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _Mockverifier.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Mockverifier *MockverifierTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _Mockverifier.Contract.contract.Transact(opts, method, params...)
}

// VerifyProof is a free data retrieval call binding the contract method 0x8d8f8a5c.
//
// Solidity: function verifyProof(bytes proof, uint256[1] pubSignals) view returns(bool)
func (_Mockverifier *MockverifierCaller) VerifyProof(opts *bind.CallOpts, proof []byte, pubSignals [1]*big.Int) (bool, error) {
	var out []interface{}
	err := _Mockverifier.contract.Call(opts, &out, "verifyProof", proof, pubSignals)

	if err != nil {
		return *new(bool), err
	}

	out0 := *abi.ConvertType(out[0], new(bool)).(*bool)

	return out0, err

}

// VerifyProof is a free data retrieval call binding the contract method 0x8d8f8a5c.
//
// Solidity: function verifyProof(bytes proof, uint256[1] pubSignals) view returns(bool)
func (_Mockverifier *MockverifierSession) VerifyProof(proof []byte, pubSignals [1]*big.Int) (bool, error) {
	return _Mockverifier.Contract.VerifyProof(&_Mockverifier.CallOpts, proof, pubSignals)
}

// VerifyProof is a free data retrieval call binding the contract method 0x8d8f8a5c.
//
// Solidity: function verifyProof(bytes proof, uint256[1] pubSignals) view returns(bool)
func (_Mockverifier *MockverifierCallerSession) VerifyProof(proof []byte, pubSignals [1]*big.Int) (bool, error) {
	return _Mockverifier.Contract.VerifyProof(&_Mockverifier.CallOpts, proof, pubSignals)
}
