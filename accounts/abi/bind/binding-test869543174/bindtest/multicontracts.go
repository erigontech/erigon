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

// ExternalLibSharedStruct is an auto generated low-level Go binding around an user-defined struct.
type ExternalLibSharedStruct struct {
	F1 *big.Int
	F2 [32]byte
}

// ContractOneABI is the input ABI used to generate the binding from.
const ContractOneABI = "[{\"constant\":true,\"inputs\":[{\"components\":[{\"internalType\":\"uint256\",\"name\":\"f1\",\"type\":\"uint256\"},{\"internalType\":\"bytes32\",\"name\":\"f2\",\"type\":\"bytes32\"}],\"internalType\":\"structExternalLib.SharedStruct\",\"name\":\"s\",\"type\":\"tuple\"}],\"name\":\"foo\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"pure\",\"type\":\"function\"}]"

// ContractOneBin is the compiled bytecode used for deploying new contracts.
var ContractOneBin = "0x60806040523480156100115760006000fd5b50610017565b6101b5806100266000396000f3fe60806040523480156100115760006000fd5b50600436106100305760003560e01c80639d8a8ba81461003657610030565b60006000fd5b610050600480360361004b91908101906100d1565b610052565b005b5b5056610171565b6000813590506100698161013d565b92915050565b6000604082840312156100825760006000fd5b61008c60406100fb565b9050600061009c848285016100bc565b60008301525060206100b08482850161005a565b60208301525092915050565b6000813590506100cb81610157565b92915050565b6000604082840312156100e45760006000fd5b60006100f28482850161006f565b91505092915050565b6000604051905081810181811067ffffffffffffffff8211171561011f5760006000fd5b8060405250919050565b6000819050919050565b6000819050919050565b61014681610129565b811415156101545760006000fd5b50565b61016081610133565b8114151561016e5760006000fd5b50565bfea365627a7a72315820749274eb7f6c01010d5322af4e1668b0a154409eb7968bd6cae5524c7ed669bb6c6578706572696d656e74616cf564736f6c634300050c0040"

// DeployContractOne deploys a new Ethereum contract, binding an instance of ContractOne to it.
func DeployContractOne(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *ContractOne, error) {
	parsed, err := abi.JSON(strings.NewReader(ContractOneABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(ContractOneBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &ContractOne{ContractOneCaller: ContractOneCaller{contract: contract}, ContractOneTransactor: ContractOneTransactor{contract: contract}, ContractOneFilterer: ContractOneFilterer{contract: contract}}, nil
}

// ContractOne is an auto generated Go binding around an Ethereum contract.
type ContractOne struct {
	ContractOneCaller     // Read-only binding to the contract
	ContractOneTransactor // Write-only binding to the contract
	ContractOneFilterer   // Log filterer for contract events
}

// ContractOneCaller is an auto generated read-only Go binding around an Ethereum contract.
type ContractOneCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ContractOneTransactor is an auto generated write-only Go binding around an Ethereum contract.
type ContractOneTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ContractOneFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type ContractOneFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ContractOneSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type ContractOneSession struct {
	Contract     *ContractOne      // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// ContractOneCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type ContractOneCallerSession struct {
	Contract *ContractOneCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts      // Call options to use throughout this session
}

// ContractOneTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type ContractOneTransactorSession struct {
	Contract     *ContractOneTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts      // Transaction auth options to use throughout this session
}

// ContractOneRaw is an auto generated low-level Go binding around an Ethereum contract.
type ContractOneRaw struct {
	Contract *ContractOne // Generic contract binding to access the raw methods on
}

// ContractOneCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type ContractOneCallerRaw struct {
	Contract *ContractOneCaller // Generic read-only contract binding to access the raw methods on
}

// ContractOneTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type ContractOneTransactorRaw struct {
	Contract *ContractOneTransactor // Generic write-only contract binding to access the raw methods on
}

// NewContractOne creates a new instance of ContractOne, bound to a specific deployed contract.
func NewContractOne(address common.Address, backend bind.ContractBackend) (*ContractOne, error) {
	contract, err := bindContractOne(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &ContractOne{ContractOneCaller: ContractOneCaller{contract: contract}, ContractOneTransactor: ContractOneTransactor{contract: contract}, ContractOneFilterer: ContractOneFilterer{contract: contract}}, nil
}

// NewContractOneCaller creates a new read-only instance of ContractOne, bound to a specific deployed contract.
func NewContractOneCaller(address common.Address, caller bind.ContractCaller) (*ContractOneCaller, error) {
	contract, err := bindContractOne(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &ContractOneCaller{contract: contract}, nil
}

// NewContractOneTransactor creates a new write-only instance of ContractOne, bound to a specific deployed contract.
func NewContractOneTransactor(address common.Address, transactor bind.ContractTransactor) (*ContractOneTransactor, error) {
	contract, err := bindContractOne(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &ContractOneTransactor{contract: contract}, nil
}

// NewContractOneFilterer creates a new log filterer instance of ContractOne, bound to a specific deployed contract.
func NewContractOneFilterer(address common.Address, filterer bind.ContractFilterer) (*ContractOneFilterer, error) {
	contract, err := bindContractOne(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &ContractOneFilterer{contract: contract}, nil
}

// bindContractOne binds a generic wrapper to an already deployed contract.
func bindContractOne(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(ContractOneABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_ContractOne *ContractOneRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _ContractOne.Contract.ContractOneCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_ContractOne *ContractOneRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _ContractOne.Contract.ContractOneTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_ContractOne *ContractOneRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _ContractOne.Contract.ContractOneTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_ContractOne *ContractOneCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _ContractOne.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_ContractOne *ContractOneTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _ContractOne.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_ContractOne *ContractOneTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _ContractOne.Contract.contract.Transact(opts, method, params...)
}

// Foo is a free data retrieval call binding the contract method 0x9d8a8ba8.
//
// Solidity: function foo((uint256,bytes32) s) pure returns()
func (_ContractOne *ContractOneCaller) Foo(opts *bind.CallOpts, s ExternalLibSharedStruct) error {
	var out []interface{}
	err := _ContractOne.contract.Call(opts, &out, "foo", s)

	if err != nil {
		return err
	}

	return err

}

// Foo is a free data retrieval call binding the contract method 0x9d8a8ba8.
//
// Solidity: function foo((uint256,bytes32) s) pure returns()
func (_ContractOne *ContractOneSession) Foo(s ExternalLibSharedStruct) error {
	return _ContractOne.Contract.Foo(&_ContractOne.CallOpts, s)
}

// Foo is a free data retrieval call binding the contract method 0x9d8a8ba8.
//
// Solidity: function foo((uint256,bytes32) s) pure returns()
func (_ContractOne *ContractOneCallerSession) Foo(s ExternalLibSharedStruct) error {
	return _ContractOne.Contract.Foo(&_ContractOne.CallOpts, s)
}

// ContractTwoABI is the input ABI used to generate the binding from.
const ContractTwoABI = "[{\"constant\":true,\"inputs\":[{\"components\":[{\"internalType\":\"uint256\",\"name\":\"f1\",\"type\":\"uint256\"},{\"internalType\":\"bytes32\",\"name\":\"f2\",\"type\":\"bytes32\"}],\"internalType\":\"structExternalLib.SharedStruct\",\"name\":\"s\",\"type\":\"tuple\"}],\"name\":\"bar\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"pure\",\"type\":\"function\"}]"

// ContractTwoBin is the compiled bytecode used for deploying new contracts.
var ContractTwoBin = "0x60806040523480156100115760006000fd5b50610017565b6101b5806100266000396000f3fe60806040523480156100115760006000fd5b50600436106100305760003560e01c8063db8ba08c1461003657610030565b60006000fd5b610050600480360361004b91908101906100d1565b610052565b005b5b5056610171565b6000813590506100698161013d565b92915050565b6000604082840312156100825760006000fd5b61008c60406100fb565b9050600061009c848285016100bc565b60008301525060206100b08482850161005a565b60208301525092915050565b6000813590506100cb81610157565b92915050565b6000604082840312156100e45760006000fd5b60006100f28482850161006f565b91505092915050565b6000604051905081810181811067ffffffffffffffff8211171561011f5760006000fd5b8060405250919050565b6000819050919050565b6000819050919050565b61014681610129565b811415156101545760006000fd5b50565b61016081610133565b8114151561016e5760006000fd5b50565bfea365627a7a723158209bc28ee7ea97c131a13330d77ec73b4493b5c59c648352da81dd288b021192596c6578706572696d656e74616cf564736f6c634300050c0040"

// DeployContractTwo deploys a new Ethereum contract, binding an instance of ContractTwo to it.
func DeployContractTwo(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *ContractTwo, error) {
	parsed, err := abi.JSON(strings.NewReader(ContractTwoABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(ContractTwoBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &ContractTwo{ContractTwoCaller: ContractTwoCaller{contract: contract}, ContractTwoTransactor: ContractTwoTransactor{contract: contract}, ContractTwoFilterer: ContractTwoFilterer{contract: contract}}, nil
}

// ContractTwo is an auto generated Go binding around an Ethereum contract.
type ContractTwo struct {
	ContractTwoCaller     // Read-only binding to the contract
	ContractTwoTransactor // Write-only binding to the contract
	ContractTwoFilterer   // Log filterer for contract events
}

// ContractTwoCaller is an auto generated read-only Go binding around an Ethereum contract.
type ContractTwoCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ContractTwoTransactor is an auto generated write-only Go binding around an Ethereum contract.
type ContractTwoTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ContractTwoFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type ContractTwoFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ContractTwoSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type ContractTwoSession struct {
	Contract     *ContractTwo      // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// ContractTwoCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type ContractTwoCallerSession struct {
	Contract *ContractTwoCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts      // Call options to use throughout this session
}

// ContractTwoTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type ContractTwoTransactorSession struct {
	Contract     *ContractTwoTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts      // Transaction auth options to use throughout this session
}

// ContractTwoRaw is an auto generated low-level Go binding around an Ethereum contract.
type ContractTwoRaw struct {
	Contract *ContractTwo // Generic contract binding to access the raw methods on
}

// ContractTwoCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type ContractTwoCallerRaw struct {
	Contract *ContractTwoCaller // Generic read-only contract binding to access the raw methods on
}

// ContractTwoTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type ContractTwoTransactorRaw struct {
	Contract *ContractTwoTransactor // Generic write-only contract binding to access the raw methods on
}

// NewContractTwo creates a new instance of ContractTwo, bound to a specific deployed contract.
func NewContractTwo(address common.Address, backend bind.ContractBackend) (*ContractTwo, error) {
	contract, err := bindContractTwo(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &ContractTwo{ContractTwoCaller: ContractTwoCaller{contract: contract}, ContractTwoTransactor: ContractTwoTransactor{contract: contract}, ContractTwoFilterer: ContractTwoFilterer{contract: contract}}, nil
}

// NewContractTwoCaller creates a new read-only instance of ContractTwo, bound to a specific deployed contract.
func NewContractTwoCaller(address common.Address, caller bind.ContractCaller) (*ContractTwoCaller, error) {
	contract, err := bindContractTwo(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &ContractTwoCaller{contract: contract}, nil
}

// NewContractTwoTransactor creates a new write-only instance of ContractTwo, bound to a specific deployed contract.
func NewContractTwoTransactor(address common.Address, transactor bind.ContractTransactor) (*ContractTwoTransactor, error) {
	contract, err := bindContractTwo(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &ContractTwoTransactor{contract: contract}, nil
}

// NewContractTwoFilterer creates a new log filterer instance of ContractTwo, bound to a specific deployed contract.
func NewContractTwoFilterer(address common.Address, filterer bind.ContractFilterer) (*ContractTwoFilterer, error) {
	contract, err := bindContractTwo(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &ContractTwoFilterer{contract: contract}, nil
}

// bindContractTwo binds a generic wrapper to an already deployed contract.
func bindContractTwo(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(ContractTwoABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_ContractTwo *ContractTwoRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _ContractTwo.Contract.ContractTwoCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_ContractTwo *ContractTwoRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _ContractTwo.Contract.ContractTwoTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_ContractTwo *ContractTwoRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _ContractTwo.Contract.ContractTwoTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_ContractTwo *ContractTwoCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _ContractTwo.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_ContractTwo *ContractTwoTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _ContractTwo.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_ContractTwo *ContractTwoTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _ContractTwo.Contract.contract.Transact(opts, method, params...)
}

// Bar is a free data retrieval call binding the contract method 0xdb8ba08c.
//
// Solidity: function bar((uint256,bytes32) s) pure returns()
func (_ContractTwo *ContractTwoCaller) Bar(opts *bind.CallOpts, s ExternalLibSharedStruct) error {
	var out []interface{}
	err := _ContractTwo.contract.Call(opts, &out, "bar", s)

	if err != nil {
		return err
	}

	return err

}

// Bar is a free data retrieval call binding the contract method 0xdb8ba08c.
//
// Solidity: function bar((uint256,bytes32) s) pure returns()
func (_ContractTwo *ContractTwoSession) Bar(s ExternalLibSharedStruct) error {
	return _ContractTwo.Contract.Bar(&_ContractTwo.CallOpts, s)
}

// Bar is a free data retrieval call binding the contract method 0xdb8ba08c.
//
// Solidity: function bar((uint256,bytes32) s) pure returns()
func (_ContractTwo *ContractTwoCallerSession) Bar(s ExternalLibSharedStruct) error {
	return _ContractTwo.Contract.Bar(&_ContractTwo.CallOpts, s)
}

// ExternalLibABI is the input ABI used to generate the binding from.
const ExternalLibABI = "[]"

// ExternalLibBin is the compiled bytecode used for deploying new contracts.
var ExternalLibBin = "0x606c6026600b82828239805160001a6073141515601857fe5b30600052607381538281f350fe73000000000000000000000000000000000000000030146080604052600436106023575b60006000fdfea365627a7a72315820518f0110144f5b3de95697d05e456a064656890d08e6f9cff47f3be710cc46a36c6578706572696d656e74616cf564736f6c634300050c0040"

// DeployExternalLib deploys a new Ethereum contract, binding an instance of ExternalLib to it.
func DeployExternalLib(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *ExternalLib, error) {
	parsed, err := abi.JSON(strings.NewReader(ExternalLibABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(ExternalLibBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &ExternalLib{ExternalLibCaller: ExternalLibCaller{contract: contract}, ExternalLibTransactor: ExternalLibTransactor{contract: contract}, ExternalLibFilterer: ExternalLibFilterer{contract: contract}}, nil
}

// ExternalLib is an auto generated Go binding around an Ethereum contract.
type ExternalLib struct {
	ExternalLibCaller     // Read-only binding to the contract
	ExternalLibTransactor // Write-only binding to the contract
	ExternalLibFilterer   // Log filterer for contract events
}

// ExternalLibCaller is an auto generated read-only Go binding around an Ethereum contract.
type ExternalLibCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ExternalLibTransactor is an auto generated write-only Go binding around an Ethereum contract.
type ExternalLibTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ExternalLibFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type ExternalLibFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ExternalLibSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type ExternalLibSession struct {
	Contract     *ExternalLib      // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// ExternalLibCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type ExternalLibCallerSession struct {
	Contract *ExternalLibCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts      // Call options to use throughout this session
}

// ExternalLibTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type ExternalLibTransactorSession struct {
	Contract     *ExternalLibTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts      // Transaction auth options to use throughout this session
}

// ExternalLibRaw is an auto generated low-level Go binding around an Ethereum contract.
type ExternalLibRaw struct {
	Contract *ExternalLib // Generic contract binding to access the raw methods on
}

// ExternalLibCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type ExternalLibCallerRaw struct {
	Contract *ExternalLibCaller // Generic read-only contract binding to access the raw methods on
}

// ExternalLibTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type ExternalLibTransactorRaw struct {
	Contract *ExternalLibTransactor // Generic write-only contract binding to access the raw methods on
}

// NewExternalLib creates a new instance of ExternalLib, bound to a specific deployed contract.
func NewExternalLib(address common.Address, backend bind.ContractBackend) (*ExternalLib, error) {
	contract, err := bindExternalLib(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &ExternalLib{ExternalLibCaller: ExternalLibCaller{contract: contract}, ExternalLibTransactor: ExternalLibTransactor{contract: contract}, ExternalLibFilterer: ExternalLibFilterer{contract: contract}}, nil
}

// NewExternalLibCaller creates a new read-only instance of ExternalLib, bound to a specific deployed contract.
func NewExternalLibCaller(address common.Address, caller bind.ContractCaller) (*ExternalLibCaller, error) {
	contract, err := bindExternalLib(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &ExternalLibCaller{contract: contract}, nil
}

// NewExternalLibTransactor creates a new write-only instance of ExternalLib, bound to a specific deployed contract.
func NewExternalLibTransactor(address common.Address, transactor bind.ContractTransactor) (*ExternalLibTransactor, error) {
	contract, err := bindExternalLib(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &ExternalLibTransactor{contract: contract}, nil
}

// NewExternalLibFilterer creates a new log filterer instance of ExternalLib, bound to a specific deployed contract.
func NewExternalLibFilterer(address common.Address, filterer bind.ContractFilterer) (*ExternalLibFilterer, error) {
	contract, err := bindExternalLib(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &ExternalLibFilterer{contract: contract}, nil
}

// bindExternalLib binds a generic wrapper to an already deployed contract.
func bindExternalLib(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(ExternalLibABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_ExternalLib *ExternalLibRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _ExternalLib.Contract.ExternalLibCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_ExternalLib *ExternalLibRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _ExternalLib.Contract.ExternalLibTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_ExternalLib *ExternalLibRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _ExternalLib.Contract.ExternalLibTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_ExternalLib *ExternalLibCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _ExternalLib.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_ExternalLib *ExternalLibTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _ExternalLib.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_ExternalLib *ExternalLibTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _ExternalLib.Contract.contract.Transact(opts, method, params...)
}
