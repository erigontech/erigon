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

// MathABI is the input ABI used to generate the binding from.
const MathABI = "[{\"constant\":true,\"inputs\":[{\"name\":\"a\",\"type\":\"uint256\"},{\"name\":\"b\",\"type\":\"uint256\"}],\"name\":\"add\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"}]"

// MathBin is the compiled bytecode used for deploying new contracts.
var MathBin = "0x60a3610024600b82828239805160001a607314601757fe5b30600052607381538281f3fe730000000000000000000000000000000000000000301460806040526004361060335760003560e01c8063771602f7146038575b600080fd5b605860048036036040811015604c57600080fd5b5080359060200135606a565b60408051918252519081900360200190f35b019056fea265627a7a723058206fc6c05f3078327f9c763edffdb5ab5f8bd212e293a1306c7d0ad05af3ad35f464736f6c63430005090032"

// DeployMath deploys a new Ethereum contract, binding an instance of Math to it.
func DeployMath(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *Math, error) {
	parsed, err := abi.JSON(strings.NewReader(MathABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(MathBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &Math{MathCaller: MathCaller{contract: contract}, MathTransactor: MathTransactor{contract: contract}, MathFilterer: MathFilterer{contract: contract}}, nil
}

// Math is an auto generated Go binding around an Ethereum contract.
type Math struct {
	MathCaller     // Read-only binding to the contract
	MathTransactor // Write-only binding to the contract
	MathFilterer   // Log filterer for contract events
}

// MathCaller is an auto generated read-only Go binding around an Ethereum contract.
type MathCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// MathTransactor is an auto generated write-only Go binding around an Ethereum contract.
type MathTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// MathFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type MathFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// MathSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type MathSession struct {
	Contract     *Math             // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// MathCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type MathCallerSession struct {
	Contract *MathCaller   // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts // Call options to use throughout this session
}

// MathTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type MathTransactorSession struct {
	Contract     *MathTransactor   // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// MathRaw is an auto generated low-level Go binding around an Ethereum contract.
type MathRaw struct {
	Contract *Math // Generic contract binding to access the raw methods on
}

// MathCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type MathCallerRaw struct {
	Contract *MathCaller // Generic read-only contract binding to access the raw methods on
}

// MathTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type MathTransactorRaw struct {
	Contract *MathTransactor // Generic write-only contract binding to access the raw methods on
}

// NewMath creates a new instance of Math, bound to a specific deployed contract.
func NewMath(address common.Address, backend bind.ContractBackend) (*Math, error) {
	contract, err := bindMath(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Math{MathCaller: MathCaller{contract: contract}, MathTransactor: MathTransactor{contract: contract}, MathFilterer: MathFilterer{contract: contract}}, nil
}

// NewMathCaller creates a new read-only instance of Math, bound to a specific deployed contract.
func NewMathCaller(address common.Address, caller bind.ContractCaller) (*MathCaller, error) {
	contract, err := bindMath(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &MathCaller{contract: contract}, nil
}

// NewMathTransactor creates a new write-only instance of Math, bound to a specific deployed contract.
func NewMathTransactor(address common.Address, transactor bind.ContractTransactor) (*MathTransactor, error) {
	contract, err := bindMath(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &MathTransactor{contract: contract}, nil
}

// NewMathFilterer creates a new log filterer instance of Math, bound to a specific deployed contract.
func NewMathFilterer(address common.Address, filterer bind.ContractFilterer) (*MathFilterer, error) {
	contract, err := bindMath(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &MathFilterer{contract: contract}, nil
}

// bindMath binds a generic wrapper to an already deployed contract.
func bindMath(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(MathABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Math *MathRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Math.Contract.MathCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Math *MathRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Math.Contract.MathTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Math *MathRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Math.Contract.MathTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Math *MathCallerRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Math.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Math *MathTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Math.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Math *MathTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Math.Contract.contract.Transact(opts, method, params...)
}

// Add is a free data retrieval call binding the contract method 0x771602f7.
//
// Solidity: function add(uint256 a, uint256 b) view returns(uint256)
func (_Math *MathCaller) Add(opts *bind.CallOpts, a *big.Int, b *big.Int) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _Math.contract.Call(opts, out, "add", a, b)
	return *ret0, err
}

// Add is a free data retrieval call binding the contract method 0x771602f7.
//
// Solidity: function add(uint256 a, uint256 b) view returns(uint256)
func (_Math *MathSession) Add(a *big.Int, b *big.Int) (*big.Int, error) {
	return _Math.Contract.Add(&_Math.CallOpts, a, b)
}

// Add is a free data retrieval call binding the contract method 0x771602f7.
//
// Solidity: function add(uint256 a, uint256 b) view returns(uint256)
func (_Math *MathCallerSession) Add(a *big.Int, b *big.Int) (*big.Int, error) {
	return _Math.Contract.Add(&_Math.CallOpts, a, b)
}

// UseLibraryABI is the input ABI used to generate the binding from.
const UseLibraryABI = "[{\"constant\":true,\"inputs\":[{\"name\":\"c\",\"type\":\"uint256\"},{\"name\":\"d\",\"type\":\"uint256\"}],\"name\":\"add\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"}]"

// UseLibraryBin is the compiled bytecode used for deploying new contracts.
var UseLibraryBin = "0x608060405234801561001057600080fd5b5061011d806100206000396000f3fe6080604052348015600f57600080fd5b506004361060285760003560e01c8063771602f714602d575b600080fd5b604d60048036036040811015604157600080fd5b5080359060200135605f565b60408051918252519081900360200190f35b600073__$b98c933f0a6ececcd167bd4f9d3299b1a0$__63771602f784846040518363ffffffff1660e01b8152600401808381526020018281526020019250505060206040518083038186803b15801560b757600080fd5b505af415801560ca573d6000803e3d6000fd5b505050506040513d602081101560df57600080fd5b5051939250505056fea265627a7a72305820eb5c38f42445604cfa43d85e3aa5ecc48b0a646456c902dd48420ae7241d06f664736f6c63430005090032"

// DeployUseLibrary deploys a new Ethereum contract, binding an instance of UseLibrary to it.
func DeployUseLibrary(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *UseLibrary, error) {
	parsed, err := abi.JSON(strings.NewReader(UseLibraryABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	mathAddr, _, _, _ := DeployMath(auth, backend)
	UseLibraryBin = strings.Replace(UseLibraryBin, "__$b98c933f0a6ececcd167bd4f9d3299b1a0$__", mathAddr.String()[2:], -1)

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(UseLibraryBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &UseLibrary{UseLibraryCaller: UseLibraryCaller{contract: contract}, UseLibraryTransactor: UseLibraryTransactor{contract: contract}, UseLibraryFilterer: UseLibraryFilterer{contract: contract}}, nil
}

// UseLibrary is an auto generated Go binding around an Ethereum contract.
type UseLibrary struct {
	UseLibraryCaller     // Read-only binding to the contract
	UseLibraryTransactor // Write-only binding to the contract
	UseLibraryFilterer   // Log filterer for contract events
}

// UseLibraryCaller is an auto generated read-only Go binding around an Ethereum contract.
type UseLibraryCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// UseLibraryTransactor is an auto generated write-only Go binding around an Ethereum contract.
type UseLibraryTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// UseLibraryFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type UseLibraryFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// UseLibrarySession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type UseLibrarySession struct {
	Contract     *UseLibrary       // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// UseLibraryCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type UseLibraryCallerSession struct {
	Contract *UseLibraryCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts     // Call options to use throughout this session
}

// UseLibraryTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type UseLibraryTransactorSession struct {
	Contract     *UseLibraryTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts     // Transaction auth options to use throughout this session
}

// UseLibraryRaw is an auto generated low-level Go binding around an Ethereum contract.
type UseLibraryRaw struct {
	Contract *UseLibrary // Generic contract binding to access the raw methods on
}

// UseLibraryCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type UseLibraryCallerRaw struct {
	Contract *UseLibraryCaller // Generic read-only contract binding to access the raw methods on
}

// UseLibraryTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type UseLibraryTransactorRaw struct {
	Contract *UseLibraryTransactor // Generic write-only contract binding to access the raw methods on
}

// NewUseLibrary creates a new instance of UseLibrary, bound to a specific deployed contract.
func NewUseLibrary(address common.Address, backend bind.ContractBackend) (*UseLibrary, error) {
	contract, err := bindUseLibrary(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &UseLibrary{UseLibraryCaller: UseLibraryCaller{contract: contract}, UseLibraryTransactor: UseLibraryTransactor{contract: contract}, UseLibraryFilterer: UseLibraryFilterer{contract: contract}}, nil
}

// NewUseLibraryCaller creates a new read-only instance of UseLibrary, bound to a specific deployed contract.
func NewUseLibraryCaller(address common.Address, caller bind.ContractCaller) (*UseLibraryCaller, error) {
	contract, err := bindUseLibrary(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &UseLibraryCaller{contract: contract}, nil
}

// NewUseLibraryTransactor creates a new write-only instance of UseLibrary, bound to a specific deployed contract.
func NewUseLibraryTransactor(address common.Address, transactor bind.ContractTransactor) (*UseLibraryTransactor, error) {
	contract, err := bindUseLibrary(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &UseLibraryTransactor{contract: contract}, nil
}

// NewUseLibraryFilterer creates a new log filterer instance of UseLibrary, bound to a specific deployed contract.
func NewUseLibraryFilterer(address common.Address, filterer bind.ContractFilterer) (*UseLibraryFilterer, error) {
	contract, err := bindUseLibrary(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &UseLibraryFilterer{contract: contract}, nil
}

// bindUseLibrary binds a generic wrapper to an already deployed contract.
func bindUseLibrary(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(UseLibraryABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_UseLibrary *UseLibraryRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _UseLibrary.Contract.UseLibraryCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_UseLibrary *UseLibraryRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _UseLibrary.Contract.UseLibraryTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_UseLibrary *UseLibraryRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _UseLibrary.Contract.UseLibraryTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_UseLibrary *UseLibraryCallerRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _UseLibrary.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_UseLibrary *UseLibraryTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _UseLibrary.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_UseLibrary *UseLibraryTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _UseLibrary.Contract.contract.Transact(opts, method, params...)
}

// Add is a free data retrieval call binding the contract method 0x771602f7.
//
// Solidity: function add(uint256 c, uint256 d) view returns(uint256)
func (_UseLibrary *UseLibraryCaller) Add(opts *bind.CallOpts, c *big.Int, d *big.Int) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _UseLibrary.contract.Call(opts, out, "add", c, d)
	return *ret0, err
}

// Add is a free data retrieval call binding the contract method 0x771602f7.
//
// Solidity: function add(uint256 c, uint256 d) view returns(uint256)
func (_UseLibrary *UseLibrarySession) Add(c *big.Int, d *big.Int) (*big.Int, error) {
	return _UseLibrary.Contract.Add(&_UseLibrary.CallOpts, c, d)
}

// Add is a free data retrieval call binding the contract method 0x771602f7.
//
// Solidity: function add(uint256 c, uint256 d) view returns(uint256)
func (_UseLibrary *UseLibraryCallerSession) Add(c *big.Int, d *big.Int) (*big.Int, error) {
	return _UseLibrary.Contract.Add(&_UseLibrary.CallOpts, c, d)
}
