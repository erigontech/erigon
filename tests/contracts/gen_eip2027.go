// Code generated - DO NOT EDIT.
// This file is a generated binding and any manual changes will be lost.

package contracts

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
	_ = abi.U256
	_ = bind.Bind
	_ = common.Big1
	_ = types.BloomLookup
	_ = event.NewSubscription
)

// Eip2027ABI is the input ABI used to generate the binding from.
const Eip2027ABI = "[{\"inputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"constructor\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"name\":\"balances\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"newBalance\",\"type\":\"uint256\"}],\"name\":\"create\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"newBalance\",\"type\":\"uint256\"}],\"name\":\"createAndException\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"newBalance\",\"type\":\"uint256\"}],\"name\":\"createAndRevert\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"remove\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"removeAndException\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"removeAndRevert\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"newBalance\",\"type\":\"uint256\"}],\"name\":\"update\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"newBalance\",\"type\":\"uint256\"}],\"name\":\"updateAndException\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"newBalance\",\"type\":\"uint256\"}],\"name\":\"updateAndRevert\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"}]"

// Eip2027Bin is the compiled bytecode used for deploying new contracts.
var Eip2027Bin = "0x608060405234801561001057600080fd5b5060646000803373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019081526020016000208190555061050a806100646000396000f3fe608060405234801561001057600080fd5b506004361061009e5760003560e01c8063c2ce0ef711610066578063c2ce0ef71461016b578063c53e5ae314610199578063cb946a07146101c7578063d592ed1f146101f5578063f64c050d146101ff5761009e565b806327e235e3146100a3578063660cc200146100fb578063780900dc1461010557806382ab890a14610133578063a7f4377914610161575b600080fd5b6100e5600480360360208110156100b957600080fd5b81019080803573ffffffffffffffffffffffffffffffffffffffff16906020019092919050505061022d565b6040518082815260200191505060405180910390f35b610103610245565b005b6101316004803603602081101561011b57600080fd5b8101908080359060200190929190505050610291565b005b61015f6004803603602081101561014957600080fd5b81019080803590602001909291905050506102d7565b005b61016961031d565b005b6101976004803603602081101561018157600080fd5b8101908080359060200190929190505050610361565b005b6101c5600480360360208110156101af57600080fd5b81019080803590602001909291905050506103af565b005b6101f3600480360360208110156101dd57600080fd5b81019080803590602001909291905050506103f7565b005b6101fd61043f565b005b61022b6004803603602081101561021557600080fd5b8101908080359060200190929190505050610486565b005b60006020528060005260406000206000915090505481565b6000803373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060009055600061028f57fe5b565b806000803373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019081526020016000208190555050565b806000803373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019081526020016000208190555050565b6000803373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060009055565b806000803373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019081526020016000208190555060006103ac57fe5b50565b806000803373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002081905550600080fd5b806000803373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002081905550600080fd5b6000803373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060009055600080fd5b806000803373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019081526020016000208190555060006104d157fe5b5056fea26469706673582212208b95c4a9bdf3319019dd91f828d39daca4d556771d8be9c9716a91b248c4440c64736f6c63430006060033"

// DeployEip2027 deploys a new Ethereum contract, binding an instance of Eip2027 to it.
func DeployEip2027(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *Eip2027, error) {
	parsed, err := abi.JSON(strings.NewReader(Eip2027ABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(Eip2027Bin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &Eip2027{Eip2027Caller: Eip2027Caller{contract: contract}, Eip2027Transactor: Eip2027Transactor{contract: contract}, Eip2027Filterer: Eip2027Filterer{contract: contract}}, nil
}

// Eip2027 is an auto generated Go binding around an Ethereum contract.
type Eip2027 struct {
	Eip2027Caller     // Read-only binding to the contract
	Eip2027Transactor // Write-only binding to the contract
	Eip2027Filterer   // Log filterer for contract events
}

// Eip2027Caller is an auto generated read-only Go binding around an Ethereum contract.
type Eip2027Caller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// Eip2027Transactor is an auto generated write-only Go binding around an Ethereum contract.
type Eip2027Transactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// Eip2027Filterer is an auto generated log filtering Go binding around an Ethereum contract events.
type Eip2027Filterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// Eip2027Session is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type Eip2027Session struct {
	Contract     *Eip2027          // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// Eip2027CallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type Eip2027CallerSession struct {
	Contract *Eip2027Caller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts  // Call options to use throughout this session
}

// Eip2027TransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type Eip2027TransactorSession struct {
	Contract     *Eip2027Transactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts  // Transaction auth options to use throughout this session
}

// Eip2027Raw is an auto generated low-level Go binding around an Ethereum contract.
type Eip2027Raw struct {
	Contract *Eip2027 // Generic contract binding to access the raw methods on
}

// Eip2027CallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type Eip2027CallerRaw struct {
	Contract *Eip2027Caller // Generic read-only contract binding to access the raw methods on
}

// Eip2027TransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type Eip2027TransactorRaw struct {
	Contract *Eip2027Transactor // Generic write-only contract binding to access the raw methods on
}

// NewEip2027 creates a new instance of Eip2027, bound to a specific deployed contract.
func NewEip2027(address common.Address, backend bind.ContractBackend) (*Eip2027, error) {
	contract, err := bindEip2027(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Eip2027{Eip2027Caller: Eip2027Caller{contract: contract}, Eip2027Transactor: Eip2027Transactor{contract: contract}, Eip2027Filterer: Eip2027Filterer{contract: contract}}, nil
}

// NewEip2027Caller creates a new read-only instance of Eip2027, bound to a specific deployed contract.
func NewEip2027Caller(address common.Address, caller bind.ContractCaller) (*Eip2027Caller, error) {
	contract, err := bindEip2027(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &Eip2027Caller{contract: contract}, nil
}

// NewEip2027Transactor creates a new write-only instance of Eip2027, bound to a specific deployed contract.
func NewEip2027Transactor(address common.Address, transactor bind.ContractTransactor) (*Eip2027Transactor, error) {
	contract, err := bindEip2027(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &Eip2027Transactor{contract: contract}, nil
}

// NewEip2027Filterer creates a new log filterer instance of Eip2027, bound to a specific deployed contract.
func NewEip2027Filterer(address common.Address, filterer bind.ContractFilterer) (*Eip2027Filterer, error) {
	contract, err := bindEip2027(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &Eip2027Filterer{contract: contract}, nil
}

// bindEip2027 binds a generic wrapper to an already deployed contract.
func bindEip2027(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(Eip2027ABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Eip2027 *Eip2027Raw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Eip2027.Contract.Eip2027Caller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Eip2027 *Eip2027Raw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Eip2027.Contract.Eip2027Transactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Eip2027 *Eip2027Raw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Eip2027.Contract.Eip2027Transactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Eip2027 *Eip2027CallerRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Eip2027.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Eip2027 *Eip2027TransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Eip2027.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Eip2027 *Eip2027TransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Eip2027.Contract.contract.Transact(opts, method, params...)
}

// Balances is a free data retrieval call binding the contract method 0x27e235e3.
//
// Solidity: function balances(address ) view returns(uint256)
func (_Eip2027 *Eip2027Caller) Balances(opts *bind.CallOpts, arg0 common.Address) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _Eip2027.contract.Call(opts, out, "balances", arg0)
	return *ret0, err
}

// Balances is a free data retrieval call binding the contract method 0x27e235e3.
//
// Solidity: function balances(address ) view returns(uint256)
func (_Eip2027 *Eip2027Session) Balances(arg0 common.Address) (*big.Int, error) {
	return _Eip2027.Contract.Balances(&_Eip2027.CallOpts, arg0)
}

// Balances is a free data retrieval call binding the contract method 0x27e235e3.
//
// Solidity: function balances(address ) view returns(uint256)
func (_Eip2027 *Eip2027CallerSession) Balances(arg0 common.Address) (*big.Int, error) {
	return _Eip2027.Contract.Balances(&_Eip2027.CallOpts, arg0)
}

// Create is a paid mutator transaction binding the contract method 0x780900dc.
//
// Solidity: function create(uint256 newBalance) returns()
func (_Eip2027 *Eip2027Transactor) Create(opts *bind.TransactOpts, newBalance *big.Int) (*types.Transaction, error) {
	return _Eip2027.contract.Transact(opts, "create", newBalance)
}

// Create is a paid mutator transaction binding the contract method 0x780900dc.
//
// Solidity: function create(uint256 newBalance) returns()
func (_Eip2027 *Eip2027Session) Create(newBalance *big.Int) (*types.Transaction, error) {
	return _Eip2027.Contract.Create(&_Eip2027.TransactOpts, newBalance)
}

// Create is a paid mutator transaction binding the contract method 0x780900dc.
//
// Solidity: function create(uint256 newBalance) returns()
func (_Eip2027 *Eip2027TransactorSession) Create(newBalance *big.Int) (*types.Transaction, error) {
	return _Eip2027.Contract.Create(&_Eip2027.TransactOpts, newBalance)
}

// CreateAndException is a paid mutator transaction binding the contract method 0xc2ce0ef7.
//
// Solidity: function createAndException(uint256 newBalance) returns()
func (_Eip2027 *Eip2027Transactor) CreateAndException(opts *bind.TransactOpts, newBalance *big.Int) (*types.Transaction, error) {
	return _Eip2027.contract.Transact(opts, "createAndException", newBalance)
}

// CreateAndException is a paid mutator transaction binding the contract method 0xc2ce0ef7.
//
// Solidity: function createAndException(uint256 newBalance) returns()
func (_Eip2027 *Eip2027Session) CreateAndException(newBalance *big.Int) (*types.Transaction, error) {
	return _Eip2027.Contract.CreateAndException(&_Eip2027.TransactOpts, newBalance)
}

// CreateAndException is a paid mutator transaction binding the contract method 0xc2ce0ef7.
//
// Solidity: function createAndException(uint256 newBalance) returns()
func (_Eip2027 *Eip2027TransactorSession) CreateAndException(newBalance *big.Int) (*types.Transaction, error) {
	return _Eip2027.Contract.CreateAndException(&_Eip2027.TransactOpts, newBalance)
}

// CreateAndRevert is a paid mutator transaction binding the contract method 0xc53e5ae3.
//
// Solidity: function createAndRevert(uint256 newBalance) returns()
func (_Eip2027 *Eip2027Transactor) CreateAndRevert(opts *bind.TransactOpts, newBalance *big.Int) (*types.Transaction, error) {
	return _Eip2027.contract.Transact(opts, "createAndRevert", newBalance)
}

// CreateAndRevert is a paid mutator transaction binding the contract method 0xc53e5ae3.
//
// Solidity: function createAndRevert(uint256 newBalance) returns()
func (_Eip2027 *Eip2027Session) CreateAndRevert(newBalance *big.Int) (*types.Transaction, error) {
	return _Eip2027.Contract.CreateAndRevert(&_Eip2027.TransactOpts, newBalance)
}

// CreateAndRevert is a paid mutator transaction binding the contract method 0xc53e5ae3.
//
// Solidity: function createAndRevert(uint256 newBalance) returns()
func (_Eip2027 *Eip2027TransactorSession) CreateAndRevert(newBalance *big.Int) (*types.Transaction, error) {
	return _Eip2027.Contract.CreateAndRevert(&_Eip2027.TransactOpts, newBalance)
}

// Remove is a paid mutator transaction binding the contract method 0xa7f43779.
//
// Solidity: function remove() returns()
func (_Eip2027 *Eip2027Transactor) Remove(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Eip2027.contract.Transact(opts, "remove")
}

// Remove is a paid mutator transaction binding the contract method 0xa7f43779.
//
// Solidity: function remove() returns()
func (_Eip2027 *Eip2027Session) Remove() (*types.Transaction, error) {
	return _Eip2027.Contract.Remove(&_Eip2027.TransactOpts)
}

// Remove is a paid mutator transaction binding the contract method 0xa7f43779.
//
// Solidity: function remove() returns()
func (_Eip2027 *Eip2027TransactorSession) Remove() (*types.Transaction, error) {
	return _Eip2027.Contract.Remove(&_Eip2027.TransactOpts)
}

// RemoveAndException is a paid mutator transaction binding the contract method 0x660cc200.
//
// Solidity: function removeAndException() returns()
func (_Eip2027 *Eip2027Transactor) RemoveAndException(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Eip2027.contract.Transact(opts, "removeAndException")
}

// RemoveAndException is a paid mutator transaction binding the contract method 0x660cc200.
//
// Solidity: function removeAndException() returns()
func (_Eip2027 *Eip2027Session) RemoveAndException() (*types.Transaction, error) {
	return _Eip2027.Contract.RemoveAndException(&_Eip2027.TransactOpts)
}

// RemoveAndException is a paid mutator transaction binding the contract method 0x660cc200.
//
// Solidity: function removeAndException() returns()
func (_Eip2027 *Eip2027TransactorSession) RemoveAndException() (*types.Transaction, error) {
	return _Eip2027.Contract.RemoveAndException(&_Eip2027.TransactOpts)
}

// RemoveAndRevert is a paid mutator transaction binding the contract method 0xd592ed1f.
//
// Solidity: function removeAndRevert() returns()
func (_Eip2027 *Eip2027Transactor) RemoveAndRevert(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Eip2027.contract.Transact(opts, "removeAndRevert")
}

// RemoveAndRevert is a paid mutator transaction binding the contract method 0xd592ed1f.
//
// Solidity: function removeAndRevert() returns()
func (_Eip2027 *Eip2027Session) RemoveAndRevert() (*types.Transaction, error) {
	return _Eip2027.Contract.RemoveAndRevert(&_Eip2027.TransactOpts)
}

// RemoveAndRevert is a paid mutator transaction binding the contract method 0xd592ed1f.
//
// Solidity: function removeAndRevert() returns()
func (_Eip2027 *Eip2027TransactorSession) RemoveAndRevert() (*types.Transaction, error) {
	return _Eip2027.Contract.RemoveAndRevert(&_Eip2027.TransactOpts)
}

// Update is a paid mutator transaction binding the contract method 0x82ab890a.
//
// Solidity: function update(uint256 newBalance) returns()
func (_Eip2027 *Eip2027Transactor) Update(opts *bind.TransactOpts, newBalance *big.Int) (*types.Transaction, error) {
	return _Eip2027.contract.Transact(opts, "update", newBalance)
}

// Update is a paid mutator transaction binding the contract method 0x82ab890a.
//
// Solidity: function update(uint256 newBalance) returns()
func (_Eip2027 *Eip2027Session) Update(newBalance *big.Int) (*types.Transaction, error) {
	return _Eip2027.Contract.Update(&_Eip2027.TransactOpts, newBalance)
}

// Update is a paid mutator transaction binding the contract method 0x82ab890a.
//
// Solidity: function update(uint256 newBalance) returns()
func (_Eip2027 *Eip2027TransactorSession) Update(newBalance *big.Int) (*types.Transaction, error) {
	return _Eip2027.Contract.Update(&_Eip2027.TransactOpts, newBalance)
}

// UpdateAndException is a paid mutator transaction binding the contract method 0xf64c050d.
//
// Solidity: function updateAndException(uint256 newBalance) returns()
func (_Eip2027 *Eip2027Transactor) UpdateAndException(opts *bind.TransactOpts, newBalance *big.Int) (*types.Transaction, error) {
	return _Eip2027.contract.Transact(opts, "updateAndException", newBalance)
}

// UpdateAndException is a paid mutator transaction binding the contract method 0xf64c050d.
//
// Solidity: function updateAndException(uint256 newBalance) returns()
func (_Eip2027 *Eip2027Session) UpdateAndException(newBalance *big.Int) (*types.Transaction, error) {
	return _Eip2027.Contract.UpdateAndException(&_Eip2027.TransactOpts, newBalance)
}

// UpdateAndException is a paid mutator transaction binding the contract method 0xf64c050d.
//
// Solidity: function updateAndException(uint256 newBalance) returns()
func (_Eip2027 *Eip2027TransactorSession) UpdateAndException(newBalance *big.Int) (*types.Transaction, error) {
	return _Eip2027.Contract.UpdateAndException(&_Eip2027.TransactOpts, newBalance)
}

// UpdateAndRevert is a paid mutator transaction binding the contract method 0xcb946a07.
//
// Solidity: function updateAndRevert(uint256 newBalance) returns()
func (_Eip2027 *Eip2027Transactor) UpdateAndRevert(opts *bind.TransactOpts, newBalance *big.Int) (*types.Transaction, error) {
	return _Eip2027.contract.Transact(opts, "updateAndRevert", newBalance)
}

// UpdateAndRevert is a paid mutator transaction binding the contract method 0xcb946a07.
//
// Solidity: function updateAndRevert(uint256 newBalance) returns()
func (_Eip2027 *Eip2027Session) UpdateAndRevert(newBalance *big.Int) (*types.Transaction, error) {
	return _Eip2027.Contract.UpdateAndRevert(&_Eip2027.TransactOpts, newBalance)
}

// UpdateAndRevert is a paid mutator transaction binding the contract method 0xcb946a07.
//
// Solidity: function updateAndRevert(uint256 newBalance) returns()
func (_Eip2027 *Eip2027TransactorSession) UpdateAndRevert(newBalance *big.Int) (*types.Transaction, error) {
	return _Eip2027.Contract.UpdateAndRevert(&_Eip2027.TransactOpts, newBalance)
}
