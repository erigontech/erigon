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

// UnderscorerABI is the input ABI used to generate the binding from.
const UnderscorerABI = "[{\"constant\":true,\"inputs\":[],\"name\":\"LowerUpperCollision\",\"outputs\":[{\"name\":\"_res\",\"type\":\"int256\"},{\"name\":\"Res\",\"type\":\"int256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"_under_scored_func\",\"outputs\":[{\"name\":\"_int\",\"type\":\"int256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"UnderscoredOutput\",\"outputs\":[{\"name\":\"_int\",\"type\":\"int256\"},{\"name\":\"_string\",\"type\":\"string\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"PurelyUnderscoredOutput\",\"outputs\":[{\"name\":\"_\",\"type\":\"int256\"},{\"name\":\"res\",\"type\":\"int256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"UpperLowerCollision\",\"outputs\":[{\"name\":\"_Res\",\"type\":\"int256\"},{\"name\":\"res\",\"type\":\"int256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"AllPurelyUnderscoredOutput\",\"outputs\":[{\"name\":\"_\",\"type\":\"int256\"},{\"name\":\"__\",\"type\":\"int256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"UpperUpperCollision\",\"outputs\":[{\"name\":\"_Res\",\"type\":\"int256\"},{\"name\":\"Res\",\"type\":\"int256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"LowerLowerCollision\",\"outputs\":[{\"name\":\"_res\",\"type\":\"int256\"},{\"name\":\"res\",\"type\":\"int256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"}]"

// UnderscorerBin is the compiled bytecode used for deploying new contracts.
var UnderscorerBin = "0x6060604052341561000f57600080fd5b6103858061001e6000396000f30060606040526004361061008e576000357c0100000000000000000000000000000000000000000000000000000000900463ffffffff16806303a592131461009357806346546dbe146100c357806367e6633d146100ec5780639df4848514610181578063af7486ab146101b1578063b564b34d146101e1578063e02ab24d14610211578063e409ca4514610241575b600080fd5b341561009e57600080fd5b6100a6610271565b604051808381526020018281526020019250505060405180910390f35b34156100ce57600080fd5b6100d6610286565b6040518082815260200191505060405180910390f35b34156100f757600080fd5b6100ff61028e565b6040518083815260200180602001828103825283818151815260200191508051906020019080838360005b8381101561014557808201518184015260208101905061012a565b50505050905090810190601f1680156101725780820380516001836020036101000a031916815260200191505b50935050505060405180910390f35b341561018c57600080fd5b6101946102dc565b604051808381526020018281526020019250505060405180910390f35b34156101bc57600080fd5b6101c46102f1565b604051808381526020018281526020019250505060405180910390f35b34156101ec57600080fd5b6101f4610306565b604051808381526020018281526020019250505060405180910390f35b341561021c57600080fd5b61022461031b565b604051808381526020018281526020019250505060405180910390f35b341561024c57600080fd5b610254610330565b604051808381526020018281526020019250505060405180910390f35b60008060016002819150809050915091509091565b600080905090565b6000610298610345565b61013a8090506040805190810160405280600281526020017f7069000000000000000000000000000000000000000000000000000000000000815250915091509091565b60008060016002819150809050915091509091565b60008060016002819150809050915091509091565b60008060016002819150809050915091509091565b60008060016002819150809050915091509091565b60008060016002819150809050915091509091565b6020604051908101604052806000815250905600a165627a7a72305820d1a53d9de9d1e3d55cb3dc591900b63c4f1ded79114f7b79b332684840e186a40029"

// DeployUnderscorer deploys a new Ethereum contract, binding an instance of Underscorer to it.
func DeployUnderscorer(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *Underscorer, error) {
	parsed, err := abi.JSON(strings.NewReader(UnderscorerABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(UnderscorerBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &Underscorer{UnderscorerCaller: UnderscorerCaller{contract: contract}, UnderscorerTransactor: UnderscorerTransactor{contract: contract}, UnderscorerFilterer: UnderscorerFilterer{contract: contract}}, nil
}

// Underscorer is an auto generated Go binding around an Ethereum contract.
type Underscorer struct {
	UnderscorerCaller     // Read-only binding to the contract
	UnderscorerTransactor // Write-only binding to the contract
	UnderscorerFilterer   // Log filterer for contract events
}

// UnderscorerCaller is an auto generated read-only Go binding around an Ethereum contract.
type UnderscorerCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// UnderscorerTransactor is an auto generated write-only Go binding around an Ethereum contract.
type UnderscorerTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// UnderscorerFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type UnderscorerFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// UnderscorerSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type UnderscorerSession struct {
	Contract     *Underscorer      // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// UnderscorerCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type UnderscorerCallerSession struct {
	Contract *UnderscorerCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts      // Call options to use throughout this session
}

// UnderscorerTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type UnderscorerTransactorSession struct {
	Contract     *UnderscorerTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts      // Transaction auth options to use throughout this session
}

// UnderscorerRaw is an auto generated low-level Go binding around an Ethereum contract.
type UnderscorerRaw struct {
	Contract *Underscorer // Generic contract binding to access the raw methods on
}

// UnderscorerCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type UnderscorerCallerRaw struct {
	Contract *UnderscorerCaller // Generic read-only contract binding to access the raw methods on
}

// UnderscorerTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type UnderscorerTransactorRaw struct {
	Contract *UnderscorerTransactor // Generic write-only contract binding to access the raw methods on
}

// NewUnderscorer creates a new instance of Underscorer, bound to a specific deployed contract.
func NewUnderscorer(address common.Address, backend bind.ContractBackend) (*Underscorer, error) {
	contract, err := bindUnderscorer(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Underscorer{UnderscorerCaller: UnderscorerCaller{contract: contract}, UnderscorerTransactor: UnderscorerTransactor{contract: contract}, UnderscorerFilterer: UnderscorerFilterer{contract: contract}}, nil
}

// NewUnderscorerCaller creates a new read-only instance of Underscorer, bound to a specific deployed contract.
func NewUnderscorerCaller(address common.Address, caller bind.ContractCaller) (*UnderscorerCaller, error) {
	contract, err := bindUnderscorer(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &UnderscorerCaller{contract: contract}, nil
}

// NewUnderscorerTransactor creates a new write-only instance of Underscorer, bound to a specific deployed contract.
func NewUnderscorerTransactor(address common.Address, transactor bind.ContractTransactor) (*UnderscorerTransactor, error) {
	contract, err := bindUnderscorer(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &UnderscorerTransactor{contract: contract}, nil
}

// NewUnderscorerFilterer creates a new log filterer instance of Underscorer, bound to a specific deployed contract.
func NewUnderscorerFilterer(address common.Address, filterer bind.ContractFilterer) (*UnderscorerFilterer, error) {
	contract, err := bindUnderscorer(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &UnderscorerFilterer{contract: contract}, nil
}

// bindUnderscorer binds a generic wrapper to an already deployed contract.
func bindUnderscorer(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(UnderscorerABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Underscorer *UnderscorerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Underscorer.Contract.UnderscorerCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Underscorer *UnderscorerRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Underscorer.Contract.UnderscorerTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Underscorer *UnderscorerRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Underscorer.Contract.UnderscorerTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Underscorer *UnderscorerCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Underscorer.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Underscorer *UnderscorerTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Underscorer.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Underscorer *UnderscorerTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Underscorer.Contract.contract.Transact(opts, method, params...)
}

// AllPurelyUnderscoredOutput is a free data retrieval call binding the contract method 0xb564b34d.
//
// Solidity: function AllPurelyUnderscoredOutput() view returns(int256 _, int256 __)
func (_Underscorer *UnderscorerCaller) AllPurelyUnderscoredOutput(opts *bind.CallOpts) (*big.Int, *big.Int, error) {
	var out []interface{}
	err := _Underscorer.contract.Call(opts, &out, "AllPurelyUnderscoredOutput")

	if err != nil {
		return *new(*big.Int), *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)
	out1 := *abi.ConvertType(out[1], new(*big.Int)).(**big.Int)

	return out0, out1, err

}

// AllPurelyUnderscoredOutput is a free data retrieval call binding the contract method 0xb564b34d.
//
// Solidity: function AllPurelyUnderscoredOutput() view returns(int256 _, int256 __)
func (_Underscorer *UnderscorerSession) AllPurelyUnderscoredOutput() (*big.Int, *big.Int, error) {
	return _Underscorer.Contract.AllPurelyUnderscoredOutput(&_Underscorer.CallOpts)
}

// AllPurelyUnderscoredOutput is a free data retrieval call binding the contract method 0xb564b34d.
//
// Solidity: function AllPurelyUnderscoredOutput() view returns(int256 _, int256 __)
func (_Underscorer *UnderscorerCallerSession) AllPurelyUnderscoredOutput() (*big.Int, *big.Int, error) {
	return _Underscorer.Contract.AllPurelyUnderscoredOutput(&_Underscorer.CallOpts)
}

// LowerLowerCollision is a free data retrieval call binding the contract method 0xe409ca45.
//
// Solidity: function LowerLowerCollision() view returns(int256 _res, int256 res)
func (_Underscorer *UnderscorerCaller) LowerLowerCollision(opts *bind.CallOpts) (*big.Int, *big.Int, error) {
	var out []interface{}
	err := _Underscorer.contract.Call(opts, &out, "LowerLowerCollision")

	if err != nil {
		return *new(*big.Int), *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)
	out1 := *abi.ConvertType(out[1], new(*big.Int)).(**big.Int)

	return out0, out1, err

}

// LowerLowerCollision is a free data retrieval call binding the contract method 0xe409ca45.
//
// Solidity: function LowerLowerCollision() view returns(int256 _res, int256 res)
func (_Underscorer *UnderscorerSession) LowerLowerCollision() (*big.Int, *big.Int, error) {
	return _Underscorer.Contract.LowerLowerCollision(&_Underscorer.CallOpts)
}

// LowerLowerCollision is a free data retrieval call binding the contract method 0xe409ca45.
//
// Solidity: function LowerLowerCollision() view returns(int256 _res, int256 res)
func (_Underscorer *UnderscorerCallerSession) LowerLowerCollision() (*big.Int, *big.Int, error) {
	return _Underscorer.Contract.LowerLowerCollision(&_Underscorer.CallOpts)
}

// LowerUpperCollision is a free data retrieval call binding the contract method 0x03a59213.
//
// Solidity: function LowerUpperCollision() view returns(int256 _res, int256 Res)
func (_Underscorer *UnderscorerCaller) LowerUpperCollision(opts *bind.CallOpts) (*big.Int, *big.Int, error) {
	var out []interface{}
	err := _Underscorer.contract.Call(opts, &out, "LowerUpperCollision")

	if err != nil {
		return *new(*big.Int), *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)
	out1 := *abi.ConvertType(out[1], new(*big.Int)).(**big.Int)

	return out0, out1, err

}

// LowerUpperCollision is a free data retrieval call binding the contract method 0x03a59213.
//
// Solidity: function LowerUpperCollision() view returns(int256 _res, int256 Res)
func (_Underscorer *UnderscorerSession) LowerUpperCollision() (*big.Int, *big.Int, error) {
	return _Underscorer.Contract.LowerUpperCollision(&_Underscorer.CallOpts)
}

// LowerUpperCollision is a free data retrieval call binding the contract method 0x03a59213.
//
// Solidity: function LowerUpperCollision() view returns(int256 _res, int256 Res)
func (_Underscorer *UnderscorerCallerSession) LowerUpperCollision() (*big.Int, *big.Int, error) {
	return _Underscorer.Contract.LowerUpperCollision(&_Underscorer.CallOpts)
}

// PurelyUnderscoredOutput is a free data retrieval call binding the contract method 0x9df48485.
//
// Solidity: function PurelyUnderscoredOutput() view returns(int256 _, int256 res)
func (_Underscorer *UnderscorerCaller) PurelyUnderscoredOutput(opts *bind.CallOpts) (*big.Int, *big.Int, error) {
	var out []interface{}
	err := _Underscorer.contract.Call(opts, &out, "PurelyUnderscoredOutput")

	if err != nil {
		return *new(*big.Int), *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)
	out1 := *abi.ConvertType(out[1], new(*big.Int)).(**big.Int)

	return out0, out1, err

}

// PurelyUnderscoredOutput is a free data retrieval call binding the contract method 0x9df48485.
//
// Solidity: function PurelyUnderscoredOutput() view returns(int256 _, int256 res)
func (_Underscorer *UnderscorerSession) PurelyUnderscoredOutput() (*big.Int, *big.Int, error) {
	return _Underscorer.Contract.PurelyUnderscoredOutput(&_Underscorer.CallOpts)
}

// PurelyUnderscoredOutput is a free data retrieval call binding the contract method 0x9df48485.
//
// Solidity: function PurelyUnderscoredOutput() view returns(int256 _, int256 res)
func (_Underscorer *UnderscorerCallerSession) PurelyUnderscoredOutput() (*big.Int, *big.Int, error) {
	return _Underscorer.Contract.PurelyUnderscoredOutput(&_Underscorer.CallOpts)
}

// UnderscoredOutput is a free data retrieval call binding the contract method 0x67e6633d.
//
// Solidity: function UnderscoredOutput() view returns(int256 _int, string _string)
func (_Underscorer *UnderscorerCaller) UnderscoredOutput(opts *bind.CallOpts) (struct {
	Int    *big.Int
	String string
}, error) {
	var out []interface{}
	err := _Underscorer.contract.Call(opts, &out, "UnderscoredOutput")

	outstruct := new(struct {
		Int    *big.Int
		String string
	})
	if err != nil {
		return *outstruct, err
	}

	outstruct.Int = *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)
	outstruct.String = *abi.ConvertType(out[1], new(string)).(*string)

	return *outstruct, err

}

// UnderscoredOutput is a free data retrieval call binding the contract method 0x67e6633d.
//
// Solidity: function UnderscoredOutput() view returns(int256 _int, string _string)
func (_Underscorer *UnderscorerSession) UnderscoredOutput() (struct {
	Int    *big.Int
	String string
}, error) {
	return _Underscorer.Contract.UnderscoredOutput(&_Underscorer.CallOpts)
}

// UnderscoredOutput is a free data retrieval call binding the contract method 0x67e6633d.
//
// Solidity: function UnderscoredOutput() view returns(int256 _int, string _string)
func (_Underscorer *UnderscorerCallerSession) UnderscoredOutput() (struct {
	Int    *big.Int
	String string
}, error) {
	return _Underscorer.Contract.UnderscoredOutput(&_Underscorer.CallOpts)
}

// UpperLowerCollision is a free data retrieval call binding the contract method 0xaf7486ab.
//
// Solidity: function UpperLowerCollision() view returns(int256 _Res, int256 res)
func (_Underscorer *UnderscorerCaller) UpperLowerCollision(opts *bind.CallOpts) (*big.Int, *big.Int, error) {
	var out []interface{}
	err := _Underscorer.contract.Call(opts, &out, "UpperLowerCollision")

	if err != nil {
		return *new(*big.Int), *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)
	out1 := *abi.ConvertType(out[1], new(*big.Int)).(**big.Int)

	return out0, out1, err

}

// UpperLowerCollision is a free data retrieval call binding the contract method 0xaf7486ab.
//
// Solidity: function UpperLowerCollision() view returns(int256 _Res, int256 res)
func (_Underscorer *UnderscorerSession) UpperLowerCollision() (*big.Int, *big.Int, error) {
	return _Underscorer.Contract.UpperLowerCollision(&_Underscorer.CallOpts)
}

// UpperLowerCollision is a free data retrieval call binding the contract method 0xaf7486ab.
//
// Solidity: function UpperLowerCollision() view returns(int256 _Res, int256 res)
func (_Underscorer *UnderscorerCallerSession) UpperLowerCollision() (*big.Int, *big.Int, error) {
	return _Underscorer.Contract.UpperLowerCollision(&_Underscorer.CallOpts)
}

// UpperUpperCollision is a free data retrieval call binding the contract method 0xe02ab24d.
//
// Solidity: function UpperUpperCollision() view returns(int256 _Res, int256 Res)
func (_Underscorer *UnderscorerCaller) UpperUpperCollision(opts *bind.CallOpts) (*big.Int, *big.Int, error) {
	var out []interface{}
	err := _Underscorer.contract.Call(opts, &out, "UpperUpperCollision")

	if err != nil {
		return *new(*big.Int), *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)
	out1 := *abi.ConvertType(out[1], new(*big.Int)).(**big.Int)

	return out0, out1, err

}

// UpperUpperCollision is a free data retrieval call binding the contract method 0xe02ab24d.
//
// Solidity: function UpperUpperCollision() view returns(int256 _Res, int256 Res)
func (_Underscorer *UnderscorerSession) UpperUpperCollision() (*big.Int, *big.Int, error) {
	return _Underscorer.Contract.UpperUpperCollision(&_Underscorer.CallOpts)
}

// UpperUpperCollision is a free data retrieval call binding the contract method 0xe02ab24d.
//
// Solidity: function UpperUpperCollision() view returns(int256 _Res, int256 Res)
func (_Underscorer *UnderscorerCallerSession) UpperUpperCollision() (*big.Int, *big.Int, error) {
	return _Underscorer.Contract.UpperUpperCollision(&_Underscorer.CallOpts)
}

// UnderScoredFunc is a free data retrieval call binding the contract method 0x46546dbe.
//
// Solidity: function _under_scored_func() view returns(int256 _int)
func (_Underscorer *UnderscorerCaller) UnderScoredFunc(opts *bind.CallOpts) (*big.Int, error) {
	var out []interface{}
	err := _Underscorer.contract.Call(opts, &out, "_under_scored_func")

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// UnderScoredFunc is a free data retrieval call binding the contract method 0x46546dbe.
//
// Solidity: function _under_scored_func() view returns(int256 _int)
func (_Underscorer *UnderscorerSession) UnderScoredFunc() (*big.Int, error) {
	return _Underscorer.Contract.UnderScoredFunc(&_Underscorer.CallOpts)
}

// UnderScoredFunc is a free data retrieval call binding the contract method 0x46546dbe.
//
// Solidity: function _under_scored_func() view returns(int256 _int)
func (_Underscorer *UnderscorerCallerSession) UnderScoredFunc() (*big.Int, error) {
	return _Underscorer.Contract.UnderScoredFunc(&_Underscorer.CallOpts)
}
