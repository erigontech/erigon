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

// Struct0 is an auto generated low-level Go binding around an user-defined struct.
type Struct0 struct {
	B [32]byte
}

// StructsABI is the input ABI used to generate the binding from.
const StructsABI = "[{\"inputs\":[],\"name\":\"F\",\"outputs\":[{\"components\":[{\"internalType\":\"bytes32\",\"name\":\"B\",\"type\":\"bytes32\"}],\"internalType\":\"structStructs.A[]\",\"name\":\"a\",\"type\":\"tuple[]\"},{\"internalType\":\"uint256[]\",\"name\":\"c\",\"type\":\"uint256[]\"},{\"internalType\":\"bool[]\",\"name\":\"d\",\"type\":\"bool[]\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"G\",\"outputs\":[{\"components\":[{\"internalType\":\"bytes32\",\"name\":\"B\",\"type\":\"bytes32\"}],\"internalType\":\"structStructs.A[]\",\"name\":\"a\",\"type\":\"tuple[]\"}],\"stateMutability\":\"view\",\"type\":\"function\"}]"

// StructsBin is the compiled bytecode used for deploying new contracts.
var StructsBin = "0x608060405234801561001057600080fd5b50610278806100206000396000f3fe608060405234801561001057600080fd5b50600436106100365760003560e01c806328811f591461003b5780636fecb6231461005b575b600080fd5b610043610070565b604051610052939291906101a0565b60405180910390f35b6100636100d6565b6040516100529190610186565b604080516002808252606082810190935282918291829190816020015b610095610131565b81526020019060019003908161008d575050805190915061026960611b9082906000906100be57fe5b60209081029190910101515293606093508392509050565b6040805160028082526060828101909352829190816020015b6100f7610131565b8152602001906001900390816100ef575050805190915061026960611b90829060009061012057fe5b602090810291909101015152905090565b60408051602081019091526000815290565b815260200190565b6000815180845260208085019450808401835b8381101561017b578151518752958201959082019060010161015e565b509495945050505050565b600060208252610199602083018461014b565b9392505050565b6000606082526101b3606083018661014b565b6020838203818501528186516101c98185610239565b91508288019350845b818110156101f3576101e5838651610143565b9484019492506001016101d2565b505084810360408601528551808252908201925081860190845b8181101561022b57825115158552938301939183019160010161020d565b509298975050505050505050565b9081526020019056fea2646970667358221220eb85327e285def14230424c52893aebecec1e387a50bb6b75fc4fdbed647f45f64736f6c63430006050033"

// DeployStructs deploys a new Ethereum contract, binding an instance of Structs to it.
func DeployStructs(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *Structs, error) {
	parsed, err := abi.JSON(strings.NewReader(StructsABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(StructsBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &Structs{StructsCaller: StructsCaller{contract: contract}, StructsTransactor: StructsTransactor{contract: contract}, StructsFilterer: StructsFilterer{contract: contract}}, nil
}

// Structs is an auto generated Go binding around an Ethereum contract.
type Structs struct {
	StructsCaller     // Read-only binding to the contract
	StructsTransactor // Write-only binding to the contract
	StructsFilterer   // Log filterer for contract events
}

// StructsCaller is an auto generated read-only Go binding around an Ethereum contract.
type StructsCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// StructsTransactor is an auto generated write-only Go binding around an Ethereum contract.
type StructsTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// StructsFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type StructsFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// StructsSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type StructsSession struct {
	Contract     *Structs          // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// StructsCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type StructsCallerSession struct {
	Contract *StructsCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts  // Call options to use throughout this session
}

// StructsTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type StructsTransactorSession struct {
	Contract     *StructsTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts  // Transaction auth options to use throughout this session
}

// StructsRaw is an auto generated low-level Go binding around an Ethereum contract.
type StructsRaw struct {
	Contract *Structs // Generic contract binding to access the raw methods on
}

// StructsCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type StructsCallerRaw struct {
	Contract *StructsCaller // Generic read-only contract binding to access the raw methods on
}

// StructsTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type StructsTransactorRaw struct {
	Contract *StructsTransactor // Generic write-only contract binding to access the raw methods on
}

// NewStructs creates a new instance of Structs, bound to a specific deployed contract.
func NewStructs(address common.Address, backend bind.ContractBackend) (*Structs, error) {
	contract, err := bindStructs(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Structs{StructsCaller: StructsCaller{contract: contract}, StructsTransactor: StructsTransactor{contract: contract}, StructsFilterer: StructsFilterer{contract: contract}}, nil
}

// NewStructsCaller creates a new read-only instance of Structs, bound to a specific deployed contract.
func NewStructsCaller(address common.Address, caller bind.ContractCaller) (*StructsCaller, error) {
	contract, err := bindStructs(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &StructsCaller{contract: contract}, nil
}

// NewStructsTransactor creates a new write-only instance of Structs, bound to a specific deployed contract.
func NewStructsTransactor(address common.Address, transactor bind.ContractTransactor) (*StructsTransactor, error) {
	contract, err := bindStructs(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &StructsTransactor{contract: contract}, nil
}

// NewStructsFilterer creates a new log filterer instance of Structs, bound to a specific deployed contract.
func NewStructsFilterer(address common.Address, filterer bind.ContractFilterer) (*StructsFilterer, error) {
	contract, err := bindStructs(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &StructsFilterer{contract: contract}, nil
}

// bindStructs binds a generic wrapper to an already deployed contract.
func bindStructs(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(StructsABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Structs *StructsRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Structs.Contract.StructsCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Structs *StructsRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Structs.Contract.StructsTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Structs *StructsRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Structs.Contract.StructsTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Structs *StructsCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Structs.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Structs *StructsTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Structs.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Structs *StructsTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Structs.Contract.contract.Transact(opts, method, params...)
}

// F is a free data retrieval call binding the contract method 0x28811f59.
//
// Solidity: function F() view returns((bytes32)[] a, uint256[] c, bool[] d)
func (_Structs *StructsCaller) F(opts *bind.CallOpts) (struct {
	A []Struct0
	C []*big.Int
	D []bool
}, error) {
	var out []interface{}
	err := _Structs.contract.Call(opts, &out, "F")

	outstruct := new(struct {
		A []Struct0
		C []*big.Int
		D []bool
	})
	if err != nil {
		return *outstruct, err
	}

	outstruct.A = *abi.ConvertType(out[0], new([]Struct0)).(*[]Struct0)
	outstruct.C = *abi.ConvertType(out[1], new([]*big.Int)).(*[]*big.Int)
	outstruct.D = *abi.ConvertType(out[2], new([]bool)).(*[]bool)

	return *outstruct, err

}

// F is a free data retrieval call binding the contract method 0x28811f59.
//
// Solidity: function F() view returns((bytes32)[] a, uint256[] c, bool[] d)
func (_Structs *StructsSession) F() (struct {
	A []Struct0
	C []*big.Int
	D []bool
}, error) {
	return _Structs.Contract.F(&_Structs.CallOpts)
}

// F is a free data retrieval call binding the contract method 0x28811f59.
//
// Solidity: function F() view returns((bytes32)[] a, uint256[] c, bool[] d)
func (_Structs *StructsCallerSession) F() (struct {
	A []Struct0
	C []*big.Int
	D []bool
}, error) {
	return _Structs.Contract.F(&_Structs.CallOpts)
}

// G is a free data retrieval call binding the contract method 0x6fecb623.
//
// Solidity: function G() view returns((bytes32)[] a)
func (_Structs *StructsCaller) G(opts *bind.CallOpts) ([]Struct0, error) {
	var out []interface{}
	err := _Structs.contract.Call(opts, &out, "G")

	if err != nil {
		return *new([]Struct0), err
	}

	out0 := *abi.ConvertType(out[0], new([]Struct0)).(*[]Struct0)

	return out0, err

}

// G is a free data retrieval call binding the contract method 0x6fecb623.
//
// Solidity: function G() view returns((bytes32)[] a)
func (_Structs *StructsSession) G() ([]Struct0, error) {
	return _Structs.Contract.G(&_Structs.CallOpts)
}

// G is a free data retrieval call binding the contract method 0x6fecb623.
//
// Solidity: function G() view returns((bytes32)[] a)
func (_Structs *StructsCallerSession) G() ([]Struct0, error) {
	return _Structs.Contract.G(&_Structs.CallOpts)
}
