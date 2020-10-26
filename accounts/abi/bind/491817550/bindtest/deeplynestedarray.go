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

// DeeplyNestedArrayABI is the input ABI used to generate the binding from.
const DeeplyNestedArrayABI = "[{\"constant\":false,\"inputs\":[{\"name\":\"arr\",\"type\":\"uint64[3][4][5]\"}],\"name\":\"storeDeepUintArray\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"retrieveDeepArray\",\"outputs\":[{\"name\":\"\",\"type\":\"uint64[3][4][5]\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[{\"name\":\"\",\"type\":\"uint256\"},{\"name\":\"\",\"type\":\"uint256\"},{\"name\":\"\",\"type\":\"uint256\"}],\"name\":\"deepUint64Array\",\"outputs\":[{\"name\":\"\",\"type\":\"uint64\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"}]"

// DeeplyNestedArrayBin is the compiled bytecode used for deploying new contracts.
var DeeplyNestedArrayBin = "0x6060604052341561000f57600080fd5b6106438061001e6000396000f300606060405260043610610057576000357c0100000000000000000000000000000000000000000000000000000000900463ffffffff168063344248551461005c5780638ed4573a1461011457806398ed1856146101ab575b600080fd5b341561006757600080fd5b610112600480806107800190600580602002604051908101604052809291906000905b828210156101055783826101800201600480602002604051908101604052809291906000905b828210156100f25783826060020160038060200260405190810160405280929190826003602002808284378201915050505050815260200190600101906100b0565b505050508152602001906001019061008a565b5050505091905050610208565b005b341561011f57600080fd5b61012761021d565b604051808260056000925b8184101561019b578284602002015160046000925b8184101561018d5782846020020151600360200280838360005b8381101561017c578082015181840152602081019050610161565b505050509050019260010192610147565b925050509260010192610132565b9250505091505060405180910390f35b34156101b657600080fd5b6101de6004808035906020019091908035906020019091908035906020019091905050610309565b604051808267ffffffffffffffff1667ffffffffffffffff16815260200191505060405180910390f35b80600090600561021992919061035f565b5050565b6102256103b0565b6000600580602002604051908101604052809291906000905b8282101561030057838260040201600480602002604051908101604052809291906000905b828210156102ed578382016003806020026040519081016040528092919082600380156102d9576020028201916000905b82829054906101000a900467ffffffffffffffff1667ffffffffffffffff16815260200190600801906020826007010492830192600103820291508084116102945790505b505050505081526020019060010190610263565b505050508152602001906001019061023e565b50505050905090565b60008360058110151561031857fe5b600402018260048110151561032957fe5b018160038110151561033757fe5b6004918282040191900660080292509250509054906101000a900467ffffffffffffffff1681565b826005600402810192821561039f579160200282015b8281111561039e5782518290600461038e9291906103df565b5091602001919060040190610375565b5b5090506103ac919061042d565b5090565b610780604051908101604052806005905b6103c9610459565b8152602001906001900390816103c15790505090565b826004810192821561041c579160200282015b8281111561041b5782518290600361040b929190610488565b50916020019190600101906103f2565b5b5090506104299190610536565b5090565b61045691905b8082111561045257600081816104499190610562565b50600401610433565b5090565b90565b610180604051908101604052806004905b6104726105a7565b81526020019060019003908161046a5790505090565b82600380016004900481019282156105255791602002820160005b838211156104ef57835183826101000a81548167ffffffffffffffff021916908367ffffffffffffffff16021790555092602001926008016020816007010492830192600103026104a3565b80156105235782816101000a81549067ffffffffffffffff02191690556008016020816007010492830192600103026104ef565b505b50905061053291906105d9565b5090565b61055f91905b8082111561055b57600081816105529190610610565b5060010161053c565b5090565b90565b50600081816105719190610610565b50600101600081816105839190610610565b50600101600081816105959190610610565b5060010160006105a59190610610565b565b6060604051908101604052806003905b600067ffffffffffffffff168152602001906001900390816105b75790505090565b61060d91905b8082111561060957600081816101000a81549067ffffffffffffffff0219169055506001016105df565b5090565b90565b50600090555600a165627a7a7230582087e5a43f6965ab6ef7a4ff056ab80ed78fd8c15cff57715a1bf34ec76a93661c0029"

// DeployDeeplyNestedArray deploys a new Ethereum contract, binding an instance of DeeplyNestedArray to it.
func DeployDeeplyNestedArray(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *DeeplyNestedArray, error) {
	parsed, err := abi.JSON(strings.NewReader(DeeplyNestedArrayABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(DeeplyNestedArrayBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &DeeplyNestedArray{DeeplyNestedArrayCaller: DeeplyNestedArrayCaller{contract: contract}, DeeplyNestedArrayTransactor: DeeplyNestedArrayTransactor{contract: contract}, DeeplyNestedArrayFilterer: DeeplyNestedArrayFilterer{contract: contract}}, nil
}

// DeeplyNestedArray is an auto generated Go binding around an Ethereum contract.
type DeeplyNestedArray struct {
	DeeplyNestedArrayCaller     // Read-only binding to the contract
	DeeplyNestedArrayTransactor // Write-only binding to the contract
	DeeplyNestedArrayFilterer   // Log filterer for contract events
}

// DeeplyNestedArrayCaller is an auto generated read-only Go binding around an Ethereum contract.
type DeeplyNestedArrayCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// DeeplyNestedArrayTransactor is an auto generated write-only Go binding around an Ethereum contract.
type DeeplyNestedArrayTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// DeeplyNestedArrayFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type DeeplyNestedArrayFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// DeeplyNestedArraySession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type DeeplyNestedArraySession struct {
	Contract     *DeeplyNestedArray // Generic contract binding to set the session for
	CallOpts     bind.CallOpts      // Call options to use throughout this session
	TransactOpts bind.TransactOpts  // Transaction auth options to use throughout this session
}

// DeeplyNestedArrayCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type DeeplyNestedArrayCallerSession struct {
	Contract *DeeplyNestedArrayCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts            // Call options to use throughout this session
}

// DeeplyNestedArrayTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type DeeplyNestedArrayTransactorSession struct {
	Contract     *DeeplyNestedArrayTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts            // Transaction auth options to use throughout this session
}

// DeeplyNestedArrayRaw is an auto generated low-level Go binding around an Ethereum contract.
type DeeplyNestedArrayRaw struct {
	Contract *DeeplyNestedArray // Generic contract binding to access the raw methods on
}

// DeeplyNestedArrayCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type DeeplyNestedArrayCallerRaw struct {
	Contract *DeeplyNestedArrayCaller // Generic read-only contract binding to access the raw methods on
}

// DeeplyNestedArrayTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type DeeplyNestedArrayTransactorRaw struct {
	Contract *DeeplyNestedArrayTransactor // Generic write-only contract binding to access the raw methods on
}

// NewDeeplyNestedArray creates a new instance of DeeplyNestedArray, bound to a specific deployed contract.
func NewDeeplyNestedArray(address common.Address, backend bind.ContractBackend) (*DeeplyNestedArray, error) {
	contract, err := bindDeeplyNestedArray(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &DeeplyNestedArray{DeeplyNestedArrayCaller: DeeplyNestedArrayCaller{contract: contract}, DeeplyNestedArrayTransactor: DeeplyNestedArrayTransactor{contract: contract}, DeeplyNestedArrayFilterer: DeeplyNestedArrayFilterer{contract: contract}}, nil
}

// NewDeeplyNestedArrayCaller creates a new read-only instance of DeeplyNestedArray, bound to a specific deployed contract.
func NewDeeplyNestedArrayCaller(address common.Address, caller bind.ContractCaller) (*DeeplyNestedArrayCaller, error) {
	contract, err := bindDeeplyNestedArray(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &DeeplyNestedArrayCaller{contract: contract}, nil
}

// NewDeeplyNestedArrayTransactor creates a new write-only instance of DeeplyNestedArray, bound to a specific deployed contract.
func NewDeeplyNestedArrayTransactor(address common.Address, transactor bind.ContractTransactor) (*DeeplyNestedArrayTransactor, error) {
	contract, err := bindDeeplyNestedArray(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &DeeplyNestedArrayTransactor{contract: contract}, nil
}

// NewDeeplyNestedArrayFilterer creates a new log filterer instance of DeeplyNestedArray, bound to a specific deployed contract.
func NewDeeplyNestedArrayFilterer(address common.Address, filterer bind.ContractFilterer) (*DeeplyNestedArrayFilterer, error) {
	contract, err := bindDeeplyNestedArray(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &DeeplyNestedArrayFilterer{contract: contract}, nil
}

// bindDeeplyNestedArray binds a generic wrapper to an already deployed contract.
func bindDeeplyNestedArray(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(DeeplyNestedArrayABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_DeeplyNestedArray *DeeplyNestedArrayRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _DeeplyNestedArray.Contract.DeeplyNestedArrayCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_DeeplyNestedArray *DeeplyNestedArrayRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _DeeplyNestedArray.Contract.DeeplyNestedArrayTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_DeeplyNestedArray *DeeplyNestedArrayRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _DeeplyNestedArray.Contract.DeeplyNestedArrayTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_DeeplyNestedArray *DeeplyNestedArrayCallerRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _DeeplyNestedArray.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_DeeplyNestedArray *DeeplyNestedArrayTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _DeeplyNestedArray.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_DeeplyNestedArray *DeeplyNestedArrayTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _DeeplyNestedArray.Contract.contract.Transact(opts, method, params...)
}

// DeepUint64Array is a free data retrieval call binding the contract method 0x98ed1856.
//
// Solidity: function deepUint64Array(uint256 , uint256 , uint256 ) view returns(uint64)
func (_DeeplyNestedArray *DeeplyNestedArrayCaller) DeepUint64Array(opts *bind.CallOpts, arg0 *big.Int, arg1 *big.Int, arg2 *big.Int) (uint64, error) {
	var (
		ret0 = new(uint64)
	)
	out := ret0
	err := _DeeplyNestedArray.contract.Call(opts, out, "deepUint64Array", arg0, arg1, arg2)
	return *ret0, err
}

// DeepUint64Array is a free data retrieval call binding the contract method 0x98ed1856.
//
// Solidity: function deepUint64Array(uint256 , uint256 , uint256 ) view returns(uint64)
func (_DeeplyNestedArray *DeeplyNestedArraySession) DeepUint64Array(arg0 *big.Int, arg1 *big.Int, arg2 *big.Int) (uint64, error) {
	return _DeeplyNestedArray.Contract.DeepUint64Array(&_DeeplyNestedArray.CallOpts, arg0, arg1, arg2)
}

// DeepUint64Array is a free data retrieval call binding the contract method 0x98ed1856.
//
// Solidity: function deepUint64Array(uint256 , uint256 , uint256 ) view returns(uint64)
func (_DeeplyNestedArray *DeeplyNestedArrayCallerSession) DeepUint64Array(arg0 *big.Int, arg1 *big.Int, arg2 *big.Int) (uint64, error) {
	return _DeeplyNestedArray.Contract.DeepUint64Array(&_DeeplyNestedArray.CallOpts, arg0, arg1, arg2)
}

// RetrieveDeepArray is a free data retrieval call binding the contract method 0x8ed4573a.
//
// Solidity: function retrieveDeepArray() view returns(uint64[3][4][5])
func (_DeeplyNestedArray *DeeplyNestedArrayCaller) RetrieveDeepArray(opts *bind.CallOpts) ([5][4][3]uint64, error) {
	var (
		ret0 = new([5][4][3]uint64)
	)
	out := ret0
	err := _DeeplyNestedArray.contract.Call(opts, out, "retrieveDeepArray")
	return *ret0, err
}

// RetrieveDeepArray is a free data retrieval call binding the contract method 0x8ed4573a.
//
// Solidity: function retrieveDeepArray() view returns(uint64[3][4][5])
func (_DeeplyNestedArray *DeeplyNestedArraySession) RetrieveDeepArray() ([5][4][3]uint64, error) {
	return _DeeplyNestedArray.Contract.RetrieveDeepArray(&_DeeplyNestedArray.CallOpts)
}

// RetrieveDeepArray is a free data retrieval call binding the contract method 0x8ed4573a.
//
// Solidity: function retrieveDeepArray() view returns(uint64[3][4][5])
func (_DeeplyNestedArray *DeeplyNestedArrayCallerSession) RetrieveDeepArray() ([5][4][3]uint64, error) {
	return _DeeplyNestedArray.Contract.RetrieveDeepArray(&_DeeplyNestedArray.CallOpts)
}

// StoreDeepUintArray is a paid mutator transaction binding the contract method 0x34424855.
//
// Solidity: function storeDeepUintArray(uint64[3][4][5] arr) returns()
func (_DeeplyNestedArray *DeeplyNestedArrayTransactor) StoreDeepUintArray(opts *bind.TransactOpts, arr [5][4][3]uint64) (*types.Transaction, error) {
	return _DeeplyNestedArray.contract.Transact(opts, "storeDeepUintArray", arr)
}

// StoreDeepUintArray is a paid mutator transaction binding the contract method 0x34424855.
//
// Solidity: function storeDeepUintArray(uint64[3][4][5] arr) returns()
func (_DeeplyNestedArray *DeeplyNestedArraySession) StoreDeepUintArray(arr [5][4][3]uint64) (*types.Transaction, error) {
	return _DeeplyNestedArray.Contract.StoreDeepUintArray(&_DeeplyNestedArray.TransactOpts, arr)
}

// StoreDeepUintArray is a paid mutator transaction binding the contract method 0x34424855.
//
// Solidity: function storeDeepUintArray(uint64[3][4][5] arr) returns()
func (_DeeplyNestedArray *DeeplyNestedArrayTransactorSession) StoreDeepUintArray(arr [5][4][3]uint64) (*types.Transaction, error) {
	return _DeeplyNestedArray.Contract.StoreDeepUintArray(&_DeeplyNestedArray.TransactOpts, arr)
}
