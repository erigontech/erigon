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

// TestRootChainABI is the input ABI used to generate the binding from.
const TestRootChainABI = "[{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"proposer\",\"type\":\"address\"},{\"indexed\":true,\"internalType\":\"uint256\",\"name\":\"headerBlockId\",\"type\":\"uint256\"},{\"indexed\":true,\"internalType\":\"uint256\",\"name\":\"reward\",\"type\":\"uint256\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"start\",\"type\":\"uint256\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"end\",\"type\":\"uint256\"},{\"indexed\":false,\"internalType\":\"bytes32\",\"name\":\"root\",\"type\":\"bytes32\"}],\"name\":\"NewHeaderBlock\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"proposer\",\"type\":\"address\"},{\"indexed\":true,\"internalType\":\"uint256\",\"name\":\"headerBlockId\",\"type\":\"uint256\"}],\"name\":\"ResetHeaderBlock\",\"type\":\"event\"},{\"inputs\":[],\"name\":\"CHAINID\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"VOTE_TYPE\",\"outputs\":[{\"internalType\":\"uint8\",\"name\":\"\",\"type\":\"uint8\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"_nextHeaderBlock\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"currentHeaderBlock\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"getLastChildBlock\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"name\":\"headerBlocks\",\"outputs\":[{\"internalType\":\"bytes32\",\"name\":\"root\",\"type\":\"bytes32\"},{\"internalType\":\"uint256\",\"name\":\"start\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"end\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"createdAt\",\"type\":\"uint256\"},{\"internalType\":\"address\",\"name\":\"proposer\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"heimdallId\",\"outputs\":[{\"internalType\":\"bytes32\",\"name\":\"\",\"type\":\"bytes32\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"networkId\",\"outputs\":[{\"internalType\":\"bytes\",\"name\":\"\",\"type\":\"bytes\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"string\",\"name\":\"_heimdallId\",\"type\":\"string\"}],\"name\":\"setHeimdallId\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"_value\",\"type\":\"uint256\"}],\"name\":\"setNextHeaderBlock\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"slash\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"bytes\",\"name\":\"data\",\"type\":\"bytes\"},{\"internalType\":\"uint256[3][]\",\"name\":\"\",\"type\":\"uint256[3][]\"}],\"name\":\"submitCheckpoint\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"bytes\",\"name\":\"\",\"type\":\"bytes\"},{\"internalType\":\"bytes\",\"name\":\"\",\"type\":\"bytes\"}],\"name\":\"submitHeaderBlock\",\"outputs\":[],\"stateMutability\":\"pure\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"numDeposits\",\"type\":\"uint256\"}],\"name\":\"updateDepositId\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"depositId\",\"type\":\"uint256\"}],\"stateMutability\":\"nonpayable\",\"type\":\"function\"}]"

// TestRootChainBin is the compiled bytecode used for deploying new contracts.
var TestRootChainBin = "0x6080604052612710600255600160035534801561001b57600080fd5b50610af88061002b6000396000f3fe608060405234801561001057600080fd5b50600436106100ea5760003560e01c8063b87e1b661161008c578063d5b844eb11610066578063d5b844eb1461020b578063ea0688b314610225578063ec7e485514610238578063fbc3dd361461024057600080fd5b8063b87e1b66146101e7578063cc79f97b146101ef578063cf24a0ea146101f857600080fd5b80635391f483116100c85780635391f483146101815780636a791f11146101a25780638d978d88146101b05780639025e64c146101b957600080fd5b80632da25de3146100ef57806341539d4a146100f15780634e43e4951461016e575b600080fd5b005b6101386100ff36600461072b565b6004602081905260009182526040909120805460018201546002830154600384015493909401549193909290916001600160a01b031685565b6040805195865260208601949094529284019190915260608301526001600160a01b0316608082015260a0015b60405180910390f35b6100ef61017c36600461078d565b610249565b61019461018f36600461072b565b61037b565b604051908152602001610165565b6100ef6100ea366004610827565b61019460025481565b6101da60405180604001604052806002815260200161053960f01b81525081565b60405161016591906108b7565b6101946104c5565b61019461053981565b6100ef61020636600461072b565b6104ea565b610213600281565b60405160ff9091168152602001610165565b6100ef610233366004610900565b6105c5565b6101946105f4565b61019460015481565b6000808080808061025c898b018b6109c9565b95509550955095509550955080610539146102b55760405162461bcd60e51b8152602060048201526014602482015273125b9d985b1a5908189bdc8818da185a5b881a5960621b60448201526064015b60405180910390fd5b6102c18686868661060b565b6103055760405162461bcd60e51b8152602060048201526015602482015274494e434f52524543545f4845414445525f4441544160581b60448201526064016102ac565b6002546040805187815260208101879052908101859052600091906001600160a01b038916907fba5de06d22af2685c6c7765f60067f7d2b08c2d29f53cdf14d67f6d1c9bfb5279060600160405180910390a4600254610367906127106106e4565b600255505060016003555050505050505050565b6005546040805162c9effd60e41b815290516000926001600160a01b031691630c9effd09160048083019260209291908290030181865afa1580156103c4573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906103e89190610a15565b6001600160a01b0316336001600160a01b0316146104525760405162461bcd60e51b815260206004820152602160248201527f554e415554484f52495a45445f4445504f5349545f4d414e414745525f4f4e4c6044820152605960f81b60648201526084016102ac565b6104666003546104606105f4565b906106e4565b60035490915061047690836106e4565b600381905561271010156104c05760405162461bcd60e51b8152602060048201526011602482015270544f4f5f4d414e595f4445504f5349545360781b60448201526064016102ac565b919050565b6000600460006104d36105f4565b815260200190815260200160002060020154905090565b6104f661271082610a32565b156105335760405162461bcd60e51b815260206004820152600d60248201526c496e76616c69642076616c756560981b60448201526064016102ac565b805b60025481101561058a5760008181526004602081905260408220828155600181018390556002810183905560038101929092550180546001600160a01b031916905561058361271082610a6a565b9050610535565b5060028190556001600355604051819033907fca1d8316287f938830e225956a7bb10fd5a1a1506dd2eb3a476751a48811720590600090a350565b806040516020016105d69190610a7d565b60408051601f19818403018152919052805160209091012060015550565b60025460009061060690612710610708565b905090565b60008061271061ffff16600254111561064b576004600061062a6105f4565b81526020019081526020016000206002015460016106489190610a6a565b90505b84811461065c5760009150506106dc565b6040805160a081018252848152602080820193845281830187815242606084019081526001600160a01b038b811660808601908152600280546000908152600496879052979097209551865596516001808701919091559251958501959095555160038401559351910180546001600160a01b0319169190921617905590505b949350505050565b60006106f08284610a6a565b90508281101561070257610702610a99565b92915050565b60008282111561071a5761071a610a99565b6107248284610aaf565b9392505050565b60006020828403121561073d57600080fd5b5035919050565b60008083601f84011261075657600080fd5b50813567ffffffffffffffff81111561076e57600080fd5b60208301915083602082850101111561078657600080fd5b9250929050565b600080600080604085870312156107a357600080fd5b843567ffffffffffffffff808211156107bb57600080fd5b6107c788838901610744565b909650945060208701359150808211156107e057600080fd5b818701915087601f8301126107f457600080fd5b81358181111561080357600080fd5b88602060608302850101111561081857600080fd5b95989497505060200194505050565b6000806000806040858703121561083d57600080fd5b843567ffffffffffffffff8082111561085557600080fd5b61086188838901610744565b9096509450602087013591508082111561087a57600080fd5b5061088787828801610744565b95989497509550505050565b60005b838110156108ae578181015183820152602001610896565b50506000910152565b60208152600082518060208401526108d6816040850160208701610893565b601f01601f19169190910160400192915050565b634e487b7160e01b600052604160045260246000fd5b60006020828403121561091257600080fd5b813567ffffffffffffffff8082111561092a57600080fd5b818401915084601f83011261093e57600080fd5b813581811115610950576109506108ea565b604051601f8201601f19908116603f01168101908382118183101715610978576109786108ea565b8160405282815287602084870101111561099157600080fd5b826020860160208301376000928101602001929092525095945050505050565b6001600160a01b03811681146109c657600080fd5b50565b60008060008060008060c087890312156109e257600080fd5b86356109ed816109b1565b9860208801359850604088013597606081013597506080810135965060a00135945092505050565b600060208284031215610a2757600080fd5b8151610724816109b1565b600082610a4f57634e487b7160e01b600052601260045260246000fd5b500690565b634e487b7160e01b600052601160045260246000fd5b8082018082111561070257610702610a54565b60008251610a8f818460208701610893565b9190910192915050565b634e487b7160e01b600052600160045260246000fd5b8181038181111561070257610702610a5456fea2646970667358221220e8aee67b63507e8745850c7b73e998c6ef6b5d41b72b45f8f1316e80e79a1ec964736f6c63430008140033"

// DeployTestRootChain deploys a new Ethereum contract, binding an instance of TestRootChain to it.
func DeployTestRootChain(auth *bind.TransactOpts, backend bind.ContractBackend) (libcommon.Address, types.Transaction, *TestRootChain, error) {
	parsed, err := abi.JSON(strings.NewReader(TestRootChainABI))
	if err != nil {
		return libcommon.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, libcommon.FromHex(TestRootChainBin), backend)
	if err != nil {
		return libcommon.Address{}, nil, nil, err
	}
	return address, tx, &TestRootChain{TestRootChainCaller: TestRootChainCaller{contract: contract}, TestRootChainTransactor: TestRootChainTransactor{contract: contract}, TestRootChainFilterer: TestRootChainFilterer{contract: contract}}, nil
}

// TestRootChain is an auto generated Go binding around an Ethereum contract.
type TestRootChain struct {
	TestRootChainCaller     // Read-only binding to the contract
	TestRootChainTransactor // Write-only binding to the contract
	TestRootChainFilterer   // Log filterer for contract events
}

// TestRootChainCaller is an auto generated read-only Go binding around an Ethereum contract.
type TestRootChainCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// TestRootChainTransactor is an auto generated write-only Go binding around an Ethereum contract.
type TestRootChainTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// TestRootChainFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type TestRootChainFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// TestRootChainSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type TestRootChainSession struct {
	Contract     *TestRootChain    // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// TestRootChainCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type TestRootChainCallerSession struct {
	Contract *TestRootChainCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts        // Call options to use throughout this session
}

// TestRootChainTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type TestRootChainTransactorSession struct {
	Contract     *TestRootChainTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts        // Transaction auth options to use throughout this session
}

// TestRootChainRaw is an auto generated low-level Go binding around an Ethereum contract.
type TestRootChainRaw struct {
	Contract *TestRootChain // Generic contract binding to access the raw methods on
}

// TestRootChainCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type TestRootChainCallerRaw struct {
	Contract *TestRootChainCaller // Generic read-only contract binding to access the raw methods on
}

// TestRootChainTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type TestRootChainTransactorRaw struct {
	Contract *TestRootChainTransactor // Generic write-only contract binding to access the raw methods on
}

// NewTestRootChain creates a new instance of TestRootChain, bound to a specific deployed contract.
func NewTestRootChain(address libcommon.Address, backend bind.ContractBackend) (*TestRootChain, error) {
	contract, err := bindTestRootChain(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &TestRootChain{TestRootChainCaller: TestRootChainCaller{contract: contract}, TestRootChainTransactor: TestRootChainTransactor{contract: contract}, TestRootChainFilterer: TestRootChainFilterer{contract: contract}}, nil
}

// NewTestRootChainCaller creates a new read-only instance of TestRootChain, bound to a specific deployed contract.
func NewTestRootChainCaller(address libcommon.Address, caller bind.ContractCaller) (*TestRootChainCaller, error) {
	contract, err := bindTestRootChain(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &TestRootChainCaller{contract: contract}, nil
}

// NewTestRootChainTransactor creates a new write-only instance of TestRootChain, bound to a specific deployed contract.
func NewTestRootChainTransactor(address libcommon.Address, transactor bind.ContractTransactor) (*TestRootChainTransactor, error) {
	contract, err := bindTestRootChain(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &TestRootChainTransactor{contract: contract}, nil
}

// NewTestRootChainFilterer creates a new log filterer instance of TestRootChain, bound to a specific deployed contract.
func NewTestRootChainFilterer(address libcommon.Address, filterer bind.ContractFilterer) (*TestRootChainFilterer, error) {
	contract, err := bindTestRootChain(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &TestRootChainFilterer{contract: contract}, nil
}

// bindTestRootChain binds a generic wrapper to an already deployed contract.
func bindTestRootChain(address libcommon.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(TestRootChainABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_TestRootChain *TestRootChainRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _TestRootChain.Contract.TestRootChainCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_TestRootChain *TestRootChainRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _TestRootChain.Contract.TestRootChainTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_TestRootChain *TestRootChainRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _TestRootChain.Contract.TestRootChainTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_TestRootChain *TestRootChainCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _TestRootChain.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_TestRootChain *TestRootChainTransactorRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _TestRootChain.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_TestRootChain *TestRootChainTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _TestRootChain.Contract.contract.Transact(opts, method, params...)
}

// CHAINID is a free data retrieval call binding the contract method 0xcc79f97b.
//
// Solidity: function CHAINID() view returns(uint256)
func (_TestRootChain *TestRootChainCaller) CHAINID(opts *bind.CallOpts) (*big.Int, error) {
	var out []interface{}
	err := _TestRootChain.contract.Call(opts, &out, "CHAINID")

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// CHAINID is a free data retrieval call binding the contract method 0xcc79f97b.
//
// Solidity: function CHAINID() view returns(uint256)
func (_TestRootChain *TestRootChainSession) CHAINID() (*big.Int, error) {
	return _TestRootChain.Contract.CHAINID(&_TestRootChain.CallOpts)
}

// CHAINID is a free data retrieval call binding the contract method 0xcc79f97b.
//
// Solidity: function CHAINID() view returns(uint256)
func (_TestRootChain *TestRootChainCallerSession) CHAINID() (*big.Int, error) {
	return _TestRootChain.Contract.CHAINID(&_TestRootChain.CallOpts)
}

// VOTETYPE is a free data retrieval call binding the contract method 0xd5b844eb.
//
// Solidity: function VOTE_TYPE() view returns(uint8)
func (_TestRootChain *TestRootChainCaller) VOTETYPE(opts *bind.CallOpts) (uint8, error) {
	var out []interface{}
	err := _TestRootChain.contract.Call(opts, &out, "VOTE_TYPE")

	if err != nil {
		return *new(uint8), err
	}

	out0 := *abi.ConvertType(out[0], new(uint8)).(*uint8)

	return out0, err

}

// VOTETYPE is a free data retrieval call binding the contract method 0xd5b844eb.
//
// Solidity: function VOTE_TYPE() view returns(uint8)
func (_TestRootChain *TestRootChainSession) VOTETYPE() (uint8, error) {
	return _TestRootChain.Contract.VOTETYPE(&_TestRootChain.CallOpts)
}

// VOTETYPE is a free data retrieval call binding the contract method 0xd5b844eb.
//
// Solidity: function VOTE_TYPE() view returns(uint8)
func (_TestRootChain *TestRootChainCallerSession) VOTETYPE() (uint8, error) {
	return _TestRootChain.Contract.VOTETYPE(&_TestRootChain.CallOpts)
}

// NextHeaderBlock is a free data retrieval call binding the contract method 0x8d978d88.
//
// Solidity: function _nextHeaderBlock() view returns(uint256)
func (_TestRootChain *TestRootChainCaller) NextHeaderBlock(opts *bind.CallOpts) (*big.Int, error) {
	var out []interface{}
	err := _TestRootChain.contract.Call(opts, &out, "_nextHeaderBlock")

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// NextHeaderBlock is a free data retrieval call binding the contract method 0x8d978d88.
//
// Solidity: function _nextHeaderBlock() view returns(uint256)
func (_TestRootChain *TestRootChainSession) NextHeaderBlock() (*big.Int, error) {
	return _TestRootChain.Contract.NextHeaderBlock(&_TestRootChain.CallOpts)
}

// NextHeaderBlock is a free data retrieval call binding the contract method 0x8d978d88.
//
// Solidity: function _nextHeaderBlock() view returns(uint256)
func (_TestRootChain *TestRootChainCallerSession) NextHeaderBlock() (*big.Int, error) {
	return _TestRootChain.Contract.NextHeaderBlock(&_TestRootChain.CallOpts)
}

// CurrentHeaderBlock is a free data retrieval call binding the contract method 0xec7e4855.
//
// Solidity: function currentHeaderBlock() view returns(uint256)
func (_TestRootChain *TestRootChainCaller) CurrentHeaderBlock(opts *bind.CallOpts) (*big.Int, error) {
	var out []interface{}
	err := _TestRootChain.contract.Call(opts, &out, "currentHeaderBlock")

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// CurrentHeaderBlock is a free data retrieval call binding the contract method 0xec7e4855.
//
// Solidity: function currentHeaderBlock() view returns(uint256)
func (_TestRootChain *TestRootChainSession) CurrentHeaderBlock() (*big.Int, error) {
	return _TestRootChain.Contract.CurrentHeaderBlock(&_TestRootChain.CallOpts)
}

// CurrentHeaderBlock is a free data retrieval call binding the contract method 0xec7e4855.
//
// Solidity: function currentHeaderBlock() view returns(uint256)
func (_TestRootChain *TestRootChainCallerSession) CurrentHeaderBlock() (*big.Int, error) {
	return _TestRootChain.Contract.CurrentHeaderBlock(&_TestRootChain.CallOpts)
}

// GetLastChildBlock is a free data retrieval call binding the contract method 0xb87e1b66.
//
// Solidity: function getLastChildBlock() view returns(uint256)
func (_TestRootChain *TestRootChainCaller) GetLastChildBlock(opts *bind.CallOpts) (*big.Int, error) {
	var out []interface{}
	err := _TestRootChain.contract.Call(opts, &out, "getLastChildBlock")

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// GetLastChildBlock is a free data retrieval call binding the contract method 0xb87e1b66.
//
// Solidity: function getLastChildBlock() view returns(uint256)
func (_TestRootChain *TestRootChainSession) GetLastChildBlock() (*big.Int, error) {
	return _TestRootChain.Contract.GetLastChildBlock(&_TestRootChain.CallOpts)
}

// GetLastChildBlock is a free data retrieval call binding the contract method 0xb87e1b66.
//
// Solidity: function getLastChildBlock() view returns(uint256)
func (_TestRootChain *TestRootChainCallerSession) GetLastChildBlock() (*big.Int, error) {
	return _TestRootChain.Contract.GetLastChildBlock(&_TestRootChain.CallOpts)
}

// HeaderBlocks is a free data retrieval call binding the contract method 0x41539d4a.
//
// Solidity: function headerBlocks(uint256 ) view returns(bytes32 root, uint256 start, uint256 end, uint256 createdAt, address proposer)
func (_TestRootChain *TestRootChainCaller) HeaderBlocks(opts *bind.CallOpts, arg0 *big.Int) (struct {
	Root      [32]byte
	Start     *big.Int
	End       *big.Int
	CreatedAt *big.Int
	Proposer  libcommon.Address
}, error) {
	var out []interface{}
	err := _TestRootChain.contract.Call(opts, &out, "headerBlocks", arg0)

	outstruct := new(struct {
		Root      [32]byte
		Start     *big.Int
		End       *big.Int
		CreatedAt *big.Int
		Proposer  libcommon.Address
	})
	if err != nil {
		return *outstruct, err
	}

	outstruct.Root = *abi.ConvertType(out[0], new([32]byte)).(*[32]byte)
	outstruct.Start = *abi.ConvertType(out[1], new(*big.Int)).(**big.Int)
	outstruct.End = *abi.ConvertType(out[2], new(*big.Int)).(**big.Int)
	outstruct.CreatedAt = *abi.ConvertType(out[3], new(*big.Int)).(**big.Int)
	outstruct.Proposer = *abi.ConvertType(out[4], new(libcommon.Address)).(*libcommon.Address)

	return *outstruct, err

}

// HeaderBlocks is a free data retrieval call binding the contract method 0x41539d4a.
//
// Solidity: function headerBlocks(uint256 ) view returns(bytes32 root, uint256 start, uint256 end, uint256 createdAt, address proposer)
func (_TestRootChain *TestRootChainSession) HeaderBlocks(arg0 *big.Int) (struct {
	Root      [32]byte
	Start     *big.Int
	End       *big.Int
	CreatedAt *big.Int
	Proposer  libcommon.Address
}, error) {
	return _TestRootChain.Contract.HeaderBlocks(&_TestRootChain.CallOpts, arg0)
}

// HeaderBlocks is a free data retrieval call binding the contract method 0x41539d4a.
//
// Solidity: function headerBlocks(uint256 ) view returns(bytes32 root, uint256 start, uint256 end, uint256 createdAt, address proposer)
func (_TestRootChain *TestRootChainCallerSession) HeaderBlocks(arg0 *big.Int) (struct {
	Root      [32]byte
	Start     *big.Int
	End       *big.Int
	CreatedAt *big.Int
	Proposer  libcommon.Address
}, error) {
	return _TestRootChain.Contract.HeaderBlocks(&_TestRootChain.CallOpts, arg0)
}

// HeimdallId is a free data retrieval call binding the contract method 0xfbc3dd36.
//
// Solidity: function heimdallId() view returns(bytes32)
func (_TestRootChain *TestRootChainCaller) HeimdallId(opts *bind.CallOpts) ([32]byte, error) {
	var out []interface{}
	err := _TestRootChain.contract.Call(opts, &out, "heimdallId")

	if err != nil {
		return *new([32]byte), err
	}

	out0 := *abi.ConvertType(out[0], new([32]byte)).(*[32]byte)

	return out0, err

}

// HeimdallId is a free data retrieval call binding the contract method 0xfbc3dd36.
//
// Solidity: function heimdallId() view returns(bytes32)
func (_TestRootChain *TestRootChainSession) HeimdallId() ([32]byte, error) {
	return _TestRootChain.Contract.HeimdallId(&_TestRootChain.CallOpts)
}

// HeimdallId is a free data retrieval call binding the contract method 0xfbc3dd36.
//
// Solidity: function heimdallId() view returns(bytes32)
func (_TestRootChain *TestRootChainCallerSession) HeimdallId() ([32]byte, error) {
	return _TestRootChain.Contract.HeimdallId(&_TestRootChain.CallOpts)
}

// NetworkId is a free data retrieval call binding the contract method 0x9025e64c.
//
// Solidity: function networkId() view returns(bytes)
func (_TestRootChain *TestRootChainCaller) NetworkId(opts *bind.CallOpts) ([]byte, error) {
	var out []interface{}
	err := _TestRootChain.contract.Call(opts, &out, "networkId")

	if err != nil {
		return *new([]byte), err
	}

	out0 := *abi.ConvertType(out[0], new([]byte)).(*[]byte)

	return out0, err

}

// NetworkId is a free data retrieval call binding the contract method 0x9025e64c.
//
// Solidity: function networkId() view returns(bytes)
func (_TestRootChain *TestRootChainSession) NetworkId() ([]byte, error) {
	return _TestRootChain.Contract.NetworkId(&_TestRootChain.CallOpts)
}

// NetworkId is a free data retrieval call binding the contract method 0x9025e64c.
//
// Solidity: function networkId() view returns(bytes)
func (_TestRootChain *TestRootChainCallerSession) NetworkId() ([]byte, error) {
	return _TestRootChain.Contract.NetworkId(&_TestRootChain.CallOpts)
}

// SubmitHeaderBlock is a free data retrieval call binding the contract method 0x6a791f11.
//
// Solidity: function submitHeaderBlock(bytes , bytes ) pure returns()
func (_TestRootChain *TestRootChainCaller) SubmitHeaderBlock(opts *bind.CallOpts, arg0 []byte, arg1 []byte) error {
	var out []interface{}
	err := _TestRootChain.contract.Call(opts, &out, "submitHeaderBlock", arg0, arg1)

	if err != nil {
		return err
	}

	return err

}

// SubmitHeaderBlock is a free data retrieval call binding the contract method 0x6a791f11.
//
// Solidity: function submitHeaderBlock(bytes , bytes ) pure returns()
func (_TestRootChain *TestRootChainSession) SubmitHeaderBlock(arg0 []byte, arg1 []byte) error {
	return _TestRootChain.Contract.SubmitHeaderBlock(&_TestRootChain.CallOpts, arg0, arg1)
}

// SubmitHeaderBlock is a free data retrieval call binding the contract method 0x6a791f11.
//
// Solidity: function submitHeaderBlock(bytes , bytes ) pure returns()
func (_TestRootChain *TestRootChainCallerSession) SubmitHeaderBlock(arg0 []byte, arg1 []byte) error {
	return _TestRootChain.Contract.SubmitHeaderBlock(&_TestRootChain.CallOpts, arg0, arg1)
}

// SetHeimdallId is a paid mutator transaction binding the contract method 0xea0688b3.
//
// Solidity: function setHeimdallId(string _heimdallId) returns()
func (_TestRootChain *TestRootChainTransactor) SetHeimdallId(opts *bind.TransactOpts, _heimdallId string) (types.Transaction, error) {
	return _TestRootChain.contract.Transact(opts, "setHeimdallId", _heimdallId)
}

// SetHeimdallId is a paid mutator transaction binding the contract method 0xea0688b3.
//
// Solidity: function setHeimdallId(string _heimdallId) returns()
func (_TestRootChain *TestRootChainSession) SetHeimdallId(_heimdallId string) (types.Transaction, error) {
	return _TestRootChain.Contract.SetHeimdallId(&_TestRootChain.TransactOpts, _heimdallId)
}

// SetHeimdallId is a paid mutator transaction binding the contract method 0xea0688b3.
//
// Solidity: function setHeimdallId(string _heimdallId) returns()
func (_TestRootChain *TestRootChainTransactorSession) SetHeimdallId(_heimdallId string) (types.Transaction, error) {
	return _TestRootChain.Contract.SetHeimdallId(&_TestRootChain.TransactOpts, _heimdallId)
}

// SetNextHeaderBlock is a paid mutator transaction binding the contract method 0xcf24a0ea.
//
// Solidity: function setNextHeaderBlock(uint256 _value) returns()
func (_TestRootChain *TestRootChainTransactor) SetNextHeaderBlock(opts *bind.TransactOpts, _value *big.Int) (types.Transaction, error) {
	return _TestRootChain.contract.Transact(opts, "setNextHeaderBlock", _value)
}

// SetNextHeaderBlock is a paid mutator transaction binding the contract method 0xcf24a0ea.
//
// Solidity: function setNextHeaderBlock(uint256 _value) returns()
func (_TestRootChain *TestRootChainSession) SetNextHeaderBlock(_value *big.Int) (types.Transaction, error) {
	return _TestRootChain.Contract.SetNextHeaderBlock(&_TestRootChain.TransactOpts, _value)
}

// SetNextHeaderBlock is a paid mutator transaction binding the contract method 0xcf24a0ea.
//
// Solidity: function setNextHeaderBlock(uint256 _value) returns()
func (_TestRootChain *TestRootChainTransactorSession) SetNextHeaderBlock(_value *big.Int) (types.Transaction, error) {
	return _TestRootChain.Contract.SetNextHeaderBlock(&_TestRootChain.TransactOpts, _value)
}

// Slash is a paid mutator transaction binding the contract method 0x2da25de3.
//
// Solidity: function slash() returns()
func (_TestRootChain *TestRootChainTransactor) Slash(opts *bind.TransactOpts) (types.Transaction, error) {
	return _TestRootChain.contract.Transact(opts, "slash")
}

// Slash is a paid mutator transaction binding the contract method 0x2da25de3.
//
// Solidity: function slash() returns()
func (_TestRootChain *TestRootChainSession) Slash() (types.Transaction, error) {
	return _TestRootChain.Contract.Slash(&_TestRootChain.TransactOpts)
}

// Slash is a paid mutator transaction binding the contract method 0x2da25de3.
//
// Solidity: function slash() returns()
func (_TestRootChain *TestRootChainTransactorSession) Slash() (types.Transaction, error) {
	return _TestRootChain.Contract.Slash(&_TestRootChain.TransactOpts)
}

// SubmitCheckpoint is a paid mutator transaction binding the contract method 0x4e43e495.
//
// Solidity: function submitCheckpoint(bytes data, uint256[3][] ) returns()
func (_TestRootChain *TestRootChainTransactor) SubmitCheckpoint(opts *bind.TransactOpts, data []byte, arg1 [][3]*big.Int) (types.Transaction, error) {
	return _TestRootChain.contract.Transact(opts, "submitCheckpoint", data, arg1)
}

// SubmitCheckpoint is a paid mutator transaction binding the contract method 0x4e43e495.
//
// Solidity: function submitCheckpoint(bytes data, uint256[3][] ) returns()
func (_TestRootChain *TestRootChainSession) SubmitCheckpoint(data []byte, arg1 [][3]*big.Int) (types.Transaction, error) {
	return _TestRootChain.Contract.SubmitCheckpoint(&_TestRootChain.TransactOpts, data, arg1)
}

// SubmitCheckpoint is a paid mutator transaction binding the contract method 0x4e43e495.
//
// Solidity: function submitCheckpoint(bytes data, uint256[3][] ) returns()
func (_TestRootChain *TestRootChainTransactorSession) SubmitCheckpoint(data []byte, arg1 [][3]*big.Int) (types.Transaction, error) {
	return _TestRootChain.Contract.SubmitCheckpoint(&_TestRootChain.TransactOpts, data, arg1)
}

// UpdateDepositId is a paid mutator transaction binding the contract method 0x5391f483.
//
// Solidity: function updateDepositId(uint256 numDeposits) returns(uint256 depositId)
func (_TestRootChain *TestRootChainTransactor) UpdateDepositId(opts *bind.TransactOpts, numDeposits *big.Int) (types.Transaction, error) {
	return _TestRootChain.contract.Transact(opts, "updateDepositId", numDeposits)
}

// UpdateDepositId is a paid mutator transaction binding the contract method 0x5391f483.
//
// Solidity: function updateDepositId(uint256 numDeposits) returns(uint256 depositId)
func (_TestRootChain *TestRootChainSession) UpdateDepositId(numDeposits *big.Int) (types.Transaction, error) {
	return _TestRootChain.Contract.UpdateDepositId(&_TestRootChain.TransactOpts, numDeposits)
}

// UpdateDepositId is a paid mutator transaction binding the contract method 0x5391f483.
//
// Solidity: function updateDepositId(uint256 numDeposits) returns(uint256 depositId)
func (_TestRootChain *TestRootChainTransactorSession) UpdateDepositId(numDeposits *big.Int) (types.Transaction, error) {
	return _TestRootChain.Contract.UpdateDepositId(&_TestRootChain.TransactOpts, numDeposits)
}

// SetHeimdallIdParams is an auto generated read-only Go binding of transcaction calldata params
type SetHeimdallIdParams struct {
	Param__heimdallId string
}

// Parse SetHeimdallId method from calldata of a transaction
//
// Solidity: function setHeimdallId(string _heimdallId) returns()
func ParseSetHeimdallId(calldata []byte) (*SetHeimdallIdParams, error) {
	if len(calldata) <= 4 {
		return nil, fmt.Errorf("invalid calldata input")
	}

	_abi, err := abi.JSON(strings.NewReader(TestRootChainABI))
	if err != nil {
		return nil, fmt.Errorf("failed to get abi of registry metadata: %w", err)
	}

	out, err := _abi.Methods["setHeimdallId"].Inputs.Unpack(calldata[4:])
	if err != nil {
		return nil, fmt.Errorf("failed to unpack setHeimdallId params data: %w", err)
	}

	var paramsResult = new(SetHeimdallIdParams)
	value := reflect.ValueOf(paramsResult).Elem()

	if value.NumField() != len(out) {
		return nil, fmt.Errorf("failed to match calldata with param field number")
	}

	out0 := *abi.ConvertType(out[0], new(string)).(*string)

	return &SetHeimdallIdParams{
		Param__heimdallId: out0,
	}, nil
}

// SetNextHeaderBlockParams is an auto generated read-only Go binding of transcaction calldata params
type SetNextHeaderBlockParams struct {
	Param__value *big.Int
}

// Parse SetNextHeaderBlock method from calldata of a transaction
//
// Solidity: function setNextHeaderBlock(uint256 _value) returns()
func ParseSetNextHeaderBlock(calldata []byte) (*SetNextHeaderBlockParams, error) {
	if len(calldata) <= 4 {
		return nil, fmt.Errorf("invalid calldata input")
	}

	_abi, err := abi.JSON(strings.NewReader(TestRootChainABI))
	if err != nil {
		return nil, fmt.Errorf("failed to get abi of registry metadata: %w", err)
	}

	out, err := _abi.Methods["setNextHeaderBlock"].Inputs.Unpack(calldata[4:])
	if err != nil {
		return nil, fmt.Errorf("failed to unpack setNextHeaderBlock params data: %w", err)
	}

	var paramsResult = new(SetNextHeaderBlockParams)
	value := reflect.ValueOf(paramsResult).Elem()

	if value.NumField() != len(out) {
		return nil, fmt.Errorf("failed to match calldata with param field number")
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return &SetNextHeaderBlockParams{
		Param__value: out0,
	}, nil
}

// SubmitCheckpointParams is an auto generated read-only Go binding of transcaction calldata params
type SubmitCheckpointParams struct {
	Param_data []byte
	Param_arg1 [][3]*big.Int
}

// Parse SubmitCheckpoint method from calldata of a transaction
//
// Solidity: function submitCheckpoint(bytes data, uint256[3][] ) returns()
func ParseSubmitCheckpoint(calldata []byte) (*SubmitCheckpointParams, error) {
	if len(calldata) <= 4 {
		return nil, fmt.Errorf("invalid calldata input")
	}

	_abi, err := abi.JSON(strings.NewReader(TestRootChainABI))
	if err != nil {
		return nil, fmt.Errorf("failed to get abi of registry metadata: %w", err)
	}

	out, err := _abi.Methods["submitCheckpoint"].Inputs.Unpack(calldata[4:])
	if err != nil {
		return nil, fmt.Errorf("failed to unpack submitCheckpoint params data: %w", err)
	}

	var paramsResult = new(SubmitCheckpointParams)
	value := reflect.ValueOf(paramsResult).Elem()

	if value.NumField() != len(out) {
		return nil, fmt.Errorf("failed to match calldata with param field number")
	}

	out0 := *abi.ConvertType(out[0], new([]byte)).(*[]byte)
	out1 := *abi.ConvertType(out[1], new([][3]*big.Int)).(*[][3]*big.Int)

	return &SubmitCheckpointParams{
		Param_data: out0, Param_arg1: out1,
	}, nil
}

// UpdateDepositIdParams is an auto generated read-only Go binding of transcaction calldata params
type UpdateDepositIdParams struct {
	Param_numDeposits *big.Int
}

// Parse UpdateDepositId method from calldata of a transaction
//
// Solidity: function updateDepositId(uint256 numDeposits) returns(uint256 depositId)
func ParseUpdateDepositId(calldata []byte) (*UpdateDepositIdParams, error) {
	if len(calldata) <= 4 {
		return nil, fmt.Errorf("invalid calldata input")
	}

	_abi, err := abi.JSON(strings.NewReader(TestRootChainABI))
	if err != nil {
		return nil, fmt.Errorf("failed to get abi of registry metadata: %w", err)
	}

	out, err := _abi.Methods["updateDepositId"].Inputs.Unpack(calldata[4:])
	if err != nil {
		return nil, fmt.Errorf("failed to unpack updateDepositId params data: %w", err)
	}

	var paramsResult = new(UpdateDepositIdParams)
	value := reflect.ValueOf(paramsResult).Elem()

	if value.NumField() != len(out) {
		return nil, fmt.Errorf("failed to match calldata with param field number")
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return &UpdateDepositIdParams{
		Param_numDeposits: out0,
	}, nil
}

// TestRootChainNewHeaderBlockIterator is returned from FilterNewHeaderBlock and is used to iterate over the raw logs and unpacked data for NewHeaderBlock events raised by the TestRootChain contract.
type TestRootChainNewHeaderBlockIterator struct {
	Event *TestRootChainNewHeaderBlock // Event containing the contract specifics and raw log

	contract *bind.BoundContract // Generic contract to use for unpacking event data
	event    string              // Event name to use for unpacking event data

	logs chan types.Log        // Log channel receiving the found contract events
	sub  ethereum.Subscription // Subscription for errors, completion and termination
	done bool                  // Whether the subscription completed delivering logs
	fail error                 // Occurred error to stop iteration
}

// Next advances the iterator to the subsequent event, returning whether there
// are any more events found. In case of a retrieval or parsing error, false is
// returned and Error() can be queried for the exact failure.
func (it *TestRootChainNewHeaderBlockIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(TestRootChainNewHeaderBlock)
			if err := it.contract.UnpackLog(it.Event, it.event, log); err != nil {
				it.fail = err
				return false
			}
			it.Event.Raw = log
			return true

		default:
			return false
		}
	}
	// Iterator still in progress, wait for either a data or an error event
	select {
	case log := <-it.logs:
		it.Event = new(TestRootChainNewHeaderBlock)
		if err := it.contract.UnpackLog(it.Event, it.event, log); err != nil {
			it.fail = err
			return false
		}
		it.Event.Raw = log
		return true

	case err := <-it.sub.Err():
		it.done = true
		it.fail = err
		return it.Next()
	}
}

// Error returns any retrieval or parsing error occurred during filtering.
func (it *TestRootChainNewHeaderBlockIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *TestRootChainNewHeaderBlockIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// TestRootChainNewHeaderBlock represents a NewHeaderBlock event raised by the TestRootChain contract.
type TestRootChainNewHeaderBlock struct {
	Proposer      libcommon.Address
	HeaderBlockId *big.Int
	Reward        *big.Int
	Start         *big.Int
	End           *big.Int
	Root          [32]byte
	Raw           types.Log // Blockchain specific contextual infos
}

// FilterNewHeaderBlock is a free log retrieval operation binding the contract event 0xba5de06d22af2685c6c7765f60067f7d2b08c2d29f53cdf14d67f6d1c9bfb527.
//
// Solidity: event NewHeaderBlock(address indexed proposer, uint256 indexed headerBlockId, uint256 indexed reward, uint256 start, uint256 end, bytes32 root)
func (_TestRootChain *TestRootChainFilterer) FilterNewHeaderBlock(opts *bind.FilterOpts, proposer []libcommon.Address, headerBlockId []*big.Int, reward []*big.Int) (*TestRootChainNewHeaderBlockIterator, error) {

	var proposerRule []interface{}
	for _, proposerItem := range proposer {
		proposerRule = append(proposerRule, proposerItem)
	}
	var headerBlockIdRule []interface{}
	for _, headerBlockIdItem := range headerBlockId {
		headerBlockIdRule = append(headerBlockIdRule, headerBlockIdItem)
	}
	var rewardRule []interface{}
	for _, rewardItem := range reward {
		rewardRule = append(rewardRule, rewardItem)
	}

	logs, sub, err := _TestRootChain.contract.FilterLogs(opts, "NewHeaderBlock", proposerRule, headerBlockIdRule, rewardRule)
	if err != nil {
		return nil, err
	}
	return &TestRootChainNewHeaderBlockIterator{contract: _TestRootChain.contract, event: "NewHeaderBlock", logs: logs, sub: sub}, nil
}

// WatchNewHeaderBlock is a free log subscription operation binding the contract event 0xba5de06d22af2685c6c7765f60067f7d2b08c2d29f53cdf14d67f6d1c9bfb527.
//
// Solidity: event NewHeaderBlock(address indexed proposer, uint256 indexed headerBlockId, uint256 indexed reward, uint256 start, uint256 end, bytes32 root)
func (_TestRootChain *TestRootChainFilterer) WatchNewHeaderBlock(opts *bind.WatchOpts, sink chan<- *TestRootChainNewHeaderBlock, proposer []libcommon.Address, headerBlockId []*big.Int, reward []*big.Int) (event.Subscription, error) {

	var proposerRule []interface{}
	for _, proposerItem := range proposer {
		proposerRule = append(proposerRule, proposerItem)
	}
	var headerBlockIdRule []interface{}
	for _, headerBlockIdItem := range headerBlockId {
		headerBlockIdRule = append(headerBlockIdRule, headerBlockIdItem)
	}
	var rewardRule []interface{}
	for _, rewardItem := range reward {
		rewardRule = append(rewardRule, rewardItem)
	}

	logs, sub, err := _TestRootChain.contract.WatchLogs(opts, "NewHeaderBlock", proposerRule, headerBlockIdRule, rewardRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(TestRootChainNewHeaderBlock)
				if err := _TestRootChain.contract.UnpackLog(event, "NewHeaderBlock", log); err != nil {
					return err
				}
				event.Raw = log

				select {
				case sink <- event:
				case err := <-sub.Err():
					return err
				case <-quit:
					return nil
				}
			case err := <-sub.Err():
				return err
			case <-quit:
				return nil
			}
		}
	}), nil
}

// ParseNewHeaderBlock is a log parse operation binding the contract event 0xba5de06d22af2685c6c7765f60067f7d2b08c2d29f53cdf14d67f6d1c9bfb527.
//
// Solidity: event NewHeaderBlock(address indexed proposer, uint256 indexed headerBlockId, uint256 indexed reward, uint256 start, uint256 end, bytes32 root)
func (_TestRootChain *TestRootChainFilterer) ParseNewHeaderBlock(log types.Log) (*TestRootChainNewHeaderBlock, error) {
	event := new(TestRootChainNewHeaderBlock)
	if err := _TestRootChain.contract.UnpackLog(event, "NewHeaderBlock", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}

// TestRootChainResetHeaderBlockIterator is returned from FilterResetHeaderBlock and is used to iterate over the raw logs and unpacked data for ResetHeaderBlock events raised by the TestRootChain contract.
type TestRootChainResetHeaderBlockIterator struct {
	Event *TestRootChainResetHeaderBlock // Event containing the contract specifics and raw log

	contract *bind.BoundContract // Generic contract to use for unpacking event data
	event    string              // Event name to use for unpacking event data

	logs chan types.Log        // Log channel receiving the found contract events
	sub  ethereum.Subscription // Subscription for errors, completion and termination
	done bool                  // Whether the subscription completed delivering logs
	fail error                 // Occurred error to stop iteration
}

// Next advances the iterator to the subsequent event, returning whether there
// are any more events found. In case of a retrieval or parsing error, false is
// returned and Error() can be queried for the exact failure.
func (it *TestRootChainResetHeaderBlockIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(TestRootChainResetHeaderBlock)
			if err := it.contract.UnpackLog(it.Event, it.event, log); err != nil {
				it.fail = err
				return false
			}
			it.Event.Raw = log
			return true

		default:
			return false
		}
	}
	// Iterator still in progress, wait for either a data or an error event
	select {
	case log := <-it.logs:
		it.Event = new(TestRootChainResetHeaderBlock)
		if err := it.contract.UnpackLog(it.Event, it.event, log); err != nil {
			it.fail = err
			return false
		}
		it.Event.Raw = log
		return true

	case err := <-it.sub.Err():
		it.done = true
		it.fail = err
		return it.Next()
	}
}

// Error returns any retrieval or parsing error occurred during filtering.
func (it *TestRootChainResetHeaderBlockIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *TestRootChainResetHeaderBlockIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// TestRootChainResetHeaderBlock represents a ResetHeaderBlock event raised by the TestRootChain contract.
type TestRootChainResetHeaderBlock struct {
	Proposer      libcommon.Address
	HeaderBlockId *big.Int
	Raw           types.Log // Blockchain specific contextual infos
}

// FilterResetHeaderBlock is a free log retrieval operation binding the contract event 0xca1d8316287f938830e225956a7bb10fd5a1a1506dd2eb3a476751a488117205.
//
// Solidity: event ResetHeaderBlock(address indexed proposer, uint256 indexed headerBlockId)
func (_TestRootChain *TestRootChainFilterer) FilterResetHeaderBlock(opts *bind.FilterOpts, proposer []libcommon.Address, headerBlockId []*big.Int) (*TestRootChainResetHeaderBlockIterator, error) {

	var proposerRule []interface{}
	for _, proposerItem := range proposer {
		proposerRule = append(proposerRule, proposerItem)
	}
	var headerBlockIdRule []interface{}
	for _, headerBlockIdItem := range headerBlockId {
		headerBlockIdRule = append(headerBlockIdRule, headerBlockIdItem)
	}

	logs, sub, err := _TestRootChain.contract.FilterLogs(opts, "ResetHeaderBlock", proposerRule, headerBlockIdRule)
	if err != nil {
		return nil, err
	}
	return &TestRootChainResetHeaderBlockIterator{contract: _TestRootChain.contract, event: "ResetHeaderBlock", logs: logs, sub: sub}, nil
}

// WatchResetHeaderBlock is a free log subscription operation binding the contract event 0xca1d8316287f938830e225956a7bb10fd5a1a1506dd2eb3a476751a488117205.
//
// Solidity: event ResetHeaderBlock(address indexed proposer, uint256 indexed headerBlockId)
func (_TestRootChain *TestRootChainFilterer) WatchResetHeaderBlock(opts *bind.WatchOpts, sink chan<- *TestRootChainResetHeaderBlock, proposer []libcommon.Address, headerBlockId []*big.Int) (event.Subscription, error) {

	var proposerRule []interface{}
	for _, proposerItem := range proposer {
		proposerRule = append(proposerRule, proposerItem)
	}
	var headerBlockIdRule []interface{}
	for _, headerBlockIdItem := range headerBlockId {
		headerBlockIdRule = append(headerBlockIdRule, headerBlockIdItem)
	}

	logs, sub, err := _TestRootChain.contract.WatchLogs(opts, "ResetHeaderBlock", proposerRule, headerBlockIdRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(TestRootChainResetHeaderBlock)
				if err := _TestRootChain.contract.UnpackLog(event, "ResetHeaderBlock", log); err != nil {
					return err
				}
				event.Raw = log

				select {
				case sink <- event:
				case err := <-sub.Err():
					return err
				case <-quit:
					return nil
				}
			case err := <-sub.Err():
				return err
			case <-quit:
				return nil
			}
		}
	}), nil
}

// ParseResetHeaderBlock is a log parse operation binding the contract event 0xca1d8316287f938830e225956a7bb10fd5a1a1506dd2eb3a476751a488117205.
//
// Solidity: event ResetHeaderBlock(address indexed proposer, uint256 indexed headerBlockId)
func (_TestRootChain *TestRootChainFilterer) ParseResetHeaderBlock(log types.Log) (*TestRootChainResetHeaderBlock, error) {
	event := new(TestRootChainResetHeaderBlock)
	if err := _TestRootChain.contract.UnpackLog(event, "ResetHeaderBlock", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}
