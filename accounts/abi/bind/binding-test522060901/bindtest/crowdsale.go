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

// CrowdsaleABI is the input ABI used to generate the binding from.
const CrowdsaleABI = "[{\"constant\":false,\"inputs\":[],\"name\":\"checkGoalReached\",\"outputs\":[],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"deadline\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"beneficiary\",\"outputs\":[{\"name\":\"\",\"type\":\"address\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"tokenReward\",\"outputs\":[{\"name\":\"\",\"type\":\"address\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"fundingGoal\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"amountRaised\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"price\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"name\":\"funders\",\"outputs\":[{\"name\":\"addr\",\"type\":\"address\"},{\"name\":\"amount\",\"type\":\"uint256\"}],\"type\":\"function\"},{\"inputs\":[{\"name\":\"ifSuccessfulSendTo\",\"type\":\"address\"},{\"name\":\"fundingGoalInEthers\",\"type\":\"uint256\"},{\"name\":\"durationInMinutes\",\"type\":\"uint256\"},{\"name\":\"etherCostOfEachToken\",\"type\":\"uint256\"},{\"name\":\"addressOfTokenUsedAsReward\",\"type\":\"address\"}],\"type\":\"constructor\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"name\":\"backer\",\"type\":\"address\"},{\"indexed\":false,\"name\":\"amount\",\"type\":\"uint256\"},{\"indexed\":false,\"name\":\"isContribution\",\"type\":\"bool\"}],\"name\":\"FundTransfer\",\"type\":\"event\"}]"

// CrowdsaleBin is the compiled bytecode used for deploying new contracts.
var CrowdsaleBin = "0x606060408190526007805460ff1916905560a0806105a883396101006040529051608051915160c05160e05160008054600160a060020a03199081169095178155670de0b6b3a7640000958602600155603c9093024201600355930260045560058054909216909217905561052f90819061007990396000f36060604052361561006c5760e060020a600035046301cb3b20811461008257806329dcb0cf1461014457806338af3eed1461014d5780636e66f6e91461015f5780637a3a0e84146101715780637b3e5e7b1461017a578063a035b1fe14610183578063dc0d3dff1461018c575b61020060075460009060ff161561032357610002565b61020060035460009042106103205760025460015490106103cb576002548154600160a060020a0316908290606082818181858883f150915460025460408051600160a060020a039390931683526020830191909152818101869052517fe842aea7a5f1b01049d752008c53c52890b1a6daf660cf39e8eec506112bbdf6945090819003909201919050a15b60405160008054600160a060020a039081169230909116319082818181858883f150506007805460ff1916600117905550505050565b6103a160035481565b6103ab600054600160a060020a031681565b6103ab600554600160a060020a031681565b6103a160015481565b6103a160025481565b6103a160045481565b6103be60043560068054829081101561000257506000526002027ff652222313e28459528d920b65115c16c04f3efc82aaedc97be59f3f377c0d3f8101547ff652222313e28459528d920b65115c16c04f3efc82aaedc97be59f3f377c0d409190910154600160a060020a03919091169082565b005b505050815481101561000257906000526020600020906002020160005060008201518160000160006101000a815481600160a060020a030219169083021790555060208201518160010160005055905050806002600082828250540192505081905550600560009054906101000a9004600160a060020a0316600160a060020a031663a9059cbb3360046000505484046040518360e060020a0281526004018083600160a060020a03168152602001828152602001925050506000604051808303816000876161da5a03f11561000257505060408051600160a060020a03331681526020810184905260018183015290517fe842aea7a5f1b01049d752008c53c52890b1a6daf660cf39e8eec506112bbdf692509081900360600190a15b50565b5060a0604052336060908152346080819052600680546001810180835592939282908280158290116102025760020281600202836000526020600020918201910161020291905b8082111561039d57805473ffffffffffffffffffffffffffffffffffffffff19168155600060019190910190815561036a565b5090565b6060908152602090f35b600160a060020a03166060908152602090f35b6060918252608052604090f35b5b60065481101561010e576006805482908110156100025760009182526002027ff652222313e28459528d920b65115c16c04f3efc82aaedc97be59f3f377c0d3f0190600680549254600160a060020a0316928490811015610002576002027ff652222313e28459528d920b65115c16c04f3efc82aaedc97be59f3f377c0d40015460405190915082818181858883f19350505050507fe842aea7a5f1b01049d752008c53c52890b1a6daf660cf39e8eec506112bbdf660066000508281548110156100025760008290526002027ff652222313e28459528d920b65115c16c04f3efc82aaedc97be59f3f377c0d3f01548154600160a060020a039190911691908490811015610002576002027ff652222313e28459528d920b65115c16c04f3efc82aaedc97be59f3f377c0d40015460408051600160a060020a0394909416845260208401919091526000838201525191829003606001919050a16001016103cc56"

// DeployCrowdsale deploys a new Ethereum contract, binding an instance of Crowdsale to it.
func DeployCrowdsale(auth *bind.TransactOpts, backend bind.ContractBackend, ifSuccessfulSendTo common.Address, fundingGoalInEthers *big.Int, durationInMinutes *big.Int, etherCostOfEachToken *big.Int, addressOfTokenUsedAsReward common.Address) (common.Address, *types.Transaction, *Crowdsale, error) {
	parsed, err := abi.JSON(strings.NewReader(CrowdsaleABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(CrowdsaleBin), backend, ifSuccessfulSendTo, fundingGoalInEthers, durationInMinutes, etherCostOfEachToken, addressOfTokenUsedAsReward)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &Crowdsale{CrowdsaleCaller: CrowdsaleCaller{contract: contract}, CrowdsaleTransactor: CrowdsaleTransactor{contract: contract}, CrowdsaleFilterer: CrowdsaleFilterer{contract: contract}}, nil
}

// Crowdsale is an auto generated Go binding around an Ethereum contract.
type Crowdsale struct {
	CrowdsaleCaller     // Read-only binding to the contract
	CrowdsaleTransactor // Write-only binding to the contract
	CrowdsaleFilterer   // Log filterer for contract events
}

// CrowdsaleCaller is an auto generated read-only Go binding around an Ethereum contract.
type CrowdsaleCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// CrowdsaleTransactor is an auto generated write-only Go binding around an Ethereum contract.
type CrowdsaleTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// CrowdsaleFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type CrowdsaleFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// CrowdsaleSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type CrowdsaleSession struct {
	Contract     *Crowdsale        // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// CrowdsaleCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type CrowdsaleCallerSession struct {
	Contract *CrowdsaleCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts    // Call options to use throughout this session
}

// CrowdsaleTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type CrowdsaleTransactorSession struct {
	Contract     *CrowdsaleTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts    // Transaction auth options to use throughout this session
}

// CrowdsaleRaw is an auto generated low-level Go binding around an Ethereum contract.
type CrowdsaleRaw struct {
	Contract *Crowdsale // Generic contract binding to access the raw methods on
}

// CrowdsaleCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type CrowdsaleCallerRaw struct {
	Contract *CrowdsaleCaller // Generic read-only contract binding to access the raw methods on
}

// CrowdsaleTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type CrowdsaleTransactorRaw struct {
	Contract *CrowdsaleTransactor // Generic write-only contract binding to access the raw methods on
}

// NewCrowdsale creates a new instance of Crowdsale, bound to a specific deployed contract.
func NewCrowdsale(address common.Address, backend bind.ContractBackend) (*Crowdsale, error) {
	contract, err := bindCrowdsale(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Crowdsale{CrowdsaleCaller: CrowdsaleCaller{contract: contract}, CrowdsaleTransactor: CrowdsaleTransactor{contract: contract}, CrowdsaleFilterer: CrowdsaleFilterer{contract: contract}}, nil
}

// NewCrowdsaleCaller creates a new read-only instance of Crowdsale, bound to a specific deployed contract.
func NewCrowdsaleCaller(address common.Address, caller bind.ContractCaller) (*CrowdsaleCaller, error) {
	contract, err := bindCrowdsale(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &CrowdsaleCaller{contract: contract}, nil
}

// NewCrowdsaleTransactor creates a new write-only instance of Crowdsale, bound to a specific deployed contract.
func NewCrowdsaleTransactor(address common.Address, transactor bind.ContractTransactor) (*CrowdsaleTransactor, error) {
	contract, err := bindCrowdsale(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &CrowdsaleTransactor{contract: contract}, nil
}

// NewCrowdsaleFilterer creates a new log filterer instance of Crowdsale, bound to a specific deployed contract.
func NewCrowdsaleFilterer(address common.Address, filterer bind.ContractFilterer) (*CrowdsaleFilterer, error) {
	contract, err := bindCrowdsale(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &CrowdsaleFilterer{contract: contract}, nil
}

// bindCrowdsale binds a generic wrapper to an already deployed contract.
func bindCrowdsale(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(CrowdsaleABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Crowdsale *CrowdsaleRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Crowdsale.Contract.CrowdsaleCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Crowdsale *CrowdsaleRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Crowdsale.Contract.CrowdsaleTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Crowdsale *CrowdsaleRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Crowdsale.Contract.CrowdsaleTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Crowdsale *CrowdsaleCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Crowdsale.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Crowdsale *CrowdsaleTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Crowdsale.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Crowdsale *CrowdsaleTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Crowdsale.Contract.contract.Transact(opts, method, params...)
}

// AmountRaised is a free data retrieval call binding the contract method 0x7b3e5e7b.
//
// Solidity: function amountRaised() returns(uint256)
func (_Crowdsale *CrowdsaleCaller) AmountRaised(opts *bind.CallOpts) (*big.Int, error) {
	var out []interface{}
	err := _Crowdsale.contract.Call(opts, &out, "amountRaised")

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// AmountRaised is a free data retrieval call binding the contract method 0x7b3e5e7b.
//
// Solidity: function amountRaised() returns(uint256)
func (_Crowdsale *CrowdsaleSession) AmountRaised() (*big.Int, error) {
	return _Crowdsale.Contract.AmountRaised(&_Crowdsale.CallOpts)
}

// AmountRaised is a free data retrieval call binding the contract method 0x7b3e5e7b.
//
// Solidity: function amountRaised() returns(uint256)
func (_Crowdsale *CrowdsaleCallerSession) AmountRaised() (*big.Int, error) {
	return _Crowdsale.Contract.AmountRaised(&_Crowdsale.CallOpts)
}

// Beneficiary is a free data retrieval call binding the contract method 0x38af3eed.
//
// Solidity: function beneficiary() returns(address)
func (_Crowdsale *CrowdsaleCaller) Beneficiary(opts *bind.CallOpts) (common.Address, error) {
	var out []interface{}
	err := _Crowdsale.contract.Call(opts, &out, "beneficiary")

	if err != nil {
		return *new(common.Address), err
	}

	out0 := *abi.ConvertType(out[0], new(common.Address)).(*common.Address)

	return out0, err

}

// Beneficiary is a free data retrieval call binding the contract method 0x38af3eed.
//
// Solidity: function beneficiary() returns(address)
func (_Crowdsale *CrowdsaleSession) Beneficiary() (common.Address, error) {
	return _Crowdsale.Contract.Beneficiary(&_Crowdsale.CallOpts)
}

// Beneficiary is a free data retrieval call binding the contract method 0x38af3eed.
//
// Solidity: function beneficiary() returns(address)
func (_Crowdsale *CrowdsaleCallerSession) Beneficiary() (common.Address, error) {
	return _Crowdsale.Contract.Beneficiary(&_Crowdsale.CallOpts)
}

// Deadline is a free data retrieval call binding the contract method 0x29dcb0cf.
//
// Solidity: function deadline() returns(uint256)
func (_Crowdsale *CrowdsaleCaller) Deadline(opts *bind.CallOpts) (*big.Int, error) {
	var out []interface{}
	err := _Crowdsale.contract.Call(opts, &out, "deadline")

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// Deadline is a free data retrieval call binding the contract method 0x29dcb0cf.
//
// Solidity: function deadline() returns(uint256)
func (_Crowdsale *CrowdsaleSession) Deadline() (*big.Int, error) {
	return _Crowdsale.Contract.Deadline(&_Crowdsale.CallOpts)
}

// Deadline is a free data retrieval call binding the contract method 0x29dcb0cf.
//
// Solidity: function deadline() returns(uint256)
func (_Crowdsale *CrowdsaleCallerSession) Deadline() (*big.Int, error) {
	return _Crowdsale.Contract.Deadline(&_Crowdsale.CallOpts)
}

// Funders is a free data retrieval call binding the contract method 0xdc0d3dff.
//
// Solidity: function funders(uint256 ) returns(address addr, uint256 amount)
func (_Crowdsale *CrowdsaleCaller) Funders(opts *bind.CallOpts, arg0 *big.Int) (struct {
	Addr   common.Address
	Amount *big.Int
}, error) {
	var out []interface{}
	err := _Crowdsale.contract.Call(opts, &out, "funders", arg0)

	outstruct := new(struct {
		Addr   common.Address
		Amount *big.Int
	})
	if err != nil {
		return *outstruct, err
	}

	outstruct.Addr = *abi.ConvertType(out[0], new(common.Address)).(*common.Address)
	outstruct.Amount = *abi.ConvertType(out[1], new(*big.Int)).(**big.Int)

	return *outstruct, err

}

// Funders is a free data retrieval call binding the contract method 0xdc0d3dff.
//
// Solidity: function funders(uint256 ) returns(address addr, uint256 amount)
func (_Crowdsale *CrowdsaleSession) Funders(arg0 *big.Int) (struct {
	Addr   common.Address
	Amount *big.Int
}, error) {
	return _Crowdsale.Contract.Funders(&_Crowdsale.CallOpts, arg0)
}

// Funders is a free data retrieval call binding the contract method 0xdc0d3dff.
//
// Solidity: function funders(uint256 ) returns(address addr, uint256 amount)
func (_Crowdsale *CrowdsaleCallerSession) Funders(arg0 *big.Int) (struct {
	Addr   common.Address
	Amount *big.Int
}, error) {
	return _Crowdsale.Contract.Funders(&_Crowdsale.CallOpts, arg0)
}

// FundingGoal is a free data retrieval call binding the contract method 0x7a3a0e84.
//
// Solidity: function fundingGoal() returns(uint256)
func (_Crowdsale *CrowdsaleCaller) FundingGoal(opts *bind.CallOpts) (*big.Int, error) {
	var out []interface{}
	err := _Crowdsale.contract.Call(opts, &out, "fundingGoal")

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// FundingGoal is a free data retrieval call binding the contract method 0x7a3a0e84.
//
// Solidity: function fundingGoal() returns(uint256)
func (_Crowdsale *CrowdsaleSession) FundingGoal() (*big.Int, error) {
	return _Crowdsale.Contract.FundingGoal(&_Crowdsale.CallOpts)
}

// FundingGoal is a free data retrieval call binding the contract method 0x7a3a0e84.
//
// Solidity: function fundingGoal() returns(uint256)
func (_Crowdsale *CrowdsaleCallerSession) FundingGoal() (*big.Int, error) {
	return _Crowdsale.Contract.FundingGoal(&_Crowdsale.CallOpts)
}

// Price is a free data retrieval call binding the contract method 0xa035b1fe.
//
// Solidity: function price() returns(uint256)
func (_Crowdsale *CrowdsaleCaller) Price(opts *bind.CallOpts) (*big.Int, error) {
	var out []interface{}
	err := _Crowdsale.contract.Call(opts, &out, "price")

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// Price is a free data retrieval call binding the contract method 0xa035b1fe.
//
// Solidity: function price() returns(uint256)
func (_Crowdsale *CrowdsaleSession) Price() (*big.Int, error) {
	return _Crowdsale.Contract.Price(&_Crowdsale.CallOpts)
}

// Price is a free data retrieval call binding the contract method 0xa035b1fe.
//
// Solidity: function price() returns(uint256)
func (_Crowdsale *CrowdsaleCallerSession) Price() (*big.Int, error) {
	return _Crowdsale.Contract.Price(&_Crowdsale.CallOpts)
}

// TokenReward is a free data retrieval call binding the contract method 0x6e66f6e9.
//
// Solidity: function tokenReward() returns(address)
func (_Crowdsale *CrowdsaleCaller) TokenReward(opts *bind.CallOpts) (common.Address, error) {
	var out []interface{}
	err := _Crowdsale.contract.Call(opts, &out, "tokenReward")

	if err != nil {
		return *new(common.Address), err
	}

	out0 := *abi.ConvertType(out[0], new(common.Address)).(*common.Address)

	return out0, err

}

// TokenReward is a free data retrieval call binding the contract method 0x6e66f6e9.
//
// Solidity: function tokenReward() returns(address)
func (_Crowdsale *CrowdsaleSession) TokenReward() (common.Address, error) {
	return _Crowdsale.Contract.TokenReward(&_Crowdsale.CallOpts)
}

// TokenReward is a free data retrieval call binding the contract method 0x6e66f6e9.
//
// Solidity: function tokenReward() returns(address)
func (_Crowdsale *CrowdsaleCallerSession) TokenReward() (common.Address, error) {
	return _Crowdsale.Contract.TokenReward(&_Crowdsale.CallOpts)
}

// CheckGoalReached is a paid mutator transaction binding the contract method 0x01cb3b20.
//
// Solidity: function checkGoalReached() returns()
func (_Crowdsale *CrowdsaleTransactor) CheckGoalReached(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Crowdsale.contract.Transact(opts, "checkGoalReached")
}

// CheckGoalReached is a paid mutator transaction binding the contract method 0x01cb3b20.
//
// Solidity: function checkGoalReached() returns()
func (_Crowdsale *CrowdsaleSession) CheckGoalReached() (*types.Transaction, error) {
	return _Crowdsale.Contract.CheckGoalReached(&_Crowdsale.TransactOpts)
}

// CheckGoalReached is a paid mutator transaction binding the contract method 0x01cb3b20.
//
// Solidity: function checkGoalReached() returns()
func (_Crowdsale *CrowdsaleTransactorSession) CheckGoalReached() (*types.Transaction, error) {
	return _Crowdsale.Contract.CheckGoalReached(&_Crowdsale.TransactOpts)
}

// CrowdsaleFundTransferIterator is returned from FilterFundTransfer and is used to iterate over the raw logs and unpacked data for FundTransfer events raised by the Crowdsale contract.
type CrowdsaleFundTransferIterator struct {
	Event *CrowdsaleFundTransfer // Event containing the contract specifics and raw log

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
func (it *CrowdsaleFundTransferIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(CrowdsaleFundTransfer)
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
		it.Event = new(CrowdsaleFundTransfer)
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
func (it *CrowdsaleFundTransferIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *CrowdsaleFundTransferIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// CrowdsaleFundTransfer represents a FundTransfer event raised by the Crowdsale contract.
type CrowdsaleFundTransfer struct {
	Backer         common.Address
	Amount         *big.Int
	IsContribution bool
	Raw            types.Log // Blockchain specific contextual infos
}

// FilterFundTransfer is a free log retrieval operation binding the contract event 0xe842aea7a5f1b01049d752008c53c52890b1a6daf660cf39e8eec506112bbdf6.
//
// Solidity: event FundTransfer(address backer, uint256 amount, bool isContribution)
func (_Crowdsale *CrowdsaleFilterer) FilterFundTransfer(opts *bind.FilterOpts) (*CrowdsaleFundTransferIterator, error) {

	logs, sub, err := _Crowdsale.contract.FilterLogs(opts, "FundTransfer")
	if err != nil {
		return nil, err
	}
	return &CrowdsaleFundTransferIterator{contract: _Crowdsale.contract, event: "FundTransfer", logs: logs, sub: sub}, nil
}

// WatchFundTransfer is a free log subscription operation binding the contract event 0xe842aea7a5f1b01049d752008c53c52890b1a6daf660cf39e8eec506112bbdf6.
//
// Solidity: event FundTransfer(address backer, uint256 amount, bool isContribution)
func (_Crowdsale *CrowdsaleFilterer) WatchFundTransfer(opts *bind.WatchOpts, sink chan<- *CrowdsaleFundTransfer) (event.Subscription, error) {

	logs, sub, err := _Crowdsale.contract.WatchLogs(opts, "FundTransfer")
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(CrowdsaleFundTransfer)
				if err := _Crowdsale.contract.UnpackLog(event, "FundTransfer", log); err != nil {
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

// ParseFundTransfer is a log parse operation binding the contract event 0xe842aea7a5f1b01049d752008c53c52890b1a6daf660cf39e8eec506112bbdf6.
//
// Solidity: event FundTransfer(address backer, uint256 amount, bool isContribution)
func (_Crowdsale *CrowdsaleFilterer) ParseFundTransfer(log types.Log) (*CrowdsaleFundTransfer, error) {
	event := new(CrowdsaleFundTransfer)
	if err := _Crowdsale.contract.UnpackLog(event, "FundTransfer", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}
