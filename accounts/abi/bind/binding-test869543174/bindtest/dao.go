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

// DAOABI is the input ABI used to generate the binding from.
const DAOABI = "[{\"constant\":true,\"inputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"name\":\"proposals\",\"outputs\":[{\"name\":\"recipient\",\"type\":\"address\"},{\"name\":\"amount\",\"type\":\"uint256\"},{\"name\":\"description\",\"type\":\"string\"},{\"name\":\"votingDeadline\",\"type\":\"uint256\"},{\"name\":\"executed\",\"type\":\"bool\"},{\"name\":\"proposalPassed\",\"type\":\"bool\"},{\"name\":\"numberOfVotes\",\"type\":\"uint256\"},{\"name\":\"currentResult\",\"type\":\"int256\"},{\"name\":\"proposalHash\",\"type\":\"bytes32\"}],\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"proposalNumber\",\"type\":\"uint256\"},{\"name\":\"transactionBytecode\",\"type\":\"bytes\"}],\"name\":\"executeProposal\",\"outputs\":[{\"name\":\"result\",\"type\":\"int256\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[{\"name\":\"\",\"type\":\"address\"}],\"name\":\"memberId\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"numProposals\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"name\":\"members\",\"outputs\":[{\"name\":\"member\",\"type\":\"address\"},{\"name\":\"canVote\",\"type\":\"bool\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"memberSince\",\"type\":\"uint256\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"debatingPeriodInMinutes\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"minimumQuorum\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"owner\",\"outputs\":[{\"name\":\"\",\"type\":\"address\"}],\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"targetMember\",\"type\":\"address\"},{\"name\":\"canVote\",\"type\":\"bool\"},{\"name\":\"memberName\",\"type\":\"string\"}],\"name\":\"changeMembership\",\"outputs\":[],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"majorityMargin\",\"outputs\":[{\"name\":\"\",\"type\":\"int256\"}],\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"beneficiary\",\"type\":\"address\"},{\"name\":\"etherAmount\",\"type\":\"uint256\"},{\"name\":\"JobDescription\",\"type\":\"string\"},{\"name\":\"transactionBytecode\",\"type\":\"bytes\"}],\"name\":\"newProposal\",\"outputs\":[{\"name\":\"proposalID\",\"type\":\"uint256\"}],\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"minimumQuorumForProposals\",\"type\":\"uint256\"},{\"name\":\"minutesForDebate\",\"type\":\"uint256\"},{\"name\":\"marginOfVotesForMajority\",\"type\":\"int256\"}],\"name\":\"changeVotingRules\",\"outputs\":[],\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"proposalNumber\",\"type\":\"uint256\"},{\"name\":\"supportsProposal\",\"type\":\"bool\"},{\"name\":\"justificationText\",\"type\":\"string\"}],\"name\":\"vote\",\"outputs\":[{\"name\":\"voteID\",\"type\":\"uint256\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[{\"name\":\"proposalNumber\",\"type\":\"uint256\"},{\"name\":\"beneficiary\",\"type\":\"address\"},{\"name\":\"etherAmount\",\"type\":\"uint256\"},{\"name\":\"transactionBytecode\",\"type\":\"bytes\"}],\"name\":\"checkProposalCode\",\"outputs\":[{\"name\":\"codeChecksOut\",\"type\":\"bool\"}],\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"newOwner\",\"type\":\"address\"}],\"name\":\"transferOwnership\",\"outputs\":[],\"type\":\"function\"},{\"inputs\":[{\"name\":\"minimumQuorumForProposals\",\"type\":\"uint256\"},{\"name\":\"minutesForDebate\",\"type\":\"uint256\"},{\"name\":\"marginOfVotesForMajority\",\"type\":\"int256\"},{\"name\":\"congressLeader\",\"type\":\"address\"}],\"type\":\"constructor\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"name\":\"proposalID\",\"type\":\"uint256\"},{\"indexed\":false,\"name\":\"recipient\",\"type\":\"address\"},{\"indexed\":false,\"name\":\"amount\",\"type\":\"uint256\"},{\"indexed\":false,\"name\":\"description\",\"type\":\"string\"}],\"name\":\"ProposalAdded\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"name\":\"proposalID\",\"type\":\"uint256\"},{\"indexed\":false,\"name\":\"position\",\"type\":\"bool\"},{\"indexed\":false,\"name\":\"voter\",\"type\":\"address\"},{\"indexed\":false,\"name\":\"justification\",\"type\":\"string\"}],\"name\":\"Voted\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"name\":\"proposalID\",\"type\":\"uint256\"},{\"indexed\":false,\"name\":\"result\",\"type\":\"int256\"},{\"indexed\":false,\"name\":\"quorum\",\"type\":\"uint256\"},{\"indexed\":false,\"name\":\"active\",\"type\":\"bool\"}],\"name\":\"ProposalTallied\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"name\":\"member\",\"type\":\"address\"},{\"indexed\":false,\"name\":\"isMember\",\"type\":\"bool\"}],\"name\":\"MembershipChanged\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"name\":\"minimumQuorum\",\"type\":\"uint256\"},{\"indexed\":false,\"name\":\"debatingPeriodInMinutes\",\"type\":\"uint256\"},{\"indexed\":false,\"name\":\"majorityMargin\",\"type\":\"int256\"}],\"name\":\"ChangeOfRules\",\"type\":\"event\"}]"

// DAOBin is the compiled bytecode used for deploying new contracts.
var DAOBin = "0x606060405260405160808061145f833960e06040529051905160a05160c05160008054600160a060020a03191633179055600184815560028490556003839055600780549182018082558280158290116100b8576003028160030283600052602060002091820191016100b891906101c8565b50506060919091015160029190910155600160a060020a0381166000146100a65760008054600160a060020a031916821790555b505050506111f18061026e6000396000f35b505060408051608081018252600080825260208281018290528351908101845281815292820192909252426060820152600780549194509250811015610002579081527fa66cc928b5edb82af9bd49922954155ab7b0942694bea4ce44661d9a8736c6889050815181546020848101517401000000000000000000000000000000000000000002600160a060020a03199290921690921760a060020a60ff021916178255604083015180516001848101805460008281528690209195600293821615610100026000190190911692909204601f9081018390048201949192919091019083901061023e57805160ff19168380011785555b50610072929150610226565b5050600060028201556001015b8082111561023a578054600160a860020a031916815560018181018054600080835592600290821615610100026000190190911604601f81901061020c57506101bb565b601f0160209004906000526020600020908101906101bb91905b8082111561023a5760008155600101610226565b5090565b828001600101855582156101af579182015b828111156101af57825182600050559160200191906001019061025056606060405236156100b95760e060020a6000350463013cf08b81146100bb578063237e9492146101285780633910682114610281578063400e3949146102995780635daf08ca146102a257806369bd34361461032f5780638160f0b5146103385780638da5cb5b146103415780639644fcbd14610353578063aa02a90f146103be578063b1050da5146103c7578063bcca1fd3146104b5578063d3c0715b146104dc578063eceb29451461058d578063f2fde38b1461067b575b005b61069c6004356004805482908110156100025790600052602060002090600a02016000506005810154815460018301546003840154600485015460068601546007870154600160a060020a03959095169750929560020194919360ff828116946101009093041692919089565b60408051602060248035600481810135601f81018590048502860185019096528585526107759581359591946044949293909201918190840183828082843750949650505050505050600060006004600050848154811015610002575090527f8a35acfbc15ff81a39ae7d344fd709f28e8600b4aa8c65c6b64bfe7fe36bd19e600a8402908101547f8a35acfbc15ff81a39ae7d344fd709f28e8600b4aa8c65c6b64bfe7fe36bd19b909101904210806101e65750600481015460ff165b8061026757508060000160009054906101000a9004600160a060020a03168160010160005054846040518084600160a060020a0316606060020a0281526014018381526020018280519060200190808383829060006004602084601f0104600f02600301f15090500193505050506040518091039020816007016000505414155b8061027757506001546005820154105b1561109257610002565b61077560043560066020526000908152604090205481565b61077560055481565b61078760043560078054829081101561000257506000526003026000805160206111d18339815191528101547fa66cc928b5edb82af9bd49922954155ab7b0942694bea4ce44661d9a8736c68a820154600160a060020a0382169260a060020a90920460ff16917fa66cc928b5edb82af9bd49922954155ab7b0942694bea4ce44661d9a8736c689019084565b61077560025481565b61077560015481565b610830600054600160a060020a031681565b604080516020604435600481810135601f81018490048402850184019095528484526100b9948135946024803595939460649492939101918190840183828082843750949650505050505050600080548190600160a060020a03908116339091161461084d57610002565b61077560035481565b604080516020604435600481810135601f8101849004840285018401909552848452610775948135946024803595939460649492939101918190840183828082843750506040805160209735808a0135601f81018a90048a0283018a019093528282529698976084979196506024909101945090925082915084018382808284375094965050505050505033600160a060020a031660009081526006602052604081205481908114806104ab5750604081205460078054909190811015610002579082526003026000805160206111d1833981519152015460a060020a900460ff16155b15610ce557610002565b6100b960043560243560443560005433600160a060020a03908116911614610b1857610002565b604080516020604435600481810135601f810184900484028501840190955284845261077594813594602480359593946064949293910191819084018382808284375094965050505050505033600160a060020a031660009081526006602052604081205481908114806105835750604081205460078054909190811015610002579082526003026000805160206111d18339815191520181505460a060020a900460ff16155b15610f1d57610002565b604080516020606435600481810135601f81018490048402850184019095528484526107759481359460248035956044359560849492019190819084018382808284375094965050505050505060006000600460005086815481101561000257908252600a027f8a35acfbc15ff81a39ae7d344fd709f28e8600b4aa8c65c6b64bfe7fe36bd19b01815090508484846040518084600160a060020a0316606060020a0281526014018381526020018280519060200190808383829060006004602084601f0104600f02600301f150905001935050505060405180910390208160070160005054149150610cdc565b6100b960043560005433600160a060020a03908116911614610f0857610002565b604051808a600160a060020a031681526020018981526020018060200188815260200187815260200186815260200185815260200184815260200183815260200182810382528981815460018160011615610100020316600290048152602001915080546001816001161561010002031660029004801561075e5780601f106107335761010080835404028352916020019161075e565b820191906000526020600020905b81548152906001019060200180831161074157829003601f168201915b50509a505050505050505050505060405180910390f35b60408051918252519081900360200190f35b60408051600160a060020a038616815260208101859052606081018390526080918101828152845460026001821615610100026000190190911604928201839052909160a08301908590801561081e5780601f106107f35761010080835404028352916020019161081e565b820191906000526020600020905b81548152906001019060200180831161080157829003601f168201915b50509550505050505060405180910390f35b60408051600160a060020a03929092168252519081900360200190f35b600160a060020a03851660009081526006602052604081205414156108a957604060002060078054918290556001820180825582801582901161095c5760030281600302836000526020600020918201910161095c9190610a4f565b600160a060020a03851660009081526006602052604090205460078054919350908390811015610002575060005250600381026000805160206111d183398151915201805474ff0000000000000000000000000000000000000000191660a060020a85021781555b60408051600160a060020a03871681526020810186905281517f27b022af4a8347100c7a041ce5ccf8e14d644ff05de696315196faae8cd50c9b929181900390910190a15050505050565b505050915081506080604051908101604052808681526020018581526020018481526020014281526020015060076000508381548110156100025790600052602060002090600302016000508151815460208481015160a060020a02600160a060020a03199290921690921774ff00000000000000000000000000000000000000001916178255604083015180516001848101805460008281528690209195600293821615610100026000190190911692909204601f90810183900482019491929190910190839010610ad357805160ff19168380011785555b50610b03929150610abb565b5050600060028201556001015b80821115610acf57805474ffffffffffffffffffffffffffffffffffffffffff1916815560018181018054600080835592600290821615610100026000190190911604601f819010610aa15750610a42565b601f016020900490600052602060002090810190610a4291905b80821115610acf5760008155600101610abb565b5090565b82800160010185558215610a36579182015b82811115610a36578251826000505591602001919060010190610ae5565b50506060919091015160029190910155610911565b600183905560028290556003819055604080518481526020810184905280820183905290517fa439d3fa452be5e0e1e24a8145e715f4fd8b9c08c96a42fd82a855a85e5d57de9181900360600190a1505050565b50508585846040518084600160a060020a0316606060020a0281526014018381526020018280519060200190808383829060006004602084601f0104600f02600301f150905001935050505060405180910390208160070160005081905550600260005054603c024201816003016000508190555060008160040160006101000a81548160ff0219169083021790555060008160040160016101000a81548160ff02191690830217905550600081600501600050819055507f646fec02522b41e7125cfc859a64fd4f4cefd5dc3b6237ca0abe251ded1fa881828787876040518085815260200184600160a060020a03168152602001838152602001806020018281038252838181518152602001915080519060200190808383829060006004602084601f0104600f02600301f150905090810190601f168015610cc45780820380516001836020036101000a031916815260200191505b509550505050505060405180910390a1600182016005555b50949350505050565b6004805460018101808355909190828015829011610d1c57600a0281600a028360005260206000209182019101610d1c9190610db8565b505060048054929450918491508110156100025790600052602060002090600a02016000508054600160a060020a031916871781556001818101879055855160028381018054600082815260209081902096975091959481161561010002600019011691909104601f90810182900484019391890190839010610ed857805160ff19168380011785555b50610b6c929150610abb565b50506001015b80821115610acf578054600160a060020a03191681556000600182810182905560028381018054848255909281161561010002600019011604601f819010610e9c57505b5060006003830181905560048301805461ffff191690556005830181905560068301819055600783018190556008830180548282559082526020909120610db2916002028101905b80821115610acf57805474ffffffffffffffffffffffffffffffffffffffffff1916815560018181018054600080835592600290821615610100026000190190911604601f819010610eba57505b5050600101610e44565b601f016020900490600052602060002090810190610dfc9190610abb565b601f016020900490600052602060002090810190610e929190610abb565b82800160010185558215610da6579182015b82811115610da6578251826000505591602001919060010190610eea565b60008054600160a060020a0319168217905550565b600480548690811015610002576000918252600a027f8a35acfbc15ff81a39ae7d344fd709f28e8600b4aa8c65c6b64bfe7fe36bd19b01905033600160a060020a0316600090815260098201602052604090205490915060ff1660011415610f8457610002565b33600160a060020a031660009081526009820160205260409020805460ff1916600190811790915560058201805490910190558315610fcd576006810180546001019055610fda565b6006810180546000190190555b7fc34f869b7ff431b034b7b9aea9822dac189a685e0b015c7d1be3add3f89128e8858533866040518085815260200184815260200183600160a060020a03168152602001806020018281038252838181518152602001915080519060200190808383829060006004602084601f0104600f02600301f150905090810190601f16801561107a5780820380516001836020036101000a031916815260200191505b509550505050505060405180910390a1509392505050565b6006810154600354901315611158578060000160009054906101000a9004600160a060020a0316600160a060020a03168160010160005054670de0b6b3a76400000284604051808280519060200190808383829060006004602084601f0104600f02600301f150905090810190601f1680156111225780820380516001836020036101000a031916815260200191505b5091505060006040518083038185876185025a03f15050505060048101805460ff191660011761ff00191661010017905561116d565b60048101805460ff191660011761ff00191690555b60068101546005820154600483015460408051888152602081019490945283810192909252610100900460ff166060830152517fd220b7272a8b6d0d7d6bcdace67b936a8f175e6d5c1b3ee438b72256b32ab3af9181900360800190a1509291505056a66cc928b5edb82af9bd49922954155ab7b0942694bea4ce44661d9a8736c688"

// DeployDAO deploys a new Ethereum contract, binding an instance of DAO to it.
func DeployDAO(auth *bind.TransactOpts, backend bind.ContractBackend, minimumQuorumForProposals *big.Int, minutesForDebate *big.Int, marginOfVotesForMajority *big.Int, congressLeader common.Address) (common.Address, *types.Transaction, *DAO, error) {
	parsed, err := abi.JSON(strings.NewReader(DAOABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(DAOBin), backend, minimumQuorumForProposals, minutesForDebate, marginOfVotesForMajority, congressLeader)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &DAO{DAOCaller: DAOCaller{contract: contract}, DAOTransactor: DAOTransactor{contract: contract}, DAOFilterer: DAOFilterer{contract: contract}}, nil
}

// DAO is an auto generated Go binding around an Ethereum contract.
type DAO struct {
	DAOCaller     // Read-only binding to the contract
	DAOTransactor // Write-only binding to the contract
	DAOFilterer   // Log filterer for contract events
}

// DAOCaller is an auto generated read-only Go binding around an Ethereum contract.
type DAOCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// DAOTransactor is an auto generated write-only Go binding around an Ethereum contract.
type DAOTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// DAOFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type DAOFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// DAOSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type DAOSession struct {
	Contract     *DAO              // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// DAOCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type DAOCallerSession struct {
	Contract *DAOCaller    // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts // Call options to use throughout this session
}

// DAOTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type DAOTransactorSession struct {
	Contract     *DAOTransactor    // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// DAORaw is an auto generated low-level Go binding around an Ethereum contract.
type DAORaw struct {
	Contract *DAO // Generic contract binding to access the raw methods on
}

// DAOCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type DAOCallerRaw struct {
	Contract *DAOCaller // Generic read-only contract binding to access the raw methods on
}

// DAOTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type DAOTransactorRaw struct {
	Contract *DAOTransactor // Generic write-only contract binding to access the raw methods on
}

// NewDAO creates a new instance of DAO, bound to a specific deployed contract.
func NewDAO(address common.Address, backend bind.ContractBackend) (*DAO, error) {
	contract, err := bindDAO(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &DAO{DAOCaller: DAOCaller{contract: contract}, DAOTransactor: DAOTransactor{contract: contract}, DAOFilterer: DAOFilterer{contract: contract}}, nil
}

// NewDAOCaller creates a new read-only instance of DAO, bound to a specific deployed contract.
func NewDAOCaller(address common.Address, caller bind.ContractCaller) (*DAOCaller, error) {
	contract, err := bindDAO(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &DAOCaller{contract: contract}, nil
}

// NewDAOTransactor creates a new write-only instance of DAO, bound to a specific deployed contract.
func NewDAOTransactor(address common.Address, transactor bind.ContractTransactor) (*DAOTransactor, error) {
	contract, err := bindDAO(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &DAOTransactor{contract: contract}, nil
}

// NewDAOFilterer creates a new log filterer instance of DAO, bound to a specific deployed contract.
func NewDAOFilterer(address common.Address, filterer bind.ContractFilterer) (*DAOFilterer, error) {
	contract, err := bindDAO(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &DAOFilterer{contract: contract}, nil
}

// bindDAO binds a generic wrapper to an already deployed contract.
func bindDAO(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(DAOABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_DAO *DAORaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _DAO.Contract.DAOCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_DAO *DAORaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _DAO.Contract.DAOTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_DAO *DAORaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _DAO.Contract.DAOTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_DAO *DAOCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _DAO.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_DAO *DAOTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _DAO.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_DAO *DAOTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _DAO.Contract.contract.Transact(opts, method, params...)
}

// CheckProposalCode is a free data retrieval call binding the contract method 0xeceb2945.
//
// Solidity: function checkProposalCode(uint256 proposalNumber, address beneficiary, uint256 etherAmount, bytes transactionBytecode) returns(bool codeChecksOut)
func (_DAO *DAOCaller) CheckProposalCode(opts *bind.CallOpts, proposalNumber *big.Int, beneficiary common.Address, etherAmount *big.Int, transactionBytecode []byte) (bool, error) {
	var out []interface{}
	err := _DAO.contract.Call(opts, &out, "checkProposalCode", proposalNumber, beneficiary, etherAmount, transactionBytecode)

	if err != nil {
		return *new(bool), err
	}

	out0 := *abi.ConvertType(out[0], new(bool)).(*bool)

	return out0, err

}

// CheckProposalCode is a free data retrieval call binding the contract method 0xeceb2945.
//
// Solidity: function checkProposalCode(uint256 proposalNumber, address beneficiary, uint256 etherAmount, bytes transactionBytecode) returns(bool codeChecksOut)
func (_DAO *DAOSession) CheckProposalCode(proposalNumber *big.Int, beneficiary common.Address, etherAmount *big.Int, transactionBytecode []byte) (bool, error) {
	return _DAO.Contract.CheckProposalCode(&_DAO.CallOpts, proposalNumber, beneficiary, etherAmount, transactionBytecode)
}

// CheckProposalCode is a free data retrieval call binding the contract method 0xeceb2945.
//
// Solidity: function checkProposalCode(uint256 proposalNumber, address beneficiary, uint256 etherAmount, bytes transactionBytecode) returns(bool codeChecksOut)
func (_DAO *DAOCallerSession) CheckProposalCode(proposalNumber *big.Int, beneficiary common.Address, etherAmount *big.Int, transactionBytecode []byte) (bool, error) {
	return _DAO.Contract.CheckProposalCode(&_DAO.CallOpts, proposalNumber, beneficiary, etherAmount, transactionBytecode)
}

// DebatingPeriodInMinutes is a free data retrieval call binding the contract method 0x69bd3436.
//
// Solidity: function debatingPeriodInMinutes() returns(uint256)
func (_DAO *DAOCaller) DebatingPeriodInMinutes(opts *bind.CallOpts) (*big.Int, error) {
	var out []interface{}
	err := _DAO.contract.Call(opts, &out, "debatingPeriodInMinutes")

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// DebatingPeriodInMinutes is a free data retrieval call binding the contract method 0x69bd3436.
//
// Solidity: function debatingPeriodInMinutes() returns(uint256)
func (_DAO *DAOSession) DebatingPeriodInMinutes() (*big.Int, error) {
	return _DAO.Contract.DebatingPeriodInMinutes(&_DAO.CallOpts)
}

// DebatingPeriodInMinutes is a free data retrieval call binding the contract method 0x69bd3436.
//
// Solidity: function debatingPeriodInMinutes() returns(uint256)
func (_DAO *DAOCallerSession) DebatingPeriodInMinutes() (*big.Int, error) {
	return _DAO.Contract.DebatingPeriodInMinutes(&_DAO.CallOpts)
}

// MajorityMargin is a free data retrieval call binding the contract method 0xaa02a90f.
//
// Solidity: function majorityMargin() returns(int256)
func (_DAO *DAOCaller) MajorityMargin(opts *bind.CallOpts) (*big.Int, error) {
	var out []interface{}
	err := _DAO.contract.Call(opts, &out, "majorityMargin")

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// MajorityMargin is a free data retrieval call binding the contract method 0xaa02a90f.
//
// Solidity: function majorityMargin() returns(int256)
func (_DAO *DAOSession) MajorityMargin() (*big.Int, error) {
	return _DAO.Contract.MajorityMargin(&_DAO.CallOpts)
}

// MajorityMargin is a free data retrieval call binding the contract method 0xaa02a90f.
//
// Solidity: function majorityMargin() returns(int256)
func (_DAO *DAOCallerSession) MajorityMargin() (*big.Int, error) {
	return _DAO.Contract.MajorityMargin(&_DAO.CallOpts)
}

// MemberId is a free data retrieval call binding the contract method 0x39106821.
//
// Solidity: function memberId(address ) returns(uint256)
func (_DAO *DAOCaller) MemberId(opts *bind.CallOpts, arg0 common.Address) (*big.Int, error) {
	var out []interface{}
	err := _DAO.contract.Call(opts, &out, "memberId", arg0)

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// MemberId is a free data retrieval call binding the contract method 0x39106821.
//
// Solidity: function memberId(address ) returns(uint256)
func (_DAO *DAOSession) MemberId(arg0 common.Address) (*big.Int, error) {
	return _DAO.Contract.MemberId(&_DAO.CallOpts, arg0)
}

// MemberId is a free data retrieval call binding the contract method 0x39106821.
//
// Solidity: function memberId(address ) returns(uint256)
func (_DAO *DAOCallerSession) MemberId(arg0 common.Address) (*big.Int, error) {
	return _DAO.Contract.MemberId(&_DAO.CallOpts, arg0)
}

// Members is a free data retrieval call binding the contract method 0x5daf08ca.
//
// Solidity: function members(uint256 ) returns(address member, bool canVote, string name, uint256 memberSince)
func (_DAO *DAOCaller) Members(opts *bind.CallOpts, arg0 *big.Int) (struct {
	Member      common.Address
	CanVote     bool
	Name        string
	MemberSince *big.Int
}, error) {
	var out []interface{}
	err := _DAO.contract.Call(opts, &out, "members", arg0)

	outstruct := new(struct {
		Member      common.Address
		CanVote     bool
		Name        string
		MemberSince *big.Int
	})

	outstruct.Member = out[0].(common.Address)
	outstruct.CanVote = out[1].(bool)
	outstruct.Name = out[2].(string)
	outstruct.MemberSince = out[3].(*big.Int)

	return *outstruct, err

}

// Members is a free data retrieval call binding the contract method 0x5daf08ca.
//
// Solidity: function members(uint256 ) returns(address member, bool canVote, string name, uint256 memberSince)
func (_DAO *DAOSession) Members(arg0 *big.Int) (struct {
	Member      common.Address
	CanVote     bool
	Name        string
	MemberSince *big.Int
}, error) {
	return _DAO.Contract.Members(&_DAO.CallOpts, arg0)
}

// Members is a free data retrieval call binding the contract method 0x5daf08ca.
//
// Solidity: function members(uint256 ) returns(address member, bool canVote, string name, uint256 memberSince)
func (_DAO *DAOCallerSession) Members(arg0 *big.Int) (struct {
	Member      common.Address
	CanVote     bool
	Name        string
	MemberSince *big.Int
}, error) {
	return _DAO.Contract.Members(&_DAO.CallOpts, arg0)
}

// MinimumQuorum is a free data retrieval call binding the contract method 0x8160f0b5.
//
// Solidity: function minimumQuorum() returns(uint256)
func (_DAO *DAOCaller) MinimumQuorum(opts *bind.CallOpts) (*big.Int, error) {
	var out []interface{}
	err := _DAO.contract.Call(opts, &out, "minimumQuorum")

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// MinimumQuorum is a free data retrieval call binding the contract method 0x8160f0b5.
//
// Solidity: function minimumQuorum() returns(uint256)
func (_DAO *DAOSession) MinimumQuorum() (*big.Int, error) {
	return _DAO.Contract.MinimumQuorum(&_DAO.CallOpts)
}

// MinimumQuorum is a free data retrieval call binding the contract method 0x8160f0b5.
//
// Solidity: function minimumQuorum() returns(uint256)
func (_DAO *DAOCallerSession) MinimumQuorum() (*big.Int, error) {
	return _DAO.Contract.MinimumQuorum(&_DAO.CallOpts)
}

// NumProposals is a free data retrieval call binding the contract method 0x400e3949.
//
// Solidity: function numProposals() returns(uint256)
func (_DAO *DAOCaller) NumProposals(opts *bind.CallOpts) (*big.Int, error) {
	var out []interface{}
	err := _DAO.contract.Call(opts, &out, "numProposals")

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// NumProposals is a free data retrieval call binding the contract method 0x400e3949.
//
// Solidity: function numProposals() returns(uint256)
func (_DAO *DAOSession) NumProposals() (*big.Int, error) {
	return _DAO.Contract.NumProposals(&_DAO.CallOpts)
}

// NumProposals is a free data retrieval call binding the contract method 0x400e3949.
//
// Solidity: function numProposals() returns(uint256)
func (_DAO *DAOCallerSession) NumProposals() (*big.Int, error) {
	return _DAO.Contract.NumProposals(&_DAO.CallOpts)
}

// Owner is a free data retrieval call binding the contract method 0x8da5cb5b.
//
// Solidity: function owner() returns(address)
func (_DAO *DAOCaller) Owner(opts *bind.CallOpts) (common.Address, error) {
	var out []interface{}
	err := _DAO.contract.Call(opts, &out, "owner")

	if err != nil {
		return *new(common.Address), err
	}

	out0 := *abi.ConvertType(out[0], new(common.Address)).(*common.Address)

	return out0, err

}

// Owner is a free data retrieval call binding the contract method 0x8da5cb5b.
//
// Solidity: function owner() returns(address)
func (_DAO *DAOSession) Owner() (common.Address, error) {
	return _DAO.Contract.Owner(&_DAO.CallOpts)
}

// Owner is a free data retrieval call binding the contract method 0x8da5cb5b.
//
// Solidity: function owner() returns(address)
func (_DAO *DAOCallerSession) Owner() (common.Address, error) {
	return _DAO.Contract.Owner(&_DAO.CallOpts)
}

// Proposals is a free data retrieval call binding the contract method 0x013cf08b.
//
// Solidity: function proposals(uint256 ) returns(address recipient, uint256 amount, string description, uint256 votingDeadline, bool executed, bool proposalPassed, uint256 numberOfVotes, int256 currentResult, bytes32 proposalHash)
func (_DAO *DAOCaller) Proposals(opts *bind.CallOpts, arg0 *big.Int) (struct {
	Recipient      common.Address
	Amount         *big.Int
	Description    string
	VotingDeadline *big.Int
	Executed       bool
	ProposalPassed bool
	NumberOfVotes  *big.Int
	CurrentResult  *big.Int
	ProposalHash   [32]byte
}, error) {
	var out []interface{}
	err := _DAO.contract.Call(opts, &out, "proposals", arg0)

	outstruct := new(struct {
		Recipient      common.Address
		Amount         *big.Int
		Description    string
		VotingDeadline *big.Int
		Executed       bool
		ProposalPassed bool
		NumberOfVotes  *big.Int
		CurrentResult  *big.Int
		ProposalHash   [32]byte
	})

	outstruct.Recipient = out[0].(common.Address)
	outstruct.Amount = out[1].(*big.Int)
	outstruct.Description = out[2].(string)
	outstruct.VotingDeadline = out[3].(*big.Int)
	outstruct.Executed = out[4].(bool)
	outstruct.ProposalPassed = out[5].(bool)
	outstruct.NumberOfVotes = out[6].(*big.Int)
	outstruct.CurrentResult = out[7].(*big.Int)
	outstruct.ProposalHash = out[8].([32]byte)

	return *outstruct, err

}

// Proposals is a free data retrieval call binding the contract method 0x013cf08b.
//
// Solidity: function proposals(uint256 ) returns(address recipient, uint256 amount, string description, uint256 votingDeadline, bool executed, bool proposalPassed, uint256 numberOfVotes, int256 currentResult, bytes32 proposalHash)
func (_DAO *DAOSession) Proposals(arg0 *big.Int) (struct {
	Recipient      common.Address
	Amount         *big.Int
	Description    string
	VotingDeadline *big.Int
	Executed       bool
	ProposalPassed bool
	NumberOfVotes  *big.Int
	CurrentResult  *big.Int
	ProposalHash   [32]byte
}, error) {
	return _DAO.Contract.Proposals(&_DAO.CallOpts, arg0)
}

// Proposals is a free data retrieval call binding the contract method 0x013cf08b.
//
// Solidity: function proposals(uint256 ) returns(address recipient, uint256 amount, string description, uint256 votingDeadline, bool executed, bool proposalPassed, uint256 numberOfVotes, int256 currentResult, bytes32 proposalHash)
func (_DAO *DAOCallerSession) Proposals(arg0 *big.Int) (struct {
	Recipient      common.Address
	Amount         *big.Int
	Description    string
	VotingDeadline *big.Int
	Executed       bool
	ProposalPassed bool
	NumberOfVotes  *big.Int
	CurrentResult  *big.Int
	ProposalHash   [32]byte
}, error) {
	return _DAO.Contract.Proposals(&_DAO.CallOpts, arg0)
}

// ChangeMembership is a paid mutator transaction binding the contract method 0x9644fcbd.
//
// Solidity: function changeMembership(address targetMember, bool canVote, string memberName) returns()
func (_DAO *DAOTransactor) ChangeMembership(opts *bind.TransactOpts, targetMember common.Address, canVote bool, memberName string) (*types.Transaction, error) {
	return _DAO.contract.Transact(opts, "changeMembership", targetMember, canVote, memberName)
}

// ChangeMembership is a paid mutator transaction binding the contract method 0x9644fcbd.
//
// Solidity: function changeMembership(address targetMember, bool canVote, string memberName) returns()
func (_DAO *DAOSession) ChangeMembership(targetMember common.Address, canVote bool, memberName string) (*types.Transaction, error) {
	return _DAO.Contract.ChangeMembership(&_DAO.TransactOpts, targetMember, canVote, memberName)
}

// ChangeMembership is a paid mutator transaction binding the contract method 0x9644fcbd.
//
// Solidity: function changeMembership(address targetMember, bool canVote, string memberName) returns()
func (_DAO *DAOTransactorSession) ChangeMembership(targetMember common.Address, canVote bool, memberName string) (*types.Transaction, error) {
	return _DAO.Contract.ChangeMembership(&_DAO.TransactOpts, targetMember, canVote, memberName)
}

// ChangeVotingRules is a paid mutator transaction binding the contract method 0xbcca1fd3.
//
// Solidity: function changeVotingRules(uint256 minimumQuorumForProposals, uint256 minutesForDebate, int256 marginOfVotesForMajority) returns()
func (_DAO *DAOTransactor) ChangeVotingRules(opts *bind.TransactOpts, minimumQuorumForProposals *big.Int, minutesForDebate *big.Int, marginOfVotesForMajority *big.Int) (*types.Transaction, error) {
	return _DAO.contract.Transact(opts, "changeVotingRules", minimumQuorumForProposals, minutesForDebate, marginOfVotesForMajority)
}

// ChangeVotingRules is a paid mutator transaction binding the contract method 0xbcca1fd3.
//
// Solidity: function changeVotingRules(uint256 minimumQuorumForProposals, uint256 minutesForDebate, int256 marginOfVotesForMajority) returns()
func (_DAO *DAOSession) ChangeVotingRules(minimumQuorumForProposals *big.Int, minutesForDebate *big.Int, marginOfVotesForMajority *big.Int) (*types.Transaction, error) {
	return _DAO.Contract.ChangeVotingRules(&_DAO.TransactOpts, minimumQuorumForProposals, minutesForDebate, marginOfVotesForMajority)
}

// ChangeVotingRules is a paid mutator transaction binding the contract method 0xbcca1fd3.
//
// Solidity: function changeVotingRules(uint256 minimumQuorumForProposals, uint256 minutesForDebate, int256 marginOfVotesForMajority) returns()
func (_DAO *DAOTransactorSession) ChangeVotingRules(minimumQuorumForProposals *big.Int, minutesForDebate *big.Int, marginOfVotesForMajority *big.Int) (*types.Transaction, error) {
	return _DAO.Contract.ChangeVotingRules(&_DAO.TransactOpts, minimumQuorumForProposals, minutesForDebate, marginOfVotesForMajority)
}

// ExecuteProposal is a paid mutator transaction binding the contract method 0x237e9492.
//
// Solidity: function executeProposal(uint256 proposalNumber, bytes transactionBytecode) returns(int256 result)
func (_DAO *DAOTransactor) ExecuteProposal(opts *bind.TransactOpts, proposalNumber *big.Int, transactionBytecode []byte) (*types.Transaction, error) {
	return _DAO.contract.Transact(opts, "executeProposal", proposalNumber, transactionBytecode)
}

// ExecuteProposal is a paid mutator transaction binding the contract method 0x237e9492.
//
// Solidity: function executeProposal(uint256 proposalNumber, bytes transactionBytecode) returns(int256 result)
func (_DAO *DAOSession) ExecuteProposal(proposalNumber *big.Int, transactionBytecode []byte) (*types.Transaction, error) {
	return _DAO.Contract.ExecuteProposal(&_DAO.TransactOpts, proposalNumber, transactionBytecode)
}

// ExecuteProposal is a paid mutator transaction binding the contract method 0x237e9492.
//
// Solidity: function executeProposal(uint256 proposalNumber, bytes transactionBytecode) returns(int256 result)
func (_DAO *DAOTransactorSession) ExecuteProposal(proposalNumber *big.Int, transactionBytecode []byte) (*types.Transaction, error) {
	return _DAO.Contract.ExecuteProposal(&_DAO.TransactOpts, proposalNumber, transactionBytecode)
}

// NewProposal is a paid mutator transaction binding the contract method 0xb1050da5.
//
// Solidity: function newProposal(address beneficiary, uint256 etherAmount, string JobDescription, bytes transactionBytecode) returns(uint256 proposalID)
func (_DAO *DAOTransactor) NewProposal(opts *bind.TransactOpts, beneficiary common.Address, etherAmount *big.Int, JobDescription string, transactionBytecode []byte) (*types.Transaction, error) {
	return _DAO.contract.Transact(opts, "newProposal", beneficiary, etherAmount, JobDescription, transactionBytecode)
}

// NewProposal is a paid mutator transaction binding the contract method 0xb1050da5.
//
// Solidity: function newProposal(address beneficiary, uint256 etherAmount, string JobDescription, bytes transactionBytecode) returns(uint256 proposalID)
func (_DAO *DAOSession) NewProposal(beneficiary common.Address, etherAmount *big.Int, JobDescription string, transactionBytecode []byte) (*types.Transaction, error) {
	return _DAO.Contract.NewProposal(&_DAO.TransactOpts, beneficiary, etherAmount, JobDescription, transactionBytecode)
}

// NewProposal is a paid mutator transaction binding the contract method 0xb1050da5.
//
// Solidity: function newProposal(address beneficiary, uint256 etherAmount, string JobDescription, bytes transactionBytecode) returns(uint256 proposalID)
func (_DAO *DAOTransactorSession) NewProposal(beneficiary common.Address, etherAmount *big.Int, JobDescription string, transactionBytecode []byte) (*types.Transaction, error) {
	return _DAO.Contract.NewProposal(&_DAO.TransactOpts, beneficiary, etherAmount, JobDescription, transactionBytecode)
}

// TransferOwnership is a paid mutator transaction binding the contract method 0xf2fde38b.
//
// Solidity: function transferOwnership(address newOwner) returns()
func (_DAO *DAOTransactor) TransferOwnership(opts *bind.TransactOpts, newOwner common.Address) (*types.Transaction, error) {
	return _DAO.contract.Transact(opts, "transferOwnership", newOwner)
}

// TransferOwnership is a paid mutator transaction binding the contract method 0xf2fde38b.
//
// Solidity: function transferOwnership(address newOwner) returns()
func (_DAO *DAOSession) TransferOwnership(newOwner common.Address) (*types.Transaction, error) {
	return _DAO.Contract.TransferOwnership(&_DAO.TransactOpts, newOwner)
}

// TransferOwnership is a paid mutator transaction binding the contract method 0xf2fde38b.
//
// Solidity: function transferOwnership(address newOwner) returns()
func (_DAO *DAOTransactorSession) TransferOwnership(newOwner common.Address) (*types.Transaction, error) {
	return _DAO.Contract.TransferOwnership(&_DAO.TransactOpts, newOwner)
}

// Vote is a paid mutator transaction binding the contract method 0xd3c0715b.
//
// Solidity: function vote(uint256 proposalNumber, bool supportsProposal, string justificationText) returns(uint256 voteID)
func (_DAO *DAOTransactor) Vote(opts *bind.TransactOpts, proposalNumber *big.Int, supportsProposal bool, justificationText string) (*types.Transaction, error) {
	return _DAO.contract.Transact(opts, "vote", proposalNumber, supportsProposal, justificationText)
}

// Vote is a paid mutator transaction binding the contract method 0xd3c0715b.
//
// Solidity: function vote(uint256 proposalNumber, bool supportsProposal, string justificationText) returns(uint256 voteID)
func (_DAO *DAOSession) Vote(proposalNumber *big.Int, supportsProposal bool, justificationText string) (*types.Transaction, error) {
	return _DAO.Contract.Vote(&_DAO.TransactOpts, proposalNumber, supportsProposal, justificationText)
}

// Vote is a paid mutator transaction binding the contract method 0xd3c0715b.
//
// Solidity: function vote(uint256 proposalNumber, bool supportsProposal, string justificationText) returns(uint256 voteID)
func (_DAO *DAOTransactorSession) Vote(proposalNumber *big.Int, supportsProposal bool, justificationText string) (*types.Transaction, error) {
	return _DAO.Contract.Vote(&_DAO.TransactOpts, proposalNumber, supportsProposal, justificationText)
}

// DAOChangeOfRulesIterator is returned from FilterChangeOfRules and is used to iterate over the raw logs and unpacked data for ChangeOfRules events raised by the DAO contract.
type DAOChangeOfRulesIterator struct {
	Event *DAOChangeOfRules // Event containing the contract specifics and raw log

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
func (it *DAOChangeOfRulesIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(DAOChangeOfRules)
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
		it.Event = new(DAOChangeOfRules)
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
func (it *DAOChangeOfRulesIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *DAOChangeOfRulesIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// DAOChangeOfRules represents a ChangeOfRules event raised by the DAO contract.
type DAOChangeOfRules struct {
	MinimumQuorum           *big.Int
	DebatingPeriodInMinutes *big.Int
	MajorityMargin          *big.Int
	Raw                     types.Log // Blockchain specific contextual infos
}

// FilterChangeOfRules is a free log retrieval operation binding the contract event 0xa439d3fa452be5e0e1e24a8145e715f4fd8b9c08c96a42fd82a855a85e5d57de.
//
// Solidity: event ChangeOfRules(uint256 minimumQuorum, uint256 debatingPeriodInMinutes, int256 majorityMargin)
func (_DAO *DAOFilterer) FilterChangeOfRules(opts *bind.FilterOpts) (*DAOChangeOfRulesIterator, error) {

	logs, sub, err := _DAO.contract.FilterLogs(opts, "ChangeOfRules")
	if err != nil {
		return nil, err
	}
	return &DAOChangeOfRulesIterator{contract: _DAO.contract, event: "ChangeOfRules", logs: logs, sub: sub}, nil
}

// WatchChangeOfRules is a free log subscription operation binding the contract event 0xa439d3fa452be5e0e1e24a8145e715f4fd8b9c08c96a42fd82a855a85e5d57de.
//
// Solidity: event ChangeOfRules(uint256 minimumQuorum, uint256 debatingPeriodInMinutes, int256 majorityMargin)
func (_DAO *DAOFilterer) WatchChangeOfRules(opts *bind.WatchOpts, sink chan<- *DAOChangeOfRules) (event.Subscription, error) {

	logs, sub, err := _DAO.contract.WatchLogs(opts, "ChangeOfRules")
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(DAOChangeOfRules)
				if err := _DAO.contract.UnpackLog(event, "ChangeOfRules", log); err != nil {
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

// ParseChangeOfRules is a log parse operation binding the contract event 0xa439d3fa452be5e0e1e24a8145e715f4fd8b9c08c96a42fd82a855a85e5d57de.
//
// Solidity: event ChangeOfRules(uint256 minimumQuorum, uint256 debatingPeriodInMinutes, int256 majorityMargin)
func (_DAO *DAOFilterer) ParseChangeOfRules(log types.Log) (*DAOChangeOfRules, error) {
	event := new(DAOChangeOfRules)
	if err := _DAO.contract.UnpackLog(event, "ChangeOfRules", log); err != nil {
		return nil, err
	}
	return event, nil
}

// DAOMembershipChangedIterator is returned from FilterMembershipChanged and is used to iterate over the raw logs and unpacked data for MembershipChanged events raised by the DAO contract.
type DAOMembershipChangedIterator struct {
	Event *DAOMembershipChanged // Event containing the contract specifics and raw log

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
func (it *DAOMembershipChangedIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(DAOMembershipChanged)
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
		it.Event = new(DAOMembershipChanged)
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
func (it *DAOMembershipChangedIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *DAOMembershipChangedIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// DAOMembershipChanged represents a MembershipChanged event raised by the DAO contract.
type DAOMembershipChanged struct {
	Member   common.Address
	IsMember bool
	Raw      types.Log // Blockchain specific contextual infos
}

// FilterMembershipChanged is a free log retrieval operation binding the contract event 0x27b022af4a8347100c7a041ce5ccf8e14d644ff05de696315196faae8cd50c9b.
//
// Solidity: event MembershipChanged(address member, bool isMember)
func (_DAO *DAOFilterer) FilterMembershipChanged(opts *bind.FilterOpts) (*DAOMembershipChangedIterator, error) {

	logs, sub, err := _DAO.contract.FilterLogs(opts, "MembershipChanged")
	if err != nil {
		return nil, err
	}
	return &DAOMembershipChangedIterator{contract: _DAO.contract, event: "MembershipChanged", logs: logs, sub: sub}, nil
}

// WatchMembershipChanged is a free log subscription operation binding the contract event 0x27b022af4a8347100c7a041ce5ccf8e14d644ff05de696315196faae8cd50c9b.
//
// Solidity: event MembershipChanged(address member, bool isMember)
func (_DAO *DAOFilterer) WatchMembershipChanged(opts *bind.WatchOpts, sink chan<- *DAOMembershipChanged) (event.Subscription, error) {

	logs, sub, err := _DAO.contract.WatchLogs(opts, "MembershipChanged")
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(DAOMembershipChanged)
				if err := _DAO.contract.UnpackLog(event, "MembershipChanged", log); err != nil {
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

// ParseMembershipChanged is a log parse operation binding the contract event 0x27b022af4a8347100c7a041ce5ccf8e14d644ff05de696315196faae8cd50c9b.
//
// Solidity: event MembershipChanged(address member, bool isMember)
func (_DAO *DAOFilterer) ParseMembershipChanged(log types.Log) (*DAOMembershipChanged, error) {
	event := new(DAOMembershipChanged)
	if err := _DAO.contract.UnpackLog(event, "MembershipChanged", log); err != nil {
		return nil, err
	}
	return event, nil
}

// DAOProposalAddedIterator is returned from FilterProposalAdded and is used to iterate over the raw logs and unpacked data for ProposalAdded events raised by the DAO contract.
type DAOProposalAddedIterator struct {
	Event *DAOProposalAdded // Event containing the contract specifics and raw log

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
func (it *DAOProposalAddedIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(DAOProposalAdded)
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
		it.Event = new(DAOProposalAdded)
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
func (it *DAOProposalAddedIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *DAOProposalAddedIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// DAOProposalAdded represents a ProposalAdded event raised by the DAO contract.
type DAOProposalAdded struct {
	ProposalID  *big.Int
	Recipient   common.Address
	Amount      *big.Int
	Description string
	Raw         types.Log // Blockchain specific contextual infos
}

// FilterProposalAdded is a free log retrieval operation binding the contract event 0x646fec02522b41e7125cfc859a64fd4f4cefd5dc3b6237ca0abe251ded1fa881.
//
// Solidity: event ProposalAdded(uint256 proposalID, address recipient, uint256 amount, string description)
func (_DAO *DAOFilterer) FilterProposalAdded(opts *bind.FilterOpts) (*DAOProposalAddedIterator, error) {

	logs, sub, err := _DAO.contract.FilterLogs(opts, "ProposalAdded")
	if err != nil {
		return nil, err
	}
	return &DAOProposalAddedIterator{contract: _DAO.contract, event: "ProposalAdded", logs: logs, sub: sub}, nil
}

// WatchProposalAdded is a free log subscription operation binding the contract event 0x646fec02522b41e7125cfc859a64fd4f4cefd5dc3b6237ca0abe251ded1fa881.
//
// Solidity: event ProposalAdded(uint256 proposalID, address recipient, uint256 amount, string description)
func (_DAO *DAOFilterer) WatchProposalAdded(opts *bind.WatchOpts, sink chan<- *DAOProposalAdded) (event.Subscription, error) {

	logs, sub, err := _DAO.contract.WatchLogs(opts, "ProposalAdded")
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(DAOProposalAdded)
				if err := _DAO.contract.UnpackLog(event, "ProposalAdded", log); err != nil {
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

// ParseProposalAdded is a log parse operation binding the contract event 0x646fec02522b41e7125cfc859a64fd4f4cefd5dc3b6237ca0abe251ded1fa881.
//
// Solidity: event ProposalAdded(uint256 proposalID, address recipient, uint256 amount, string description)
func (_DAO *DAOFilterer) ParseProposalAdded(log types.Log) (*DAOProposalAdded, error) {
	event := new(DAOProposalAdded)
	if err := _DAO.contract.UnpackLog(event, "ProposalAdded", log); err != nil {
		return nil, err
	}
	return event, nil
}

// DAOProposalTalliedIterator is returned from FilterProposalTallied and is used to iterate over the raw logs and unpacked data for ProposalTallied events raised by the DAO contract.
type DAOProposalTalliedIterator struct {
	Event *DAOProposalTallied // Event containing the contract specifics and raw log

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
func (it *DAOProposalTalliedIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(DAOProposalTallied)
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
		it.Event = new(DAOProposalTallied)
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
func (it *DAOProposalTalliedIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *DAOProposalTalliedIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// DAOProposalTallied represents a ProposalTallied event raised by the DAO contract.
type DAOProposalTallied struct {
	ProposalID *big.Int
	Result     *big.Int
	Quorum     *big.Int
	Active     bool
	Raw        types.Log // Blockchain specific contextual infos
}

// FilterProposalTallied is a free log retrieval operation binding the contract event 0xd220b7272a8b6d0d7d6bcdace67b936a8f175e6d5c1b3ee438b72256b32ab3af.
//
// Solidity: event ProposalTallied(uint256 proposalID, int256 result, uint256 quorum, bool active)
func (_DAO *DAOFilterer) FilterProposalTallied(opts *bind.FilterOpts) (*DAOProposalTalliedIterator, error) {

	logs, sub, err := _DAO.contract.FilterLogs(opts, "ProposalTallied")
	if err != nil {
		return nil, err
	}
	return &DAOProposalTalliedIterator{contract: _DAO.contract, event: "ProposalTallied", logs: logs, sub: sub}, nil
}

// WatchProposalTallied is a free log subscription operation binding the contract event 0xd220b7272a8b6d0d7d6bcdace67b936a8f175e6d5c1b3ee438b72256b32ab3af.
//
// Solidity: event ProposalTallied(uint256 proposalID, int256 result, uint256 quorum, bool active)
func (_DAO *DAOFilterer) WatchProposalTallied(opts *bind.WatchOpts, sink chan<- *DAOProposalTallied) (event.Subscription, error) {

	logs, sub, err := _DAO.contract.WatchLogs(opts, "ProposalTallied")
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(DAOProposalTallied)
				if err := _DAO.contract.UnpackLog(event, "ProposalTallied", log); err != nil {
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

// ParseProposalTallied is a log parse operation binding the contract event 0xd220b7272a8b6d0d7d6bcdace67b936a8f175e6d5c1b3ee438b72256b32ab3af.
//
// Solidity: event ProposalTallied(uint256 proposalID, int256 result, uint256 quorum, bool active)
func (_DAO *DAOFilterer) ParseProposalTallied(log types.Log) (*DAOProposalTallied, error) {
	event := new(DAOProposalTallied)
	if err := _DAO.contract.UnpackLog(event, "ProposalTallied", log); err != nil {
		return nil, err
	}
	return event, nil
}

// DAOVotedIterator is returned from FilterVoted and is used to iterate over the raw logs and unpacked data for Voted events raised by the DAO contract.
type DAOVotedIterator struct {
	Event *DAOVoted // Event containing the contract specifics and raw log

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
func (it *DAOVotedIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(DAOVoted)
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
		it.Event = new(DAOVoted)
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
func (it *DAOVotedIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *DAOVotedIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// DAOVoted represents a Voted event raised by the DAO contract.
type DAOVoted struct {
	ProposalID    *big.Int
	Position      bool
	Voter         common.Address
	Justification string
	Raw           types.Log // Blockchain specific contextual infos
}

// FilterVoted is a free log retrieval operation binding the contract event 0xc34f869b7ff431b034b7b9aea9822dac189a685e0b015c7d1be3add3f89128e8.
//
// Solidity: event Voted(uint256 proposalID, bool position, address voter, string justification)
func (_DAO *DAOFilterer) FilterVoted(opts *bind.FilterOpts) (*DAOVotedIterator, error) {

	logs, sub, err := _DAO.contract.FilterLogs(opts, "Voted")
	if err != nil {
		return nil, err
	}
	return &DAOVotedIterator{contract: _DAO.contract, event: "Voted", logs: logs, sub: sub}, nil
}

// WatchVoted is a free log subscription operation binding the contract event 0xc34f869b7ff431b034b7b9aea9822dac189a685e0b015c7d1be3add3f89128e8.
//
// Solidity: event Voted(uint256 proposalID, bool position, address voter, string justification)
func (_DAO *DAOFilterer) WatchVoted(opts *bind.WatchOpts, sink chan<- *DAOVoted) (event.Subscription, error) {

	logs, sub, err := _DAO.contract.WatchLogs(opts, "Voted")
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(DAOVoted)
				if err := _DAO.contract.UnpackLog(event, "Voted", log); err != nil {
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

// ParseVoted is a log parse operation binding the contract event 0xc34f869b7ff431b034b7b9aea9822dac189a685e0b015c7d1be3add3f89128e8.
//
// Solidity: event Voted(uint256 proposalID, bool position, address voter, string justification)
func (_DAO *DAOFilterer) ParseVoted(log types.Log) (*DAOVoted, error) {
	event := new(DAOVoted)
	if err := _DAO.contract.UnpackLog(event, "Voted", log); err != nil {
		return nil, err
	}
	return event, nil
}
