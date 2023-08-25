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

// RootReceiverABI is the input ABI used to generate the binding from.
const RootReceiverABI = "[{\"inputs\":[{\"internalType\":\"address\",\"name\":\"_checkpointManager\",\"type\":\"address\"}],\"stateMutability\":\"nonpayable\",\"type\":\"constructor\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"internalType\":\"address\",\"name\":\"_source\",\"type\":\"address\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"_amount\",\"type\":\"uint256\"}],\"name\":\"received\",\"type\":\"event\"},{\"inputs\":[],\"name\":\"SEND_MESSAGE_EVENT_SIG\",\"outputs\":[{\"internalType\":\"bytes32\",\"name\":\"\",\"type\":\"bytes32\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"checkpointManager\",\"outputs\":[{\"internalType\":\"contractICheckpointManager\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"bytes32\",\"name\":\"\",\"type\":\"bytes32\"}],\"name\":\"processedExits\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"bytes\",\"name\":\"inputData\",\"type\":\"bytes\"}],\"name\":\"receiveMessage\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"name\":\"senders\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"}]"

// RootReceiverBin is the compiled bytecode used for deploying new contracts.
var RootReceiverBin = "0x608060405234801561001057600080fd5b50604051611ed1380380611ed183398101604081905261002f91610054565b600080546001600160a01b0319166001600160a01b0392909216919091179055610084565b60006020828403121561006657600080fd5b81516001600160a01b038116811461007d57600080fd5b9392505050565b611e3e806100936000396000f3fe608060405234801561001057600080fd5b50600436106100575760003560e01c80630e387de61461005c578063607f2d4214610096578063982fb9d8146100c9578063c0857ba0146100e9578063f953cec714610114575b600080fd5b6100837f8c5261668696ce22758910d05bab8f186d6eb247ceac2af2e82c7dc17669b03681565b6040519081526020015b60405180910390f35b6100b96100a436600461196a565b60016020526000908152604090205460ff1681565b604051901515815260200161008d565b6100836100d736600461199b565b60026020526000908152604090205481565b6000546100fc906001600160a01b031681565b6040516001600160a01b03909116815260200161008d565b610127610122366004611a25565b610129565b005b60008061013583610148565b9150915061014382826103cf565b505050565b600060606000610157846104bb565b905060006101648261051a565b9050600061017183610549565b905060008161017f84610572565b6101888661072e565b60405160200161019a93929190611ac8565b60408051601f1981840301815291815281516020928301206000818152600190935291205490915060ff16156102235760405162461bcd60e51b8152602060048201526024808201527f4678526f6f7454756e6e656c3a20455849545f414c52454144595f50524f434560448201526314d4d15160e21b60648201526084015b60405180910390fd5b60008181526001602081905260408220805460ff191690911790556102478561074a565b9050600061025482610893565b9050600061026187610923565b9050610281610271846020015190565b8761027b8a61093f565b8461095b565b6102d95760405162461bcd60e51b815260206004820152602360248201527f4678526f6f7454756e6e656c3a20494e56414c49445f524543454950545f505260448201526227a7a360e91b606482015260840161021a565b610307856102e689610c28565b6102ef8a610c44565b846102f98c610c60565b6103028d610c7c565b610c98565b600061031283610db2565b90507f8c5261668696ce22758910d05bab8f186d6eb247ceac2af2e82c7dc17669b036610348610343836000610dee565b610e26565b146103955760405162461bcd60e51b815260206004820152601f60248201527f4678526f6f7454756e6e656c3a20494e56414c49445f5349474e415455524500604482015260640161021a565b60006103a084610ea1565b8060200190518101906103b39190611af5565b90506103be84610ebd565b9c909b509950505050505050505050565b6000806000838060200190518101906103e89190611b6b565b919450925090506001600160a01b038316301461043a5760405162461bcd60e51b815260206004820152601060248201526f24b73b30b634b2103932b1b2b4bb32b960811b604482015260640161021a565b6001600160a01b03821660009081526002602052604090205461045d8282611bc4565b6001600160a01b0384166000818152600260209081526040918290209390935580519182529181018490527ff11e547d796cc64acdf758e7cee90439494fd886a19159454aa61e473fdbafef910160405180910390a1505050505050565b60408051602081019091526060815260006105056105008460408051808201825260008082526020918201528151808301909252825182529182019181019190915290565b610ee6565b60408051602081019091529081529392505050565b6060610543826000015160088151811061053657610536611bd7565b6020026020010151610ffb565b92915050565b6000610543826000015160028151811061056557610565611bd7565b6020026020010151610e26565b604080516020810190915260008152815160609190156105435760008061059a600086611097565b60f81c905060018114806105b157508060ff166003145b15610658576001855160026105c69190611bed565b6105d09190611c04565b6001600160401b038111156105e7576105e76119b8565b6040519080825280601f01601f191660200182016040528015610611576020820181803683370190505b5092506000610621600187611097565b9050808460008151811061063757610637611bd7565b60200101906001600160f81b031916908160001a90535060019250506106bb565b6002855160026106689190611bed565b6106729190611c04565b6001600160401b03811115610689576106896119b8565b6040519080825280601f01601f1916602001820160405280156106b3576020820181803683370190505b509250600091505b60ff82165b8351811015610725576106ea6106d960ff851683611c04565b6106e4906002611bc4565b87611097565b8482815181106106fc576106fc611bd7565b60200101906001600160f81b031916908160001a9053508061071d81611c17565b9150506106c0565b50505092915050565b6000610543826000015160098151811061056557610565611bd7565b61076e60405180606001604052806060815260200160608152602001600081525090565b610788826000015160068151811061053657610536611bd7565b6020828101829052604080518082018252600080825290830152805180820190915282518152918101908201526107be81611118565b156107d3576107cc81610ee6565b825261087f565b602082015180516000906107e990600190611c04565b6001600160401b03811115610800576108006119b8565b6040519080825280601f01601f19166020018201604052801561082a576020820181803683370190505b50905060008083602101915082602001905061084882828551611153565b60408051808201825260008082526020918201528151808301909252845182528085019082015261087890610ee6565b8652505050505b6108888361072e565b604083015250919050565b6040805160808101825260009181018281526060808301939093528152602081019190915260006108e183600001516003815181106108d4576108d4611bd7565b6020026020010151610ee6565b8360400151815181106108f6576108f6611bd7565b60200260200101519050604051806040016040528082815260200161091a83610ee6565b90529392505050565b6000610543826000015160058151811061056557610565611bd7565b6060610543826000015160078151811061053657610536611bd7565b60008061098f8460408051808201825260008082526020918201528151808301909252825182529182019181019190915290565b9050600061099c826111de565b9050606080856000806109ae8b610572565b905080516000036109c9576000975050505050505050610c20565b60005b8651811015610c175781518311156109ef57600098505050505050505050610c20565b610a11878281518110610a0457610a04611bd7565b60200260200101516112e8565b955085805190602001208414610a3257600098505050505050505050610c20565b610a54878281518110610a4757610a47611bd7565b60200260200101516111de565b94508451601103610b335781518303610ac0578c80519060200120610a9286601081518110610a8557610a85611bd7565b6020026020010151611366565b8051906020012003610aaf57600198505050505050505050610c20565b600098505050505050505050610c20565b6000828481518110610ad457610ad4611bd7565b016020015160f81c90506010811115610af95760009950505050505050505050610c20565b610b1e868260ff1681518110610b1157610b11611bd7565b6020026020010151611402565b9450610b2b600185611bc4565b935050610c05565b8451600203610aaf576000610b5e610b5787600081518110610a8557610a85611bd7565b8486611430565b8351909150610b6d8286611bc4565b03610bc0578d80519060200120610b9087600181518110610a8557610a85611bd7565b8051906020012003610bae5760019950505050505050505050610c20565b60009950505050505050505050610c20565b80600003610bda5760009950505050505050505050610c20565b610be48185611bc4565b9350610bfc86600181518110610b1157610b11611bd7565b9450610c059050565b80610c0f81611c17565b9150506109cc565b50505050505050505b949350505050565b6000610543826000015160038151811061056557610565611bd7565b6000610543826000015160048151811061056557610565611bd7565b6000610543826000015160008151811061056557610565611bd7565b6060610543826000015160018151811061053657610536611bd7565b600080546040516320a9cea560e11b81526004810185905282916001600160a01b0316906341539d4a9060240160a060405180830381865afa158015610ce2573d6000803e3d6000fd5b505050506040513d601f19601f82011682018060405250810190610d069190611c30565b50505091509150610d5c8189610d1c9190611c04565b6040805160208082018d90528183018c9052606082018b905260808083018b90528351808403909101815260a09092019092528051910120908486611537565b610da85760405162461bcd60e51b815260206004820152601c60248201527f4678526f6f7454756e6e656c3a20494e56414c49445f48454144455200000000604482015260640161021a565b5050505050505050565b6040805160208101909152606081526040518060200160405280610de684602001516001815181106108d4576108d4611bd7565b905292915050565b60408051808201909152600080825260208201528251805183908110610e1657610e16611bd7565b6020026020010151905092915050565b805160009015801590610e3b57508151602110155b610e4457600080fd5b6000610e53836020015161169f565b90506000818460000151610e679190611c04565b9050600080838660200151610e7c9190611bc4565b9050805191506020831015610e9857826020036101000a820491505b50949350505050565b6060610543826020015160028151811061053657610536611bd7565b60006105438260200151600081518110610ed957610ed9611bd7565b6020026020010151611721565b6060610ef182611118565b610efa57600080fd5b6000610f058361173b565b90506000816001600160401b03811115610f2157610f216119b8565b604051908082528060200260200182016040528015610f6657816020015b6040805180820190915260008082526020820152815260200190600190039081610f3f5790505b5090506000610f78856020015161169f565b8560200151610f879190611bc4565b90506000805b84811015610ff057610f9e836117c0565b9150604051806040016040528083815260200184815250848281518110610fc757610fc7611bd7565b6020908102919091010152610fdc8284611bc4565b925080610fe881611c17565b915050610f8d565b509195945050505050565b805160609061100957600080fd5b6000611018836020015161169f565b9050600081846000015161102c9190611c04565b90506000816001600160401b03811115611048576110486119b8565b6040519080825280601f01601f191660200182016040528015611072576020820181803683370190505b5090506000816020019050610e988487602001516110909190611bc4565b8285611864565b60006110a4600284611c93565b156110de576010826110b7600286611ca7565b815181106110c7576110c7611bd7565b01602001516110d9919060f81c611cbb565b61110e565b6010826110ec600286611ca7565b815181106110fc576110fc611bd7565b016020015161110e919060f81c611cdd565b60f81b9392505050565b8051600090810361112b57506000919050565b6020820151805160001a9060c0821015611149575060009392505050565b5060019392505050565b8060000361116057505050565b602081106111985782518252611177602084611bc4565b9250611184602083611bc4565b9150611191602082611c04565b9050611160565b806000036111a557505050565b600060016111b4836020611c04565b6111c090610100611de3565b6111ca9190611c04565b935183518516941916939093179091525050565b60606111e982611118565b6111f257600080fd5b60006111fd836118a9565b90506000816001600160401b03811115611219576112196119b8565b60405190808252806020026020018201604052801561125e57816020015b60408051808201909152600080825260208201528152602001906001900390816112375790505b5090506000611270856020015161169f565b856020015161127f9190611bc4565b90506000805b84811015610ff057611296836117c0565b91506040518060400160405280838152602001848152508482815181106112bf576112bf611bd7565b60209081029190910101526112d48284611bc4565b9250806112e081611c17565b915050611285565b6060600082600001516001600160401b03811115611308576113086119b8565b6040519080825280601f01601f191660200182016040528015611332576020820181803683370190505b50905080516000036113445792915050565b600081602001905061135f8460200151828660000151611925565b5092915050565b805160609061137457600080fd5b6000611383836020015161169f565b905060008184600001516113979190611c04565b90506000816001600160401b038111156113b3576113b36119b8565b6040519080825280601f01601f1916602001820160405280156113dd576020820181803683370190505b5090506000816020019050610e988487602001516113fb9190611bc4565b8285611925565b805160009060211461141357600080fd5b600080836020015160016114279190611bc4565b51949350505050565b6000808061143d86610572565b9050600081516001600160401b0381111561145a5761145a6119b8565b6040519080825280601f01601f191660200182016040528015611484576020820181803683370190505b509050845b82516114959087611bc4565b8110156115085760008782815181106114b0576114b0611bd7565b01602001516001600160f81b031916905080836114cd8985611c04565b815181106114dd576114dd611bd7565b60200101906001600160f81b031916908160001a90535050808061150090611c17565b915050611489565b508080519060200120828051906020012003611527578151925061152c565b600092505b509095945050505050565b6000602082516115479190611c93565b1561158b5760405162461bcd60e51b8152602060048201526014602482015273092dcecc2d8d2c840e0e4dedecc40d8cadccee8d60631b604482015260640161021a565b60006020835161159b9190611ca7565b90506115a8816002611de3565b85106115ee5760405162461bcd60e51b81526020600482015260156024820152744c65616620696e64657820697320746f6f2062696760581b604482015260640161021a565b60008660205b855181116116915785810151925061160d600289611c93565b600003611645576040805160208101849052908101849052606001604051602081830303815290604052805190602001209150611672565b60408051602081018590529081018390526060016040516020818303038152906040528051906020012091505b61167d600289611ca7565b975061168a602082611bc4565b90506115f4565b509094149695505050505050565b8051600090811a60808110156116b85750600092915050565b60b88110806116d3575060c081108015906116d3575060f881105b156116e15750600192915050565b60c0811015611715576116f6600160b8611def565b6117039060ff1682611c04565b61170e906001611bc4565b9392505050565b6116f6600160f8611def565b805160009060151461173257600080fd5b61054382610e26565b8051600090810361174e57506000919050565b60008061175e846020015161169f565b846020015161176d9190611bc4565b90506000846000015185602001516117859190611bc4565b90505b808210156117b757611799826117c0565b6117a39083611bc4565b9150826117af81611c17565b935050611788565b50909392505050565b80516000908190811a60808110156117db576001915061135f565b60b8811015611801576117ef608082611c04565b6117fa906001611bc4565b915061135f565b60c081101561182e5760b78103600185019450806020036101000a8551046001820181019350505061135f565b60f8811015611842576117ef60c082611c04565b60019390930151602084900360f7016101000a900490920160f5190192915050565b8060000361187157505050565b602081106111985782518252611888602084611bc4565b9250611895602083611bc4565b91506118a2602082611c04565b9050611871565b805160009081036118bc57506000919050565b6000806118cc846020015161169f565b84602001516118db9190611bc4565b90506000846000015185602001516118f39190611bc4565b90505b808210156117b757611907826117c0565b6119119083611bc4565b91508261191d81611c17565b9350506118f6565b8060000361193257505050565b602081106111985782518252611949602084611bc4565b9250611956602083611bc4565b9150611963602082611c04565b9050611932565b60006020828403121561197c57600080fd5b5035919050565b6001600160a01b038116811461199857600080fd5b50565b6000602082840312156119ad57600080fd5b813561170e81611983565b634e487b7160e01b600052604160045260246000fd5b604051601f8201601f191681016001600160401b03811182821017156119f6576119f66119b8565b604052919050565b60006001600160401b03821115611a1757611a176119b8565b50601f01601f191660200190565b600060208284031215611a3757600080fd5b81356001600160401b03811115611a4d57600080fd5b8201601f81018413611a5e57600080fd5b8035611a71611a6c826119fe565b6119ce565b818152856020838501011115611a8657600080fd5b81602084016020830137600091810160200191909152949350505050565b60005b83811015611abf578181015183820152602001611aa7565b50506000910152565b83815260008351611ae0816020850160208801611aa4565b60209201918201929092526040019392505050565b600060208284031215611b0757600080fd5b81516001600160401b03811115611b1d57600080fd5b8201601f81018413611b2e57600080fd5b8051611b3c611a6c826119fe565b818152856020838501011115611b5157600080fd5b611b62826020830160208601611aa4565b95945050505050565b600080600060608486031215611b8057600080fd5b8351611b8b81611983565b6020850151909350611b9c81611983565b80925050604084015190509250925092565b634e487b7160e01b600052601160045260246000fd5b8082018082111561054357610543611bae565b634e487b7160e01b600052603260045260246000fd5b808202811582820484141761054357610543611bae565b8181038181111561054357610543611bae565b600060018201611c2957611c29611bae565b5060010190565b600080600080600060a08688031215611c4857600080fd5b855194506020860151935060408601519250606086015191506080860151611c6f81611983565b809150509295509295909350565b634e487b7160e01b600052601260045260246000fd5b600082611ca257611ca2611c7d565b500690565b600082611cb657611cb6611c7d565b500490565b600060ff831680611cce57611cce611c7d565b8060ff84160691505092915050565b600060ff831680611cf057611cf0611c7d565b8060ff84160491505092915050565b600181815b80851115611d3a578160001904821115611d2057611d20611bae565b80851615611d2d57918102915b93841c9390800290611d04565b509250929050565b600082611d5157506001610543565b81611d5e57506000610543565b8160018114611d745760028114611d7e57611d9a565b6001915050610543565b60ff841115611d8f57611d8f611bae565b50506001821b610543565b5060208310610133831016604e8410600b8410161715611dbd575081810a610543565b611dc78383611cff565b8060001904821115611ddb57611ddb611bae565b029392505050565b600061170e8383611d42565b60ff828116828216039081111561054357610543611bae56fea2646970667358221220a924e520bf4f9d5629bc95702236e2702455bf9b57c4e9e4e344c7c7d7576a2b64736f6c63430008140033"

// DeployRootReceiver deploys a new Ethereum contract, binding an instance of RootReceiver to it.
func DeployRootReceiver(auth *bind.TransactOpts, backend bind.ContractBackend, _checkpointManager libcommon.Address) (libcommon.Address, types.Transaction, *RootReceiver, error) {
	parsed, err := abi.JSON(strings.NewReader(RootReceiverABI))
	if err != nil {
		return libcommon.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, libcommon.FromHex(RootReceiverBin), backend, _checkpointManager)
	if err != nil {
		return libcommon.Address{}, nil, nil, err
	}
	return address, tx, &RootReceiver{RootReceiverCaller: RootReceiverCaller{contract: contract}, RootReceiverTransactor: RootReceiverTransactor{contract: contract}, RootReceiverFilterer: RootReceiverFilterer{contract: contract}}, nil
}

// RootReceiver is an auto generated Go binding around an Ethereum contract.
type RootReceiver struct {
	RootReceiverCaller     // Read-only binding to the contract
	RootReceiverTransactor // Write-only binding to the contract
	RootReceiverFilterer   // Log filterer for contract events
}

// RootReceiverCaller is an auto generated read-only Go binding around an Ethereum contract.
type RootReceiverCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// RootReceiverTransactor is an auto generated write-only Go binding around an Ethereum contract.
type RootReceiverTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// RootReceiverFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type RootReceiverFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// RootReceiverSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type RootReceiverSession struct {
	Contract     *RootReceiver     // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// RootReceiverCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type RootReceiverCallerSession struct {
	Contract *RootReceiverCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts       // Call options to use throughout this session
}

// RootReceiverTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type RootReceiverTransactorSession struct {
	Contract     *RootReceiverTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts       // Transaction auth options to use throughout this session
}

// RootReceiverRaw is an auto generated low-level Go binding around an Ethereum contract.
type RootReceiverRaw struct {
	Contract *RootReceiver // Generic contract binding to access the raw methods on
}

// RootReceiverCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type RootReceiverCallerRaw struct {
	Contract *RootReceiverCaller // Generic read-only contract binding to access the raw methods on
}

// RootReceiverTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type RootReceiverTransactorRaw struct {
	Contract *RootReceiverTransactor // Generic write-only contract binding to access the raw methods on
}

// NewRootReceiver creates a new instance of RootReceiver, bound to a specific deployed contract.
func NewRootReceiver(address libcommon.Address, backend bind.ContractBackend) (*RootReceiver, error) {
	contract, err := bindRootReceiver(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &RootReceiver{RootReceiverCaller: RootReceiverCaller{contract: contract}, RootReceiverTransactor: RootReceiverTransactor{contract: contract}, RootReceiverFilterer: RootReceiverFilterer{contract: contract}}, nil
}

// NewRootReceiverCaller creates a new read-only instance of RootReceiver, bound to a specific deployed contract.
func NewRootReceiverCaller(address libcommon.Address, caller bind.ContractCaller) (*RootReceiverCaller, error) {
	contract, err := bindRootReceiver(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &RootReceiverCaller{contract: contract}, nil
}

// NewRootReceiverTransactor creates a new write-only instance of RootReceiver, bound to a specific deployed contract.
func NewRootReceiverTransactor(address libcommon.Address, transactor bind.ContractTransactor) (*RootReceiverTransactor, error) {
	contract, err := bindRootReceiver(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &RootReceiverTransactor{contract: contract}, nil
}

// NewRootReceiverFilterer creates a new log filterer instance of RootReceiver, bound to a specific deployed contract.
func NewRootReceiverFilterer(address libcommon.Address, filterer bind.ContractFilterer) (*RootReceiverFilterer, error) {
	contract, err := bindRootReceiver(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &RootReceiverFilterer{contract: contract}, nil
}

// bindRootReceiver binds a generic wrapper to an already deployed contract.
func bindRootReceiver(address libcommon.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(RootReceiverABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_RootReceiver *RootReceiverRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _RootReceiver.Contract.RootReceiverCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_RootReceiver *RootReceiverRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _RootReceiver.Contract.RootReceiverTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_RootReceiver *RootReceiverRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _RootReceiver.Contract.RootReceiverTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_RootReceiver *RootReceiverCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _RootReceiver.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_RootReceiver *RootReceiverTransactorRaw) Transfer(opts *bind.TransactOpts) (types.Transaction, error) {
	return _RootReceiver.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_RootReceiver *RootReceiverTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (types.Transaction, error) {
	return _RootReceiver.Contract.contract.Transact(opts, method, params...)
}

// SENDMESSAGEEVENTSIG is a free data retrieval call binding the contract method 0x0e387de6.
//
// Solidity: function SEND_MESSAGE_EVENT_SIG() view returns(bytes32)
func (_RootReceiver *RootReceiverCaller) SENDMESSAGEEVENTSIG(opts *bind.CallOpts) ([32]byte, error) {
	var out []interface{}
	err := _RootReceiver.contract.Call(opts, &out, "SEND_MESSAGE_EVENT_SIG")

	if err != nil {
		return *new([32]byte), err
	}

	out0 := *abi.ConvertType(out[0], new([32]byte)).(*[32]byte)

	return out0, err

}

// SENDMESSAGEEVENTSIG is a free data retrieval call binding the contract method 0x0e387de6.
//
// Solidity: function SEND_MESSAGE_EVENT_SIG() view returns(bytes32)
func (_RootReceiver *RootReceiverSession) SENDMESSAGEEVENTSIG() ([32]byte, error) {
	return _RootReceiver.Contract.SENDMESSAGEEVENTSIG(&_RootReceiver.CallOpts)
}

// SENDMESSAGEEVENTSIG is a free data retrieval call binding the contract method 0x0e387de6.
//
// Solidity: function SEND_MESSAGE_EVENT_SIG() view returns(bytes32)
func (_RootReceiver *RootReceiverCallerSession) SENDMESSAGEEVENTSIG() ([32]byte, error) {
	return _RootReceiver.Contract.SENDMESSAGEEVENTSIG(&_RootReceiver.CallOpts)
}

// CheckpointManager is a free data retrieval call binding the contract method 0xc0857ba0.
//
// Solidity: function checkpointManager() view returns(address)
func (_RootReceiver *RootReceiverCaller) CheckpointManager(opts *bind.CallOpts) (libcommon.Address, error) {
	var out []interface{}
	err := _RootReceiver.contract.Call(opts, &out, "checkpointManager")

	if err != nil {
		return *new(libcommon.Address), err
	}

	out0 := *abi.ConvertType(out[0], new(libcommon.Address)).(*libcommon.Address)

	return out0, err

}

// CheckpointManager is a free data retrieval call binding the contract method 0xc0857ba0.
//
// Solidity: function checkpointManager() view returns(address)
func (_RootReceiver *RootReceiverSession) CheckpointManager() (libcommon.Address, error) {
	return _RootReceiver.Contract.CheckpointManager(&_RootReceiver.CallOpts)
}

// CheckpointManager is a free data retrieval call binding the contract method 0xc0857ba0.
//
// Solidity: function checkpointManager() view returns(address)
func (_RootReceiver *RootReceiverCallerSession) CheckpointManager() (libcommon.Address, error) {
	return _RootReceiver.Contract.CheckpointManager(&_RootReceiver.CallOpts)
}

// ProcessedExits is a free data retrieval call binding the contract method 0x607f2d42.
//
// Solidity: function processedExits(bytes32 ) view returns(bool)
func (_RootReceiver *RootReceiverCaller) ProcessedExits(opts *bind.CallOpts, arg0 [32]byte) (bool, error) {
	var out []interface{}
	err := _RootReceiver.contract.Call(opts, &out, "processedExits", arg0)

	if err != nil {
		return *new(bool), err
	}

	out0 := *abi.ConvertType(out[0], new(bool)).(*bool)

	return out0, err

}

// ProcessedExits is a free data retrieval call binding the contract method 0x607f2d42.
//
// Solidity: function processedExits(bytes32 ) view returns(bool)
func (_RootReceiver *RootReceiverSession) ProcessedExits(arg0 [32]byte) (bool, error) {
	return _RootReceiver.Contract.ProcessedExits(&_RootReceiver.CallOpts, arg0)
}

// ProcessedExits is a free data retrieval call binding the contract method 0x607f2d42.
//
// Solidity: function processedExits(bytes32 ) view returns(bool)
func (_RootReceiver *RootReceiverCallerSession) ProcessedExits(arg0 [32]byte) (bool, error) {
	return _RootReceiver.Contract.ProcessedExits(&_RootReceiver.CallOpts, arg0)
}

// Senders is a free data retrieval call binding the contract method 0x982fb9d8.
//
// Solidity: function senders(address ) view returns(uint256)
func (_RootReceiver *RootReceiverCaller) Senders(opts *bind.CallOpts, arg0 libcommon.Address) (*big.Int, error) {
	var out []interface{}
	err := _RootReceiver.contract.Call(opts, &out, "senders", arg0)

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// Senders is a free data retrieval call binding the contract method 0x982fb9d8.
//
// Solidity: function senders(address ) view returns(uint256)
func (_RootReceiver *RootReceiverSession) Senders(arg0 libcommon.Address) (*big.Int, error) {
	return _RootReceiver.Contract.Senders(&_RootReceiver.CallOpts, arg0)
}

// Senders is a free data retrieval call binding the contract method 0x982fb9d8.
//
// Solidity: function senders(address ) view returns(uint256)
func (_RootReceiver *RootReceiverCallerSession) Senders(arg0 libcommon.Address) (*big.Int, error) {
	return _RootReceiver.Contract.Senders(&_RootReceiver.CallOpts, arg0)
}

// ReceiveMessage is a paid mutator transaction binding the contract method 0xf953cec7.
//
// Solidity: function receiveMessage(bytes inputData) returns()
func (_RootReceiver *RootReceiverTransactor) ReceiveMessage(opts *bind.TransactOpts, inputData []byte) (types.Transaction, error) {
	return _RootReceiver.contract.Transact(opts, "receiveMessage", inputData)
}

// ReceiveMessage is a paid mutator transaction binding the contract method 0xf953cec7.
//
// Solidity: function receiveMessage(bytes inputData) returns()
func (_RootReceiver *RootReceiverSession) ReceiveMessage(inputData []byte) (types.Transaction, error) {
	return _RootReceiver.Contract.ReceiveMessage(&_RootReceiver.TransactOpts, inputData)
}

// ReceiveMessage is a paid mutator transaction binding the contract method 0xf953cec7.
//
// Solidity: function receiveMessage(bytes inputData) returns()
func (_RootReceiver *RootReceiverTransactorSession) ReceiveMessage(inputData []byte) (types.Transaction, error) {
	return _RootReceiver.Contract.ReceiveMessage(&_RootReceiver.TransactOpts, inputData)
}

// ReceiveMessageParams is an auto generated read-only Go binding of transcaction calldata params
type ReceiveMessageParams struct {
	Param_inputData []byte
}

// Parse ReceiveMessage method from calldata of a transaction
//
// Solidity: function receiveMessage(bytes inputData) returns()
func ParseReceiveMessage(calldata []byte) (*ReceiveMessageParams, error) {
	if len(calldata) <= 4 {
		return nil, fmt.Errorf("invalid calldata input")
	}

	_abi, err := abi.JSON(strings.NewReader(RootReceiverABI))
	if err != nil {
		return nil, fmt.Errorf("failed to get abi of registry metadata: %w", err)
	}

	out, err := _abi.Methods["receiveMessage"].Inputs.Unpack(calldata[4:])
	if err != nil {
		return nil, fmt.Errorf("failed to unpack receiveMessage params data: %w", err)
	}

	var paramsResult = new(ReceiveMessageParams)
	value := reflect.ValueOf(paramsResult).Elem()

	if value.NumField() != len(out) {
		return nil, fmt.Errorf("failed to match calldata with param field number")
	}

	out0 := *abi.ConvertType(out[0], new([]byte)).(*[]byte)

	return &ReceiveMessageParams{
		Param_inputData: out0,
	}, nil
}

// RootReceiverReceivedIterator is returned from FilterReceived and is used to iterate over the raw logs and unpacked data for Received events raised by the RootReceiver contract.
type RootReceiverReceivedIterator struct {
	Event *RootReceiverReceived // Event containing the contract specifics and raw log

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
func (it *RootReceiverReceivedIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(RootReceiverReceived)
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
		it.Event = new(RootReceiverReceived)
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
func (it *RootReceiverReceivedIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *RootReceiverReceivedIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// RootReceiverReceived represents a Received event raised by the RootReceiver contract.
type RootReceiverReceived struct {
	Source libcommon.Address
	Amount *big.Int
	Raw    types.Log // Blockchain specific contextual infos
}

// FilterReceived is a free log retrieval operation binding the contract event 0xf11e547d796cc64acdf758e7cee90439494fd886a19159454aa61e473fdbafef.
//
// Solidity: event received(address _source, uint256 _amount)
func (_RootReceiver *RootReceiverFilterer) FilterReceived(opts *bind.FilterOpts) (*RootReceiverReceivedIterator, error) {

	logs, sub, err := _RootReceiver.contract.FilterLogs(opts, "received")
	if err != nil {
		return nil, err
	}
	return &RootReceiverReceivedIterator{contract: _RootReceiver.contract, event: "received", logs: logs, sub: sub}, nil
}

// WatchReceived is a free log subscription operation binding the contract event 0xf11e547d796cc64acdf758e7cee90439494fd886a19159454aa61e473fdbafef.
//
// Solidity: event received(address _source, uint256 _amount)
func (_RootReceiver *RootReceiverFilterer) WatchReceived(opts *bind.WatchOpts, sink chan<- *RootReceiverReceived) (event.Subscription, error) {

	logs, sub, err := _RootReceiver.contract.WatchLogs(opts, "received")
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(RootReceiverReceived)
				if err := _RootReceiver.contract.UnpackLog(event, "received", log); err != nil {
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

// ParseReceived is a log parse operation binding the contract event 0xf11e547d796cc64acdf758e7cee90439494fd886a19159454aa61e473fdbafef.
//
// Solidity: event received(address _source, uint256 _amount)
func (_RootReceiver *RootReceiverFilterer) ParseReceived(log types.Log) (*RootReceiverReceived, error) {
	event := new(RootReceiverReceived)
	if err := _RootReceiver.contract.UnpackLog(event, "received", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}
