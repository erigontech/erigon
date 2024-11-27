package aa

import (
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/accounts/abi"
	libcommon "github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/core/types"
)

// ABIAccountAbstractTxn an equivalent of a solidity struct only used to encode the 'transaction' parameter
type ABIAccountAbstractTxn struct {
	// NOTE: these were big.Int and were changed to uint256
	Sender                      common.Address
	NonceKey                    *uint256.Int
	Nonce                       *uint256.Int
	ValidationGasLimit          *uint256.Int
	PaymasterValidationGasLimit *uint256.Int
	PostOpGasLimit              *uint256.Int
	CallGasLimit                *uint256.Int
	MaxFeePerGas                *uint256.Int
	MaxPriorityFeePerGas        *uint256.Int
	BuilderFee                  *uint256.Int
	Paymaster                   common.Address
	PaymasterData               []byte
	Deployer                    common.Address
	DeployerData                []byte
	ExecutionData               []byte
	AuthorizationData           []byte
}

const AccountAbstractionABIJSON = `[{"type":"function","name":"validateTransaction","inputs":[{"name":"version","type":"uint256"},{"name":"txHash","type":"bytes32"},{"name":"transaction","type":"bytes"}]},{"type":"function","name":"validatePaymasterTransaction","inputs":[{"name":"version","type":"uint256"},{"name":"txHash","type":"bytes32"},{"name":"transaction","type":"bytes"}]},{"type":"function","name":"postPaymasterTransaction","inputs":[{"name":"success","type":"bool"},{"name":"actualGasCost","type":"uint256"},{"name":"context","type":"bytes"}]},{"type":"function","name":"acceptAccount","inputs":[{"name":"validAfter","type":"uint256"},{"name":"validUntil","type":"uint256"}]},{"type":"function","name":"acceptPaymaster","inputs":[{"name":"validAfter","type":"uint256"},{"name":"validUntil","type":"uint256"},{"name":"context","type":"bytes"}]},{"type":"function","name":"sigFailAccount","inputs":[{"name":"validAfter","type":"uint256"},{"name":"validUntil","type":"uint256"}]},{"type":"function","name":"sigFailPaymaster","inputs":[{"name":"validAfter","type":"uint256"},{"name":"validUntil","type":"uint256"},{"name":"context","type":"bytes"}]},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"paymaster","type":"address"},{"indexed":false,"internalType":"uint256","name":"nonceKey","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"nonceSequence","type":"uint256"},{"indexed":false,"internalType":"bool","name":"executionStatus","type":"uint256"}],"name":"RIP7560TransactionEvent","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"nonceKey","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"nonceSequence","type":"uint256"},{"indexed":false,"internalType":"bytes","name":"revertReason","type":"bytes"}],"name":"RIP7560TransactionRevertReason","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"paymaster","type":"address"},{"indexed":false,"internalType":"uint256","name":"nonceKey","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"nonceSequence","type":"uint256"},{"indexed":false,"internalType":"bytes","name":"revertReason","type":"bytes"}],"name":"RIP7560TransactionPostOpRevertReason","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"paymaster","type":"address"},{"indexed":true,"internalType":"address","name":"deployer","type":"address"}],"name":"RIP7560AccountDeployed","type":"event"}]`
const AccountAbstractionABIVersion = 0
const PaymasterMaxContextSize = 65536

var AccountAbstractionABI, _ = abi.JSON(strings.NewReader(AccountAbstractionABIJSON))

func decodeMethodParamsToInterface(output interface{}, methodName string, input []byte) error {
	m, err := AccountAbstractionABI.MethodById(input)
	if err != nil {
		return fmt.Errorf("unable to decode %s: %w", methodName, err)
	}
	if methodName != m.Name {
		return fmt.Errorf("unable to decode %s: got wrong method %s", methodName, m.Name)
	}
	params, err := m.Inputs.Unpack(input[4:])
	if err != nil {
		return fmt.Errorf("unable to decode %s: %w", methodName, err)
	}
	err = m.Inputs.Copy(output, params)
	if err != nil {
		return fmt.Errorf("unable to decode %s: %v", methodName, err)
	}
	return nil
}

type AcceptAccountData struct {
	ValidAfter *uint256.Int
	ValidUntil *uint256.Int
}

type AcceptPaymasterData struct {
	ValidAfter *uint256.Int
	ValidUntil *uint256.Int
	Context    []byte
}

func AbiDecodeAcceptAccount(input []byte, allowSigFail bool) (*AcceptAccountData, error) {
	acceptAccountData := &AcceptAccountData{}
	err := decodeMethodParamsToInterface(acceptAccountData, "acceptAccount", input)
	if err != nil && allowSigFail {
		err = decodeMethodParamsToInterface(acceptAccountData, "sigFailAccount", input)
	}
	if err != nil {
		return nil, err
	}
	return acceptAccountData, nil
}

func AbiDecodeAcceptPaymaster(input []byte, allowSigFail bool) (*AcceptPaymasterData, error) {
	acceptPaymasterData := &AcceptPaymasterData{}
	err := decodeMethodParamsToInterface(acceptPaymasterData, "acceptPaymaster", input)
	if err != nil && allowSigFail {
		err = decodeMethodParamsToInterface(acceptPaymasterData, "sigFailPaymaster", input)
	}
	if err != nil {
		return nil, err
	}
	if len(acceptPaymasterData.Context) > PaymasterMaxContextSize {
		return nil, errors.New("paymaster return data: context too large")
	}
	return acceptPaymasterData, err
}

func AbiEncodeRIP7560TransactionEvent(
	txn *types.AccountAbstractionTransaction,
	executionStatus uint64,
) (topics []common.Hash, data []byte, error error) {
	id := AccountAbstractionABI.Events["RIP7560TransactionEvent"].ID
	paymaster := txn.Paymaster
	if paymaster == nil {
		paymaster = &common.Address{}
	}
	deployer := txn.Deployer
	if deployer == nil {
		deployer = &common.Address{}
	}
	inputs := AccountAbstractionABI.Events["RIP7560TransactionEvent"].Inputs
	data, error = inputs.NonIndexed().Pack(
		txn.NonceKey,
		big.NewInt(int64(txn.Nonce)),
		big.NewInt(int64(executionStatus)),
	)
	if error != nil {
		return nil, nil, error
	}
	topics = []common.Hash{id, {}, {}}
	topics[1] = [32]byte(libcommon.LeftPadBytes(txn.SenderAddress.Bytes()[:], 32))
	topics[2] = [32]byte(libcommon.LeftPadBytes(paymaster.Bytes()[:], 32))
	return topics, data, nil
}

func AbiEncodeRIP7560AccountDeployedEvent(
	txn *types.AccountAbstractionTransaction,
) (topics []common.Hash, data []byte, err error) {
	id := AccountAbstractionABI.Events["RIP7560AccountDeployed"].ID
	paymaster := txn.Paymaster
	if paymaster == nil {
		paymaster = &common.Address{}
	}
	deployer := txn.Deployer
	if deployer == nil {
		deployer = &common.Address{}
	}
	topics = []common.Hash{id, {}, {}, {}}
	topics[1] = [32]byte(libcommon.LeftPadBytes(txn.SenderAddress.Bytes()[:], 32))
	topics[2] = [32]byte(libcommon.LeftPadBytes(paymaster.Bytes()[:], 32))
	topics[3] = [32]byte(libcommon.LeftPadBytes(deployer.Bytes()[:], 32))
	return topics, make([]byte, 0), nil
}

func AbiEncodeRIP7560TransactionRevertReasonEvent(
	txn *types.AccountAbstractionTransaction,
	revertData []byte,
) (topics []common.Hash, data []byte, error error) {
	id := AccountAbstractionABI.Events["RIP7560TransactionRevertReason"].ID
	inputs := AccountAbstractionABI.Events["RIP7560TransactionRevertReason"].Inputs
	data, error = inputs.NonIndexed().Pack(
		txn.NonceKey,
		big.NewInt(int64(txn.Nonce)),
		revertData,
	)
	if error != nil {
		return nil, nil, error
	}
	topics = []common.Hash{id, {}}
	topics[1] = [32]byte(libcommon.LeftPadBytes(txn.SenderAddress.Bytes()[:], 32))
	return topics, data, nil
}

func AbiEncodeRIP7560TransactionPostOpRevertReasonEvent(
	txn *types.AccountAbstractionTransaction,
	revertData []byte,
) (topics []common.Hash, data []byte, error error) {
	id := AccountAbstractionABI.Events["RIP7560TransactionPostOpRevertReason"].ID
	paymaster := txn.Paymaster
	if paymaster == nil {
		paymaster = &common.Address{}
	}
	inputs := AccountAbstractionABI.Events["RIP7560TransactionPostOpRevertReason"].Inputs
	data, error = inputs.NonIndexed().Pack(
		txn.NonceKey,
		big.NewInt(int64(txn.Nonce)),
		revertData,
	)
	if error != nil {
		return nil, nil, error
	}
	topics = []common.Hash{id, {}, {}}
	topics[1] = [32]byte(libcommon.LeftPadBytes(txn.SenderAddress.Bytes()[:], 32))
	topics[2] = [32]byte(libcommon.LeftPadBytes(paymaster.Bytes()[:], 32))
	return topics, data, nil
}
