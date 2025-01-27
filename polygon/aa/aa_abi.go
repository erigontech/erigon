package aa

import (
	"errors"
	"math/big"
	"strings"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/accounts/abi"
)

//go:generate abigen -abi aa.abi -pkg aa -out ./gen_aa.go

/// DECODING

const PaymasterMaxContextSize = 65536

type AcceptAccountData struct {
	ValidAfter, ValidUntil uint64
}

func DecodeAcceptAccount(input []byte) (*AcceptAccountData, error) {
	p, err := ParseAaSigFailAccountParams(input)
	if err != nil {
		return nil, err
	}

	return &AcceptAccountData{
		ValidAfter: p.Param_validAfter.Uint64(),
		ValidUntil: p.Param_validUntil.Uint64(),
	}, nil
}

type AcceptPaymasterData struct {
	ValidAfter, ValidUntil uint64
	Context                []byte
}

func DecodeAcceptPaymaster(input []byte) (*AcceptPaymasterData, error) {
	p, err := ParseAaAcceptPaymasterParams(input)
	if err != nil {
		return nil, err
	}

	if len(p.Param_context) > PaymasterMaxContextSize {
		return nil, errors.New("paymaster return data: context too large")
	}
	return &AcceptPaymasterData{
		ValidAfter: p.Param_validAfter.Uint64(),
		ValidUntil: p.Param_validUntil.Uint64(),
		Context:    p.Param_context,
	}, err
}

/// ENCODING

const AccountAbstractionABIJSON = `[{"type":"function","name":"validateTransaction","inputs":[{"name":"version","type":"uint256"},{"name":"txHash","type":"bytes32"},{"name":"transaction","type":"bytes"}]},{"type":"function","name":"validatePaymasterTransaction","inputs":[{"name":"version","type":"uint256"},{"name":"txHash","type":"bytes32"},{"name":"transaction","type":"bytes"}]},{"type":"function","name":"postPaymasterTransaction","inputs":[{"name":"success","type":"bool"},{"name":"actualGasCost","type":"uint256"},{"name":"context","type":"bytes"}]},{"type":"function","name":"acceptAccount","inputs":[{"name":"validAfter","type":"uint256"},{"name":"validUntil","type":"uint256"}]},{"type":"function","name":"acceptPaymaster","inputs":[{"name":"validAfter","type":"uint256"},{"name":"validUntil","type":"uint256"},{"name":"context","type":"bytes"}]},{"type":"function","name":"sigFailAccount","inputs":[{"name":"validAfter","type":"uint256"},{"name":"validUntil","type":"uint256"}]},{"type":"function","name":"sigFailPaymaster","inputs":[{"name":"validAfter","type":"uint256"},{"name":"validUntil","type":"uint256"},{"name":"context","type":"bytes"}]},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"paymaster","type":"address"},{"indexed":false,"internalType":"uint256","name":"nonceKey","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"nonceSequence","type":"uint256"},{"indexed":false,"internalType":"bool","name":"executionStatus","type":"uint256"}],"name":"RIP7560TransactionEvent","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"nonceKey","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"nonceSequence","type":"uint256"},{"indexed":false,"internalType":"bytes","name":"revertReason","type":"bytes"}],"name":"RIP7560TransactionRevertReason","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"paymaster","type":"address"},{"indexed":false,"internalType":"uint256","name":"nonceKey","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"nonceSequence","type":"uint256"},{"indexed":false,"internalType":"bytes","name":"revertReason","type":"bytes"}],"name":"RIP7560TransactionPostOpRevertReason","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"paymaster","type":"address"},{"indexed":true,"internalType":"address","name":"deployer","type":"address"}],"name":"RIP7560AccountDeployed","type":"event"}]`
const AccountAbstractionABIVersion = 0

var AccountAbstractionABI, _ = abi.JSON(strings.NewReader(AccountAbstractionABIJSON))

func EncodeRIP7560TransactionEvent(
	executionStatus, nonce uint64,
	nonceKey *uint256.Int,
	paymaster, deployer, sender *common.Address,
) (topics []common.Hash, data []byte, error error) {
	id := AccountAbstractionABI.Events["RIP7560TransactionEvent"].ID
	if paymaster == nil {
		paymaster = &common.Address{}
	}
	if deployer == nil {
		deployer = &common.Address{}
	}
	inputs := AccountAbstractionABI.Events["RIP7560TransactionEvent"].Inputs
	data, error = inputs.NonIndexed().Pack(
		nonceKey,
		big.NewInt(int64(nonce)),
		big.NewInt(int64(executionStatus)),
	)
	if error != nil {
		return nil, nil, error
	}
	topics = []common.Hash{id, {}, {}}
	topics[1] = [32]byte(common.LeftPadBytes(sender.Bytes()[:], 32))
	topics[2] = [32]byte(common.LeftPadBytes(paymaster.Bytes()[:], 32))
	return topics, data, nil
}

func EncodeRIP7560AccountDeployedEvent(paymaster, deployer, sender *common.Address) (topics []common.Hash, data []byte, err error) {
	id := AccountAbstractionABI.Events["RIP7560AccountDeployed"].ID
	if paymaster == nil {
		paymaster = &common.Address{}
	}
	if deployer == nil {
		deployer = &common.Address{}
	}
	topics = []common.Hash{id, {}, {}, {}}
	topics[1] = [32]byte(common.LeftPadBytes(sender.Bytes()[:], 32))
	topics[2] = [32]byte(common.LeftPadBytes(paymaster.Bytes()[:], 32))
	topics[3] = [32]byte(common.LeftPadBytes(deployer.Bytes()[:], 32))
	return topics, make([]byte, 0), nil
}

func EncodeRIP7560TransactionRevertReasonEvent(
	revertData []byte,
	nonce uint64,
	nonceKey *uint256.Int,
	sender *common.Address,
) (topics []common.Hash, data []byte, error error) {
	id := AccountAbstractionABI.Events["RIP7560TransactionRevertReason"].ID
	inputs := AccountAbstractionABI.Events["RIP7560TransactionRevertReason"].Inputs
	data, error = inputs.NonIndexed().Pack(
		nonceKey,
		big.NewInt(int64(nonce)),
		revertData,
	)
	if error != nil {
		return nil, nil, error
	}
	topics = []common.Hash{id, {}}
	topics[1] = [32]byte(common.LeftPadBytes(sender.Bytes()[:], 32))
	return topics, data, nil
}

func EncodeRIP7560TransactionPostOpRevertReasonEvent(
	revertData []byte,
	nonce uint64,
	nonceKey *uint256.Int,
	paymaster, sender *common.Address,
) (topics []common.Hash, data []byte, error error) {
	id := AccountAbstractionABI.Events["RIP7560TransactionPostOpRevertReason"].ID
	if paymaster == nil {
		paymaster = &common.Address{}
	}
	inputs := AccountAbstractionABI.Events["RIP7560TransactionPostOpRevertReason"].Inputs
	data, error = inputs.NonIndexed().Pack(
		nonceKey,
		big.NewInt(int64(nonce)),
		revertData,
	)
	if error != nil {
		return nil, nil, error
	}
	topics = []common.Hash{id, {}, {}}
	topics[1] = [32]byte(common.LeftPadBytes(sender.Bytes()[:], 32))
	topics[2] = [32]byte(common.LeftPadBytes(paymaster.Bytes()[:], 32))
	return topics, data, nil
}

/// FRAMES

func EncodeTxnForFrame(frameType string, signingHash common.Hash, txAbiEncoding []byte) ([]byte, error) {
	return AccountAbstractionABI.Pack(frameType, big.NewInt(AccountAbstractionABIVersion), signingHash, txAbiEncoding)
}

func EncodePostOpFrame(paymasterContext []byte, gasUsed *big.Int, executionSuccess bool) ([]byte, error) {
	return AccountAbstractionABI.Pack("postPaymasterTransaction", executionSuccess, gasUsed, paymasterContext)
}

/// TRANSACTION
