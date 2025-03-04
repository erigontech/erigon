package types

import (
	_ "embed"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/accounts/abi"
)

//go:embed aa.abi
var AccountAbstractionABIJSON string

const AccountAbstractionABIVersion = 0

var AccountAbstractionABI, _ = abi.JSON(strings.NewReader(AccountAbstractionABIJSON))

/// DECODING

const PaymasterMaxContextSize = 65536

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
	ValidAfter, ValidUntil uint64
}

func DecodeAcceptAccount(input []byte) (*AcceptAccountData, error) {
	acceptAccountData := &AcceptAccountData{}
	err := decodeMethodParamsToInterface(acceptAccountData, "acceptAccount", input)
	if err != nil {
		return nil, err
	}
	return acceptAccountData, nil
}

type AcceptPaymasterData struct {
	ValidAfter, ValidUntil uint64
	Context                []byte
}

func DecodeAcceptPaymaster(input []byte) (*AcceptPaymasterData, error) {
	acceptPaymasterData := &AcceptPaymasterData{}
	err := decodeMethodParamsToInterface(acceptPaymasterData, "acceptPaymaster", input)
	if err != nil {
		return nil, err
	}
	if len(acceptPaymasterData.Context) > PaymasterMaxContextSize {
		return nil, errors.New("paymaster return data: context too large")
	}
	return acceptPaymasterData, err
}

/// ENCODING

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
	topics[1] = [32]byte(common.LeftPadBytes(sender.Bytes(), 32))
	topics[2] = [32]byte(common.LeftPadBytes(paymaster.Bytes(), 32))
	topics[3] = [32]byte(common.LeftPadBytes(deployer.Bytes(), 32))
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
	topics[1] = [32]byte(common.LeftPadBytes(sender.Bytes(), 32))
	topics[2] = [32]byte(common.LeftPadBytes(paymaster.Bytes(), 32))
	topics[3] = [32]byte(common.LeftPadBytes(deployer.Bytes(), 32))
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
	topics[1] = [32]byte(common.LeftPadBytes(sender.Bytes(), 32))
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
	topics[1] = [32]byte(common.LeftPadBytes(sender.Bytes(), 32))
	topics[2] = [32]byte(common.LeftPadBytes(paymaster.Bytes(), 32))
	return topics, data, nil
}

/// FRAMES

func EncodeTxnForFrame(frameType string, signingHash common.Hash, txAbiEncoding []byte) ([]byte, error) {
	return AccountAbstractionABI.Pack(frameType, big.NewInt(AccountAbstractionABIVersion), signingHash, txAbiEncoding)
}

func EncodePostOpFrame(paymasterContext []byte, gasUsed *big.Int, executionSuccess bool) ([]byte, error) {
	return AccountAbstractionABI.Pack("postPaymasterTransaction", executionSuccess, gasUsed, paymasterContext)
}
