package span

import (
	"encoding/hex"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/bor/abi"
	"github.com/ledgerwatch/erigon/consensus/bor/valset"
	"github.com/ledgerwatch/erigon/params/networkname"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"
)

type ChainSpanner struct {
	validatorSet abi.ABI
	chainConfig  *chain.Config
	logger       log.Logger
}

func NewChainSpanner(validatorSet abi.ABI, chainConfig *chain.Config, logger log.Logger) *ChainSpanner {
	return &ChainSpanner{
		validatorSet: validatorSet,
		chainConfig:  chainConfig,
		logger:       logger,
	}
}

// GetCurrentSpan get current span from contract
func (c *ChainSpanner) GetCurrentSpan(syscall consensus.SystemCall) (*Span, error) {

	// method
	const method = "getCurrentSpan"

	data, err := c.validatorSet.Pack(method)
	if err != nil {
		c.logger.Error("Unable to pack tx for getCurrentSpan", "error", err)
		return nil, err
	}

	result, err := syscall(libcommon.HexToAddress(c.chainConfig.Bor.ValidatorContract), data)
	if err != nil {
		return nil, err
	}

	// span result
	ret := new(struct {
		Number     *big.Int
		StartBlock *big.Int
		EndBlock   *big.Int
	})

	if err := c.validatorSet.UnpackIntoInterface(ret, method, result); err != nil {
		return nil, err
	}

	// create new span
	span := Span{
		ID:         ret.Number.Uint64(),
		StartBlock: ret.StartBlock.Uint64(),
		EndBlock:   ret.EndBlock.Uint64(),
	}

	return &span, nil
}

// GetCurrentValidatorsFromContract get current validators from genesis contract
func (c *ChainSpanner) GetCurrentValidatorsFromContract(blockNumber uint64, syscall consensus.SystemCall) ([]*valset.Validator, error) {

	// method
	const method = "getBorValidators"

	data, err := c.validatorSet.Pack(method, big.NewInt(0).SetUint64(blockNumber))
	if err != nil {
		log.Error("Unable to pack tx for getValidator", "error", err)
		return nil, err
	}
	result, err := syscall(libcommon.HexToAddress(c.chainConfig.Bor.ValidatorContract), data)
	if err != nil {
		return nil, err
	}
	var (
		ret0 = new([]common.Address)
		ret1 = new([]*big.Int)
	)

	out := &[]interface{}{
		ret0,
		ret1,
	}

	if err := c.validatorSet.UnpackIntoInterface(out, method, result); err != nil {
		return nil, err
	}

	valz := make([]*valset.Validator, len(*ret0))
	for i, a := range *ret0 {
		valz[i] = &valset.Validator{
			Address:     a,
			VotingPower: (*ret1)[i].Int64(),
		}
	}

	return valz, nil
}

func (c *ChainSpanner) GetCurrentValidators(blockNumber uint64, signer libcommon.Address, syscall consensus.SystemCall, getSpanForBlock func(blockNum uint64) (*HeimdallSpan, error)) ([]*valset.Validator, error) {
	// Use signer as validator in case of bor devent

	if c.chainConfig.ChainName == networkname.BorDevnetChainName {
		validators := []*valset.Validator{
			{
				ID:               1,
				Address:          signer,
				VotingPower:      1000,
				ProposerPriority: 1,
			},
		}

		return validators, nil
	}

	vals, err := c.GetCurrentValidatorsFromContract(blockNumber, syscall)
	if err != nil {
		return nil, err
	}

	return vals, nil
}

func (c *ChainSpanner) GetCurrentProducers(blockNumber uint64, signer libcommon.Address, syscall consensus.SystemCall, getSpanForBlock func(blockNum uint64) (*HeimdallSpan, error)) ([]*valset.Validator, error) {
	// Use signer as validator in case of bor devent

	if c.chainConfig.ChainName == networkname.BorDevnetChainName {
		validators := []*valset.Validator{
			{
				ID:               1,
				Address:          signer,
				VotingPower:      1000,
				ProposerPriority: 1,
			},
		}

		return validators, nil
	}

	vals, err := c.GetCurrentValidatorsFromContract(blockNumber, syscall)
	if err != nil {
		return nil, err
	}
	return vals, nil
}

func (c *ChainSpanner) CommitSpan(heimdallSpan HeimdallSpan, syscall consensus.SystemCall) error {

	// method
	const method = "commitSpan"

	// get validators bytes
	validators := make([]valset.MinimalVal, 0, len(heimdallSpan.ValidatorSet.Validators))
	for _, val := range heimdallSpan.ValidatorSet.Validators {
		validators = append(validators, val.MinimalVal())
	}
	validatorBytes, err := rlp.EncodeToBytes(validators)
	if err != nil {
		return err
	}

	// get producers bytes
	producers := make([]valset.MinimalVal, 0, len(heimdallSpan.SelectedProducers))
	for _, val := range heimdallSpan.SelectedProducers {
		producers = append(producers, val.MinimalVal())
	}
	producerBytes, err := rlp.EncodeToBytes(producers)
	if err != nil {
		return err
	}

	c.logger.Debug("✅ Committing new span",
		"id", heimdallSpan.ID,
		"startBlock", heimdallSpan.StartBlock,
		"endBlock", heimdallSpan.EndBlock,
		"validatorBytes", hex.EncodeToString(validatorBytes),
		"producerBytes", hex.EncodeToString(producerBytes),
	)

	// get packed data
	data, err := c.validatorSet.Pack(method,
		big.NewInt(0).SetUint64(heimdallSpan.ID),
		big.NewInt(0).SetUint64(heimdallSpan.StartBlock),
		big.NewInt(0).SetUint64(heimdallSpan.EndBlock),
		validatorBytes,
		producerBytes,
	)
	if err != nil {
		c.logger.Error("Unable to pack tx for commitSpan", "error", err)
		return err
	}

	_, err = syscall(libcommon.HexToAddress(c.chainConfig.Bor.ValidatorContract), data)

	return err
}
