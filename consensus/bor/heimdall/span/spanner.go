package span

import (
	"encoding/hex"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/bor/abi"
	"github.com/ledgerwatch/erigon/consensus/bor/valset"
	"github.com/ledgerwatch/erigon/params/networkname"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"
)

var devnetSigner = libcommon.HexToAddress("0x67b1d87101671b127f5f8714789c7192f7ad340e")

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

func (c *ChainSpanner) GetCurrentValidators(blockNumber uint64, signer libcommon.Address, getSpanForBlock func(blockNum uint64) (*HeimdallSpan, error)) ([]*valset.Validator, error) {
	// Use signer as validator in case of bor devent
	if c.chainConfig.ChainName == networkname.BorDevnetChainName {
		c.logger.Info("Spanner returning pre-set validator set")
		validators := []*valset.Validator{
			{
				ID:               1,
				Address:          devnetSigner,
				VotingPower:      1000,
				ProposerPriority: 1,
			},
		}

		return validators, nil
	}

	span, err := getSpanForBlock(blockNumber)
	if err != nil {
		return nil, err
	}

	return span.ValidatorSet.Validators, nil
}

func (c *ChainSpanner) GetCurrentProducers(blockNumber uint64, signer libcommon.Address, getSpanForBlock func(blockNum uint64) (*HeimdallSpan, error)) ([]*valset.Validator, error) {
	// Use signer as validator in case of bor devent
	if c.chainConfig.ChainName == networkname.BorDevnetChainName {
		validators := []*valset.Validator{
			{
				ID:               1,
				Address:          devnetSigner,
				VotingPower:      1000,
				ProposerPriority: 1,
			},
		}

		return validators, nil
	}

	span, err := getSpanForBlock(blockNumber)
	if err != nil {
		return nil, err
	}

	producers := make([]*valset.Validator, len(span.SelectedProducers))
	for i := range span.SelectedProducers {
		producers[i] = &span.SelectedProducers[i]
	}

	return producers, nil
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
