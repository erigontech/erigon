package bor

import (
	"encoding/hex"
	"encoding/json"
	"math/big"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
	"github.com/ledgerwatch/erigon/polygon/bor/valset"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/rlp"
)

//go:generate mockgen -destination=./spanner_mock.go -package=bor . Spanner
type Spanner interface {
	GetCurrentSpan(syscall consensus.SystemCall) (*heimdall.Span, error)
	GetCurrentValidators(spanId uint64, signer libcommon.Address, chain consensus.ChainHeaderReader) ([]*valset.Validator, error)
	GetCurrentProducers(spanId uint64, signer libcommon.Address, chain consensus.ChainHeaderReader) ([]*valset.Validator, error)
	CommitSpan(heimdallSpan heimdall.HeimdallSpan, syscall consensus.SystemCall) error
}

type ABI interface {
	Pack(name string, args ...interface{}) ([]byte, error)
	UnpackIntoInterface(v interface{}, name string, data []byte) error
}

type ChainSpanner struct {
	validatorSet    ABI
	chainConfig     *chain.Config
	borConfig       *borcfg.BorConfig
	logger          log.Logger
	withoutHeimdall bool
}

func NewChainSpanner(validatorSet ABI, chainConfig *chain.Config, withoutHeimdall bool, logger log.Logger) *ChainSpanner {
	borConfig := chainConfig.Bor.(*borcfg.BorConfig)
	return &ChainSpanner{
		validatorSet:    validatorSet,
		chainConfig:     chainConfig,
		borConfig:       borConfig,
		logger:          logger,
		withoutHeimdall: withoutHeimdall,
	}
}

// GetCurrentSpan get current span from contract
func (c *ChainSpanner) GetCurrentSpan(syscall consensus.SystemCall) (*heimdall.Span, error) {

	// method
	const method = "getCurrentSpan"

	data, err := c.validatorSet.Pack(method)
	if err != nil {
		c.logger.Error("[bor] Unable to pack tx for getCurrentSpan", "error", err)
		return nil, err
	}

	result, err := syscall(libcommon.HexToAddress(c.borConfig.ValidatorContract), data)
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
	span := heimdall.Span{
		ID:         ret.Number.Uint64(),
		StartBlock: ret.StartBlock.Uint64(),
		EndBlock:   ret.EndBlock.Uint64(),
	}

	return &span, nil
}

func (c *ChainSpanner) GetCurrentValidators(spanId uint64, signer libcommon.Address, chain consensus.ChainHeaderReader) ([]*valset.Validator, error) {
	// Use hardcoded bor devnet valset if chain-name = bor-devnet
	if NetworkNameVals[c.chainConfig.ChainName] != nil && c.withoutHeimdall {
		return NetworkNameVals[c.chainConfig.ChainName], nil
	}

	spanBytes := chain.BorSpan(spanId)
	var span heimdall.HeimdallSpan
	if err := json.Unmarshal(spanBytes, &span); err != nil {
		return nil, err
	}

	return span.ValidatorSet.Validators, nil
}

func (c *ChainSpanner) GetCurrentProducers(spanId uint64, signer libcommon.Address, chain consensus.ChainHeaderReader) ([]*valset.Validator, error) {
	// Use hardcoded bor devnet valset if chain-name = bor-devnet
	if NetworkNameVals[c.chainConfig.ChainName] != nil && c.withoutHeimdall {
		return NetworkNameVals[c.chainConfig.ChainName], nil
	}

	spanBytes := chain.BorSpan(spanId)
	var span heimdall.HeimdallSpan
	if err := json.Unmarshal(spanBytes, &span); err != nil {
		return nil, err
	}

	producers := make([]*valset.Validator, len(span.SelectedProducers))
	for i := range span.SelectedProducers {
		producers[i] = &span.SelectedProducers[i]
	}

	return producers, nil
}

func (c *ChainSpanner) CommitSpan(heimdallSpan heimdall.HeimdallSpan, syscall consensus.SystemCall) error {

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

	c.logger.Debug("[bor] ✅ Committing new span",
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
		c.logger.Error("[bor] Unable to pack tx for commitSpan", "error", err)
		return err
	}

	_, err = syscall(libcommon.HexToAddress(c.borConfig.ValidatorContract), data)

	return err
}
