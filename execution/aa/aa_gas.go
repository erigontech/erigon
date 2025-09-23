package aa

import (
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/execution/chain/params"
	"github.com/erigontech/erigon/execution/types"
)

func chargeGas(
	header *types.Header,
	tx *types.AccountAbstractionTransaction,
	gasPool *core.GasPool,
	ibs *state.IntraBlockState,
	preTxCost uint64,
) error {
	baseFee := uint256.MustFromBig(header.BaseFee)
	effectiveGasPrice := new(uint256.Int).Add(baseFee, tx.GetEffectiveGasTip(baseFee))

	totalGasLimit := preTxCost + tx.ValidationGasLimit + tx.PaymasterValidationGasLimit + tx.GasLimit + tx.PostOpGasLimit
	preCharge := new(uint256.Int).SetUint64(totalGasLimit)
	preCharge = preCharge.Mul(preCharge, effectiveGasPrice)

	chargeFrom := tx.GasPayer()
	balance, err := ibs.GetBalance(*chargeFrom)
	if err != nil {
		return err
	}

	if balance.Cmp(preCharge) < 0 {
		return fmt.Errorf("%w: RIP-7560 address %v have %v want %v", core.ErrInsufficientFunds, chargeFrom.Hex(), &balance, preCharge)
	}

	if err := ibs.SubBalance(*chargeFrom, *preCharge, 0); err != nil {
		return err
	}

	if err := gasPool.SubGas(totalGasLimit); err != nil {
		return newValidationPhaseError(err, nil, "block gas limit", false)
	}

	return nil
}

func refundGas(
	header *types.Header,
	tx *types.AccountAbstractionTransaction,
	ibs *state.IntraBlockState,
	gasUsed uint64,
) error {
	baseFee := uint256.MustFromBig(header.BaseFee)
	effectiveGasPrice := new(uint256.Int).Add(baseFee, tx.GetEffectiveGasTip(baseFee))
	actualGasCost := new(uint256.Int).Mul(effectiveGasPrice, new(uint256.Int).SetUint64(gasUsed))

	totalGasLimit := params.TxAAGas + tx.ValidationGasLimit + tx.PaymasterValidationGasLimit + tx.GasLimit + tx.PostOpGasLimit
	preCharge := new(uint256.Int).SetUint64(totalGasLimit)
	preCharge = preCharge.Mul(preCharge, effectiveGasPrice)

	refund := new(uint256.Int).Sub(preCharge, actualGasCost)

	chargeFrom := tx.GasPayer()

	if err := ibs.AddBalance(*chargeFrom, *refund, tracing.BalanceIncreaseGasReturn); err != nil {
		return err
	}

	return nil
}

func payCoinbase(
	header *types.Header,
	tx *types.AccountAbstractionTransaction,
	ibs *state.IntraBlockState,
	gasUsed uint64,
	coinbase common.Address,
) error {
	baseFee := uint256.MustFromBig(header.BaseFee)
	effectiveTip := u256.Num0

	if tx.FeeCap.Gt(baseFee) {
		effectiveTip = math.U256Min(tx.Tip, new(uint256.Int).Sub(tx.FeeCap, baseFee))
	}

	amount := new(uint256.Int).SetUint64(gasUsed)
	amount.Mul(amount, effectiveTip)
	return ibs.AddBalance(coinbase, *amount, tracing.BalanceIncreaseRewardTransactionFee)
}
