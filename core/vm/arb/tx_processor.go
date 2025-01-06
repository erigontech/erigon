package arb

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/erigontech/nitro-erigon/arbos/l1pricing"
	"github.com/holiman/uint256"

	"github.com/erigontech/nitro-erigon/arbos/util"
	"github.com/erigontech/nitro-erigon/util/arbmath"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	glog "github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/nitro-erigon/arbos/arbosState"
	"github.com/erigontech/nitro-erigon/arbos/retryables"
)

var arbosAddress = types.ArbosAddress

const GasEstimationL1PricePadding arbmath.Bips = 11000 // pad estimates by 10%

// A TxProcessor is created and freed for every L2 transaction.
// It tracks state for ArbOS, allowing it infuence in Geth's tx processing.
// Public fields are accessible in precompiles.
type TxProcessor struct {
	msg              *types.Message
	state            *arbosState.ArbosState
	PosterFee        *uint256.Int // set once in GasChargingHook to track L1 calldata costs
	posterGas        uint64
	computeHoldGas   uint64 // amount of gas temporarily held to prevent compute from exceeding the gas limit
	delayedInbox     bool   // whether this tx was submitted through the delayed inbox
	Contracts        []*vm.Contract
	Programs         map[common.Address]uint // # of distinct context spans for each program
	TopTxType        *byte                   // set once in StartTxHook
	evm              *vm.EVM
	CurrentRetryable *common.Hash
	CurrentRefundTo  *common.Address

	// Caches for the latest L1 block number and hash,
	// for the NUMBER and BLOCKHASH opcodes.
	cachedL1BlockNumber *uint64
	cachedL1BlockHashes map[uint64]common.Hash
}

func NewTxProcessor(evm *vm.EVM, msg *types.Message) *TxProcessor {
	tracingInfo := util.NewTracingInfo(evm, msg.From(), arbosAddress, util.TracingBeforeEVM)
	arbosState := arbosState.OpenSystemArbosStateOrPanic(evm.IntraBlockState(), tracingInfo, false)
	return &TxProcessor{
		msg:                 msg,
		state:               arbosState,
		PosterFee:           new(uint256.Int),
		posterGas:           0,
		delayedInbox:        evm.Context.Coinbase != l1pricing.BatchPosterAddress,
		Contracts:           []*vm.Contract{},
		Programs:            make(map[common.Address]uint),
		TopTxType:           nil,
		evm:                 evm,
		CurrentRetryable:    nil,
		CurrentRefundTo:     nil,
		cachedL1BlockNumber: nil,
		cachedL1BlockHashes: make(map[uint64]common.Hash),
	}
}

func (p *TxProcessor) PushContract(contract *vm.Contract) {
	p.Contracts = append(p.Contracts, contract)

	if !contract.IsDelegateOrCallcode() {
		p.Programs[contract.Address()]++
	}
}

func (p *TxProcessor) PopContract() {
	newLen := len(p.Contracts) - 1
	popped := p.Contracts[newLen]
	p.Contracts = p.Contracts[:newLen]

	if !popped.IsDelegateOrCallcode() {
		p.Programs[popped.Address()]--
	}
}

func takeFunds256(pool, take *uint256.Int) *uint256.Int {
	if take.Sign() < 0 {
		panic("Attempted to take a negative amount of funds")
	}
	if pool.Lt(take) {
		oldPool := pool.Clone()
		pool.Clear()
		return oldPool
	}
	pool.Sub(pool, take)
	return take
}

// Attempts to subtract up to `take` from `pool` without going negative.
// Returns the amount subtracted from `pool`.
func takeFunds(pool *big.Int, take *big.Int) *big.Int {
	if take.Sign() < 0 {
		panic("Attempted to take a negative amount of funds")
	}
	if arbmath.BigLessThan(pool, take) {
		oldPool := new(big.Int).Set(pool)
		pool.Set(common.Big0)
		return oldPool
	}
	pool.Sub(pool, take)
	return new(big.Int).Set(take)
}

func (p *TxProcessor) ExecuteWASM(scope *vm.ScopeContext, input []byte, interpreter *vm.EVMInterpreter) ([]byte, error) {
	contract := scope.Contract
	acting := contract.Address()

	var tracingInfo *util.TracingInfo
	if p.evm.Config().Tracer != nil {
		caller := contract.CallerAddress
		tracingInfo = util.NewTracingInfo(p.evm, caller, acting, util.TracingDuringEVM)
	}

	// reentrant if more than one open same-actor context span exists
	reentrant := p.Programs[acting] > 1

	return p.state.Programs().CallProgram(
		scope,
		p.evm.IntraBlockState(),
		p.state.ArbOSVersion(),
		interpreter,
		tracingInfo,
		input,
		reentrant,
		p.RunMode(),
	)
}

// depreacated ? method
func (p *TxProcessor) RunMode() types.MessageRunMode {
	return p.msg.TxRunMode
}

func (p *TxProcessor) StartTxHook() (endTxNow bool, gasUsed uint64, err error, returnData []byte) {
	// This hook is called before gas charging and will end the state transition if endTxNow is set to true
	// Hence, we must charge for any l2 resources if endTxNow is returned true

	underlyingTx := p.msg
	if underlyingTx == nil {
		return false, 0, nil, nil
	}

	// var tracingInfo *util.TracingInfo
	// startTracer := func() func() {
	tipe := underlyingTx.Type()
	p.TopTxType = &tipe
	// evm := p.evm

	// startTracer := func() func() {
	// 	tracer := evm.Config().Tracer
	// 	if tracer == nil {
	// 		return func() {}
	// 	}
	// 	evm.IncrementDepth() // fake a call
	// 	from := p.msg.From()
	// 	tracer.CaptureStart(evm, from, p.msg.To(), false, p.msg.Data, p.msg.GasLimit, p.msg.Value())

	// 	tracingInfo = util.NewTracingInfo(evm, from, *p.msg.To, util.TracingDuringEVM)
	// 	p.state = arbosState.OpenSystemArbosStateOrPanic(evm.IntraBlockState(), tracingInfo, false)

	// 	return func() {
	// 		tracer.CaptureEnd(nil, p.state.Burner.Burned(), nil)
	// 		evm.DecrementDepth() // fake the return to the first faked call

	// 		tracingInfo = util.NewTracingInfo(evm, from, *p.msg.To(), util.TracingAfterEVM)
	// 		p.state = arbosState.OpenSystemArbosStateOrPanic(evm.IntraBlockState(), tracingInfo, false)
	// 	}
	// }

	statedb := p.evm.IntraBlockState()
	// underlyingTx.
	switch tx := underlyingTx.GetInner().(type) {
	case *types.ArbitrumDepositTx:
		from := p.msg.From()
		to := p.msg.To()
		value := p.msg.Value
		if to == nil {
			return true, 0, errors.New("eth deposit has no To address"), nil
		}
		util.MintBalance(&from, value, statedb, util.TracingBeforeEVM, "deposit")
		defer (startTracer())()
		// We intentionally use the variant here that doesn't do tracing,
		// because this transfer is represented as the outer eth transaction.
		// This transfer is necessary because we don't actually invoke the EVM.
		// Since MintBalance already called AddBalance on `from`,
		// we don't have EIP-161 concerns around not touching `from`.
		core.Transfer(statedb, from, *to, uint256.MustFromBig(value))
		return true, 0, nil, nil
	case *types.ArbitrumInternalTx:
		defer (startTracer())()
		if p.msg.From != arbosAddress {
			return false, 0, errors.New("internal tx not from arbAddress"), nil
		}
		err = ApplyInternalTxUpdate(tx, p.state, evm)
		return true, 0, err, nil
	case *types.ArbitrumSubmitRetryableTx:
		defer (startTracer())()
		ticketId := underlyingTx.Hash()
		escrow := retryables.RetryableEscrowAddress(ticketId)
		networkFeeAccount, _ := p.state.NetworkFeeAccount()
		from := tx.From
		scenario := util.TracingDuringEVM

		// mint funds with the deposit, then charge fees later
		availableRefund := new(big.Int).Set(tx.DepositValue)
		takeFunds(availableRefund, tx.RetryValue)
		util.MintBalance(&tx.From, tx.DepositValue, evm, scenario, "deposit")

		transfer := func(from, to *common.Address, amount *big.Int) error {
			return util.TransferBalance(from, to, amount, evm, scenario, "during evm execution")
		}

		// check that the user has enough balance to pay for the max submission fee
		balanceAfterMint := statedb.GetBalance(tx.From)
		if balanceAfterMint.ToBig().Cmp(tx.MaxSubmissionFee) < 0 {
			err := fmt.Errorf(
				"insufficient funds for max submission fee: address %v have %v want %v",
				tx.From, balanceAfterMint, tx.MaxSubmissionFee,
			)
			return true, 0, err, nil
		}

		submissionFee := retryables.RetryableSubmissionFee(len(tx.RetryData), tx.L1BaseFee)
		if arbmath.BigLessThan(tx.MaxSubmissionFee, submissionFee) {
			// should be impossible as this is checked at L1
			err := fmt.Errorf(
				"max submission fee %v is less than the actual submission fee %v",
				tx.MaxSubmissionFee, submissionFee,
			)
			return true, 0, err, nil
		}

		// collect the submission fee
		if err := transfer(&tx.From, &networkFeeAccount, submissionFee); err != nil {
			// should be impossible as we just checked that they have enough balance for the max submission fee,
			// and we also checked that the max submission fee is at least the actual submission fee
			glog.Error("failed to transfer submissionFee", "err", err)
			return true, 0, err, nil
		}
		withheldSubmissionFee := takeFunds(availableRefund, submissionFee)

		// refund excess submission fee
		submissionFeeRefund := takeFunds(availableRefund, arbmath.BigSub(tx.MaxSubmissionFee, submissionFee))
		if err := transfer(&tx.From, &tx.FeeRefundAddr, submissionFeeRefund); err != nil {
			// should never happen as from's balance should be at least availableRefund at this point
			glog.Error("failed to transfer submissionFeeRefund", "err", err)
		}

		// move the callvalue into escrow
		if callValueErr := transfer(&tx.From, &escrow, tx.RetryValue); callValueErr != nil {
			// The sender doesn't have enough balance to pay for the retryable's callvalue.
			// Since we can't create the retryable, we should refund the submission fee.
			// First, we give the submission fee back to the transaction sender:
			if err := transfer(&networkFeeAccount, &tx.From, submissionFee); err != nil {
				glog.Error("failed to refund submissionFee", "err", err)
			}
			// Then, as limited by availableRefund, we attempt to move the refund to the fee refund address.
			// If the deposit value was lower than the submission fee, only some (or none) of the submission fee may be moved.
			// In that case, any amount up to the deposit value will be refunded to the fee refund address,
			// with the rest remaining in the transaction sender's address (as that's where the funds were pulled from).
			if err := transfer(&tx.From, &tx.FeeRefundAddr, withheldSubmissionFee); err != nil {
				glog.Error("failed to refund withheldSubmissionFee", "err", err)
			}
			return true, 0, callValueErr, nil
		}

		time := evm.Context.Time
		timeout := time + retryables.RetryableLifetimeSeconds

		// we charge for creating the retryable and reaping the next expired one on L1
		retryable, err := p.state.RetryableState().CreateRetryable(
			ticketId,
			timeout,
			tx.From,
			tx.RetryTo,
			tx.RetryValue,
			tx.Beneficiary,
			tx.RetryData,
		)
		p.state.Restrict(err)

		err = EmitTicketCreatedEvent(evm, ticketId)
		if err != nil {
			glog.Error("failed to emit TicketCreated event", "err", err)
		}

		balance := statedb.GetBalance(tx.From)
		// evm.Context.BaseFee is already lowered to 0 when vm runs with NoBaseFee flag and 0 gas price
		effectiveBaseFee := evm.Context.BaseFee
		usergas := p.msg.GasLimit

		maxGasCost := arbmath.BigMulByUint(tx.GasFeeCap, usergas)
		maxFeePerGasTooLow := arbmath.BigLessThan(tx.GasFeeCap, effectiveBaseFee)
		if arbmath.BigLessThan(balance.ToBig(), maxGasCost) || usergas < params.TxGas || maxFeePerGasTooLow {
			// User either specified too low of a gas fee cap, didn't have enough balance to pay for gas,
			// or the specified gas limit is below the minimum transaction gas cost.
			// Either way, attempt to refund the gas costs, since we're not doing the auto-redeem.
			gasCostRefund := takeFunds(availableRefund, maxGasCost)
			if err := transfer(&tx.From, &tx.FeeRefundAddr, gasCostRefund); err != nil {
				// should never happen as from's balance should be at least availableRefund at this point
				glog.Error("failed to transfer gasCostRefund", "err", err)
			}
			return true, 0, nil, ticketId.Bytes()
		}

		// pay for the retryable's gas and update the pools
		gascost := arbmath.BigMulByUint(effectiveBaseFee, usergas)
		networkCost := gascost
		if p.state.ArbOSVersion() >= 11 {
			infraFeeAccount, err := p.state.InfraFeeAccount()
			p.state.Restrict(err)
			if infraFeeAccount != (common.Address{}) {
				minBaseFee, err := p.state.L2PricingState().MinBaseFeeWei()
				p.state.Restrict(err)
				infraFee := arbmath.BigMin(minBaseFee, effectiveBaseFee)
				infraCost := arbmath.BigMulByUint(infraFee, usergas)
				infraCost = takeFunds(networkCost, infraCost)
				if err := transfer(&tx.From, &infraFeeAccount, infraCost); err != nil {
					glog.Error("failed to transfer gas cost to infrastructure fee account", "err", err)
					return true, 0, nil, ticketId.Bytes()
				}
			}
		}
		if arbmath.BigGreaterThan(networkCost, common.Big0) {
			if err := transfer(&tx.From, &networkFeeAccount, networkCost); err != nil {
				// should be impossible because we just checked the tx.From balance
				glog.Error("failed to transfer gas cost to network fee account", "err", err)
				return true, 0, nil, ticketId.Bytes()
			}
		}

		withheldGasFunds := takeFunds(availableRefund, gascost) // gascost is conceptually charged before the gas price refund
		gasPriceRefund := arbmath.BigMulByUint(arbmath.BigSub(tx.GasFeeCap, effectiveBaseFee), tx.Gas)
		if gasPriceRefund.Sign() < 0 {
			// This should only be possible during gas estimation mode
			gasPriceRefund.SetInt64(0)
		}
		gasPriceRefund = takeFunds(availableRefund, gasPriceRefund)
		if err := transfer(&tx.From, &tx.FeeRefundAddr, gasPriceRefund); err != nil {
			glog.Error("failed to transfer gasPriceRefund", "err", err)
		}
		availableRefund.Add(availableRefund, withheldGasFunds)
		availableRefund.Add(availableRefund, withheldSubmissionFee)

		// emit RedeemScheduled event
		retryTxInner, err := retryable.MakeTx(
			underlyingTx.ChainId(),
			0,
			effectiveBaseFee,
			usergas,
			ticketId,
			tx.FeeRefundAddr,
			availableRefund,
			submissionFee,
		)
		p.state.Restrict(err)

		_, err = retryable.IncrementNumTries()
		p.state.Restrict(err)

		err = EmitReedeemScheduledEvent(
			evm,
			usergas,
			retryTxInner.Nonce,
			ticketId,
			types.NewArbTx(retryTxInner).Hash(),
			tx.FeeRefundAddr,
			availableRefund,
			submissionFee,
		)
		if err != nil {
			glog.Error("failed to emit RedeemScheduled event", "err", err)
		}

		if tracer := evm.Config().Tracer; tracer != nil {
			redeem, err := util.PackArbRetryableTxRedeem(ticketId)
			if err == nil {
				tracingInfo.MockCall(redeem, usergas, from, types.ArbRetryableTxAddress, common.Big0)
			} else {
				glog.Error("failed to abi-encode auto-redeem", "err", err)
			}
		}

		return true, usergas, nil, ticketId.Bytes()
	case *types.ArbitrumRetryTx:

		// Transfer callvalue from escrow
		escrow := retryables.RetryableEscrowAddress(tx.TicketId)
		scenario := util.TracingBeforeEVM
		if err := util.TransferBalance(&escrow, &tx.From, tx.Value, evm, scenario, "escrow"); err != nil {
			return true, 0, err, nil
		}

		// The redeemer has pre-paid for this tx's gas
		prepaid := arbmath.BigMulByUint(evm.Context.BaseFee, tx.Gas)
		util.MintBalance(&tx.From, prepaid, evm, scenario, "prepaid")
		ticketId := tx.TicketId
		refundTo := tx.RefundTo
		p.CurrentRetryable = &ticketId
		p.CurrentRefundTo = &refundTo
	}
	return false, 0, nil, nil
}

func GetPosterGas(state *arbosState.ArbosState, baseFee *big.Int, runMode types.MessageRunMode, posterCost *big.Int) uint64 {
	if runMode == types.MessageGasEstimationMode {
		// Suggest the amount of gas needed for a given amount of ETH is higher in case of congestion.
		// This will help the user pad the total they'll pay in case the price rises a bit.
		// Note, reducing the poster cost will increase share the network fee gets, not reduce the total.

		minGasPrice, _ := state.L2PricingState().MinBaseFeeWei()

		adjustedPrice := arbmath.BigMulByFrac(baseFee, 7, 8) // assume congestion
		if arbmath.BigLessThan(adjustedPrice, minGasPrice.ToBig()) {
			adjustedPrice = minGasPrice.ToBig()
		}
		baseFee = adjustedPrice

		// Pad the L1 cost in case the L1 gas price rises
		posterCost = arbmath.BigMulByBips(posterCost, GasEstimationL1PricePadding)
	}

	return arbmath.BigToUintSaturating(arbmath.BigDiv(posterCost, baseFee))
}

func (p *TxProcessor) GasChargingHook(gasRemaining *uint64) (common.Address, error) {
	// Because a user pays a 1-dimensional gas price, we must re-express poster L1 calldata costs
	// as if the user was buying an equivalent amount of L2 compute gas. This hook determines what
	// that cost looks like, ensuring the user can pay and saving the result for later reference.

	var gasNeededToStartEVM uint64
	tipReceipient, _ := p.state.NetworkFeeAccount()
	var basefee *big.Int
	if p.evm.Context.BaseFeeInBlock != nil {
		basefee = p.evm.Context.BaseFeeInBlock.ToBig()
	} else {
		basefee = p.evm.Context.BaseFee.ToBig()
	}

	var poster common.Address
	if !p.msg.TxRunMode.ExecutedOnChain() {
		poster = l1pricing.BatchPosterAddress
	} else {
		poster = p.evm.Context.Coinbase
	}

	// todo ???
	if p.msg.TxRunMode.ExecutedOnChain() {
		p.msg.SkipL1Charging = false
	}
	if basefee.Sign() > 0 && !p.msg.SkipL1Charging {
		// Since tips go to the network, and not to the poster, we use the basefee.
		// Note, this only determines the amount of gas bought, not the price per gas.

		brotliCompressionLevel, err := p.state.BrotliCompressionLevel()
		if err != nil {
			return common.Address{}, fmt.Errorf("failed to get brotli compression level: %w", err)
		}
		posterCost, calldataUnits := p.state.L1PricingState().PosterDataCost(p.msg, poster, brotliCompressionLevel)
		if calldataUnits > 0 {
			p.state.Restrict(p.state.L1PricingState().AddToUnitsSinceUpdate(calldataUnits))
		}
		p.posterGas = GetPosterGas(p.state, basefee, p.msg.TxRunMode, posterCost)
		p.PosterFee = uint256.MustFromBig(arbmath.BigMulByUint(basefee, p.posterGas)) // round down
		gasNeededToStartEVM = p.posterGas
	}

	if *gasRemaining < gasNeededToStartEVM {
		// the user couldn't pay for call data, so give up
		return tipReceipient, core.ErrIntrinsicGas
	}
	*gasRemaining -= gasNeededToStartEVM

	if p.msg.TxRunMode != types.MessageEthcallMode {
		// If this is a real tx, limit the amount of computed based on the gas pool.
		// We do this by charging extra gas, and then refunding it later.
		gasAvailable, _ := p.state.L2PricingState().PerBlockGasLimit()
		if *gasRemaining > gasAvailable {
			p.computeHoldGas = *gasRemaining - gasAvailable
			*gasRemaining = gasAvailable
		}
	}
	return tipReceipient, nil
}

// func (p *TxProcessor) RunMode() core.MessageRunMode {
// 	return p.msg.TxRunMode
// }

func (p *TxProcessor) NonrefundableGas() uint64 {
	// EVM-incentivized activity like freeing storage should only refund amounts paid to the network address,
	// which represents the overall burden to node operators. A poster's costs, then, should not be eligible
	// for this refund.
	return p.posterGas
}

func (p *TxProcessor) ForceRefundGas() uint64 {
	return p.computeHoldGas
}

func (p *TxProcessor) EndTxHook(gasLeft uint64, success bool) {

	underlyingTx := p.msg
	networkFeeAccount, _ := p.state.NetworkFeeAccount()
	scenario := util.TracingAfterEVM

	if gasLeft > p.msg.Gas() {
		panic("Tx somehow refunds gas after computation")
	}
	gasUsed := p.msg.Gas() - gasLeft

	if underlyingTx != nil && underlyingTx.Type() == types.ArbitrumRetryTxType {
		inner, _ := underlyingTx.GetInner().(*types.ArbitrumRetryTx)
		effectiveBaseFee := inner.GasFeeCap
		if p.msg.TxRunMode.ExecutedOnChain() && !arbmath.BigEquals(effectiveBaseFee, p.evm.Context.BaseFee.ToBig()) {
			log.Error(
				"ArbitrumRetryTx GasFeeCap doesn't match basefee in commit mode",
				"txHash", underlyingTx.Hash(),
				"gasFeeCap", inner.GasFeeCap,
				"baseFee", p.evm.Context.BaseFee,
			)
			// revert to the old behavior to avoid diverging from older nodes
			effectiveBaseFee = p.evm.Context.BaseFee
		}

		// undo Geth's refund to the From address
		gasRefund := uint256.MustFromBig(arbmath.BigMulByUint(effectiveBaseFee, gasLeft))
		err := util.BurnBalance(&inner.From, gasRefund, p.evm, scenario, "undoRefund")
		if err != nil {
			log.Error("Uh oh, Geth didn't refund the user", inner.From, gasRefund)
		}
		ibs := p.evm.IntraBlockState()

		maxRefund := new(uint256.Int).Set(inner.MaxRefund)
		refund := func(refundFrom common.Address, amount *uint256.Int) {
			const errLog = "fee address doesn't have enough funds to give user refund"

			logMissingRefund := func(err error) {
				if !errors.Is(err, vm.ErrInsufficientBalance) {
					log.Error("unexpected error refunding balance", "err", err, "feeAddress", refundFrom)
					return
				}
				logLevel := log.Error
				codeSize, err := ibs.GetCodeSize(refundFrom)
				if err != nil {
					return
				}

				isContract := codeSize > 0
				if isContract {
					// It's expected that the balance might not still be in this address if it's a contract.
					logLevel = log.Debug
				}
				logLevel(errLog, "err", err, "feeAddress", refundFrom)
			}

			// Refund funds to the fee refund address without overdrafting the L1 deposit.
			toRefundAddr := takeFunds256(maxRefund, amount)
			err = util.TransferBalance(ibs, &refundFrom, &inner.RefundTo, toRefundAddr, p.evm, scenario, "refund")
			if err != nil {
				// Normally the network fee address should be holding any collected fees.
				// However, in theory, they could've been transferred out during the redeem attempt.
				// If the network fee address doesn't have the necessary balance, log an error and don't give a refund.
				logMissingRefund(err)
			}
			// Any extra refund can't be given to the fee refund address if it didn't come from the L1 deposit.
			// Instead, give the refund to the retryable from address.
			err = util.TransferBalance(ibs, &refundFrom, &inner.From, new(uint256.Int).Sub(amount, toRefundAddr), p.evm, scenario, "refund")
			if err != nil {
				logMissingRefund(err)
			}
		}

		if success {
			// If successful, refund the submission fee.
			refund(networkFeeAccount, inner.SubmissionFeeRefund)
		} else {
			// The submission fee is still taken from the L1 deposit earlier, even if it's not refunded.
			takeFunds256(maxRefund, inner.SubmissionFeeRefund)
		}
		// Conceptually, the gas charge is taken from the L1 deposit pool if possible.
		takeFunds256(maxRefund, new(uint256.Int).Mul(effectiveBaseFee, uint256.NewInt(gasUsed)))
		// Refund any unused gas, without overdrafting the L1 deposit.
		networkRefund := gasRefund
		if p.state.ArbOSVersion() >= 11 {
			infraFeeAccount, err := p.state.InfraFeeAccount()
			p.state.Restrict(err)
			if infraFeeAccount != (common.Address{}) {
				minBaseFee, err := p.state.L2PricingState().MinBaseFeeWei()
				p.state.Restrict(err)

				// TODO MinBaseFeeWei change during RetryTx execution may cause incorrect calculation of the part of the refund that should be taken from infraFeeAccount. Unless the balances of network and infra fee accounts are too low, the amount transferred to refund address should remain correct.
				infraFee := minBaseFee.Clone()
				if minBaseFee.Gt(effectiveBaseFee) {
					infraFee = effectiveBaseFee.Clone()
				}
				infraFee.Mul(infraFee, uint256.NewInt(gasLeft))
				// infraRefund := arbmath.BigMulByUint(infraFee, gasLeft)
				infraRefund := takeFunds256(networkRefund, infraFee)
				refund(infraFeeAccount, infraRefund)
			}
		}
		refund(networkFeeAccount, networkRefund)

		if success {
			// we don't want to charge for this
			tracingInfo := util.NewTracingInfo(p.evm, arbosAddress, p.msg.From(), scenario)
			state := arbosState.OpenSystemArbosStateOrPanic(p.evm.IntraBlockState(), tracingInfo, false)
			_, _ = state.RetryableState().DeleteRetryable(inner.TicketId, p.evm, scenario)
		} else {
			// return the Callvalue to escrow
			escrow := retryables.RetryableEscrowAddress(inner.TicketId)
			err := util.TransferBalance(ibs, &inner.From, &escrow, inner.Value, p.evm, scenario, "escrow")
			if err != nil {
				// should be impossible because geth credited the inner.Value to inner.From before the transaction
				// and the transaction reverted
				panic(err)
			}
		}
		// we've already credited the network fee account, but we didn't charge the gas pool yet
		p.state.Restrict(p.state.L2PricingState().AddToGasPool(-arbmath.SaturatingCast[int64](gasUsed)))
		return
	}

	var basefee *big.Int
	if p.evm.Context.BaseFeeInBlock != nil {
		basefee = p.evm.Context.BaseFeeInBlock.ToBig()
	} else {
		basefee = p.evm.Context.BaseFee.ToBig()
	}
	totalCost := arbmath.BigMul(basefee, arbmath.UintToBig(gasUsed)) // total cost = price of gas * gas burnt
	computeCost := arbmath.BigSub(totalCost, p.PosterFee.ToBig())    // total cost = network's compute + poster's L1 costs
	if computeCost.Sign() < 0 {
		// Uh oh, there's a bug in our charging code.
		// Give all funds to the network account and continue.

		log.Error("total cost < poster cost", "gasUsed", gasUsed, "basefee", basefee, "posterFee", p.PosterFee)
		p.PosterFee = uint256.NewInt(0)
		computeCost = totalCost
	}

	purpose := "feeCollection"
	if p.state.ArbOSVersion() > 4 {
		infraFeeAccount, err := p.state.InfraFeeAccount()
		p.state.Restrict(err)
		if infraFeeAccount != (common.Address{}) {
			minBaseFee, err := p.state.L2PricingState().MinBaseFeeWei()
			p.state.Restrict(err)
			infraFee := arbmath.BigMin(minBaseFee.ToBig(), basefee)
			computeGas := arbmath.SaturatingUSub(gasUsed, p.posterGas)
			infraComputeCost := arbmath.BigMulByUint(infraFee, computeGas)
			util.MintBalance(&infraFeeAccount, uint256.MustFromBig(infraComputeCost), p.evm, scenario, purpose)
			computeCost = arbmath.BigSub(computeCost, infraComputeCost)
		}
	}
	if arbmath.BigGreaterThan(computeCost, common.Big0) {
		util.MintBalance(&networkFeeAccount, uint256.MustFromBig(computeCost), p.evm, scenario, purpose)
	}
	posterFeeDestination := l1pricing.L1PricerFundsPoolAddress
	if p.state.ArbOSVersion() < 2 {
		posterFeeDestination = p.evm.Context.Coinbase
	}
	util.MintBalance(&posterFeeDestination, p.PosterFee, p.evm, scenario, purpose)
	if p.state.ArbOSVersion() >= 10 {
		if _, err := p.state.L1PricingState().AddToL1FeesAvailable(p.PosterFee.ToBig()); err != nil {
			log.Error("failed to update L1FeesAvailable: ", "err", err)
		}
	}

	if p.msg.GasPrice().ToBig().Sign() > 0 { // in tests, gas price could be 0
		// ArbOS's gas pool is meant to enforce the computational speed-limit.
		// We don't want to remove from the pool the poster's L1 costs (as expressed in L2 gas in this func)
		// Hence, we deduct the previously saved poster L2-gas-equivalent to reveal the compute-only gas

		var computeGas uint64
		if gasUsed > p.posterGas {
			// Don't include posterGas in computeGas as it doesn't represent processing time.
			computeGas = gasUsed - p.posterGas
		} else {
			// Somehow, the core message transition succeeded, but we didn't burn the posterGas.
			// An invariant was violated. To be safe, subtract the entire gas used from the gas pool.
			log.Error("total gas used < poster gas component", "gasUsed", gasUsed, "posterGas", p.posterGas)
			computeGas = gasUsed
		}
		p.state.Restrict(p.state.L2PricingState().AddToGasPool(-arbmath.SaturatingCast[int64](computeGas)))
	}
}

func (p *TxProcessor) ScheduledTxes() types.Transactions {
	scheduled := types.Transactions{}
	time := p.evm.Context.Time
	// p.evm.Context.BaseFee is already lowered to 0 when vm runs with NoBaseFee flag and 0 gas price
	effectiveBaseFee := p.evm.Context.BaseFee
	chainID := p.evm.ChainConfig().ChainID

	logs := p.evm.IntraBlockState().GetCurrentTxLogs()
	for _, log := range logs {
		if log.Address != ArbRetryableTxAddress || log.Topics[0] != RedeemScheduledEventID {
			continue
		}
		event, err := util.ParseRedeemScheduledLog(log)
		if err != nil {
			glog.Error("Failed to parse RedeemScheduled log", "err", err)
			continue
		}
		retryable, err := p.state.RetryableState().OpenRetryable(event.TicketId, time)
		if err != nil || retryable == nil {
			continue
		}
		redeem, _ := retryable.MakeTx(
			chainID,
			event.SequenceNum,
			effectiveBaseFee.ToBig(),
			event.DonatedGas,
			event.TicketId,
			event.GasDonor,
			event.MaxRefund,
			event.SubmissionFeeRefund,
		)
		scheduled = append(scheduled, types.NewArbTx(redeem))
	}
	return scheduled
}

func (p *TxProcessor) L1BlockNumber(blockCtx evmtypes.BlockContext) (uint64, error) {
	if p.cachedL1BlockNumber != nil {
		return *p.cachedL1BlockNumber, nil
	}
	tracingInfo := util.NewTracingInfo(p.evm, p.msg.From(), arbosAddress, util.TracingDuringEVM)
	state, err := arbosState.OpenSystemArbosState(p.evm.IntraBlockState(), tracingInfo, false)
	if err != nil {
		return 0, err
	}
	blockNum, err := state.Blockhashes().L1BlockNumber()
	if err != nil {
		return 0, err
	}
	p.cachedL1BlockNumber = &blockNum
	return blockNum, nil
}

func (p *TxProcessor) L1BlockHash(blockCtx evmtypes.BlockContext, l1BlockNumber uint64) (common.Hash, error) {
	hash, cached := p.cachedL1BlockHashes[l1BlockNumber]
	if cached {
		return hash, nil
	}
	tracingInfo := util.NewTracingInfo(p.evm, p.msg.From(), arbosAddress, util.TracingDuringEVM)
	state, err := arbosState.OpenSystemArbosState(p.evm.IntraBlockState(), tracingInfo, false)
	if err != nil {
		return common.Hash{}, err
	}
	hash, err = state.Blockhashes().BlockHash(l1BlockNumber)
	if err != nil {
		return common.Hash{}, err
	}
	p.cachedL1BlockHashes[l1BlockNumber] = hash
	return hash, nil
}

func (p *TxProcessor) DropTip() bool {
	version := p.state.ArbOSVersion()
	return version != 9 || p.delayedInbox
}

func (p *TxProcessor) GetPaidGasPrice() *big.Int {
	gasPrice := p.evm.GasPrice
	version := p.state.ArbOSVersion()
	if version != 9 {
		// p.evm.Context.BaseFee is already lowered to 0 when vm runs with NoBaseFee flag and 0 gas price
		gasPrice = p.evm.Context.BaseFee
	}
	return gasPrice.ToBig()
}

func (p *TxProcessor) GasPriceOp(evm *vm.EVM) *big.Int {
	if p.state.ArbOSVersion() >= 3 {
		return p.GetPaidGasPrice()
	}
	return evm.GasPrice.ToBig()
}

func (p *TxProcessor) FillReceiptInfo(receipt *types.Receipt) {
	receipt.GasUsedForL1 = p.posterGas
}

func (p *TxProcessor) MsgIsNonMutating() bool {
	if p.msg == nil {
		return false
	}
	mode := p.msg.TxRunMode
	return mode == types.MessageGasEstimationMode || mode == types.MessageEthcallMode
}
