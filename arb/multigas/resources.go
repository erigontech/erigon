package multigas

import (
	"encoding/json"
	"fmt"
	"math/bits"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/execution/protocol/params"
)

// ResourceKind represents a dimension for the multi-dimensional gas.
type ResourceKind uint8

const (
	ResourceKindUnknown ResourceKind = iota
	ResourceKindComputation
	ResourceKindHistoryGrowth
	ResourceKindStorageAccess
	ResourceKindStorageGrowth
	ResourceKindL1Calldata
	ResourceKindL2Calldata
	ResourceKindWasmComputation
	NumResourceKind
)

func (rk ResourceKind) String() string {
	switch rk {
	case ResourceKindUnknown:
		return "Unknown"
	case ResourceKindComputation:
		return "Computation"
	case ResourceKindHistoryGrowth:
		return "HistoryGrowth"
	case ResourceKindStorageAccess:
		return "StorageAccess"
	case ResourceKindStorageGrowth:
		return "StorageGrowth"
	case ResourceKindL1Calldata:
		return "L1Calldata"
	case ResourceKindL2Calldata:
		return "L2Calldata"
	case ResourceKindWasmComputation:
		return "WasmComputation"
	default:
		return fmt.Sprintf("ResourceKind(%d)", uint8(rk))
	}
}

func CheckResourceKind(id uint8) (ResourceKind, error) {
	if id <= uint8(ResourceKindUnknown) || id >= uint8(NumResourceKind) {
		return ResourceKindUnknown, fmt.Errorf("invalid resource id: %v", id)
	}
	return ResourceKind(id), nil
}

// MultiGas tracks gas usage across multiple resource kinds, while also
// maintaining a single-dimensional total gas sum and refund amount.
type MultiGas struct {
	gas    [NumResourceKind]uint64
	total  uint64
	refund uint64
}

// Pair represents a single resource kind and its associated gas amount.
type Pair struct {
	Kind   ResourceKind
	Amount uint64
}

// ZeroGas creates a MultiGas value with all fields set to zero.
func ZeroGas() MultiGas {
	return MultiGas{}
}

// NewMultiGas creates a new MultiGas with the given resource kind initialized to `amount`.
// All other kinds are zero. The total is also set to `amount`.
func NewMultiGas(kind ResourceKind, amount uint64) MultiGas {
	var mg MultiGas
	mg.gas[kind] = amount
	mg.total = amount
	return mg
}

// MultiGasFromPairs creates a new MultiGas from resource–amount pairs.
// Intended for constant-like construction; panics on overflow.
func MultiGasFromPairs(pairs ...Pair) MultiGas {
	var mg MultiGas
	for _, p := range pairs {
		newTotal, c := bits.Add64(mg.total, p.Amount, 0)
		if c != 0 {
			panic("multigas overflow")
		}
		mg.gas[p.Kind] = p.Amount
		mg.total = newTotal
	}
	return mg
}

// UnknownGas returns a MultiGas initialized with unknown gas.
func UnknownGas(amount uint64) MultiGas {
	return NewMultiGas(ResourceKindUnknown, amount)
}

// ComputationGas returns a MultiGas initialized with computation gas.
func ComputationGas(amount uint64) MultiGas {
	return NewMultiGas(ResourceKindComputation, amount)
}

// HistoryGrowthGas returns a MultiGas initialized with history growth gas.
func HistoryGrowthGas(amount uint64) MultiGas {
	return NewMultiGas(ResourceKindHistoryGrowth, amount)
}

// StorageAccessGas returns a MultiGas initialized with storage access gas.
func StorageAccessGas(amount uint64) MultiGas {
	return NewMultiGas(ResourceKindStorageAccess, amount)
}

// StorageGrowthGas returns a MultiGas initialized with storage growth gas.
func StorageGrowthGas(amount uint64) MultiGas {
	return NewMultiGas(ResourceKindStorageGrowth, amount)
}

// L1CalldataGas returns a MultiGas initialized with L1 calldata gas.
func L1CalldataGas(amount uint64) MultiGas {
	return NewMultiGas(ResourceKindL1Calldata, amount)
}

// L2CalldataGas returns a MultiGas initialized with L2 calldata gas.
func L2CalldataGas(amount uint64) MultiGas {
	return NewMultiGas(ResourceKindL2Calldata, amount)
}

// WasmComputationGas returns a MultiGas initialized with computation gas used for WASM (Stylus contracts).
func WasmComputationGas(amount uint64) MultiGas {
	return NewMultiGas(ResourceKindWasmComputation, amount)
}

// Get returns the gas amount for the specified resource kind.
func (z MultiGas) Get(kind ResourceKind) uint64 {
	return z.gas[kind]
}

// With returns a copy of z with the given resource kind set to amount.
// The total is adjusted accordingly. It returns the updated value and true if an overflow occurred.
func (z MultiGas) With(kind ResourceKind, amount uint64) (MultiGas, bool) {
	res := z
	newTotal, c := bits.Add64(z.total-z.gas[kind], amount, 0)
	if c != 0 {
		return z, true
	}
	res.gas[kind] = amount
	res.total = newTotal
	return res, false
}

// GetRefund gets the SSTORE refund computed at the end of the transaction.
func (z MultiGas) GetRefund() uint64 {
	return z.refund
}

// WithRefund returns a copy of z with its refund set to amount.
func (z MultiGas) WithRefund(amount uint64) MultiGas {
	res := z
	res.refund = amount
	return res
}

// SafeAdd returns a copy of z with the per-kind, total, and refund gas
// added to the values from x. It returns the updated value and true if
// an overflow occurred.
func (z MultiGas) SafeAdd(x MultiGas) (MultiGas, bool) {
	res := z

	for i := 0; i < int(NumResourceKind); i++ {
		v, c := bits.Add64(res.gas[i], x.gas[i], 0)
		if c != 0 {
			return z, true
		}
		res.gas[i] = v
	}

	t, c := bits.Add64(res.total, x.total, 0)
	if c != 0 {
		return z, true
	}
	res.total = t

	r, c := bits.Add64(res.refund, x.refund, 0)
	if c != 0 {
		return z, true
	}
	res.refund = r

	return res, false
}

// SaturatingAdd returns a copy of z with the per-kind, total, and refund gas
// added to the values from x. On overflow, the affected field(s) are clamped
// to MaxUint64.
func (z MultiGas) SaturatingAdd(x MultiGas) MultiGas {
	res := z

	for i := 0; i < int(NumResourceKind); i++ {
		if v, c := bits.Add64(res.gas[i], x.gas[i], 0); c != 0 {
			res.gas[i] = ^uint64(0) // clamp
		} else {
			res.gas[i] = v
		}
	}

	if t, c := bits.Add64(res.total, x.total, 0); c != 0 {
		res.total = ^uint64(0) // clamp
	} else {
		res.total = t
	}

	if r, c := bits.Add64(res.refund, x.refund, 0); c != 0 {
		res.refund = ^uint64(0) // clamp
	} else {
		res.refund = r
	}

	return res
}

// SaturatingAddInto adds x into z in place (per kind, total, and refund).
// On overflow, the affected field(s) are clamped to MaxUint64.
// This is a hot-path helper; the public immutable API remains preferred elsewhere.
func (z *MultiGas) SaturatingAddInto(x MultiGas) {
	for i := 0; i < int(NumResourceKind); i++ {
		if v, c := bits.Add64(z.gas[i], x.gas[i], 0); c != 0 {
			z.gas[i] = ^uint64(0) // clamp
		} else {
			z.gas[i] = v
		}
	}
	if t, c := bits.Add64(z.total, x.total, 0); c != 0 {
		z.total = ^uint64(0) // clamp
	} else {
		z.total = t
	}
	if r, c := bits.Add64(z.refund, x.refund, 0); c != 0 {
		z.refund = ^uint64(0) // clamp
	} else {
		z.refund = r
	}
}

// SafeSub returns a copy of z with the per-kind, total, and refund gas
// subtracted by the values from x. It returns the updated value and true if
// a underflow occurred.
func (z MultiGas) SafeSub(x MultiGas) (MultiGas, bool) {
	res := z

	for i := 0; i < int(NumResourceKind); i++ {
		v, b := bits.Sub64(res.gas[i], x.gas[i], 0)
		if b != 0 {
			return z, true
		}
		res.gas[i] = v
	}

	t, b := bits.Sub64(res.total, x.total, 0)
	if b != 0 {
		return z, true
	}
	res.total = t

	r, b := bits.Sub64(res.refund, x.refund, 0)
	if b != 0 {
		return z, true
	}
	res.refund = r

	return res, false
}

// SaturatingSub returns a copy of z with the per-kind, total, and refund gas
// subtracted by the values from x. On underflow, the affected field(s) are
// clamped to zero.
func (z MultiGas) SaturatingSub(x MultiGas) MultiGas {
	res := z

	for i := 0; i < int(NumResourceKind); i++ {
		if v, c := bits.Sub64(res.gas[i], x.gas[i], 0); c != 0 {
			res.gas[i] = uint64(0) // clamp
		} else {
			res.gas[i] = v
		}
	}

	if t, c := bits.Sub64(res.total, x.total, 0); c != 0 {
		res.total = uint64(0) // clamp
	} else {
		res.total = t
	}

	if r, c := bits.Sub64(res.refund, x.refund, 0); c != 0 {
		res.refund = uint64(0) // clamp
	} else {
		res.refund = r
	}

	return res
}

// SafeIncrement returns a copy of z with the given resource kind
// and the total incremented by gas. It returns the updated value and true if
// an overflow occurred.
func (z MultiGas) SafeIncrement(kind ResourceKind, gas uint64) (MultiGas, bool) {
	res := z

	newValue, c := bits.Add64(z.gas[kind], gas, 0)
	if c != 0 {
		return res, true
	}

	newTotal, c := bits.Add64(z.total, gas, 0)
	if c != 0 {
		return res, true
	}

	res.gas[kind] = newValue
	res.total = newTotal
	return res, false
}

// SaturatingIncrement returns a copy of z with the given resource kind
// and the total incremented by gas. On overflow, the field(s) are clamped to MaxUint64.
func (z MultiGas) SaturatingIncrement(kind ResourceKind, gas uint64) MultiGas {
	res := z

	if v, c := bits.Add64(res.gas[kind], gas, 0); c != 0 {
		res.gas[kind] = ^uint64(0) // clamp
	} else {
		res.gas[kind] = v
	}

	if t, c := bits.Add64(res.total, gas, 0); c != 0 {
		res.total = ^uint64(0) // clamp
	} else {
		res.total = t
	}

	return res
}

// SaturatingIncrementInto increments the given resource kind and the total
// in place by gas. On overflow, the affected field(s) are clamped to MaxUint64.
// Unlike SaturatingIncrement, this method mutates the receiver directly and
// is intended for VM hot paths where avoiding value copies is critical.
func (z *MultiGas) SaturatingIncrementInto(kind ResourceKind, gas uint64) {
	if v, c := bits.Add64(z.gas[kind], gas, 0); c != 0 {
		z.gas[kind] = ^uint64(0)
	} else {
		z.gas[kind] = v
	}

	if t, c := bits.Add64(z.total, gas, 0); c != 0 {
		z.total = ^uint64(0)
	} else {
		z.total = t
	}
}

// SingleGas returns the single-dimensional total gas.
func (z MultiGas) SingleGas() uint64 {
	return z.total
}

func (z MultiGas) IsZero() bool {
	return z.total == 0 && z.refund == 0 && z.gas == [NumResourceKind]uint64{}
}

// multiGasJSON is an auxiliary type for JSON marshaling/unmarshaling of MultiGas.
type multiGasJSON struct {
	Unknown         hexutil.Uint64 `json:"unknown"`
	Computation     hexutil.Uint64 `json:"computation"`
	HistoryGrowth   hexutil.Uint64 `json:"historyGrowth"`
	StorageAccess   hexutil.Uint64 `json:"storageAccess"`
	StorageGrowth   hexutil.Uint64 `json:"storageGrowth"`
	L1Calldata      hexutil.Uint64 `json:"l1Calldata"`
	L2Calldata      hexutil.Uint64 `json:"l2Calldata"`
	WasmComputation hexutil.Uint64 `json:"wasmComputation"`
	Refund          hexutil.Uint64 `json:"refund"`
	Total           hexutil.Uint64 `json:"total"`
}

// MarshalJSON implements json.Marshaler for MultiGas.
func (z MultiGas) MarshalJSON() ([]byte, error) {
	return json.Marshal(multiGasJSON{
		Unknown:         hexutil.Uint64(z.gas[ResourceKindUnknown]),
		Computation:     hexutil.Uint64(z.gas[ResourceKindComputation]),
		HistoryGrowth:   hexutil.Uint64(z.gas[ResourceKindHistoryGrowth]),
		StorageAccess:   hexutil.Uint64(z.gas[ResourceKindStorageAccess]),
		StorageGrowth:   hexutil.Uint64(z.gas[ResourceKindStorageGrowth]),
		L1Calldata:      hexutil.Uint64(z.gas[ResourceKindL1Calldata]),
		L2Calldata:      hexutil.Uint64(z.gas[ResourceKindL2Calldata]),
		WasmComputation: hexutil.Uint64(z.gas[ResourceKindWasmComputation]),
		Refund:          hexutil.Uint64(z.refund),
		Total:           hexutil.Uint64(z.total),
	})
}

// UnmarshalJSON implements json.Unmarshaler for MultiGas.
func (z *MultiGas) UnmarshalJSON(data []byte) error {
	var j multiGasJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return err
	}
	*z = ZeroGas()
	z.gas[ResourceKindUnknown] = uint64(j.Unknown)
	z.gas[ResourceKindComputation] = uint64(j.Computation)
	z.gas[ResourceKindHistoryGrowth] = uint64(j.HistoryGrowth)
	z.gas[ResourceKindStorageAccess] = uint64(j.StorageAccess)
	z.gas[ResourceKindStorageGrowth] = uint64(j.StorageGrowth)
	z.gas[ResourceKindL1Calldata] = uint64(j.L1Calldata)
	z.gas[ResourceKindL2Calldata] = uint64(j.L2Calldata)
	z.gas[ResourceKindWasmComputation] = uint64(j.WasmComputation)
	z.refund = uint64(j.Refund)
	z.total = uint64(j.Total)
	return nil
}

//
//// IntrinsicMultiGas returns the intrinsic gas as a multi-gas.
//func IntrinsicMultiGas(data []byte, accessList types.AccessList, authList []types.SetCodeAuthorization, isContractCreation, isHomestead, isEIP2028, isEIP3860 bool) (MultiGas, error) {
//	// Set the starting gas for the raw transaction
//	var gas MultiGas
//	if isContractCreation && isHomestead {
//		gas.SaturatingIncrementInto(ResourceKindComputation, params.TxGasContractCreation)
//	} else {
//		gas.SaturatingIncrementInto(ResourceKindComputation, params.TxGas)
//	}
//	dataLen := uint64(len(data))
//	// Bump the required gas by the amount of transactional data
//	if dataLen > 0 {
//		// Zero and non-zero bytes are priced differently
//		z := uint64(bytes.Count(data, []byte{0}))
//		nz := dataLen - z
//
//		// Make sure we don't exceed uint64 for all data combinations
//		nonZeroGas := params.TxDataNonZeroGasFrontier
//		if isEIP2028 {
//			nonZeroGas = params.TxDataNonZeroGasEIP2028
//		}
//		if (math.MaxUint64-gas.SingleGas())/nonZeroGas < nz {
//			return ZeroGas(), vm.ErrGasUintOverflow
//		}
//		gas.SaturatingIncrementInto(ResourceKindL2Calldata, nz*nonZeroGas)
//
//		if (math.MaxUint64-gas.SingleGas())/params.TxDataZeroGas < z {
//			return ZeroGas(), vm.ErrGasUintOverflow
//		}
//		gas.SaturatingIncrementInto(ResourceKindL2Calldata, z*params.TxDataZeroGas)
//
//		if isContractCreation && isEIP3860 {
//			lenWords := toWordSize(dataLen)
//			if (math.MaxUint64-gas.SingleGas())/params.InitCodeWordGas < lenWords {
//				return ZeroGas(), vm.ErrGasUintOverflow
//			}
//			gas.SaturatingIncrementInto(ResourceKindComputation, lenWords*params.InitCodeWordGas)
//		}
//	}
//	if lenAccessList != nil {
//		gas.SaturatingIncrementInto(ResourceKindStorageAccess, lenAccessList*params.TxAccessListAddressGas)
//		gas.SaturatingIncrementInto(ResourceKindStorageAccess, uint64(accessList.StorageKeys())*params.TxAccessListStorageKeyGas)
//	}
//	if authList != nil {
//		gas.SaturatingIncrementInto(ResourceKindStorageGrowth, uint64(len(authList))*params.CallNewAccountGas)
//	}
//	return gas, nil
//}

// IntrinsicMultiGas returns the intrinsic gas as a multi-gas. (TODO: move to arb package)
func IntrinsicMultiGas(data []byte, accessListLen, storageKeysLen uint64, isContractCreation bool, isEIP2, isEIP2028, isEIP3860, isEIP7623, isAATxn bool, authorizationsLen uint64) (MultiGas, uint64, bool) {
	dataLen := uint64(len(data))
	dataNonZeroLen := uint64(0)
	for _, byt := range data {
		if byt != 0 {
			dataNonZeroLen++
		}
	}

	gas := ZeroGas()
	// Set the starting gas for the raw transaction
	if isContractCreation && isEIP2 {
		gas.SaturatingIncrementInto(ResourceKindComputation, params.TxGasContractCreation)
	} else if isAATxn {
		gas.SaturatingIncrementInto(ResourceKindComputation, params.TxAAGas)
	} else {
		gas.SaturatingIncrementInto(ResourceKindComputation, params.TxGas)
	}
	floorGas7623 := params.TxGas

	// Bump the required gas by the amount of transactional data
	if dataLen > 0 {
		// Zero and non-zero bytes are priced differently
		nz := dataNonZeroLen
		// Make sure we don't exceed uint64 for all data combinations
		nonZeroGas := params.TxDataNonZeroGasFrontier
		if isEIP2028 {
			nonZeroGas = params.TxDataNonZeroGasEIP2028
		}

		if (math.MaxUint64-gas.SingleGas())/nonZeroGas < nz {
			return ZeroGas(), 0, true
		}
		gas.SaturatingIncrementInto(ResourceKindL2Calldata, nz*nonZeroGas)

		z := dataLen - nz

		if (math.MaxUint64-gas.SingleGas())/params.TxDataZeroGas < z {
			return ZeroGas(), 0, true
		}
		gas.SaturatingIncrementInto(ResourceKindL2Calldata, z*params.TxDataZeroGas)

		if isContractCreation && isEIP3860 {
			lenWords := toWordSize(dataLen)
			if (math.MaxUint64-gas.SingleGas())/params.InitCodeWordGas < lenWords {
				return ZeroGas(), 0, true
			}
			gas.SaturatingIncrementInto(ResourceKindComputation, lenWords*params.InitCodeWordGas)
		}

		if isEIP7623 {
			tokenLen := dataLen + 3*nz
			dataGas, overflow := math.SafeMul(tokenLen, params.TxTotalCostFloorPerToken)
			if overflow {
				return ZeroGas(), 0, true
			}
			floorGas7623, overflow = math.SafeAdd(floorGas7623, dataGas)
			if overflow {
				return ZeroGas(), 0, true
			}
		}
	}
	if accessListLen > 0 {
		gas.SaturatingIncrementInto(ResourceKindStorageAccess, accessListLen*params.TxAccessListAddressGas)
		gas.SaturatingIncrementInto(ResourceKindStorageAccess, storageKeysLen*params.TxAccessListStorageKeyGas)
	}

	if authorizationsLen > 0 {
		gas.SaturatingIncrementInto(ResourceKindStorageGrowth, authorizationsLen*params.CallNewAccountGas)
	}

	return gas, floorGas7623, false
}

func toWordSize(size uint64) uint64 {
	if size > math.MaxUint64-31 {
		return math.MaxUint64/32 + 1
	}
	return (size + 31) / 32
}

func (z MultiGas) String() string {
	s := "mG:\n\t"
	for i := 0; i < int(NumResourceKind); i++ {
		s += fmt.Sprintf("%s: %d, ", ResourceKind(i).String(), z.gas[ResourceKind(i)])
		if i%4 == 0 && i > 0 {
			s += "\n\t"
		}
	}
	s += fmt.Sprintf("Total: %d, Refund: %d", z.total, z.refund)
	return s
}
