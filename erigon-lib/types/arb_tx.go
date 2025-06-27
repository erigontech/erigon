package types

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	cmath "github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/rlp"
)

var (
	ErrGasFeeCapTooLow  = errors.New("fee cap less than base fee")
	errShortTypedTx     = errors.New("typed transaction too short")
	errInvalidYParity   = errors.New("'yParity' field must be 0 or 1")
	errVYParityMismatch = errors.New("'v' and 'yParity' fields do not match")
	errVYParityMissing  = errors.New("missing 'yParity' or 'v' field in transaction")
)

// getPooledBuffer retrieves a buffer from the pool and creates a byte slice of the
// requested size from it.
//
// The caller should return the *bytes.Buffer object back into encodeBufferPool after use!
// The returned byte slice must not be used after returning the buffer.
func getPooledBuffer(size uint64) ([]byte, *bytes.Buffer, error) {
	if size > math.MaxInt {
		return nil, nil, fmt.Errorf("can't get buffer of size %d", size)
	}
	buf := encodeBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	buf.Grow(int(size))
	b := buf.Bytes()[:int(size)]
	return b, buf, nil
}

// ArbTx is an Arbitrum transaction.
type ArbTx struct {
	BlobTxWrapper
	inner Transaction // Consensus contents of a transaction
	// sidecar *BlobTxSidecar
	time time.Time // Time first seen locally (spam avoidance)

	// Arbitrum cache: must be atomically accessed
	CalldataUnits uint64

	// caches
	hash atomic.Value
	size atomic.Value
	from atomic.Value
}

// NewTx creates a new transaction.
func NewArbTx(inner Transaction) *ArbTx {
	tx := new(ArbTx)
	tx.setDecoded(inner.Unwrap(), 0)
	return tx
}

// EncodeRLP implements rlp.Encoder
// func (tx *ArbTx) EncodeRLP(w io.Writer) error {
// 	if tx.Type() == LegacyTxType {
// 		return rlp.Encode(w, tx.inner)
// 	}
// 	// It's an EIP-2718 typed TX envelope.
// 	buf := encodeBufferPool.Get().(*bytes.Buffer)
// 	defer encodeBufferPool.Put(buf)
// 	buf.Reset()
// 	if err := tx.encodeTyped(buf); err != nil {
// 		return err
// 	}
// 	return rlp.Encode(w, buf.Bytes())
// }

// encodeTyped writes the canonical encoding of a typed transaction to w.
func (tx *ArbTx) encodeTyped(w *bytes.Buffer) error {
	w.WriteByte(tx.Type())
	return tx.inner.EncodeRLP(w)
}

func (tx *ArbTx) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (*Message, error) {
	msg, err := tx.Tx.AsMessage(s, baseFee, rules)
	if err == nil {
		msg.Tx = tx
	}
	return msg, err
}

// MarshalBinary returns the canonical encoding of the transaction.
// For legacy transactions, it returns the RLP encoding. For EIP-2718 typed
// transactions, it returns the type and payload.
// func (tx *ArbTx) MarshalBinary() ([]byte, error) {
// 	if tx.Type() == LegacyTxType {
// 		return rlp.EncodeToBytes(tx.inner)
// 	}
// 	var buf bytes.Buffer
// 	err := tx.encodeTyped(&buf)
// 	return buf.Bytes(), err
// }

// DecodeRLP implements rlp.Decoder
func (tx *ArbTx) DecodeRLP(s *rlp.Stream) error {
	kind, size, err := s.Kind()
	switch {
	case err != nil:
		return err
	case kind == rlp.List:
		// It's a legacy transaction.
		var inner LegacyTx
		err := s.Decode(&inner)
		if err == nil {
			tx.setDecoded(&inner, rlp.ListSize(size))
		}
		return err
	case kind == rlp.Byte:
		return errShortTypedTx
	default:
		//b, buf, err := getPooledBuffer(size)
		//if err != nil {
		//	return err
		//}
		//defer encodeBufferPool.Put(buf)
		//s.

		// It's an EIP-2718 typed TX envelope.
		// First read the tx payload bytes into a temporary buffer.
		b, err := s.Bytes()
		if err != nil {
			return err
		}
		// Now decode the inner transaction.
		inner, err := tx.decodeTyped(b, true)
		if err == nil {
			tx.setDecoded(inner, size)
		}
		return err
	}
}

// UnmarshalBinary decodes the canonical encoding of transactions.
// It supports legacy RLP transactions and EIP-2718 typed transactions.
func (tx *ArbTx) UnmarshalBinary(b []byte) error {
	if len(b) > 0 && b[0] > 0x7f {
		// It's a legacy transaction.
		var data LegacyTx
		err := rlp.DecodeBytes(b, &data)
		if err != nil {
			return err
		}
		tx.setDecoded(&data, uint64(len(b)))
		return nil
	}
	// It's an EIP-2718 typed transaction envelope.
	inner, err := tx.decodeTyped(b, false)
	if err != nil {
		return err
	}
	tx.setDecoded(inner, uint64(len(b)))
	return nil
}

// decodeTyped decodes a typed transaction from the canonical format.
func (tx *ArbTx) decodeTyped(b []byte, arbParsing bool) (Transaction, error) {
	if len(b) <= 1 {
		return nil, errShortTypedTx
	}
	var inner Transaction
	if arbParsing {
		switch b[0] {
		case ArbitrumDepositTxType:
			inner = new(ArbitrumDepositTx)
		case ArbitrumInternalTxType:
			inner = new(ArbitrumInternalTx)
		case ArbitrumUnsignedTxType:
			inner = new(ArbitrumUnsignedTx)
		case ArbitrumContractTxType:
			inner = new(ArbitrumContractTx)
		case ArbitrumRetryTxType:
			inner = new(ArbitrumRetryTx)
		case ArbitrumSubmitRetryableTxType:
			inner = new(ArbitrumSubmitRetryableTx)
		case ArbitrumLegacyTxType:
			inner = new(ArbitrumLegacyTxData)
		default:
			arbParsing = false
		}
	}
	if !arbParsing {
		switch b[0] {
		case AccessListTxType:
			inner = new(AccessListTx)
		case DynamicFeeTxType:
			inner = new(DynamicFeeTransaction)
		case BlobTxType:
			inner = new(BlobTx)
		default:
			return nil, ErrTxTypeNotSupported
		}
	}
	s := rlp.NewStream(bytes.NewReader(b[1:]), uint64(len(b)-1))
	err := inner.DecodeRLP(s)
	return inner, err
}

// setDecoded sets the inner transaction and size after decoding.
func (tx *ArbTx) setDecoded(inner Transaction, size uint64) {
	tx.inner = inner
	tx.time = time.Now()
	if size > 0 {
		tx.size.Store(size)
	}
}

// Protected says whether the transaction is replay-protected.
func (tx *ArbTx) Protected() bool {
	switch tx := tx.inner.(type) {
	case *LegacyTx:
		return !tx.V.IsZero() && isProtectedV(&tx.V)
	default:
		return true
	}
}

// Type returns the transaction type.
func (tx *ArbTx) Type() uint8 {
	return tx.inner.Type()
	//return tx.inner.txType()
}

func (tx *ArbTx) GetInner() Transaction {
	return tx.inner
}

// ChainId returns the EIP155 chain ID of the transaction. The return value will always be
// non-nil. For legacy transactions which are not replay-protected, the return value is
// zero.
func (tx *ArbTx) ChainId() *big.Int {
	return tx.inner.GetChainID().ToBig()
}

// Data returns the input data of the transaction.
func (tx *ArbTx) Data() []byte { return tx.inner.GetData() }

// AccessList returns the access list of the transaction.
func (tx *ArbTx) AccessList() AccessList { return tx.inner.GetAccessList() }

// Gas returns the gas limit of the transaction.
func (tx *ArbTx) Gas() uint64 { return tx.inner.GetGasLimit() }

// GasPrice returns the gas price of the transaction.
// TODO same as .GasFeeCap()?
func (tx *ArbTx) GasPrice() *big.Int { return new(big.Int).Set(tx.inner.GetFeeCap().ToBig()) }

// GasTipCap returns the gasTipCap per gas of the transaction.
func (tx *ArbTx) GasTipCap() *big.Int { return new(big.Int).Set(tx.inner.GetTipCap().ToBig()) }

// GasFeeCap returns the fee cap per gas of the transaction.
func (tx *ArbTx) GasFeeCap() *big.Int { return new(big.Int).Set(tx.inner.GetFeeCap().ToBig()) }

// Value returns the ether amount of the transaction.
func (tx *ArbTx) Value() *big.Int { return new(big.Int).Set(tx.inner.GetValue().ToBig()) }

// Nonce returns the sender account nonce of the transaction.
func (tx *ArbTx) Nonce() uint64 { return tx.inner.GetNonce() }

// To returns the recipient address of the transaction.
// For contract-creation transactions, To returns nil.
func (tx *ArbTx) To() *common.Address {
	return copyAddressPtr(tx.inner.GetTo())
}

// Cost returns (gas * gasPrice) + (blobGas * blobGasPrice) + value.
func (tx *ArbTx) Cost() *big.Int {
	total := new(big.Int).Mul(tx.GasPrice(), new(big.Int).SetUint64(tx.Gas()))
	if tx.Type() == BlobTxType {
		total.Add(total, new(big.Int).Mul(tx.BlobGasFeeCap(), new(big.Int).SetUint64(tx.BlobGas())))
	}
	total.Add(total, tx.Value())
	return total
}

// GasFeeCapCmp compares the fee cap of two transactions.
func (tx *ArbTx) GasFeeCapCmp(other *ArbTx) int {
	return tx.inner.GetFeeCap().ToBig().Cmp(other.inner.GetFeeCap().ToBig())
}

// GasFeeCapIntCmp compares the fee cap of the transaction against the given fee cap.
func (tx *ArbTx) GasFeeCapIntCmp(other *big.Int) int {
	return tx.inner.GetFeeCap().ToBig().Cmp(other)
}

// GasTipCapCmp compares the gasTipCap of two transactions.
func (tx *ArbTx) GasTipCapCmp(other *ArbTx) int {
	return tx.inner.GetTipCap().Cmp(other.inner.GetTipCap())
}

// GasTipCapIntCmp compares the gasTipCap of the transaction against the given gasTipCap.
func (tx *ArbTx) GasTipCapIntCmp(other *big.Int) int {
	return tx.inner.GetTipCap().ToBig().Cmp(other)
}

// EffectiveGasTip returns the effective miner gasTipCap for the given base fee.
// Note: if the effective gasTipCap is negative, this method returns both error
// the actual negative value, _and_ ErrGasFeeCapTooLow
func (tx *ArbTx) EffectiveGasTip(baseFee *big.Int) (*big.Int, error) {
	if baseFee == nil {
		return tx.GasTipCap(), nil
	}
	var err error
	gasFeeCap := tx.GasFeeCap()
	if gasFeeCap.Cmp(baseFee) == -1 {
		err = ErrGasFeeCapTooLow
	}
	minn := tx.GasTipCap()
	gasCap := gasFeeCap.Sub(gasFeeCap, baseFee)
	if minn.Cmp(gasCap) > 0 {
		minn = gasCap
	}
	return minn, err
}

// EffectiveGasTipValue is identical to EffectiveGasTip, but does not return an
// error in case the effective gasTipCap is negative
func (tx *ArbTx) EffectiveGasTipValue(baseFee *big.Int) *big.Int {
	effectiveTip, _ := tx.EffectiveGasTip(baseFee)
	return effectiveTip
}

// EffectiveGasTipCmp compares the effective gasTipCap of two transactions assuming the given base fee.
func (tx *ArbTx) EffectiveGasTipCmp(other *ArbTx, baseFee *big.Int) int {
	if baseFee == nil {
		return tx.GasTipCapCmp(other)
	}
	return tx.EffectiveGasTipValue(baseFee).Cmp(other.EffectiveGasTipValue(baseFee))
}

// EffectiveGasTipIntCmp compares the effective gasTipCap of a transaction to the given gasTipCap.
func (tx *ArbTx) EffectiveGasTipIntCmp(other *big.Int, baseFee *big.Int) int {
	if baseFee == nil {
		return tx.GasTipCapIntCmp(other)
	}
	return tx.EffectiveGasTipValue(baseFee).Cmp(other)
}

// BlobGas returns the blob gas limit of the transaction for blob transactions, 0 otherwise.
func (tx *ArbTx) BlobGas() uint64 {
	if blobtx, ok := tx.inner.(*BlobTx); ok {
		return blobtx.GetBlobGas()
	}
	return 0
}

// BlobGasFeeCap returns the blob gas fee cap per blob gas of the transaction for blob transactions, nil otherwise.
func (tx *ArbTx) BlobGasFeeCap() *big.Int {
	if blobtx, ok := tx.inner.(*BlobTx); ok {
		return blobtx.GetFeeCap().ToBig()
	}
	return nil
}

// BlobHashes returns the hashes of the blob commitments for blob transactions, nil otherwise.
func (tx *ArbTx) BlobHashes() []common.Hash {
	if blobtx, ok := tx.inner.(*BlobTx); ok {
		return blobtx.GetBlobHashes()
	}
	return nil
}

// BlobTxSidecar returns the sidecar of a blob transaction, nil otherwise.
func (tx *ArbTx) BlobTxSidecar() *BlobTxWrapper {
	//if blobtx, ok := tx.inner.(*BlobTx); ok {
	//	//return blobtx.Get
	//}
	return &tx.BlobTxWrapper
}

// BlobGasFeeCapCmp compares the blob fee cap of two transactions.
func (tx *ArbTx) BlobGasFeeCapCmp(other *ArbTx) int {
	return tx.BlobGasFeeCap().Cmp(other.BlobGasFeeCap())
}

// BlobGasFeeCapIntCmp compares the blob fee cap of the transaction against the given blob fee cap.
func (tx *ArbTx) BlobGasFeeCapIntCmp(other *big.Int) int {
	return tx.BlobGasFeeCap().Cmp(other)
}

//// WithoutBlobTxSidecar returns a copy of tx with the blob sidecar removed.
//func (tx *ArbTx) WithoutBlobTxSidecar() *ArbTx {
//	blobtx, ok := tx.inner.(*BlobTx)
//	if !ok {
//		return tx
//	}
//	cpy := &ArbTx{
//		inner: blobtx.withoutSidecar(),
//		time:  tx.time,
//	}
//	// Note: tx.size cache not carried over because the sidecar is included in size!
//	if h := tx.hash.Load(); h != nil {
//		cpy.hash.Store(h)
//	}
//	if f := tx.from.Load(); f != nil {
//		cpy.from.Store(f)
//	}
//	return cpy
//}

// BlobTxSidecar contains the blobs of a blob transaction.
// type BlobTxSidecar struct {
// 	Blobs       []kzg4844.Blob          // Blobs needed by the blob pool
// 	Commitments []kzg4844.KZGCommitment // Commitments needed by the blob pool
// 	Proofs      []kzg4844.KZGProof      // Proofs needed by the blob pool
// }

// // BlobHashes computes the blob hashes of the given blobs.
// func (sc *BlobTxSidecar) BlobHashes() []common.Hash {
// 	hasher := sha256.New()
// 	h := make([]common.Hash, len(sc.Commitments))
// 	for i := range sc.Blobs {
// 		h[i] = kzg4844.CalcBlobHashV1(hasher, &sc.Commitments[i])
// 	}
// 	return h
// }

// // encodedSize computes the RLP size of the sidecar elements. This does NOT return the
// // encoded size of the BlobTxSidecar, it's just a helper for tx.Size().
// func (sc *BlobTxSidecar) encodedSize() uint64 {
// 	var blobs, commitments, proofs uint64
// 	for i := range sc.Blobs {
// 		blobs += rlp.BytesSize(sc.Blobs[i][:])
// 	}
// 	for i := range sc.Commitments {
// 		commitments += rlp.BytesSize(sc.Commitments[i][:])
// 	}
// 	for i := range sc.Proofs {
// 		proofs += rlp.BytesSize(sc.Proofs[i][:])
// 	}
// 	return rlp.ListSize(blobs) + rlp.ListSize(commitments) + rlp.ListSize(proofs)
// }

// WithBlobTxSidecar returns a copy of tx with the blob sidecar added.
// TODO figure out how to add the sidecar
func (tx *ArbTx) WithBlobTxSidecar(sideCar *BlobTxWrapper) *ArbTx {
	//blobtx, ok := tx.inner.(*BlobTx)
	//if !ok {
	//	return tx
	//}
	cpy := &ArbTx{
		inner: tx.inner,
		//inner: blobtx.withSidecar(sideCar),
		// sidecar: sideCar,
		time: tx.time,
	}
	// Note: tx.size cache not carried over because the sidecar is included in size!
	if h := tx.hash.Load(); h != nil {
		cpy.hash.Store(h)
	}
	if f := tx.from.Load(); f != nil {
		cpy.from.Store(f)
	}
	return cpy
}

// SetTime sets the decoding time of a transaction. This is used by tests to set
// arbitrary times and by persistent transaction pools when loading old txs from
// disk.
func (tx *ArbTx) SetTime(t time.Time) {
	tx.time = t
}

// Time returns the time when the transaction was first seen on the network. It
// is a heuristic to prefer mining older txs vs new all other things equal.
func (tx *ArbTx) Time() time.Time {
	return tx.time
}

// Hash returns the transaction hash.
func (tx *ArbTx) Hash() common.Hash {
	if hash := tx.hash.Load(); hash != nil {
		return hash.(common.Hash)
	}

	var h common.Hash
	if tx.Type() == LegacyTxType {
		h = rlpHash(tx.inner)
	} else if tx.Type() == ArbitrumLegacyTxType {
		h = tx.inner.(*ArbitrumLegacyTxData).HashOverride
	} else {
		h = prefixedRlpHash(tx.Type(), tx.inner)
	}
	tx.hash.Store(h)
	return h
}

// Size returns the true encoded storage size of the transaction, either by encoding
// and returning it, or returning a previously cached value.
// func (tx *ArbTx) Size() uint64 {
// 	if size := tx.size.Load(); size != nil {
// 		return size.(uint64)
// 	}

// 	// Cache miss, encode and cache.
// 	// Note we rely on the assumption that all tx.inner values are RLP-encoded!
// 	c := writeCounter(0)
// 	rlp.Encode(&c, &tx.inner)
// 	size := uint64(c)

// 	// For blob transactions, add the size of the blob content and the outer list of the
// 	// tx + sidecar encoding.
// 	if sc := tx.BlobTxSidecar(); sc != nil {
// 		size += rlp.ListSize(sc.encodedSize())
// 	}

// 	// For typed transactions, the encoding also includes the leading type byte.
// 	if tx.Type() != LegacyTxType {
// 		size += 1
// 	}

// 	tx.size.Store(size)
// 	return size
// }

// WithSignature returns a new transaction with the given signature.
// This signature needs to be in the [R || S || V] format where V is 0 or 1.
// func (tx *ArbTx) WithSignature(signer Signer, sig []byte) (*ArbTx, error) {
// 	r, s, v, err := signer.SignatureValues(tx, sig)
// 	if err != nil {
// 		return nil, err
// 	}
// 	if r == nil || s == nil || v == nil {
// 		return nil, fmt.Errorf("%w: r: %s, s: %s, v: %s", ErrInvalidSig, r, s, v)
// 	}
// 	cpy := tx.inner.copy()
// 	cpy.setSignatureValues(signer.ChainID(), v, r, s)
// 	return &ArbTx{inner: cpy, time: tx.time}, nil
// }

// ArbTxs implements DerivableList for transactions.
type ArbTxs []*ArbTx

func WrapArbTransactions(txs ArbTxs) Transactions {
	txns := make([]Transaction, len(txs))
	for i := 0; i < len(txs); i++ {
		txns[i] = Transaction(txs[i])
	}
	return txns
}

// Len returns the length of s.
func (s ArbTxs) Len() int { return len(s) }

// EncodeIndex encodes the i'th transaction to w. Note that this does not check for errors
// because we assume that *ArbTx will only ever contain valid txs that were either
// constructed by decoding or via public API in this package.
func (s ArbTxs) EncodeIndex(i int, w *bytes.Buffer) {
	tx := s[i]
	if tx.Type() == LegacyTxType {
		rlp.Encode(w, tx.inner)
	} else if tx.Type() == ArbitrumLegacyTxType {
		arbData := tx.inner.(*ArbitrumLegacyTxData)
		arbData.EncodeOnlyLegacyInto(w)
	} else {
		tx.encodeTyped(w)
	}
}

// TxDifference returns a new set which is the difference between a and b.
// func TxDifference(a, b ArbTxs) ArbTxs {
// 	keep := make(ArbTxs, 0, len(a))

// 	remove := make(map[common.Hash]struct{})
// 	for _, tx := range b {
// 		remove[tx.Hash()] = struct{}{}
// 	}

// 	for _, tx := range a {
// 		if _, ok := remove[tx.Hash()]; !ok {
// 			keep = append(keep, tx)
// 		}
// 	}

// 	return keep
// }

// HashDifference returns a new set which is the difference between a and b.
func HashDifference(a, b []common.Hash) []common.Hash {
	keep := make([]common.Hash, 0, len(a))

	remove := make(map[common.Hash]struct{})
	for _, hash := range b {
		remove[hash] = struct{}{}
	}

	for _, hash := range a {
		if _, ok := remove[hash]; !ok {
			keep = append(keep, hash)
		}
	}

	return keep
}

// TxByNonce implements the sort interface to allow sorting a list of transactions
// by their nonces. This is usually only useful for sorting transactions from a
// single account, otherwise a nonce comparison doesn't make much sense.
// type TxByNonce ArbTxs

// func (s TxByNonce) Len() int           { return len(s) }
// func (s TxByNonce) Less(i, j int) bool { return s[i].Nonce() < s[j].Nonce() }
// func (s TxByNonce) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// copyAddressPtr copies an address.
func copyAddressPtr(a *common.Address) *common.Address {
	if a == nil {
		return nil
	}
	cpy := *a
	return &cpy
}

// // TransactionToMessage converts a transaction into a Message.
func TransactionToMessage(tx Transaction, s ArbitrumSigner, baseFee *big.Int, runmode MessageRunMode) (msg *Message, err error) {
	// tx.AsMessage(s types.Signer, baseFee *big.Int, rules *chain.Rules)
	msg = &Message{
		TxRunMode: runmode,
		Tx:        tx,

		nonce:    tx.GetNonce(),
		gasLimit: tx.GetGasLimit(),
		gasPrice: *tx.GetFeeCap(),
		feeCap:   *tx.GetFeeCap(),
		tipCap:   *tx.GetTipCap(),
		to:       tx.GetTo(),
		// value:             tx.GetValue(),
		amount:            *tx.GetValue(), // TODO amount is value?
		data:              tx.GetData(),
		accessList:        tx.GetAccessList(),
		SkipAccountChecks: false, // tx.SkipAccountChecks(), // TODO Arbitrum upstream this was init'd to false
		blobHashes:        tx.GetBlobHashes(),
		// maxFeePerBlobGas:  tx.GetBlobGasFeeCap(),
		// BlobGasFeeCap:     tx.GetBlobGasFeeCap(), // TODO
	}
	// If baseFee provided, set gasPrice to effectiveGasPrice.
	if baseFee != nil {
		msg.gasPrice.SetFromBig(cmath.BigMin(msg.gasPrice.ToBig().Add(msg.tipCap.ToBig(), baseFee), msg.feeCap.ToBig()))
	}
	msg.from, err = s.Sender(tx)
	return msg, err
}
