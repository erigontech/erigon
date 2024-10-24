package types

import (
	"bytes"
	"fmt"
	"math/big"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon/rlp"
)

const RUNS = 100 // for local tests increase this number

type TRand struct {
	rnd *rand.Rand
}

func NewTRand() *TRand {
	seed := time.Now().UnixNano()
	src := rand.NewSource(seed)
	return &TRand{rnd: rand.New(src)}
}

func (tr *TRand) RandIntInRange(min, max int) int {
	return (tr.rnd.Intn(max-min) + min)
}

func (tr *TRand) RandUint64() *uint64 {
	a := tr.rnd.Uint64()
	return &a
}

func (tr *TRand) RandBig() *big.Int {
	return big.NewInt(int64(tr.rnd.Int()))
}

func (tr *TRand) RandBytes(size int) []byte {
	arr := make([]byte, size)
	for i := 0; i < size; i++ {
		arr[i] = byte(tr.rnd.Intn(256))
	}
	return arr
}

func (tr *TRand) RandAddress() libcommon.Address {
	return libcommon.Address(tr.RandBytes(20))
}

func (tr *TRand) RandHash() libcommon.Hash {
	return libcommon.Hash(tr.RandBytes(32))
}

func (tr *TRand) RandBloom() Bloom {
	return Bloom(tr.RandBytes(BloomByteLength))
}

func (tr *TRand) RandWithdrawal() *Withdrawal {
	return &Withdrawal{
		Index:     tr.rnd.Uint64(),
		Validator: tr.rnd.Uint64(),
		Address:   tr.RandAddress(),
		Amount:    tr.rnd.Uint64(),
	}
}

func (tr *TRand) RandWithdrawalRequest() *WithdrawalRequest {
	return &WithdrawalRequest{
		RequestData: [WithdrawalRequestDataLen]byte(tr.RandBytes(WithdrawalRequestDataLen)),
	}
}

func (tr *TRand) RandDepositRequest() *DepositRequest {
	return &DepositRequest{
		Pubkey:                [48]byte(tr.RandBytes(48)),
		WithdrawalCredentials: tr.RandHash(),
		Amount:                *tr.RandUint64(),
		Signature:             [96]byte(tr.RandBytes(96)),
		Index:                 *tr.RandUint64(),
	}
}

func (tr *TRand) RandConsolidationRequest() *ConsolidationRequest {
	return &ConsolidationRequest{
		SourceAddress: [20]byte(tr.RandBytes(20)),
		SourcePubKey:  [48]byte(tr.RandBytes(48)),
		TargetPubKey:  [48]byte(tr.RandBytes(48)),
	}
}

func (tr *TRand) RandRequest() Request {
	switch tr.rnd.Intn(3) {
	case 0:
		return tr.RandDepositRequest()
	case 1:
		return tr.RandWithdrawalRequest()
	case 2:
		return tr.RandConsolidationRequest()
	default:
		return nil // unreachable code
	}
}

func (tr *TRand) RandHeader() *Header {
	wHash := tr.RandHash()
	pHash := tr.RandHash()
	return &Header{
		ParentHash:            tr.RandHash(),                              // libcommon.Hash
		UncleHash:             tr.RandHash(),                              // libcommon.Hash
		Coinbase:              tr.RandAddress(),                           // libcommon.Address
		Root:                  tr.RandHash(),                              // libcommon.Hash
		TxHash:                tr.RandHash(),                              // libcommon.Hash
		ReceiptHash:           tr.RandHash(),                              // libcommon.Hash
		Bloom:                 tr.RandBloom(),                             // Bloom
		Difficulty:            tr.RandBig(),                               // *big.Int
		Number:                tr.RandBig(),                               // *big.Int
		GasLimit:              *tr.RandUint64(),                           // uint64
		GasUsed:               *tr.RandUint64(),                           // uint64
		Time:                  *tr.RandUint64(),                           // uint64
		Extra:                 tr.RandBytes(tr.RandIntInRange(128, 1024)), // []byte
		MixDigest:             tr.RandHash(),                              // libcommon.Hash
		Nonce:                 BlockNonce(tr.RandBytes(8)),                // BlockNonce
		BaseFee:               tr.RandBig(),                               // *big.Int
		WithdrawalsHash:       &wHash,                                     // *libcommon.Hash
		BlobGasUsed:           tr.RandUint64(),                            // *uint64
		ExcessBlobGas:         tr.RandUint64(),                            // *uint64
		ParentBeaconBlockRoot: &pHash,                                     //*libcommon.Hash
	}
}

func (tr *TRand) RandAccessTuple() types2.AccessTuple {
	n := tr.RandIntInRange(1, 5)
	sk := make([]libcommon.Hash, n)
	for i := 0; i < n; i++ {
		sk[i] = tr.RandHash()
	}
	return types2.AccessTuple{
		Address:     tr.RandAddress(),
		StorageKeys: sk,
	}
}

func (tr *TRand) RandAccessList(size int) types2.AccessList {
	al := make([]types2.AccessTuple, size)
	for i := 0; i < size; i++ {
		al[i] = tr.RandAccessTuple()
	}
	return al
}

func (tr *TRand) RandAuthorizations(size int) []Authorization {
	auths := make([]Authorization, size)
	for i := 0; i < size; i++ {
		auths[i] = Authorization{
			ChainID: uint256.NewInt(*tr.RandUint64()),
			Address: tr.RandAddress(),
			Nonce:   *tr.RandUint64(),
			V:       *uint256.NewInt(*tr.RandUint64()),
			R:       *uint256.NewInt(*tr.RandUint64()),
			S:       *uint256.NewInt(*tr.RandUint64()),
		}
	}
	return auths
}

func (tr *TRand) RandTransaction() Transaction {
	txType := tr.RandIntInRange(0, 5) // LegacyTxType, AccessListTxType, DynamicFeeTxType, BlobTxType, SetCodeTxType
	to := tr.RandAddress()
	commonTx := CommonTx{
		Nonce: *tr.RandUint64(),
		Gas:   *tr.RandUint64(),
		To:    &to,
		Value: uint256.NewInt(*tr.RandUint64()), // wei amount
		Data:  tr.RandBytes(tr.RandIntInRange(128, 1024)),
		V:     *uint256.NewInt(*tr.RandUint64()),
		R:     *uint256.NewInt(*tr.RandUint64()),
		S:     *uint256.NewInt(*tr.RandUint64()),
	}
	switch txType {
	case LegacyTxType:
		return &LegacyTx{
			CommonTx: commonTx,
			GasPrice: uint256.NewInt(*tr.RandUint64()),
		}
	case AccessListTxType:
		return &AccessListTx{
			LegacyTx: LegacyTx{
				CommonTx: commonTx,
				GasPrice: uint256.NewInt(*tr.RandUint64()),
			},
			ChainID:    uint256.NewInt(*tr.RandUint64()),
			AccessList: tr.RandAccessList(tr.RandIntInRange(1, 5)),
		}
	case DynamicFeeTxType:
		return &DynamicFeeTransaction{
			CommonTx:   commonTx,
			ChainID:    uint256.NewInt(*tr.RandUint64()),
			Tip:        uint256.NewInt(*tr.RandUint64()),
			FeeCap:     uint256.NewInt(*tr.RandUint64()),
			AccessList: tr.RandAccessList(tr.RandIntInRange(1, 5)),
		}
	case BlobTxType:
		r := *tr.RandUint64()
		return &BlobTx{
			DynamicFeeTransaction: DynamicFeeTransaction{
				CommonTx:   commonTx,
				ChainID:    uint256.NewInt(*tr.RandUint64()),
				Tip:        uint256.NewInt(*tr.RandUint64()),
				FeeCap:     uint256.NewInt(*tr.RandUint64()),
				AccessList: tr.RandAccessList(tr.RandIntInRange(1, 5)),
			},
			MaxFeePerBlobGas:    uint256.NewInt(r),
			BlobVersionedHashes: tr.RandHashes(tr.RandIntInRange(1, 2)),
		}
	case SetCodeTxType:
		return &SetCodeTransaction{
			DynamicFeeTransaction: DynamicFeeTransaction{
				CommonTx:   commonTx,
				ChainID:    uint256.NewInt(*tr.RandUint64()),
				Tip:        uint256.NewInt(*tr.RandUint64()),
				FeeCap:     uint256.NewInt(*tr.RandUint64()),
				AccessList: tr.RandAccessList(tr.RandIntInRange(1, 5)),
			},
			Authorizations: tr.RandAuthorizations(tr.RandIntInRange(0, 5)),
		}
	default:
		fmt.Printf("unexpected txType %v", txType)
		panic("unexpected txType")
	}
}

func (tr *TRand) RandHashes(size int) []libcommon.Hash {
	hashes := make([]libcommon.Hash, size)
	for i := 0; i < size; i++ {
		hashes[i] = tr.RandHash()
	}
	return hashes
}

func (tr *TRand) RandTransactions(size int) []Transaction {
	txns := make([]Transaction, size)
	for i := 0; i < size; i++ {
		txns[i] = tr.RandTransaction()
	}
	return txns
}

func (tr *TRand) RandRawTransactions(size int) [][]byte {
	txns := make([][]byte, size)
	for i := 0; i < size; i++ {
		txns[i] = tr.RandBytes(tr.RandIntInRange(1, 1023))
	}
	return txns
}

func (tr *TRand) RandHeaders(size int) []*Header {
	uncles := make([]*Header, size)
	for i := 0; i < size; i++ {
		uncles[i] = tr.RandHeader()
	}
	return uncles
}

func (tr *TRand) RandWithdrawals(size int) []*Withdrawal {
	withdrawals := make([]*Withdrawal, size)
	for i := 0; i < size; i++ {
		withdrawals[i] = tr.RandWithdrawal()
	}
	return withdrawals
}

func (tr *TRand) RandRequests(size int) []Request {
	requests := make([]Request, size)
	for i := 0; i < size; i++ {
		requests[i] = tr.RandRequest()
	}
	return requests
}

func (tr *TRand) RandRawBody() *RawBody {
	return &RawBody{
		Transactions: tr.RandRawTransactions(tr.RandIntInRange(1, 6)),
		Uncles:       tr.RandHeaders(tr.RandIntInRange(1, 6)),
		Withdrawals:  tr.RandWithdrawals(tr.RandIntInRange(1, 6)),
		Requests:     tr.RandRequests(tr.RandIntInRange(1, 6)),
	}
}

func (tr *TRand) RandRawBlock(setNil bool) *RawBlock {
	if setNil {
		return &RawBlock{
			Header: tr.RandHeader(),
			Body: &RawBody{
				Uncles:      nil,
				Withdrawals: nil,
				// Deposits:     nil,
			},
		}
	}

	return &RawBlock{
		Header: tr.RandHeader(),
		Body:   tr.RandRawBody(),
	}
}

func (tr *TRand) RandBody() *Body {
	return &Body{
		Transactions: tr.RandTransactions(tr.RandIntInRange(1, 6)),
		Uncles:       tr.RandHeaders(tr.RandIntInRange(1, 6)),
		Withdrawals:  tr.RandWithdrawals(tr.RandIntInRange(1, 6)),
		Requests:     tr.RandRequests(tr.RandIntInRange(1, 6)),
	}
}

func isEqualBytes(a, b []byte) bool {
	for i := range a {
		if a[i] != b[i] {
			fmt.Printf("%v != %v at %v", a[i], b[i], i)
			return false
		}
	}
	return true
}

func check(t *testing.T, f string, want, got interface{}) {
	if !reflect.DeepEqual(want, got) {
		t.Errorf("%s mismatch: want %v, got %v", f, want, got)
	}
}

func checkHeaders(t *testing.T, a, b *Header) {
	check(t, "Header.ParentHash", a.ParentHash, b.ParentHash)
	check(t, "Header.UncleHash", a.UncleHash, b.UncleHash)
	check(t, "Header.Coinbase", a.Coinbase, b.Coinbase)
	check(t, "Header.Root", a.Root, b.Root)
	check(t, "Header.TxHash", a.TxHash, b.TxHash)
	check(t, "Header.ReceiptHash", a.ReceiptHash, b.ReceiptHash)
	check(t, "Header.Bloom", a.Bloom, b.Bloom)
	check(t, "Header.Difficulty", a.Difficulty, b.Difficulty)
	check(t, "Header.Number", a.Number, b.Number)
	check(t, "Header.GasLimit", a.GasLimit, b.GasLimit)
	check(t, "Header.GasUsed", a.GasUsed, b.GasUsed)
	check(t, "Header.Time", a.Time, b.Time)
	check(t, "Header.Extra", a.Extra, b.Extra)
	check(t, "Header.MixDigest", a.MixDigest, b.MixDigest)
	check(t, "Header.Nonce", a.Nonce, b.Nonce)
	check(t, "Header.BaseFee", a.BaseFee, b.BaseFee)
	check(t, "Header.WithdrawalsHash", a.WithdrawalsHash, b.WithdrawalsHash)
	check(t, "Header.BlobGasUsed", a.BlobGasUsed, b.BlobGasUsed)
	check(t, "Header.ExcessBlobGas", a.ExcessBlobGas, b.ExcessBlobGas)
	check(t, "Header.ParentBeaconBlockRoot", a.ParentBeaconBlockRoot, b.ParentBeaconBlockRoot)
}

func checkWithdrawals(t *testing.T, a, b *Withdrawal) {
	check(t, "Withdrawal.Index", a.Index, b.Index)
	check(t, "Withdrawal.Validator", a.Validator, b.Validator)
	check(t, "Withdrawal.Address", a.Address, b.Address)
	check(t, "Withdrawal.Amount", a.Amount, b.Amount)
}

func compareTransactions(t *testing.T, a, b Transaction) {
	v1, r1, s1 := a.RawSignatureValues()
	v2, r2, s2 := b.RawSignatureValues()
	check(t, "Tx.Type", a.Type(), b.Type())
	check(t, "Tx.GetChainID", a.GetChainID(), b.GetChainID())
	check(t, "Tx.GetNonce", a.GetNonce(), b.GetNonce())
	check(t, "Tx.GetPrice", a.GetPrice(), b.GetPrice())
	check(t, "Tx.GetTip", a.GetTip(), b.GetTip())
	check(t, "Tx.GetFeeCap", a.GetFeeCap(), b.GetFeeCap())
	check(t, "Tx.GetBlobHashes", a.GetBlobHashes(), b.GetBlobHashes())
	check(t, "Tx.GetGas", a.GetGas(), b.GetGas())
	check(t, "Tx.GetBlobGas", a.GetBlobGas(), b.GetBlobGas())
	check(t, "Tx.GetValue", a.GetValue(), b.GetValue())
	check(t, "Tx.GetTo", a.GetTo(), b.GetTo())
	check(t, "Tx.GetData", a.GetData(), b.GetData())
	check(t, "Tx.GetAccessList", a.GetAccessList(), b.GetAccessList())
	check(t, "Tx.V", v1, v2)
	check(t, "Tx.R", r1, r2)
	check(t, "Tx.S", s1, s2)
}

func compareDeposits(t *testing.T, a, b *DepositRequest) {
	check(t, "Deposit.Pubkey", a.Pubkey, b.Pubkey)
	check(t, "Deposit.WithdrawalCredentials", a.WithdrawalCredentials, b.WithdrawalCredentials)
	check(t, "Deposit.Amount", a.Amount, b.Amount)
	check(t, "Deposit.Signature", a.Signature, b.Signature)
	check(t, "Deposit.Index", a.Index, b.Index)
}

func compareWithdrawalRequests(t *testing.T, a, b *WithdrawalRequest) {
	check(t, "WithdrawalRequest.Amount", a.RequestData, b.RequestData)
}

func compareConsolidationRequests(t *testing.T, a, b *ConsolidationRequest) {
	check(t, "ConsolidationRequest.SourceAddress", a.SourceAddress, b.SourceAddress)
	check(t, "ConsolidationRequest.SourcePubKey", a.SourcePubKey, b.SourcePubKey)
	check(t, "ConsolidationRequest.TargetPubKey", a.TargetPubKey, b.TargetPubKey)
}

func checkRequests(t *testing.T, a, b Request) {
	if a.RequestType() != b.RequestType() {
		t.Errorf("request type mismatch: request-a: %v, request-b: %v", a.RequestType(), b.RequestType())
	}

	switch a.RequestType() {
	case DepositRequestType:
		a, aok := a.(*DepositRequest)
		b, bok := b.(*DepositRequest)
		if aok && bok {
			compareDeposits(t, a, b)
		} else {
			t.Errorf("type assertion failed: %v %v %v %v", a.RequestType(), aok, b.RequestType(), bok)
		}
	case WithdrawalRequestType:
		a, aok := a.(*WithdrawalRequest)
		b, bok := b.(*WithdrawalRequest)
		if aok && bok {
			compareWithdrawalRequests(t, a, b)
		} else {
			t.Errorf("type assertion failed: %v %v %v %v", a.RequestType(), aok, b.RequestType(), bok)
		}
	case ConsolidationRequestType:
		a, aok := a.(*ConsolidationRequest)
		b, bok := b.(*ConsolidationRequest)
		if aok && bok {
			compareConsolidationRequests(t, a, b)
		} else {
			t.Errorf("type assertion failed: %v %v %v %v", a.RequestType(), aok, b.RequestType(), bok)
		}
	default:
		t.Errorf("unknown request type: %v", a.RequestType())
	}
}

func compareHeaders(t *testing.T, a, b []*Header) error {
	auLen, buLen := len(a), len(b)
	if auLen != buLen {
		return fmt.Errorf("uncles len mismatch: expected: %v, got: %v", auLen, buLen)
	}

	for i := 0; i < auLen; i++ {
		checkHeaders(t, a[i], b[i])
	}
	return nil
}

func compareWithdrawals(t *testing.T, a, b []*Withdrawal) error {
	awLen, bwLen := len(a), len(b)
	if awLen != bwLen {
		return fmt.Errorf("withdrawals len mismatch: expected: %v, got: %v", awLen, bwLen)
	}

	for i := 0; i < awLen; i++ {
		checkWithdrawals(t, a[i], b[i])
	}
	return nil
}

func compareRequests(t *testing.T, a, b Requests) error {
	arLen, brLen := len(a), len(b)
	if arLen != brLen {
		return fmt.Errorf("requests len mismatch: expected: %v, got: %v", arLen, brLen)
	}

	for i := 0; i < arLen; i++ {
		checkRequests(t, a[i], b[i])
	}
	return nil
}

func compareRawBodies(t *testing.T, a, b *RawBody) error {

	atLen, btLen := len(a.Transactions), len(b.Transactions)
	if atLen != btLen {
		return fmt.Errorf("transactions len mismatch: expected: %v, got: %v", atLen, btLen)
	}

	for i := 0; i < atLen; i++ {
		if !isEqualBytes(a.Transactions[i], b.Transactions[i]) {
			return fmt.Errorf("byte transactions are not equal")
		}
	}

	compareHeaders(t, a.Uncles, b.Uncles)
	compareWithdrawals(t, a.Withdrawals, b.Withdrawals)
	compareRequests(t, a.Requests, b.Requests)

	return nil
}

func compareBodies(t *testing.T, a, b *Body) error {

	atLen, btLen := len(a.Transactions), len(b.Transactions)
	if atLen != btLen {
		return fmt.Errorf("txns len mismatch: expected: %v, got: %v", atLen, btLen)
	}

	for i := 0; i < atLen; i++ {
		compareTransactions(t, a.Transactions[i], b.Transactions[i])
	}

	compareHeaders(t, a.Uncles, b.Uncles)
	compareWithdrawals(t, a.Withdrawals, b.Withdrawals)
	compareRequests(t, a.Requests, b.Requests)

	return nil
}

// func TestRawBodyEncodeDecodeRLP(t *testing.T) {
// 	tr := NewTRand()
// 	var buf bytes.Buffer
// 	for i := 0; i < RUNS; i++ {
// 		enc := tr.RandRawBody()
// 		buf.Reset()
// 		if err := enc.EncodeRLP(&buf); err != nil {
// 			t.Errorf("error: RawBody.EncodeRLP(): %v", err)
// 		}

// 		s := rlp.NewStream(bytes.NewReader(buf.Bytes()), 0)

// 		dec := &RawBody{}
// 		if err := dec.DecodeRLP(s); err != nil {
// 			t.Errorf("error: RawBody.DecodeRLP(): %v", err)
// 			panic(err)
// 		}

// 		if err := compareRawBodies(t, enc, dec); err != nil {
// 			t.Errorf("error: compareRawBodies: %v", err)
// 		}
// 	}
// }

func TestBodyEncodeDecodeRLP(t *testing.T) {
	tr := NewTRand()
	var buf bytes.Buffer
	for i := 0; i < RUNS; i++ {
		enc := tr.RandBody()
		buf.Reset()
		if err := enc.EncodeRLP(&buf); err != nil {
			t.Errorf("error: RawBody.EncodeRLP(): %v", err)
		}

		s := rlp.NewStream(bytes.NewReader(buf.Bytes()), 0)
		dec := &Body{}
		if err := dec.DecodeRLP(s); err != nil {
			t.Errorf("error: RawBody.DecodeRLP(): %v", err)
			panic(err)
		}

		if err := compareBodies(t, enc, dec); err != nil {
			t.Errorf("error: compareBodies: %v", err)
		}
	}
}

func TestDepositEncodeDecode(t *testing.T) {
	tr := NewTRand()
	var buf bytes.Buffer
	for i := 0; i < RUNS; i++ {
		a := tr.RandDepositRequest()
		buf.Reset()
		if err := a.EncodeRLP(&buf); err != nil {
			t.Errorf("error: deposit.EncodeRLP(): %v", err)
		}
		b := new(DepositRequest)
		if err := b.DecodeRLP(buf.Bytes()); err != nil {
			t.Errorf("error: Deposit.DecodeRLP(): %v", err)
		}
		compareDeposits(t, a, b)
	}
}

func TestConsolidationReqsEncodeDecode(t *testing.T) {
	tr := NewTRand()
	var buf bytes.Buffer
	for i := 0; i < RUNS; i++ {
		a := tr.RandConsolidationRequest()
		buf.Reset()
		if err := a.EncodeRLP(&buf); err != nil {
			t.Errorf("error: deposit.EncodeRLP(): %v", err)
		}
		b := new(ConsolidationRequest)
		if err := b.DecodeRLP(buf.Bytes()); err != nil {
			t.Errorf("error: Deposit.DecodeRLP(): %v", err)
		}
		compareConsolidationRequests(t, a, b)
	}
}

func TestWithdrawalReqsEncodeDecode(t *testing.T) {
	wx1 := WithdrawalRequest{
		RequestData: [WithdrawalRequestDataLen]byte(hexutility.MustDecodeHex("0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001fefefefefefefefe")),
	}
	wx2 := WithdrawalRequest{
		RequestData: [WithdrawalRequestDataLen]byte(hexutility.MustDecodeHex("0x8a0a19589531694250d570040a0c4b74576919b8000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001fefefefefefefefe")),
	}

	var wx3, wx4 WithdrawalRequest
	var buf1, buf2 bytes.Buffer
	wx1.EncodeRLP(&buf1)
	wx2.EncodeRLP(&buf2)

	wx3.DecodeRLP(buf1.Bytes())
	wx4.DecodeRLP(buf2.Bytes())

	if wx1.RequestData != wx3.RequestData || wx2.RequestData != wx4.RequestData {
		t.Errorf("error: incorrect encode/decode for WithdrawalRequest")
	}
}
