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
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon/rlp"
)

const RUNS = 100

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

func (tr *TRand) RandTransaction() Transaction {
	txType := tr.RandIntInRange(0, 4)
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
	fmt.Println("TYPE: ", txType)
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
		return &BlobTx{
			DynamicFeeTransaction: DynamicFeeTransaction{
				CommonTx:   commonTx,
				ChainID:    uint256.NewInt(*tr.RandUint64()),
				Tip:        uint256.NewInt(*tr.RandUint64()),
				FeeCap:     uint256.NewInt(*tr.RandUint64()),
				AccessList: tr.RandAccessList(tr.RandIntInRange(1, 5)),
			},
			MaxFeePerBlobGas:    uint256.NewInt(*tr.RandUint64()),
			BlobVersionedHashes: tr.RandHashes(tr.RandIntInRange(1, 5)),
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
func (tr *TRand) RandRawBody() *RawBody {
	return &RawBody{
		Transactions: tr.RandRawTransactions(tr.RandIntInRange(1, 6)),
		Uncles:       tr.RandHeaders(tr.RandIntInRange(1, 6)),
		Withdrawals:  tr.RandWithdrawals(tr.RandIntInRange(1, 6)),
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

func check(t *testing.T, f string, got, want interface{}) {
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s mismatch: got %v, want %v", f, got, want)
	}
}

func compareHeaders(t *testing.T, a, b *Header) {
	check(t, "Header.ParentHash", a.ParentHash, b.ParentHash)
	check(t, "Withdrawal.UncleHash", a.UncleHash, b.UncleHash)
	check(t, "Withdrawal.Coinbase", a.Coinbase, b.Coinbase)
	check(t, "Withdrawal.Root", a.Root, b.Root)
	check(t, "Withdrawal.TxHash", a.TxHash, b.TxHash)
	check(t, "Withdrawal.ReceiptHash", a.ReceiptHash, b.ReceiptHash)
	check(t, "Withdrawal.Bloom", a.Bloom, b.Bloom)
	check(t, "Withdrawal.Difficulty", a.Difficulty, b.Difficulty)
	check(t, "Withdrawal.Number", a.Number, b.Number)
	check(t, "Withdrawal.GasLimit", a.GasLimit, b.GasLimit)
	check(t, "Withdrawal.GasUsed", a.GasUsed, b.GasUsed)
	check(t, "Withdrawal.Time", a.Time, b.Time)
	check(t, "Withdrawal.Extra", a.Extra, b.Extra)
	check(t, "Withdrawal.MixDigest", a.MixDigest, b.MixDigest)
	check(t, "Withdrawal.Nonce", a.Nonce, b.Nonce)
	check(t, "Withdrawal.BaseFee", a.BaseFee, b.BaseFee)
	check(t, "Withdrawal.WithdrawalsHash", a.WithdrawalsHash, b.WithdrawalsHash)
	check(t, "Withdrawal.BlobGasUsed", a.BlobGasUsed, b.BlobGasUsed)
	check(t, "Withdrawal.ExcessBlobGas", a.ExcessBlobGas, b.ExcessBlobGas)
	check(t, "Withdrawal.ParentBeaconBlockRoot", a.ParentBeaconBlockRoot, b.ParentBeaconBlockRoot)
}

func compareWithdrawals(t *testing.T, a, b *Withdrawal) {
	check(t, "Withdrawal.Index", a.Index, b.Index)
	check(t, "Withdrawal.Validator", a.Validator, b.Validator)
	check(t, "Withdrawal.Address", a.Address, b.Address)
	check(t, "Withdrawal.Amount", a.Amount, b.Amount)
}

// func compareDeposits(t *testing.T, a, b *Deposit) {
// 	check(t, "Deposit.Pubkey", a.Index, b.Index)
// 	check(t, "Deposit.WithdrawalCredentials", a.WithdrawalCredentials, b.WithdrawalCredentials)
// 	check(t, "Deposit.Amount", a.Amount, b.Amount)
// 	check(t, "Deposit.Signature", a.Signature, b.Signature)
// 	check(t, "Deposit.Index", a.Index, b.Index)
// }

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

	auLen, buLen := len(a.Uncles), len(b.Uncles)
	if auLen != buLen {
		return fmt.Errorf("uncles len mismatch: expected: %v, got: %v", auLen, buLen)
	}

	for i := 0; i < auLen; i++ {
		compareHeaders(t, a.Uncles[i], b.Uncles[i])
	}

	awLen, bwLen := len(a.Withdrawals), len(b.Withdrawals)
	if awLen != bwLen {
		return fmt.Errorf("withdrawals len mismatch: expected: %v, got: %v", auLen, buLen)
	}

	for i := 0; i < awLen; i++ {
		compareWithdrawals(t, a.Withdrawals[i], b.Withdrawals[i])
	}

	return nil
}

func compareBlocks(t *testing.T, a, b *Block) error {

	compareHeaders(t, a.header, b.header)

	auLen, buLen := len(a.uncles), len(b.uncles)
	if auLen != buLen {
		return fmt.Errorf("uncles len mismatch: expected: %v, got: %v", auLen, buLen)
	}

	for i := 0; i < auLen; i++ {
		compareHeaders(t, a.uncles[i], b.uncles[i])
	}

	awLen, bwLen := len(a.withdrawals), len(b.withdrawals)
	if awLen != bwLen {
		return fmt.Errorf("withdrawals len mismatch: expected: %v, got: %v", auLen, buLen)
	}

	for i := 0; i < awLen; i++ {
		compareWithdrawals(t, a.withdrawals[i], b.withdrawals[i])
	}

	// adLen, bdLen := len(a.deposits), len(b.deposits)
	// if adLen != bdLen {
	// 	return fmt.Errorf("deposits len mismatch: expected: %v, got: %v", adLen, bdLen)
	// }

	// for i := 0; i < adLen; i++ {
	// 	compareDeposits(t, a.deposits[i], b.deposits[i])
	// }

	return nil
}

func TestRawBodyEncodeDecodeRLP(t *testing.T) {
	tr := NewTRand()

	for i := 0; i < RUNS; i++ {
		enc := tr.RandRawBody()
		var buf bytes.Buffer
		if err := enc.EncodeRLP(&buf); err != nil {
			t.Errorf("error: RawBody.EncodeRLP(): %v", err)
		}

		s := rlp.NewStream(bytes.NewReader(buf.Bytes()), 0)

		dec := &RawBody{}
		if err := dec.DecodeRLP(s); err != nil {
			t.Errorf("error: RawBody.DecodeRLP(): %v", err)
			panic(err)
		}

		if err := compareRawBodies(t, enc, dec); err != nil {
			t.Errorf("error: compareRawBodies: %v", err)
		}
	}
}

func TestBodyEncodeDecodeRLP(t *testing.T) {
	tr := NewTRand()

	for i := 0; i < RUNS; i++ {
		// rawBlock := randRawBlock(rnd, false)
		enc := tr.RandBody()
		var buf bytes.Buffer
		if err := enc.EncodeRLP(&buf); err != nil {
			t.Errorf("error: RawBody.EncodeRLP(): %v", err)
		}

		s := rlp.NewStream(bytes.NewReader(buf.Bytes()), 0)

		dec := &Body{}
		if err := dec.DecodeRLP(s); err != nil {
			t.Errorf("error: RawBody.DecodeRLP(): %v", err)
			panic(err)
		}

		// if err := compareBlocks(t, enc, dec); err != nil {
		// 	t.Errorf("error: compareRawBodies: %v", err)
		// }
	}
}
