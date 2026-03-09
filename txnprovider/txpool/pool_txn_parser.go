// Copyright 2021 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package txpool

import (
	"bytes"
	"errors"
	"fmt"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

const (
	LegacyTxnType     byte = 0
	AccessListTxnType byte = 1 // EIP-2930
	DynamicFeeTxnType byte = 2 // EIP-1559
	BlobTxnType       byte = 3 // EIP-4844
	SetCodeTxnType    byte = 4 // EIP-7702
	AATxnType         byte = 5 // RIP-7560
)

var ErrParseTxn = fmt.Errorf("%w transaction", rlp.ErrParse)
var ErrRejected = errors.New("rejected")
var ErrAlreadyKnown = errors.New("already known")
var ErrRlpTooBig = errors.New("txn rlp too big")

type TxnParseConfig struct {
	ChainID uint256.Int
}

// TxnParseContext is object that is required to parse transactions and turn transaction payload into TxnSlot objects
// usage of TxContext helps avoid extra memory allocations
type TxnParseContext struct {
	validateRlp     func([]byte) error
	cfg             TxnParseConfig
	withSender      bool
	allowPreEip2s   bool // Allow s > secp256k1n/2; see EIP-2
	chainIDRequired bool
}

func NewTxnParseContext(chainID uint256.Int) *TxnParseContext {
	if chainID.IsZero() {
		panic("wrong chainID")
	}
	ctx := &TxnParseContext{
		withSender: true,
	}

	// behave as of London enabled
	ctx.cfg.ChainID.Set(&chainID)
	return ctx
}

// Set the RLP validate function
func (ctx *TxnParseContext) ValidateRLP(f func(txnRlp []byte) error) { ctx.validateRlp = f }

// Set the with sender flag
func (ctx *TxnParseContext) WithSender(v bool) { ctx.withSender = v }

// Set the AllowPreEIP2s flag
func (ctx *TxnParseContext) WithAllowPreEip2s(v bool) { ctx.allowPreEip2s = v }

// Set ChainID-Required flag in the Parse context and return it
func (ctx *TxnParseContext) ChainIDRequired() *TxnParseContext {
	ctx.chainIDRequired = true
	return ctx
}

func PeekTransactionType(serialized []byte) (byte, error) {
	dataPos, _, legacy, err := rlp.Prefix(serialized, 0)
	if err != nil {
		return LegacyTxnType, fmt.Errorf("%w: size Prefix: %s", ErrParseTxn, err) //nolint
	}
	if legacy {
		return LegacyTxnType, nil
	}
	return serialized[dataPos], nil
}

// ParseTransaction extracts all the information from the transactions's payload (RLP) necessary to build TxnSlot.
// It also performs syntactic validation of the transactions.
// wrappedWithBlobs means that for blob (type 3) transactions the full version with blobs/commitments/proofs is expected
// (see https://eips.ethereum.org/EIPS/eip-4844#networking).
func (ctx *TxnParseContext) ParseTransaction(payload []byte, pos int, slot *TxnSlot, sender []byte, hasEnvelope, wrappedWithBlobs bool, validateHash func([]byte) error) (p int, err error) {
	if len(payload) == 0 {
		return 0, fmt.Errorf("%w: empty rlp", ErrParseTxn)
	}
	if ctx.withSender && len(sender) != length.Addr {
		return 0, fmt.Errorf("%w: expect sender buffer of len %d", ErrParseTxn, length.Addr)
	}

	// Step 1: Determine transaction boundaries in the payload.
	// Legacy transactions have list prefix, EIP-2718 typed transactions have string prefix.
	dataPos, dataLen, legacy, err := rlp.Prefix(payload, pos)
	if err != nil {
		return 0, fmt.Errorf("%w: size Prefix: %s", ErrParseTxn, err) //nolint
	}
	if dataLen == 0 {
		return 0, fmt.Errorf("%w: transaction must be either 1 list or 1 string", ErrParseTxn)
	}
	if dataLen == 1 && !legacy {
		if hasEnvelope {
			return 0, fmt.Errorf("%w: expected envelope in the payload, got %x", ErrParseTxn, payload[dataPos:dataPos+dataLen])
		}
	}

	// Extract the raw transaction bytes and compute the end position.
	// Detect actual envelope from the RLP prefix: dataPos > pos means a multi-byte
	// string prefix wraps the typed transaction bytes.
	var txBytes []byte
	if legacy {
		// Legacy tx: full RLP list
		txBytes = payload[pos : dataPos+dataLen]
		p = dataPos + dataLen
	} else if dataPos > pos {
		// Typed tx with RLP string envelope: inner bytes are the binary encoding
		txBytes = payload[dataPos : dataPos+dataLen]
		p = dataPos + dataLen
	} else {
		// Typed tx without envelope: type byte at pos, followed by RLP list
		listPos, listLen, err := rlp.ParseList(payload, pos+1)
		if err != nil {
			return 0, fmt.Errorf("%w: typed tx list: %s", ErrParseTxn, err) //nolint
		}
		txBytes = payload[pos : listPos+listLen]
		p = listPos + listLen
	}

	// Step 2: Validate RLP if configured.
	if ctx.validateRlp != nil {
		if err := ctx.validateRlp(txBytes); err != nil {
			return 0, err
		}
	}

	// Step 3: Decode the transaction using standard types.
	var txn types.Transaction
	if legacy {
		txn, err = types.DecodeTransaction(txBytes)
	} else {
		txn, err = types.UnmarshalTransactionFromBinary(txBytes, wrappedWithBlobs)
	}
	if err != nil {
		return 0, fmt.Errorf("%w: %s", ErrParseTxn, err) //nolint
	}

	// Step 4: Validate chain ID for non-legacy transactions.
	if !legacy {
		chainID := txn.GetChainID()
		if chainID == nil || chainID.IsZero() {
			if ctx.chainIDRequired {
				return 0, fmt.Errorf("%w: chainID is required", ErrParseTxn)
			}
		} else if !chainID.Eq(&ctx.cfg.ChainID) {
			return 0, fmt.Errorf("%w: %s, %d (expected %d)", ErrParseTxn, "invalid chainID", chainID.Uint64(), ctx.cfg.ChainID.Uint64())
		}
	}

	// Step 5: Store the decoded transaction and compute hash.
	slot.Txn = txn
	hash := txn.Hash()
	slot.IDHash = hash

	if validateHash != nil {
		if err := validateHash(slot.IDHash[:]); err != nil {
			return p, err
		}
	}

	// Step 6: Store raw RLP bytes.
	slot.Rlp = txBytes

	// Step 7: Populate fields that are kept on TxnSlot.
	slot.Nonce = txn.GetNonce()

	// AA-specific fields.
	if txn.Type() == types.AccountAbstractionTxType {
		populateAAFields(slot, txn)
	}

	// Blob-specific validation and bundle extraction (EIP-4844).
	if bt, ok := txn.(*types.BlobTxWrapper); ok {
		// Validate wrapper version (only 0 and 1 are defined).
		if bt.WrapperVersion > 1 {
			return 0, fmt.Errorf("%w: unsupported blob wrapper version %d", ErrParseTxn, bt.WrapperVersion)
		}

		// Validate commitment/blob count.
		numBlobs := len(bt.Blobs)
		numCommitments := len(bt.Commitments)
		if numCommitments > numBlobs {
			return 0, fmt.Errorf("%w: more commitments than blobs (%d vs %d)", ErrParseTxn, numCommitments, numBlobs)
		}
		if numCommitments < numBlobs {
			return 0, fmt.Errorf("%w: fewer commitments than blobs (%d vs %d)", ErrParseTxn, numCommitments, numBlobs)
		}

		// Validate proof count per wrapper version.
		numProofs := len(bt.Proofs)
		if bt.WrapperVersion == 0 && numProofs != numBlobs {
			return 0, fmt.Errorf("%w: wrong number of proofs for v0 wrapper (%d proofs, %d blobs)", ErrParseTxn, numProofs, numBlobs)
		}
		if bt.WrapperVersion == 1 && numProofs != numBlobs*128 {
			return 0, fmt.Errorf("%w: wrong number of proofs for v1 wrapper (%d proofs, expected %d)", ErrParseTxn, numProofs, numBlobs*128)
		}

		// Extract blob bundles.
		if numBlobs > 0 {
			proofsPerBlob := numProofs / numBlobs
			slot.BlobBundles = make([]PoolBlobBundle, numBlobs)
			for i := 0; i < numBlobs; i++ {
				slot.BlobBundles[i] = PoolBlobBundle{
					Commitment: goethkzg.KZGCommitment(bt.Commitments[i]),
					Blob:       bt.Blobs[i][:],
					Proofs:     make([]goethkzg.KZGProof, 0, proofsPerBlob),
				}
				for j := 0; j < proofsPerBlob; j++ {
					slot.BlobBundles[i].Proofs = append(slot.BlobBundles[i].Proofs,
						goethkzg.KZGProof(bt.Proofs[i*proofsPerBlob+j]))
				}
			}
		}
	}

	// Authorization signers for SetCode txns (EIP-7702).
	if txn.Type() == types.SetCodeTxType {
		auths := txn.GetAuthorizations()
		slot.AuthAndNonces = make([]AuthAndNonce, 0, len(auths))
		var hashBuf [32]byte
		for _, auth := range auths {
			if !auth.ChainID.IsUint64() {
				return 0, fmt.Errorf("%w: authorization chainId is too big: %s", ErrParseTxn, &auth.ChainID)
			}
			authCopy := auth
			authority, err := authCopy.RecoverSigner(bytes.NewBuffer(nil), hashBuf[:])
			if err != nil {
				return 0, fmt.Errorf("%w: recover authorization signer: %s stack: %s", ErrParseTxn, err, dbg.Stack()) //nolint
			}
			slot.AuthAndNonces = append(slot.AuthAndNonces, AuthAndNonce{*authority, auth.Nonce})
		}
	}

	// Step 8: Recover sender if needed.
	if ctx.withSender && len(sender) == length.Addr {
		if txn.Type() == types.AccountAbstractionTxType {
			if aaTx, ok := txn.(*types.AccountAbstractionTransaction); ok {
				senderAddr := aaTx.SenderAddress.Value()
				copy(sender, senderAddr[:])
				if aaTx.Paymaster != nil {
					copy(sender, aaTx.Paymaster[:])
				}
			}
		} else {
			signer := types.LatestSignerForChainID(ctx.cfg.ChainID.ToBig())
			if ctx.allowPreEip2s && txn.Type() == types.LegacyTxType {
				signer.SetMalleable(true)
			}
			addr, err := txn.Sender(*signer)
			if err != nil {
				return 0, fmt.Errorf("%w: recovering sender from signature: %s", ErrParseTxn, err) //nolint
			}
			addrBytes := addr.Value()
			copy(sender, addrBytes[:])
		}
	}

	return p, nil
}

// populateAAFields fills AA-specific fields on TxnSlot from a decoded AccountAbstractionTransaction.
func populateAAFields(slot *TxnSlot, txn types.Transaction) {
	aaTx, ok := txn.(*types.AccountAbstractionTransaction)
	if !ok {
		return
	}
	addr := aaTx.SenderAddress.Value()
	slot.SenderAddress = &addr
	slot.Paymaster = aaTx.Paymaster
	slot.Deployer = aaTx.Deployer
	slot.SenderValidationData = aaTx.SenderValidationData
	slot.PaymasterData = aaTx.PaymasterData
	slot.DeployerData = aaTx.DeployerData
	slot.ExecutionData = aaTx.ExecutionData
	slot.ValidationGasLimit = aaTx.ValidationGasLimit
	slot.PaymasterValidationGasLimit = aaTx.PaymasterValidationGasLimit
	slot.PostOpGasLimit = aaTx.PostOpGasLimit
	if aaTx.NonceKey != nil {
		slot.NonceKey.Set(aaTx.NonceKey)
	}
	if aaTx.BuilderFee != nil {
		slot.BuilderFee.Set(aaTx.BuilderFee)
	}
}

type AuthAndNonce struct {
	authority common.Address
	nonce     uint64
}

type PoolBlobBundle struct {
	Commitment goethkzg.KZGCommitment
	Blob       []byte
	Proofs     []goethkzg.KZGProof // Can be 1 or more Proofs/CellProofs
}

// TxnSlot contains information extracted from an Ethereum transaction, which is enough to manage it inside the transaction.
// Also, it contains some auxiliary information, like ephemeral fields, and indices within priority queues
type TxnSlot struct {
	Txn      types.Transaction // The decoded standard transaction
	Rlp      []byte            // Is set to nil after flushing to db, frees memory, later we look for it in the db, if needed
	SenderID uint64            // SenderID - require external mapping to it's address
	Nonce    uint64            // Nonce of the transaction (kept as field for btree search sentinel)
	IDHash   [32]byte          // Transaction hash for the purposes of using it as a transaction Id
	Traced   bool              // Whether transaction needs to be traced throughout transaction pool code and generate debug printing

	BlobBundles   []PoolBlobBundle // EIP-4844: extracted blob bundles from wrapper
	AuthAndNonces []AuthAndNonce   // Indexed authorization signers + nonces for EIP-7702 txns (type-4)

	// RIP-7560: account abstraction
	SenderAddress, Paymaster, Deployer                               *common.Address
	SenderValidationData, PaymasterData, DeployerData, ExecutionData []byte
	PostOpGasLimit, ValidationGasLimit, PaymasterValidationGasLimit  uint64
	NonceKey, BuilderFee                                             uint256.Int
}

// Accessor methods that delegate to the stored Transaction.
// These handle nil Txn (used in btree search sentinels and test stubs).

var _zeroUint256 uint256.Int

func (tx *TxnSlot) TxType() byte {
	if tx.Txn != nil {
		return tx.Txn.Type()
	}
	return 0
}

func (tx *TxnSlot) GetTip() *uint256.Int {
	if tx.Txn != nil {
		if v := tx.Txn.GetTipCap(); v != nil {
			return v
		}
	}
	return &_zeroUint256
}

func (tx *TxnSlot) GetFeeCap() *uint256.Int {
	if tx.Txn != nil {
		if v := tx.Txn.GetFeeCap(); v != nil {
			return v
		}
	}
	return &_zeroUint256
}

func (tx *TxnSlot) GetValue() *uint256.Int {
	if tx.Txn != nil {
		if v := tx.Txn.GetValue(); v != nil {
			return v
		}
	}
	return &_zeroUint256
}

func (tx *TxnSlot) GetGas() uint64 {
	if tx.Txn != nil {
		return tx.Txn.GetGasLimit()
	}
	return 0
}

func (tx *TxnSlot) IsCreation() bool {
	if tx.Txn != nil {
		return tx.Txn.IsContractDeploy()
	}
	return false
}

func (tx *TxnSlot) GetChainID() *uint256.Int {
	if tx.Txn != nil {
		if v := tx.Txn.GetChainID(); v != nil {
			return v
		}
	}
	return &_zeroUint256
}

func (tx *TxnSlot) GetBlobFeeCap() *uint256.Int {
	if tx.Txn != nil {
		switch bt := tx.Txn.(type) {
		case *types.BlobTxWrapper:
			if bt.Tx.MaxFeePerBlobGas != nil {
				return bt.Tx.MaxFeePerBlobGas
			}
		case *types.BlobTx:
			if bt.MaxFeePerBlobGas != nil {
				return bt.MaxFeePerBlobGas
			}
		}
	}
	return &_zeroUint256
}

func (tx *TxnSlot) GetBlobHashes() []common.Hash {
	if tx.Txn != nil {
		return tx.Txn.GetBlobHashes()
	}
	return nil
}

func (tx *TxnSlot) GetSize() uint32 { return uint32(len(tx.Rlp)) }

func (tx *TxnSlot) GetDataLen() int {
	if tx.Txn == nil {
		return 0
	}
	if tx.Txn.Type() == types.AccountAbstractionTxType {
		if aaTx, ok := tx.Txn.(*types.AccountAbstractionTransaction); ok {
			return len(aaTx.DeployerData) + len(aaTx.PaymasterData) + len(aaTx.ExecutionData)
		}
	}
	return len(tx.Txn.GetData())
}

func (tx *TxnSlot) GetDataNonZeroLen() int {
	if tx.Txn == nil {
		return 0
	}
	var data []byte
	if tx.Txn.Type() == types.AccountAbstractionTxType {
		if aaTx, ok := tx.Txn.(*types.AccountAbstractionTransaction); ok {
			data = make([]byte, 0, len(aaTx.DeployerData)+len(aaTx.PaymasterData)+len(aaTx.ExecutionData))
			data = append(data, aaTx.DeployerData...)
			data = append(data, aaTx.PaymasterData...)
			data = append(data, aaTx.ExecutionData...)
		}
	} else {
		data = tx.Txn.GetData()
	}
	count := 0
	for _, b := range data {
		if b != 0 {
			count++
		}
	}
	return count
}

func (tx *TxnSlot) GetAccessListAddrCount() int {
	if tx.Txn != nil {
		return len(tx.Txn.GetAccessList())
	}
	return 0
}

func (tx *TxnSlot) GetAccessListStorCount() int {
	if tx.Txn != nil {
		return tx.Txn.GetAccessList().StorageKeys()
	}
	return 0
}

func (tx *TxnSlot) PrintDebug(prefix string) {
	fmt.Printf("%s: senderID=%d,nonce=%d,tip=%d,v=%d\n", prefix, tx.SenderID, tx.Nonce, tx.GetTip(), tx.GetValue().Uint64())
}

func (tx *TxnSlot) Blobs() [][]byte {
	b := make([][]byte, 0, len(tx.BlobBundles))
	for _, bb := range tx.BlobBundles {
		b = append(b, bb.Blob)
	}
	return b
}

func (tx *TxnSlot) Commitments() []goethkzg.KZGCommitment {
	c := make([]goethkzg.KZGCommitment, 0, len(tx.BlobBundles))
	for _, bb := range tx.BlobBundles {
		c = append(c, bb.Commitment)
	}
	return c
}

func (tx *TxnSlot) Proofs() []goethkzg.KZGProof {
	p := make([]goethkzg.KZGProof, 0, len(tx.BlobBundles))
	for _, bb := range tx.BlobBundles {
		p = append(p, bb.Proofs...)
	}
	return p
}

// ToProtoAccountAbstractionTxn converts a TxnSlot to a typesproto.AccountAbstractionTransaction
func (tx *TxnSlot) ToProtoAccountAbstractionTxn() *typesproto.AccountAbstractionTransaction {
	if tx == nil {
		return nil
	}

	var paymasterData, deployerData, paymaster, deployer []byte
	if tx.PaymasterData != nil {
		paymasterData = tx.PaymasterData
	}
	if tx.DeployerData != nil {
		deployerData = tx.DeployerData
	}
	if tx.Paymaster != nil {
		paymaster = tx.Paymaster.Bytes()
	}
	if tx.Deployer != nil {
		deployer = tx.Deployer.Bytes()
	}

	return &typesproto.AccountAbstractionTransaction{
		Nonce:                       tx.Nonce,
		ChainId:                     tx.GetChainID().Bytes(),
		Tip:                         tx.GetTip().Bytes(),
		FeeCap:                      tx.GetFeeCap().Bytes(),
		Gas:                         tx.GetGas(),
		SenderAddress:               tx.SenderAddress.Bytes(),
		SenderValidationData:        tx.SenderValidationData,
		ExecutionData:               tx.ExecutionData,
		Paymaster:                   paymaster,
		PaymasterData:               paymasterData,
		Deployer:                    deployer,
		DeployerData:                deployerData,
		BuilderFee:                  tx.BuilderFee.Bytes(),
		ValidationGasLimit:          tx.ValidationGasLimit,
		PaymasterValidationGasLimit: tx.PaymasterValidationGasLimit,
		PostOpGasLimit:              tx.PostOpGasLimit,
		NonceKey:                    tx.NonceKey.Bytes(),
	}
}

type TxnSlots struct {
	Txns    []*TxnSlot
	Senders Addresses
	IsLocal []bool
}

func (s *TxnSlots) Valid() error {
	if len(s.Txns) != len(s.IsLocal) {
		return fmt.Errorf("TxnSlots: expect equal len of isLocal=%d and txns=%d", len(s.IsLocal), len(s.Txns))
	}
	if len(s.Txns) != s.Senders.Len() {
		return fmt.Errorf("TxnSlots: expect equal len of senders=%d and txns=%d", s.Senders.Len(), len(s.Txns))
	}
	return nil
}

var zeroAddr = make([]byte, length.Addr)

// Resize internal arrays to len=targetSize, shrinks if need. It rely on `append` algorithm to realloc
func (s *TxnSlots) Resize(targetSize uint) {
	for uint(len(s.Txns)) < targetSize {
		s.Txns = append(s.Txns, nil)
	}
	for uint(s.Senders.Len()) < targetSize {
		s.Senders = append(s.Senders, addressesGrowth...)
	}
	for uint(len(s.IsLocal)) < targetSize {
		s.IsLocal = append(s.IsLocal, false)
	}
	//todo: set nil to overflow txns
	oldLen := uint(len(s.Txns))
	s.Txns = s.Txns[:targetSize]
	for i := oldLen; i < targetSize; i++ {
		s.Txns[i] = nil
	}
	s.Senders = s.Senders[:length.Addr*targetSize]
	for i := oldLen; i < targetSize; i++ {
		copy(s.Senders.At(int(i)), zeroAddr)
	}
	s.IsLocal = s.IsLocal[:targetSize]
	for i := oldLen; i < targetSize; i++ {
		s.IsLocal[i] = false
	}
}

func (s *TxnSlots) Append(slot *TxnSlot, sender []byte, isLocal bool) {
	n := len(s.Txns)
	s.Resize(uint(len(s.Txns) + 1))
	s.Txns[n] = slot
	s.IsLocal[n] = isLocal
	copy(s.Senders.At(n), sender)
}

type Addresses []byte // flatten list of 20-byte addresses

// AddressAt returns an address at the given index in the flattened list.
// Use this method if you want to reduce memory allocations
func (h Addresses) AddressAt(i int) common.Address {
	return *(*[length.Addr]byte)(h[i*length.Addr : (i+1)*length.Addr])
}

func (h Addresses) At(i int) []byte {
	return h[i*length.Addr : (i+1)*length.Addr]
}

func (h Addresses) Len() int {
	return len(h) / length.Addr
}

type TxnsRlp struct {
	Txns    [][]byte
	Senders Addresses
	IsLocal []bool
}

// Resize internal arrays to len=targetSize, shrinks if need. It rely on `append` algorithm to realloc
func (r *TxnsRlp) Resize(targetSize uint) {
	for uint(len(r.Txns)) < targetSize {
		r.Txns = append(r.Txns, nil)
	}
	for uint(r.Senders.Len()) < targetSize {
		r.Senders = append(r.Senders, addressesGrowth...)
	}
	for uint(len(r.IsLocal)) < targetSize {
		r.IsLocal = append(r.IsLocal, false)
	}
	//todo: set nil to overflow txns
	r.Txns = r.Txns[:targetSize]
	r.Senders = r.Senders[:length.Addr*targetSize]
	r.IsLocal = r.IsLocal[:targetSize]
}

var addressesGrowth = make([]byte, length.Addr)
