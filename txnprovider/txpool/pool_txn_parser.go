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
	keccak "github.com/erigontech/fastkeccak"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
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

// TxnParseContext is object that is required to parse transactions and turn transaction payload into TxnSlot objects
// usage of TxContext helps avoid extra memory allocations
type TxnParseContext struct {
	validateRlp     func([]byte) error
	chainID         uint256.Int
	signer          *types.Signer // cached signer for sender recovery (non-malleable)
	malleableSigner *types.Signer // cached signer that accepts pre-EIP-2 malleable signatures
	keccak          keccak.KeccakState
	bytesReader     bytes.Reader // reusable reader to avoid allocation per parse
	authBuf         bytes.Buffer // reusable buffer for authorization signer recovery
	authHashBuf     [32]byte     // reusable hash buffer for authorization signer recovery
	innerTxBuf      []byte       // reusable buffer for blob wrapper inner tx bytes
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
		keccak:     keccak.NewFastKeccak(),
	}

	// behave as of London enabled
	ctx.chainID.Set(&chainID)
	ctx.signer = types.LatestSignerForChainID(chainID.ToBig())

	malleable := *ctx.signer
	malleable.SetMalleable(true)
	ctx.malleableSigner = &malleable

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

// decodeTxn decodes a transaction from raw bytes using the reusable bytes.Reader
// on the context to avoid per-call allocations. It dispatches to the right
// transaction type's DecodeRLP directly instead of going through
// UnmarshalTransactionFromBinary (which creates a second bytes.Reader + stream
// for typed txns).
func (ctx *TxnParseContext) decodeTxn(txBytes []byte) (types.Transaction, error) {
	var txn types.Transaction

	// Legacy transactions: full RLP list starting with 0xc0..0xff
	if txBytes[0] >= 0xc0 {
		ctx.bytesReader.Reset(txBytes)
		s, done := rlp.NewStreamFromPool(&ctx.bytesReader, uint64(len(txBytes)))
		defer done()
		legacy := &types.LegacyTx{}
		if err := legacy.DecodeRLP(s); err != nil {
			return nil, err
		}
		return legacy, nil
	}

	// Typed transactions: type byte + RLP body
	switch txBytes[0] {
	case AccessListTxnType:
		txn = &types.AccessListTx{}
	case DynamicFeeTxnType:
		txn = &types.DynamicFeeTransaction{}
	case BlobTxnType:
		txn = &types.BlobTx{}
	case SetCodeTxnType:
		txn = &types.SetCodeTransaction{}
	case AATxnType:
		txn = &types.AccountAbstractionTransaction{}
	default:
		return nil, fmt.Errorf("unknown transaction type: %d", txBytes[0])
	}

	ctx.bytesReader.Reset(txBytes[1:]) // skip type byte
	s, done := rlp.NewStreamFromPool(&ctx.bytesReader, uint64(len(txBytes)-1))
	defer done()
	if err := txn.DecodeRLP(s); err != nil {
		return nil, err
	}
	if s.Remaining() != 0 {
		return nil, fmt.Errorf("trailing bytes after transaction")
	}
	return txn, nil
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
	// For blob wrappers, we parse the wrapper structure manually (zero-copy for
	// large blob data) and decode only the inner tx_payload_body via the standard
	// decoder, producing a *types.BlobTx (not *types.BlobTxWrapper).
	var txn types.Transaction
	var innerTxBytes []byte // only set for wrapped blob txns
	if !legacy && txBytes[0] == BlobTxnType && wrappedWithBlobs {
		// Structure: type_byte || rlp([ rlp(tx_body), [wrapper_version], blobs, commitments, proofs ])
		wrapperListPos, wrapperListLen, err := rlp.ParseList(txBytes, 1)
		if err != nil {
			return 0, fmt.Errorf("%w: blob wrapper list: %s", ErrParseTxn, err) //nolint
		}

		// Parse the inner tx_payload_body (first element of the wrapper list).
		innerBodyPos, innerBodyLen, err := rlp.ParseList(txBytes, wrapperListPos)
		if err != nil {
			return 0, fmt.Errorf("%w: blob tx body in wrapper: %s", ErrParseTxn, err) //nolint
		}
		// innerTxBytes = type_byte + rlp_list(tx_body)
		// The body RLP (with its list prefix) starts at wrapperListPos.
		innerLen := 1 + innerBodyPos + innerBodyLen - wrapperListPos
		if cap(ctx.innerTxBuf) < innerLen {
			ctx.innerTxBuf = make([]byte, innerLen)
		}
		innerTxBytes = ctx.innerTxBuf[:innerLen]
		innerTxBytes[0] = BlobTxnType
		copy(innerTxBytes[1:], txBytes[wrapperListPos:innerBodyPos+innerBodyLen])

		// Decode only the inner tx (no blobs/commitments/proofs).
		txn, err = ctx.decodeTxn(innerTxBytes)
		if err != nil {
			return 0, fmt.Errorf("%w: %s", ErrParseTxn, err) //nolint
		}

		// Now parse the remaining wrapper elements: [wrapper_version], blobs, commitments, proofs.
		wp := innerBodyPos + innerBodyLen // position after inner tx body

		// Check for optional wrapper_version byte.
		proofsPerBlob := 1
		_, dvLen, err := rlp.ParseString(txBytes, wp)
		if err == nil && dvLen == 1 {
			if txBytes[wp] != 0x01 {
				return 0, fmt.Errorf("%w: invalid wrapper version: expected 1, got %d", ErrParseTxn, txBytes[wp])
			}
			wp++
			proofsPerBlob = int(params.CellsPerExtBlob)
		}

		// Parse blobs list (zero-copy: slices point into original payload).
		blobListPos, blobListLen, err := rlp.ParseList(txBytes, wp)
		if err != nil {
			return 0, fmt.Errorf("%w: blobs len: %s", ErrParseTxn, err) //nolint
		}
		blobPos := blobListPos
		blobIdx := 0
		for blobPos < blobListPos+blobListLen {
			slot.BlobBundles = append(slot.BlobBundles, PoolBlobBundle{})
			blobPos, err = rlp.StringOfLen(txBytes, blobPos, params.BlobSize)
			if err != nil {
				return 0, fmt.Errorf("%w: blob: %s", ErrParseTxn, err) //nolint
			}
			slot.BlobBundles[blobIdx].Blob = txBytes[blobPos : blobPos+params.BlobSize]
			blobPos += params.BlobSize
			blobIdx++
		}
		if blobPos != blobListPos+blobListLen {
			return 0, fmt.Errorf("%w: extraneous space in blobs", ErrParseTxn)
		}
		wp = blobPos

		// Parse commitments list.
		commitListPos, commitListLen, err := rlp.ParseList(txBytes, wp)
		if err != nil {
			return 0, fmt.Errorf("%w: commitments len: %s", ErrParseTxn, err) //nolint
		}
		commitPos := commitListPos
		blobIdx = 0
		for commitPos < commitListPos+commitListLen {
			if blobIdx >= len(slot.BlobBundles) {
				return 0, fmt.Errorf("%w: more commitments than blobs (%d > %d)", ErrParseTxn, blobIdx+1, len(slot.BlobBundles))
			}
			commitPos, err = rlp.StringOfLen(txBytes, commitPos, 48)
			if err != nil {
				return 0, fmt.Errorf("%w: commitment: %s", ErrParseTxn, err) //nolint
			}
			var commitment goethkzg.KZGCommitment
			copy(commitment[:], txBytes[commitPos:commitPos+48])
			slot.BlobBundles[blobIdx].Commitment = commitment
			commitPos += 48
			blobIdx++
		}
		if blobIdx != len(slot.BlobBundles) {
			return 0, fmt.Errorf("%w: fewer commitments than blobs (%d < %d)", ErrParseTxn, blobIdx, len(slot.BlobBundles))
		}
		if commitPos != commitListPos+commitListLen {
			return 0, fmt.Errorf("%w: extraneous space in commitments", ErrParseTxn)
		}
		wp = commitPos

		// Parse proofs list.
		proofListPos, proofListLen, err := rlp.ParseList(txBytes, wp)
		if err != nil {
			return 0, fmt.Errorf("%w: proofs len: %s", ErrParseTxn, err) //nolint
		}
		proofPos := proofListPos
		proofsList := make([]goethkzg.KZGProof, 0, proofsPerBlob*len(slot.BlobBundles))
		for proofPos < proofListPos+proofListLen {
			proofPos, err = rlp.StringOfLen(txBytes, proofPos, 48)
			if err != nil {
				return 0, fmt.Errorf("%w: proof: %s", ErrParseTxn, err) //nolint
			}
			var proof goethkzg.KZGProof
			copy(proof[:], txBytes[proofPos:proofPos+48])
			proofsList = append(proofsList, proof)
			proofPos += 48
		}
		if len(proofsList) != proofsPerBlob*len(slot.BlobBundles) {
			return 0, fmt.Errorf("%w: unexpected proofs len=%d expected=%d", ErrParseTxn, len(proofsList), proofsPerBlob*len(slot.BlobBundles))
		}
		for blobIdx = 0; blobIdx < len(slot.BlobBundles); blobIdx++ {
			slot.BlobBundles[blobIdx].Proofs = proofsList[blobIdx*proofsPerBlob : (blobIdx+1)*proofsPerBlob]
		}
		if proofPos != proofListPos+proofListLen {
			return 0, fmt.Errorf("%w: extraneous space in proofs", ErrParseTxn)
		}
		wp = proofPos

		if wp != wrapperListPos+wrapperListLen {
			return 0, fmt.Errorf("%w: extraneous elements in blobs wrapper", ErrParseTxn)
		}
	} else {
		txn, err = ctx.decodeTxn(txBytes)
		if err != nil {
			return 0, fmt.Errorf("%w: %s", ErrParseTxn, err) //nolint
		}
	}

	// Step 4: Validate chain ID for all transaction types.
	// For legacy txns, GetChainID() derives chain ID from V (returns zero for pre-EIP-155
	// unprotected txns, which is valid — chainIDRequired only applies to typed txns).
	chainID := txn.GetChainID()
	if chainID.IsZero() {
		if !legacy && ctx.chainIDRequired {
			return 0, fmt.Errorf("%w: chainID is required", ErrParseTxn)
		}
	} else if !chainID.Eq(&ctx.chainID) {
		return 0, fmt.Errorf("%w: %s, %s (expected %s)", ErrParseTxn, "invalid chainID", chainID, &ctx.chainID)
	}

	// Step 5: Store the decoded transaction and compute hash.
	// Compute hash directly from raw RLP bytes (avoids re-encoding the transaction
	// which txn.Hash() would do via rlpHash/prefixedRlpHash + reflection).
	// For wrapped blob txns, the hash is of the inner tx_payload_body, not the full wrapper.
	slot.Txn = txn
	ctx.keccak.Reset()
	if innerTxBytes != nil {
		ctx.keccak.Write(innerTxBytes) //nolint:errcheck
	} else {
		ctx.keccak.Write(txBytes) //nolint:errcheck
	}
	ctx.keccak.Read(slot.IDHash[:]) //nolint:errcheck

	if validateHash != nil {
		if err := validateHash(slot.IDHash[:]); err != nil {
			return p, err
		}
	}

	// Step 6: Store raw RLP bytes and cache size (Rlp may be nilled later after DB flush).
	// For wrapped blob txns, store the full wrapper RLP (needed for PooledTransactions gossip),
	// but Size reflects the full wrapper.
	slot.Rlp = txBytes
	slot.Size = uint32(len(txBytes))

	// Step 7: Populate fields that are kept on TxnSlot.
	slot.Nonce = txn.GetNonce()

	// Authorization signers for SetCode txns (EIP-7702).
	if txn.Type() == types.SetCodeTxType {
		auths := txn.GetAuthorizations()
		slot.AuthAndNonces = make([]AuthAndNonce, 0, len(auths))
		for _, auth := range auths {
			if !auth.ChainID.IsUint64() {
				return 0, fmt.Errorf("%w: authorization chainId is too big: %s", ErrParseTxn, &auth.ChainID)
			}
			authCopy := auth
			ctx.authBuf.Reset()
			authority, err := authCopy.RecoverSigner(&ctx.authBuf, ctx.authHashBuf[:])
			if err != nil {
				return 0, fmt.Errorf("%w: recover authorization signer: %s stack: %s", ErrParseTxn, err, dbg.Stack()) //nolint
			}
			slot.AuthAndNonces = append(slot.AuthAndNonces, AuthAndNonce{*authority, auth.Nonce})
		}
	}

	// Step 8: Recover sender if needed.
	if ctx.withSender && len(sender) == length.Addr {
		if aaTx, ok := txn.(*types.AccountAbstractionTransaction); ok {
			senderAddr := aaTx.SenderAddress.Value()
			copy(sender, senderAddr[:])
			if aaTx.Paymaster != nil {
				copy(sender, aaTx.Paymaster[:])
			}
		} else {
			signer := ctx.signer
			if ctx.allowPreEip2s && txn.Type() == types.LegacyTxType {
				signer = ctx.malleableSigner
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

type AuthAndNonce struct {
	authority common.Address
	nonce     uint64
}

// PoolBlobBundle holds a single blob's data for the GetBlobs API.
type PoolBlobBundle struct {
	Commitment goethkzg.KZGCommitment
	Blob       []byte
	Proofs     []goethkzg.KZGProof // Can be 1 or more Proofs/CellProofs
}

// TxnSlot contains information extracted from an Ethereum transaction, which is enough to manage it inside the transaction.
// Also, it contains some auxiliary information, like ephemeral fields, and indices within priority queues.
// Txn must be non-nil for all slots except the btree search sentinel (which only uses SenderID and Nonce).
type TxnSlot struct {
	Txn      types.Transaction // The decoded standard transaction (BlobTx, not BlobTxWrapper, for blob txns)
	Rlp      []byte            // Is set to nil after flushing to db, frees memory, later we look for it in the db, if needed
	SenderID uint64            // SenderID - require external mapping to it's address
	Nonce    uint64            // Nonce of the transaction (kept as field for btree search sentinel)
	IDHash   [32]byte          // Transaction hash for the purposes of using it as a transaction Id
	Traced   bool              // Whether transaction needs to be traced throughout transaction pool code and generate debug printing
	Size     uint32            // Cached size of the RLP payload (persists after Rlp is set to nil)

	BlobBundles   []PoolBlobBundle // Zero-copy blob data for EIP-4844 wrapped blob txns
	AuthAndNonces []AuthAndNonce   // Indexed authorization signers + nonces for EIP-7702 txns (type-4)
}

// Accessor methods that delegate to the stored Transaction.

func (tx *TxnSlot) TxType() byte                 { return tx.Txn.Type() }
func (tx *TxnSlot) GetGas() uint64               { return tx.Txn.GetGasLimit() }
func (tx *TxnSlot) IsCreation() bool             { return tx.Txn.IsContractDeploy() }
func (tx *TxnSlot) GetBlobHashes() []common.Hash { return tx.Txn.GetBlobHashes() }

func (tx *TxnSlot) GetTipCap() *uint256.Int { return tx.Txn.GetTipCap() }
func (tx *TxnSlot) GetFeeCap() *uint256.Int { return tx.Txn.GetFeeCap() }
func (tx *TxnSlot) GetValue() *uint256.Int  { return tx.Txn.GetValue() }

func (tx *TxnSlot) GetBlobFeeCap() *uint256.Int {
	if bt, ok := tx.Txn.(*types.BlobTx); ok {
		return &bt.MaxFeePerBlobGas
	}
	return new(uint256.Int)
}

func (tx *TxnSlot) GetDataLen() int {
	if aaTx, ok := tx.Txn.(*types.AccountAbstractionTransaction); ok {
		return len(aaTx.DeployerData) + len(aaTx.PaymasterData) + len(aaTx.ExecutionData)
	}
	return len(tx.Txn.GetData())
}

func (tx *TxnSlot) GetDataNonZeroLen() int {
	if aaTx, ok := tx.Txn.(*types.AccountAbstractionTransaction); ok {
		return mdgas.CountNonZeroBytes(aaTx.DeployerData) + mdgas.CountNonZeroBytes(aaTx.PaymasterData) + mdgas.CountNonZeroBytes(aaTx.ExecutionData)
	}
	return mdgas.CountNonZeroBytes(tx.Txn.GetData())
}

func (tx *TxnSlot) GetAccessListAddrCount() int { return len(tx.Txn.GetAccessList()) }
func (tx *TxnSlot) GetAccessListStorCount() int { return tx.Txn.GetAccessList().StorageKeys() }

func (tx *TxnSlot) PrintDebug(prefix string) {
	fmt.Printf("%s: senderID=%d,nonce=%d,tip=%s,v=%s\n", prefix, tx.SenderID, tx.Nonce, tx.GetTipCap(), tx.GetValue())
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

// BlobBundle returns the blob, commitment, and proofs for the i-th blob.
func (tx *TxnSlot) BlobBundle(i int) (blob []byte, commitment goethkzg.KZGCommitment, proofs []goethkzg.KZGProof) {
	if i >= len(tx.BlobBundles) {
		return nil, goethkzg.KZGCommitment{}, nil
	}
	bb := &tx.BlobBundles[i]
	return bb.Blob, bb.Commitment, bb.Proofs
}

// ToProtoAccountAbstractionTxn converts a TxnSlot to a typesproto.AccountAbstractionTransaction
func (tx *TxnSlot) ToProtoAccountAbstractionTxn() *typesproto.AccountAbstractionTransaction {
	if tx == nil {
		return nil
	}
	aaTx, ok := tx.Txn.(*types.AccountAbstractionTransaction)
	if !ok {
		return nil
	}

	var paymasterData, deployerData, paymaster, deployer, builderFee, nonceKey []byte
	if aaTx.PaymasterData != nil {
		paymasterData = aaTx.PaymasterData
	}
	if aaTx.DeployerData != nil {
		deployerData = aaTx.DeployerData
	}
	if aaTx.Paymaster != nil {
		paymaster = aaTx.Paymaster.Bytes()
	}
	if aaTx.Deployer != nil {
		deployer = aaTx.Deployer.Bytes()
	}
	if aaTx.BuilderFee != nil {
		builderFee = aaTx.BuilderFee.Bytes()
	}
	if aaTx.NonceKey != nil {
		nonceKey = aaTx.NonceKey.Bytes()
	}

	senderAddr := aaTx.SenderAddress.Value()
	return &typesproto.AccountAbstractionTransaction{
		Nonce:                       aaTx.Nonce,
		ChainId:                     aaTx.ChainID.Bytes(),
		Tip:                         aaTx.Tip.Bytes(),
		FeeCap:                      aaTx.FeeCap.Bytes(),
		Gas:                         aaTx.GasLimit,
		SenderAddress:               senderAddr[:],
		SenderValidationData:        aaTx.SenderValidationData,
		ExecutionData:               aaTx.ExecutionData,
		Paymaster:                   paymaster,
		PaymasterData:               paymasterData,
		Deployer:                    deployer,
		DeployerData:                deployerData,
		BuilderFee:                  builderFee,
		ValidationGasLimit:          aaTx.ValidationGasLimit,
		PaymasterValidationGasLimit: aaTx.PaymasterValidationGasLimit,
		PostOpGasLimit:              aaTx.PostOpGasLimit,
		NonceKey:                    nonceKey,
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
	Txns      [][]byte
	ParsedTxn []types.Transaction // populated by best() when slot.Txn is available
	Senders   Addresses
	IsLocal   []bool
}

// Resize internal arrays to len=targetSize, shrinks if need. It rely on `append` algorithm to realloc
func (r *TxnsRlp) Resize(targetSize uint) {
	for uint(len(r.Txns)) < targetSize {
		r.Txns = append(r.Txns, nil)
	}
	for uint(len(r.ParsedTxn)) < targetSize {
		r.ParsedTxn = append(r.ParsedTxn, nil)
	}
	for uint(r.Senders.Len()) < targetSize {
		r.Senders = append(r.Senders, addressesGrowth...)
	}
	for uint(len(r.IsLocal)) < targetSize {
		r.IsLocal = append(r.IsLocal, false)
	}
	//todo: set nil to overflow txns
	r.Txns = r.Txns[:targetSize]
	r.ParsedTxn = r.ParsedTxn[:targetSize]
	r.Senders = r.Senders[:length.Addr*targetSize]
	r.IsLocal = r.IsLocal[:targetSize]
}

var addressesGrowth = make([]byte, length.Addr)
