package types

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/rlp"
)

type BlockAccessList []*AccountChanges

// DOS protection from very large RLP inputs, remove later if unnecessary
const (
	maxBlockAccessListBytes     = 4 << 20 // 4 MiB, >0.93 MiB worst-case described in EIP
	maxBlockAccessAccounts      = 1 << 19 // 524,288 accounts ~= 350 accounts per tx at 1500 tx/block
	maxSlotChangesPerAccount    = 1 << 18 // 262,144 slots per account
	maxStorageChangesPerSlot    = math.MaxUint16 + 1
	maxStorageReadsPerAccount   = 1 << 18
	maxIndexedChangesPerAccount = math.MaxUint16 + 1
)

type AccountChanges struct {
	Address        common.Address
	StorageChanges []*SlotChanges
	StorageReads   []common.Hash
	BalanceChanges []*BalanceChange
	NonceChanges   []*NonceChange
	CodeChanges    []*CodeChange
}

type SlotChanges struct {
	Slot    common.Hash
	Changes []*StorageChange
}

type StorageChange struct {
	Index uint16
	Value common.Hash
}

type BalanceChange struct {
	Index uint16
	Value *big.Int
}

type NonceChange struct {
	Index uint16
	Value uint64
}

type CodeChange struct {
	Index uint16
	Data  []byte
}

// indexedChange interface for generic validation of change types with indices
type indexedChange interface {
	*BalanceChange | *NonceChange | *CodeChange
	GetIndex() uint16
}

// GetIndex methods for indexedChange interface
func (bc *BalanceChange) GetIndex() uint16 { return bc.Index }
func (nc *NonceChange) GetIndex() uint16   { return nc.Index }
func (cc *CodeChange) GetIndex() uint16    { return cc.Index }

func (ac *AccountChanges) EncodingSize() int {
	size := 21 // address (1 prefix + 20 bytes)
	storageChangesLen := EncodingSizeGenericList(ac.StorageChanges)
	size += rlp.ListPrefixLen(storageChangesLen) + storageChangesLen
	storageReadsLen := encodingSizeHashList(ac.StorageReads)
	balanceChangesLen := EncodingSizeGenericList(ac.BalanceChanges)
	nonceChangesLen := EncodingSizeGenericList(ac.NonceChanges)
	codeChangesLen := EncodingSizeGenericList(ac.CodeChanges)
	size += storageReadsLen
	size += rlp.ListPrefixLen(balanceChangesLen) + balanceChangesLen
	size += rlp.ListPrefixLen(nonceChangesLen) + nonceChangesLen
	size += rlp.ListPrefixLen(codeChangesLen) + codeChangesLen
	return size
}

func (ac *AccountChanges) EncodeRLP(w io.Writer) error {
	if err := ac.validate(); err != nil {
		return err
	}
	encodingSize := ac.EncodingSize()
	b := newEncodingBuf()
	defer releaseEncodingBuf(b)

	if err := rlp.EncodeStructSizePrefix(encodingSize, w, b[:]); err != nil {
		return err
	}

	b[0] = 128 + 20
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(ac.Address[:]); err != nil {
		return err
	}

	if err := encodeBlockAccessList(ac.StorageChanges, w, b[:]); err != nil {
		return err
	}
	if err := encodeHashList(ac.StorageReads, w, b[:]); err != nil {
		return err
	}
	if err := encodeBlockAccessList(ac.BalanceChanges, w, b[:]); err != nil {
		return err
	}
	if err := encodeBlockAccessList(ac.NonceChanges, w, b[:]); err != nil {
		return err
	}
	return encodeBlockAccessList(ac.CodeChanges, w, b[:])
}

func (ac *AccountChanges) DecodeRLP(s *rlp.Stream) error {
	if size, err := s.List(); err != nil {
		return err
	} else if size > maxBlockAccessListBytes {
		return fmt.Errorf("account changes payload exceeds maximum size (%d bytes)", size)
	}

	if err := s.ReadBytes(ac.Address[:]); err != nil {
		return fmt.Errorf("read Address: %w", err)
	}

	list, err := decodeSlotChangesList(s)
	if err != nil {
		return err
	}
	ac.StorageChanges = list

	reads, err := decodeHashList(s)
	if err != nil {
		return err
	}
	ac.StorageReads = reads

	tmpBalances, err := decodeBalanceChanges(s)
	if err != nil {
		return err
	}
	ac.BalanceChanges = tmpBalances

	nonces, err := decodeNonceChanges(s)
	if err != nil {
		return err
	}
	ac.NonceChanges = nonces

	codes, err := decodeCodeChanges(s)
	if err != nil {
		return err
	}
	ac.CodeChanges = codes

	if err := ac.validate(); err != nil {
		return err
	}

	return s.ListEnd()
}

func (sc *SlotChanges) EncodingSize() int {
	size := rlp.StringLen(sc.Slot[:])
	changesLen := EncodingSizeGenericList(sc.Changes)
	size += rlp.ListPrefixLen(changesLen) + changesLen
	return size
}

func (sc *SlotChanges) EncodeRLP(w io.Writer) error {
	if err := sc.validate(); err != nil {
		return err
	}

	b := newEncodingBuf()
	defer releaseEncodingBuf(b)

	encodingSize := sc.EncodingSize()
	if err := rlp.EncodeStructSizePrefix(encodingSize, w, b[:]); err != nil {
		return err
	}

	if err := rlp.EncodeString(sc.Slot[:], w, b[:]); err != nil {
		return err
	}

	return encodeBlockAccessList(sc.Changes, w, b[:])
}

func (sc *SlotChanges) DecodeRLP(s *rlp.Stream) error {
	if size, err := s.List(); err != nil {
		return err
	} else if size > maxBlockAccessListBytes {
		return fmt.Errorf("slot changes payload exceeds maximum size (%d bytes)", size)
	}
	if err := s.ReadBytes(sc.Slot[:]); err != nil {
		return fmt.Errorf("read Slot: %w", err)
	}

	changes, err := decodeStorageChanges(s)
	if err != nil {
		return err
	}
	sc.Changes = changes

	if err := sc.validate(); err != nil {
		return err
	}

	return s.ListEnd()
}

func (sc *StorageChange) EncodingSize() int {
	size := 1 + rlp.IntLenExcludingHead(uint64(sc.Index))
	size += rlp.StringLen(sc.Value[:])
	return size
}

func (sc *StorageChange) EncodeRLP(w io.Writer) error {
	b := newEncodingBuf()
	defer releaseEncodingBuf(b)

	encodingSize := sc.EncodingSize()
	if err := rlp.EncodeStructSizePrefix(encodingSize, w, b[:]); err != nil {
		return err
	}
	if err := rlp.EncodeInt(uint64(sc.Index), w, b[:]); err != nil {
		return err
	}
	return rlp.EncodeString(sc.Value[:], w, b[:])
}

func (sc *StorageChange) DecodeRLP(s *rlp.Stream) error {
	if _, err := s.List(); err != nil {
		return err
	}
	idx, err := s.Uint()
	if err != nil {
		return fmt.Errorf("read Index: %w", err)
	}
	if idx > math.MaxUint16 {
		return fmt.Errorf("block access index overflow: %d", idx)
	}
	sc.Index = uint16(idx)
	b, err := s.Bytes()
	if err != nil {
		return fmt.Errorf("read Value: %w", err)
	}
	if len(b) != len(common.Hash{}) {
		return fmt.Errorf("wrong size for storage value: %d", len(b))
	}
	sc.Value = common.BytesToHash(b)
	return s.ListEnd()
}

func (bc *BalanceChange) EncodingSize() int {
	size := 1 + rlp.IntLenExcludingHead(uint64(bc.Index))
	size++
	if bc.Value != nil {
		size += rlp.BigIntLenExcludingHead(bc.Value)
	}
	return size
}

func (bc *BalanceChange) EncodeRLP(w io.Writer) error {
	if err := bc.validate(); err != nil {
		return err
	}

	b := newEncodingBuf()
	defer releaseEncodingBuf(b)

	encodingSize := bc.EncodingSize()
	if err := rlp.EncodeStructSizePrefix(encodingSize, w, b[:]); err != nil {
		return err
	}
	if err := rlp.EncodeInt(uint64(bc.Index), w, b[:]); err != nil {
		return err
	}
	value, err := normaliseBigInt(bc.Value)
	if err != nil {
		return err
	}
	return rlp.EncodeBigInt(value, w, b[:])
}

func (bc *BalanceChange) DecodeRLP(s *rlp.Stream) error {
	if _, err := s.List(); err != nil {
		return err
	}
	idx, err := s.Uint()
	if err != nil {
		return fmt.Errorf("read Index: %w", err)
	}
	if idx > math.MaxUint16 {
		return fmt.Errorf("block access index overflow: %d", idx)
	}
	bc.Index = uint16(idx)
	b, err := s.Uint256Bytes()
	if err != nil {
		return fmt.Errorf("read Value: %w", err)
	}
	bc.Value = new(big.Int).SetBytes(b)
	if err := bc.validate(); err != nil {
		return err
	}
	return s.ListEnd()
}

func (nc *NonceChange) EncodingSize() int {
	size := 1 + rlp.IntLenExcludingHead(uint64(nc.Index))
	size++
	size += rlp.IntLenExcludingHead(nc.Value)
	return size
}

func (nc *NonceChange) EncodeRLP(w io.Writer) error {
	b := newEncodingBuf()
	defer releaseEncodingBuf(b)

	encodingSize := nc.EncodingSize()
	if err := rlp.EncodeStructSizePrefix(encodingSize, w, b[:]); err != nil {
		return err
	}
	if err := rlp.EncodeInt(uint64(nc.Index), w, b[:]); err != nil {
		return err
	}
	return rlp.EncodeInt(nc.Value, w, b[:])
}

func (nc *NonceChange) DecodeRLP(s *rlp.Stream) error {
	if _, err := s.List(); err != nil {
		return err
	}
	idx, err := s.Uint()
	if err != nil {
		return fmt.Errorf("read Index: %w", err)
	}
	if idx > math.MaxUint16 {
		return fmt.Errorf("block access index overflow: %d", idx)
	}
	nc.Index = uint16(idx)
	value, err := s.Uint()
	if err != nil {
		return fmt.Errorf("read Value: %w", err)
	}
	nc.Value = value
	return s.ListEnd()
}

func (cc *CodeChange) EncodingSize() int {
	size := 1 + rlp.IntLenExcludingHead(uint64(cc.Index))
	size += rlp.StringLen(cc.Data)
	return size
}

func (cc *CodeChange) EncodeRLP(w io.Writer) error {
	b := newEncodingBuf()
	defer releaseEncodingBuf(b)

	encodingSize := cc.EncodingSize()
	if err := rlp.EncodeStructSizePrefix(encodingSize, w, b[:]); err != nil {
		return err
	}
	if err := rlp.EncodeInt(uint64(cc.Index), w, b[:]); err != nil {
		return err
	}
	return rlp.EncodeString(cc.Data, w, b[:])
}

func (cc *CodeChange) DecodeRLP(s *rlp.Stream) error {
	if _, err := s.List(); err != nil {
		return err
	}
	idx, err := s.Uint()
	if err != nil {
		return fmt.Errorf("read Index: %w", err)
	}
	if idx > math.MaxUint16 {
		return fmt.Errorf("block access index overflow: %d", idx)
	}
	cc.Index = uint16(idx)
	data, err := s.Bytes()
	if err != nil {
		return fmt.Errorf("read Data: %w", err)
	}
	cc.Data = data
	return s.ListEnd()
}

func encodeBlockAccessList[T rlpEncodable](items []T, w io.Writer, buf []byte) error {
	total := EncodingSizeGenericList(items)
	if err := rlp.EncodeStructSizePrefix(total, w, buf); err != nil {
		return err
	}
	for _, item := range items {
		if err := item.EncodeRLP(w); err != nil {
			return err
		}
	}
	return nil
}

func encodeHashList(hashes []common.Hash, w io.Writer, buf []byte) error {
	if err := validateStorageReads(hashes); err != nil {
		return err
	}
	total := 0
	for i := range hashes {
		total += rlp.StringLen(hashes[i][:])
	}
	if err := rlp.EncodeStructSizePrefix(total, w, buf); err != nil {
		return err
	}
	for i := range hashes {
		if err := rlp.EncodeString(hashes[i][:], w, buf); err != nil {
			return err
		}
	}
	return nil
}

func encodingSizeHashList(hashes []common.Hash) int {
	size := 0
	for i := range hashes {
		size += rlp.StringLen(hashes[i][:])
	}
	return rlp.ListPrefixLen(size) + size
}

func decodeBlockAccessList(out *BlockAccessList, s *rlp.Stream) error {
	var err error
	var size uint64
	if size, err = s.List(); err != nil {
		if errors.Is(err, rlp.EOL) {
			*out = nil
			return nil
		}
		return err
	}
	if size > maxBlockAccessListBytes {
		return fmt.Errorf("block access list payload exceeds maximum size (%d bytes)", size)
	}
	var changes []*AccountChanges
	var prevAddr common.Address
	var hasPrev bool

	for {
		var ac AccountChanges
		if err = ac.DecodeRLP(s); err != nil {
			break
		}
		if hasPrev && bytes.Compare(prevAddr[:], ac.Address[:]) >= 0 {
			err = fmt.Errorf("block access list addresses must be strictly increasing (prev=%s current=%s)", prevAddr.Hex(), ac.Address.Hex())
			break
		}
		acCopy := ac
		changes = append(changes, &acCopy)
		if len(changes) > maxBlockAccessAccounts {
			err = fmt.Errorf("block access list exceeds maximum accounts (%d)", maxBlockAccessAccounts)
			break
		}
		prevAddr = ac.Address
		hasPrev = true
	}
	if err = checkErrListEnd(s, err); err != nil {
		return err
	}
	if len(changes) == 0 {
		*out = nil
		return nil
	}
	*out = changes
	return nil
}

func decodeSlotChangesList(s *rlp.Stream) ([]*SlotChanges, error) {
	var err error
	var size uint64
	if size, err = s.List(); err != nil {
		if errors.Is(err, rlp.EOL) {
			return nil, nil
		}
		return nil, err
	}
	if size > maxBlockAccessListBytes {
		return nil, fmt.Errorf("slot changes list payload exceeds maximum size (%d bytes)", size)
	}
	var out []*SlotChanges
	var prevSlot common.Hash
	var hasPrev bool
	for {
		sc := new(SlotChanges)
		if err = sc.DecodeRLP(s); err != nil {
			break
		}
		if hasPrev && bytes.Compare(prevSlot[:], sc.Slot[:]) >= 0 {
			err = fmt.Errorf("storage slot list must be strictly increasing (prev=%x current=%x)", prevSlot, sc.Slot)
			break
		}
		out = append(out, sc)
		if len(out) > maxSlotChangesPerAccount {
			err = fmt.Errorf("storage slot change list exceeds maximum entries (%d)", maxSlotChangesPerAccount)
			break
		}
		prevSlot = sc.Slot
		hasPrev = true
	}
	if err = checkErrListEnd(s, err); err != nil {
		return nil, err
	}
	if err := validateSlotChangeList(out); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeStorageChanges(s *rlp.Stream) ([]*StorageChange, error) {
	var err error
	if _, err = s.List(); err != nil {
		if errors.Is(err, rlp.EOL) {
			return nil, nil
		}
		return nil, err
	}
	var out []*StorageChange
	for {
		change := new(StorageChange)
		if err = change.DecodeRLP(s); err != nil {
			break
		}
		out = append(out, change)
		if len(out) > maxStorageChangesPerSlot {
			err = fmt.Errorf("storage change list exceeds maximum entries (%d)", maxStorageChangesPerSlot)
			break
		}
	}
	if err = checkErrListEnd(s, err); err != nil {
		return nil, err
	}
	if err := validateStorageChangeEntries(out); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeBalanceChanges(s *rlp.Stream) ([]*BalanceChange, error) {
	var err error
	if _, err = s.List(); err != nil {
		if errors.Is(err, rlp.EOL) {
			return nil, nil
		}
		return nil, err
	}
	var out []*BalanceChange
	for {
		change := new(BalanceChange)
		if err = change.DecodeRLP(s); err != nil {
			break
		}
		out = append(out, change)
		if len(out) > maxIndexedChangesPerAccount {
			err = fmt.Errorf("balance change list exceeds maximum entries (%d)", maxIndexedChangesPerAccount)
			break
		}
	}
	if err = checkErrListEnd(s, err); err != nil {
		return nil, err
	}
	if err := validateBalanceChangeList(out); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeNonceChanges(s *rlp.Stream) ([]*NonceChange, error) {
	var err error
	if _, err = s.List(); err != nil {
		if errors.Is(err, rlp.EOL) {
			return nil, nil
		}
		return nil, err
	}
	var out []*NonceChange
	var lastIdx uint16
	var hasLast bool
	for {
		change := new(NonceChange)
		if err = change.DecodeRLP(s); err != nil {
			break
		}
		if hasLast && change.Index <= lastIdx {
			err = fmt.Errorf("nonce change indices must be strictly increasing (prev=%d current=%d)", lastIdx, change.Index)
			break
		}
		out = append(out, change)
		if len(out) > maxIndexedChangesPerAccount {
			err = fmt.Errorf("nonce change list exceeds maximum entries (%d)", maxIndexedChangesPerAccount)
			break
		}
		lastIdx = change.Index
		hasLast = true
	}
	if err = checkErrListEnd(s, err); err != nil {
		return nil, err
	}
	if err := validateNonceChangeList(out); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeCodeChanges(s *rlp.Stream) ([]*CodeChange, error) {
	var err error
	if _, err = s.List(); err != nil {
		if errors.Is(err, rlp.EOL) {
			return nil, nil
		}
		return nil, err
	}
	var out []*CodeChange
	var lastIdx uint16
	var hasLast bool
	for {
		change := new(CodeChange)
		if err = change.DecodeRLP(s); err != nil {
			break
		}
		out = append(out, change)
		if len(out) > maxIndexedChangesPerAccount {
			err = fmt.Errorf("code change list exceeds maximum entries (%d)", maxIndexedChangesPerAccount)
			break
		}
		if hasLast && change.Index <= lastIdx {
			err = fmt.Errorf("code change indices must be strictly increasing (prev=%d current=%d)", lastIdx, change.Index)
			break
		}
		lastIdx = change.Index
		hasLast = true
	}
	if err = checkErrListEnd(s, err); err != nil {
		return nil, err
	}
	if err := validateCodeChangeList(out); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeHashList(s *rlp.Stream) ([]common.Hash, error) {
	var err error
	var size uint64
	if size, err = s.List(); err != nil {
		if errors.Is(err, rlp.EOL) {
			return nil, nil
		}
		return nil, err
	}
	if size > maxBlockAccessListBytes {
		return nil, fmt.Errorf("storage read list payload exceeds maximum size (%d bytes)", size)
	}
	var hashes []common.Hash
	for {
		var h common.Hash
		if err = s.ReadBytes(h[:]); err != nil {
			break
		}
		hashes = append(hashes, h)
		if len(hashes) > maxStorageReadsPerAccount {
			err = fmt.Errorf("storage read list exceeds maximum entries (%d)", maxStorageReadsPerAccount)
			break
		}
	}
	if err = checkErrListEnd(s, err); err != nil {
		return nil, err
	}
	if err := validateStorageReads(hashes); err != nil {
		return nil, err
	}
	return hashes, nil
}

func releaseEncodingBuf(buf *encodingBuf) {
	if buf == nil {
		return
	}
	*buf = encodingBuf{}
	pooledBuf.Put(buf)
}

func (bal BlockAccessList) Hash() common.Hash {
	if len(bal) == 0 {
		return empty.BlockAccessListHash
	}
	return rlpHash(bal)
}

func (bal BlockAccessList) Validate() error {
	if len(bal) == 0 {
		return nil
	}
	if len(bal) > maxBlockAccessAccounts {
		return fmt.Errorf("block access list contains too many accounts (%d > %d)", len(bal), maxBlockAccessAccounts)
	}
	var prev common.Address
	var hasPrev bool
	for i, account := range bal {
		if account == nil {
			return fmt.Errorf("entry %d is nil", i)
		}
		if hasPrev && bytes.Compare(prev[:], account.Address[:]) >= 0 {
			return fmt.Errorf("account addresses must be strictly increasing (index %d)", i)
		}
		if err := account.validate(); err != nil {
			return fmt.Errorf("account %s: %w", account.Address.Hex(), err)
		}
		prev = account.Address
		hasPrev = true
	}
	return nil
}

func (ac *AccountChanges) validate() error {
	if ac == nil {
		return errors.New("nil account changes")
	}
	if err := validateSlotChangeList(ac.StorageChanges); err != nil {
		return fmt.Errorf("storage_changes: %w", err)
	}
	if err := validateStorageReads(ac.StorageReads); err != nil {
		return fmt.Errorf("storage_reads: %w", err)
	}
	if err := validateBalanceChangeList(ac.BalanceChanges); err != nil {
		return fmt.Errorf("balance_changes: %w", err)
	}
	if err := validateNonceChangeList(ac.NonceChanges); err != nil {
		return fmt.Errorf("nonce_changes: %w", err)
	}
	if err := validateCodeChangeList(ac.CodeChanges); err != nil {
		return fmt.Errorf("code_changes: %w", err)
	}
	return nil
}

func (sc *SlotChanges) validate() error {
	if sc == nil {
		return errors.New("nil slot change entry")
	}
	if err := validateStorageChangeEntries(sc.Changes); err != nil {
		return err
	}
	return nil
}

func validateStorageChangeEntries(changes []*StorageChange) error {
	if len(changes) == 0 {
		return nil
	}
	if len(changes) > maxStorageChangesPerSlot {
		return fmt.Errorf("too many storage change entries (%d > %d)", len(changes), maxStorageChangesPerSlot)
	}
	var lastIdx uint16
	var hasLast bool
	for i, change := range changes {
		if change == nil {
			return fmt.Errorf("entry %d is nil", i)
		}
		if hasLast && change.Index <= lastIdx {
			return fmt.Errorf("indices must be strictly increasing (index %d)", i)
		}
		lastIdx = change.Index
		hasLast = true
	}
	return nil
}

func validateStorageReads(reads []common.Hash) error {
	return validateHashOrdering(reads, maxStorageReadsPerAccount, "storage reads")
}

func (bc *BalanceChange) validate() error {
	if bc == nil {
		return errors.New("nil balance change entry")
	}
	if bc.Value != nil {
		if bc.Value.Sign() < 0 {
			return errors.New("balance change value cannot be negative")
		}
		// balance should fit in uint256
		if bc.Value.BitLen() > 256 {
			return errors.New("balance change exceeds 256 bits")
		}
	}
	return nil
}

func validateBalanceChangeList(changes []*BalanceChange) error {
	return validateIndexedChanges(changes, maxIndexedChangesPerAccount, "balance")
}

func validateNonceChangeList(changes []*NonceChange) error {
	return validateIndexedChanges(changes, maxIndexedChangesPerAccount, "nonce")
}

func validateCodeChangeList(changes []*CodeChange) error {
	return validateIndexedChanges(changes, maxIndexedChangesPerAccount, "code")
}

func normaliseBigInt(v *big.Int) (*big.Int, error) {
	if v == nil {
		return new(big.Int), nil
	}
	if v.Sign() < 0 {
		return nil, errors.New("block access balance change cannot be negative")
	}
	// balance should fit in uint256
	if v.BitLen() > 256 {
		return nil, fmt.Errorf("block access balance change exceeds 256 bits")
	}
	return v, nil
}

// validateIndexedChanges validates that indices are strictly increasing
func validateIndexedChanges[T indexedChange](changes []T, maxCount int, typeName string) error {
	if len(changes) == 0 {
		return nil
	}
	if len(changes) > maxCount {
		return fmt.Errorf("too many %s changes (%d > %d)", typeName, len(changes), maxCount)
	}
	for i, change := range changes {
		if change == nil {
			return fmt.Errorf("entry %d is nil", i)
		}
	}
	for i := 1; i < len(changes); i++ {
		if changes[i].GetIndex() <= changes[i-1].GetIndex() {
			return fmt.Errorf("indices must be strictly increasing (index %d)", i)
		}
	}
	return nil
}

// validateHashOrdering validates that a slice of hashes is strictly increasing
func validateHashOrdering(hashes []common.Hash, maxCount int, typeName string) error {
	if len(hashes) == 0 {
		return nil
	}
	if len(hashes) > maxCount {
		return fmt.Errorf("too many %s (%d > %d)", typeName, len(hashes), maxCount)
	}
	for i := 1; i < len(hashes); i++ {
		if bytes.Compare(hashes[i-1][:], hashes[i][:]) >= 0 {
			return fmt.Errorf("%s must be strictly increasing (index %d)", typeName, i)
		}
	}
	return nil
}

// validateSlotChangeList validates a slice of SlotChanges with hash ordering and nil checks
func validateSlotChangeList(slots []*SlotChanges) error {
	if len(slots) == 0 {
		return nil
	}
	if len(slots) > maxSlotChangesPerAccount {
		return fmt.Errorf("too many storage slot entries (%d > %d)", len(slots), maxSlotChangesPerAccount)
	}
	for i, slot := range slots {
		if slot == nil {
			return fmt.Errorf("entry %d is nil", i)
		}
	}
	for i := 1; i < len(slots); i++ {
		if bytes.Compare(slots[i-1].Slot[:], slots[i].Slot[:]) >= 0 {
			return fmt.Errorf("slots must be strictly increasing (index %d)", i)
		}
	}
	return nil
}
