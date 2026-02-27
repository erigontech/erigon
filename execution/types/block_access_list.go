package types

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/executionproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
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
	Address        accounts.Address
	StorageChanges []*SlotChanges
	StorageReads   []accounts.StorageKey
	BalanceChanges []*BalanceChange
	NonceChanges   []*NonceChange
	CodeChanges    []*CodeChange
}

type SlotChanges struct {
	Slot    accounts.StorageKey
	Changes []*StorageChange
}

type StorageChange struct {
	Index uint16
	Value uint256.Int
}

type BalanceChange struct {
	Index uint16
	Value uint256.Int
}

type NonceChange struct {
	Index uint16
	Value uint64
}

type CodeChange struct {
	Index    uint16
	Bytecode []byte
}

// indexedChange interface for generic validation of change types with indices
type indexedChange interface {
	*StorageChange | *BalanceChange | *NonceChange | *CodeChange
	GetIndex() uint16
}

// GetIndex methods for indexedChange interface
func (bc *BalanceChange) GetIndex() uint16 { return bc.Index }
func (nc *NonceChange) GetIndex() uint16   { return nc.Index }
func (cc *CodeChange) GetIndex() uint16    { return cc.Index }
func (sc *StorageChange) GetIndex() uint16 { return sc.Index }

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
	address := ac.Address.Value()
	if _, err := w.Write(address[:]); err != nil {
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

	var address common.Address
	if err := s.ReadBytes(address[:]); err != nil {
		return fmt.Errorf("read Address: %w", err)
	}
	ac.Address = accounts.InternAddress(address)
	list, err := decodeSlotChangesList(s)
	if err != nil {
		return err
	}
	ac.StorageChanges = list

	reads, err := decodeStorageKeys(s)
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

func (ac *AccountChanges) Normalize() {
	if len(ac.StorageChanges) > 1 {
		sort.Slice(ac.StorageChanges, func(i, j int) bool {
			return ac.StorageChanges[i].Slot.Cmp(ac.StorageChanges[j].Slot) < 0
		})
	}

	for _, slotChange := range ac.StorageChanges {
		if len(slotChange.Changes) > 1 {
			sortByIndex(slotChange.Changes)
			slotChange.Changes = dedupByIndex(slotChange.Changes)
		}
	}

	if len(ac.StorageReads) > 1 {
		sortHashes(ac.StorageReads)
		ac.StorageReads = dedupByEquality(ac.StorageReads)
	}

	if len(ac.BalanceChanges) > 1 {
		sortByIndex(ac.BalanceChanges)
		ac.BalanceChanges = dedupByIndex(ac.BalanceChanges)
	}
	if len(ac.NonceChanges) > 1 {
		sortByIndex(ac.NonceChanges)
		ac.NonceChanges = dedupByIndex(ac.NonceChanges)
	}
	if len(ac.CodeChanges) > 1 {
		sortByIndex(ac.CodeChanges)
		ac.CodeChanges = dedupByIndex(ac.CodeChanges)
	}
}

func (sc *SlotChanges) EncodingSize() int {
	slot := sc.Slot.Value()
	slotInt := uint256FromHash(slot)
	size := rlp.Uint256Len(slotInt)
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
	slot := sc.Slot.Value()
	slotInt := uint256FromHash(slot)
	if err := rlp.EncodeUint256(slotInt, w, b[:]); err != nil {
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
	slot, err := decodeUint256Hash(s)
	if err != nil {
		return fmt.Errorf("read Slot: %w", err)
	}
	sc.Slot = accounts.InternKey(slot)
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
	size := rlp.U64Len(uint64(sc.Index))
	size += rlp.Uint256Len(sc.Value)

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
	return rlp.EncodeUint256(sc.Value, w, b[:])
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
	err = s.ReadUint256(&sc.Value)
	if err != nil {
		return fmt.Errorf("read Value: %w", err)
	}
	return s.ListEnd()
}

func (bc *BalanceChange) EncodingSize() int {
	size := rlp.U64Len(uint64(bc.Index))
	size += rlp.Uint256Len(bc.Value)
	return size
}

func (bc *BalanceChange) EncodeRLP(w io.Writer) error {
	b := newEncodingBuf()
	defer releaseEncodingBuf(b)

	encodingSize := bc.EncodingSize()
	if err := rlp.EncodeStructSizePrefix(encodingSize, w, b[:]); err != nil {
		return err
	}
	if err := rlp.EncodeInt(uint64(bc.Index), w, b[:]); err != nil {
		return err
	}
	return rlp.EncodeUint256(bc.Value, w, b[:])
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
	valBytes, err := s.Bytes()
	if err != nil {
		return fmt.Errorf("read Value: %w", err)
	}
	if len(valBytes) > 32 {
		return fmt.Errorf("read Value: integer too large")
	}
	bc.Value.SetBytes(valBytes)
	return s.ListEnd()
}

func (nc *NonceChange) EncodingSize() int {
	size := rlp.U64Len(uint64(nc.Index))
	size += rlp.U64Len(nc.Value)
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
	size := rlp.U64Len(uint64(cc.Index))
	size += rlp.StringLen(cc.Bytecode)
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
	return rlp.EncodeString(cc.Bytecode, w, b[:])
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
		return fmt.Errorf("read Bytecode: %w", err)
	}
	cc.Bytecode = data
	return s.ListEnd()
}

func dedupByIndex[T interface{ GetIndex() uint16 }](changes []T) []T {
	if len(changes) == 0 {
		return changes
	}
	out := changes[:1]
	for i := 1; i < len(changes); i++ {
		if changes[i].GetIndex() == out[len(out)-1].GetIndex() {
			out[len(out)-1] = changes[i]
			continue
		}
		out = append(out, changes[i])
	}
	return out
}

func dedupByEquality[T comparable](items []T) []T {
	if len(items) == 0 {
		return items
	}
	out := items[:1]
	for i := 1; i < len(items); i++ {
		if items[i] == out[len(out)-1] {
			continue
		}
		out = append(out, items[i])
	}
	return out
}

func sortByIndex[T interface{ GetIndex() uint16 }](changes []T) {
	sort.Slice(changes, func(i, j int) bool {
		return changes[i].GetIndex() < changes[j].GetIndex()
	})
}

func sortByBytes[T interface{ GetBytes() []byte }](items []T) {
	sort.Slice(items, func(i, j int) bool {
		return bytes.Compare(items[i].GetBytes(), items[j].GetBytes()) < 0
	})
}

func sortHashes(hashes []accounts.StorageKey) {
	sort.Slice(hashes, func(i, j int) bool {
		return hashes[i].Cmp(hashes[j]) < 0
	})
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

func encodeHashList(hashes []accounts.StorageKey, w io.Writer, buf []byte) error {
	if err := validateStorageReads(hashes); err != nil {
		return err
	}
	total := 0
	for i := range hashes {
		hash := hashes[i].Value()
		hashInt := uint256FromHash(hash)
		total += rlp.Uint256Len(hashInt)
	}
	if err := rlp.EncodeStructSizePrefix(total, w, buf); err != nil {
		return err
	}
	for i := range hashes {
		hash := hashes[i].Value()
		hashInt := uint256FromHash(hash)
		if err := rlp.EncodeUint256(hashInt, w, buf); err != nil {
			return err
		}
	}
	return nil
}

func encodingSizeHashList(hashes []accounts.StorageKey) int {
	size := 0
	for i := range hashes {
		hash := hashes[i].Value()
		hashInt := uint256FromHash(hash)
		size += rlp.Uint256Len(hashInt)
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
		address := ac.Address.Value()
		if hasPrev && bytes.Compare(prevAddr[:], address[:]) >= 0 {
			err = fmt.Errorf("block access list addresses must be strictly increasing (prev=%s current=%s)", prevAddr.Hex(), address.Hex())
			break
		}
		acCopy := ac
		changes = append(changes, &acCopy)
		if len(changes) > maxBlockAccessAccounts {
			err = fmt.Errorf("block access list exceeds maximum accounts (%d)", maxBlockAccessAccounts)
			break
		}
		prevAddr = address
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

// DecodeBlockAccessListBytes decodes an RLP-encoded block access list and returns it.
func DecodeBlockAccessListBytes(data []byte) (BlockAccessList, error) {
	stream := rlp.NewStream(bytes.NewReader(data), 0)
	var bal BlockAccessList
	if err := decodeBlockAccessList(&bal, stream); err != nil {
		return nil, err
	}
	return bal, nil
}

// EncodeBlockAccessListBytes encodes a block access list into RLP bytes.
func EncodeBlockAccessListBytes(bal BlockAccessList) ([]byte, error) {
	if len(bal) == 0 {
		return []byte{0xc0}, nil
	}
	if err := bal.Validate(); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	encBuf := newEncodingBuf()
	defer releaseEncodingBuf(encBuf)
	if err := encodeBlockAccessList(bal, &buf, encBuf[:]); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
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
		slot := sc.Slot.Value()
		if hasPrev && bytes.Compare(prevSlot[:], slot[:]) >= 0 {
			err = fmt.Errorf("storage slot list must be strictly increasing (prev=%x current=%x)", prevSlot, sc.Slot)
			break
		}
		out = append(out, sc)
		if len(out) > maxSlotChangesPerAccount {
			err = fmt.Errorf("storage slot change list exceeds maximum entries (%d)", maxSlotChangesPerAccount)
			break
		}
		prevSlot = slot
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

func decodeStorageKeys(s *rlp.Stream) ([]accounts.StorageKey, error) {
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
	var hashes []accounts.StorageKey
	for {
		var h common.Hash
		h, err = decodeUint256Hash(s)
		if err != nil {
			break
		}
		hashes = append(hashes, accounts.InternKey(h))
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

func uint256FromHash(h common.Hash) uint256.Int {
	var out uint256.Int
	out.SetBytes(h[:])
	return out
}

func decodeUint256Hash(s *rlp.Stream) (common.Hash, error) {
	raw, err := s.Bytes()
	if err != nil {
		return common.Hash{}, err
	}
	if len(raw) > 32 {
		return common.Hash{}, fmt.Errorf("integer too large")
	}
	var out common.Hash
	copy(out[32-len(raw):], raw)
	return out, nil
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
		address := account.Address.Value()
		if hasPrev && bytes.Compare(prev[:], address[:]) >= 0 {
			return fmt.Errorf("account addresses must be strictly increasing (index %d)", i)
		}
		if err := account.validate(); err != nil {
			return fmt.Errorf("account %s: %w", address.Hex(), err)
		}
		prev = address
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

func validateStorageReads(reads []accounts.StorageKey) error {
	return validateHashOrdering(reads, maxStorageReadsPerAccount, "storage reads")
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
func validateHashOrdering(hashes []accounts.StorageKey, maxCount int, typeName string) error {
	if len(hashes) == 0 {
		return nil
	}
	if len(hashes) > maxCount {
		return fmt.Errorf("too many %s (%d > %d)", typeName, len(hashes), maxCount)
	}
	for i := 1; i < len(hashes); i++ {
		if hashes[i-1].Cmp(hashes[i]) >= 0 {
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
		if slots[i-1].Slot.Cmp(slots[i].Slot) >= 0 {
			return fmt.Errorf("slots must be strictly increasing (index %d)", i)
		}
	}
	return nil
}

// DebugString renders the block access list into a human-readable multiline string.
func (bal BlockAccessList) DebugString() string {
	var sb strings.Builder
	bal.DebugPrint(&sb)
	return sb.String()
}

func (bal BlockAccessList) DebugPrint(w io.Writer) {
	fmt.Fprintf(w, "accounts=%d", len(bal))
	for i, account := range bal {
		if account == nil {
			fmt.Fprintf(w, "\n[%d] <nil>", i)
			continue
		}
		fmt.Fprintf(w, "\n[%d] addr=%s", i, account.Address.Value().Hex())
		if len(account.StorageChanges) > 0 {
			fmt.Fprint(w, "\n  storageChanges:")
			for _, slotChange := range account.StorageChanges {
				fmt.Fprint(w, "\n    - slot=")
				fmt.Fprint(w, slotChange.Slot.Value().Hex())
				fmt.Fprint(w, " changes=[")
				for j, change := range slotChange.Changes {
					if j > 0 {
						fmt.Fprint(w, " ")
					}
					if change == nil {
						fmt.Fprint(w, "<nil>")
						continue
					}
					fmt.Fprintf(w, "%d:%s", change.Index, change.Value.Hex())
				}
				fmt.Fprint(w, "]")
			}
		}
		if len(account.StorageReads) > 0 {
			fmt.Fprint(w, "\n  storageReads=[")
			for j, read := range account.StorageReads {
				if j > 0 {
					fmt.Fprint(w, " ")
				}
				fmt.Fprint(w, read.Value().Hex())
			}
			fmt.Fprint(w, "]")
		}
		if len(account.BalanceChanges) > 0 {
			fmt.Fprint(w, "\n  balanceChanges=[")
			for j, change := range account.BalanceChanges {
				if j > 0 {
					fmt.Fprint(w, " ")
				}
				if change == nil {
					fmt.Fprint(w, "<nil>")
					continue
				}
				fmt.Fprintf(w, "%d:%s", change.Index, change.Value.Hex())
			}
			fmt.Fprint(w, "]")
		}
		if len(account.NonceChanges) > 0 {
			fmt.Fprint(w, "\n  nonceChanges=[")
			for j, change := range account.NonceChanges {
				if j > 0 {
					fmt.Fprint(w, " ")
				}
				if change == nil {
					fmt.Fprint(w, "<nil>")
					continue
				}
				fmt.Fprintf(w, "%d:%d", change.Index, change.Value)
			}
			fmt.Fprint(w, "]")
		}
		if len(account.CodeChanges) > 0 {
			fmt.Fprint(w, "\n  codeChanges=[")
			for j, change := range account.CodeChanges {
				if j > 0 {
					fmt.Fprint(w, " ")
				}
				if change == nil {
					fmt.Fprint(w, "<nil>")
					continue
				}
				fmt.Fprintf(w, "%d:len(%d)", change.Index, len(change.Bytecode))
			}
			fmt.Fprint(w, "]")
		}
	}
}

func ConvertBlockAccessListFromTypesProto(protoList []*typesproto.BlockAccessListAccount) hexutil.Bytes {
	if protoList == nil {
		return nil
	}
	bal := make(BlockAccessList, len(protoList))
	for i, acc := range protoList {
		bal[i] = &AccountChanges{
			Address: accounts.InternAddress(gointerfaces.ConvertH160toAddress(acc.Address)),
		}
		if acc.StorageChanges != nil {
			bal[i].StorageChanges = make([]*SlotChanges, len(acc.StorageChanges))
			for j, sc := range acc.StorageChanges {
				bal[i].StorageChanges[j] = &SlotChanges{
					Slot: accounts.InternKey(gointerfaces.ConvertH256ToHash(sc.Slot)),
				}
				if sc.Changes != nil {
					bal[i].StorageChanges[j].Changes = make([]*StorageChange, len(sc.Changes))
					for k, c := range sc.Changes {
						val := gointerfaces.ConvertH256ToUint256Int(c.Value)
						bal[i].StorageChanges[j].Changes[k] = &StorageChange{
							Index: uint16(c.Index),
							Value: *val,
						}
					}
				}
			}
		}
		if acc.StorageReads != nil {
			bal[i].StorageReads = make([]accounts.StorageKey, len(acc.StorageReads))
			for j, r := range acc.StorageReads {
				bal[i].StorageReads[j] = accounts.InternKey(gointerfaces.ConvertH256ToHash(r))
			}
		}
		if acc.BalanceChanges != nil {
			bal[i].BalanceChanges = make([]*BalanceChange, len(acc.BalanceChanges))
			for j, bc := range acc.BalanceChanges {
				val := gointerfaces.ConvertH256ToUint256Int(bc.Value)
				bal[i].BalanceChanges[j] = &BalanceChange{
					Index: uint16(bc.Index),
					Value: *val,
				}
			}
		}
		if acc.NonceChanges != nil {
			bal[i].NonceChanges = make([]*NonceChange, len(acc.NonceChanges))
			for j, nc := range acc.NonceChanges {
				bal[i].NonceChanges[j] = &NonceChange{
					Index: uint16(nc.Index),
					Value: nc.Value,
				}
			}
		}
		if acc.CodeChanges != nil {
			bal[i].CodeChanges = make([]*CodeChange, len(acc.CodeChanges))
			for j, cc := range acc.CodeChanges {
				bal[i].CodeChanges[j] = &CodeChange{
					Index:    uint16(cc.Index),
					Bytecode: cc.Data,
				}
			}
		}
	}
	encoded, err := rlp.EncodeToBytes(bal)
	if err != nil {
		return nil
	}
	return encoded
}

func ConvertBlockAccessListToTypesProto(bal BlockAccessList) []*typesproto.BlockAccessListAccount {
	if bal == nil {
		return nil
	}
	out := make([]*typesproto.BlockAccessListAccount, 0, len(bal))
	for _, account := range bal {
		if account == nil {
			continue
		}
		balAccount := &typesproto.BlockAccessListAccount{
			Address: gointerfaces.ConvertAddressToH160(account.Address.Value()),
		}
		for _, storageChange := range account.StorageChanges {
			if storageChange == nil {
				continue
			}
			slotChanges := &typesproto.BlockAccessListSlotChanges{
				Slot: gointerfaces.ConvertHashToH256(storageChange.Slot.Value()),
			}
			for _, change := range storageChange.Changes {
				if change == nil {
					continue
				}
				slotChanges.Changes = append(slotChanges.Changes, &typesproto.BlockAccessListStorageChange{
					Index: uint32(change.Index),
					Value: gointerfaces.ConvertUint256IntToH256(&change.Value),
				})
			}
			balAccount.StorageChanges = append(balAccount.StorageChanges, slotChanges)
		}
		for _, read := range account.StorageReads {
			balAccount.StorageReads = append(balAccount.StorageReads, gointerfaces.ConvertHashToH256(read.Value()))
		}
		for _, balanceChange := range account.BalanceChanges {
			if balanceChange == nil {
				continue
			}
			val := balanceChange.Value
			balAccount.BalanceChanges = append(balAccount.BalanceChanges, &typesproto.BlockAccessListBalanceChange{
				Index: uint32(balanceChange.Index),
				Value: gointerfaces.ConvertUint256IntToH256(&val),
			})
		}
		for _, nonceChange := range account.NonceChanges {
			if nonceChange == nil {
				continue
			}
			balAccount.NonceChanges = append(balAccount.NonceChanges, &typesproto.BlockAccessListNonceChange{
				Index: uint32(nonceChange.Index),
				Value: nonceChange.Value,
			})
		}
		for _, codeChange := range account.CodeChanges {
			if codeChange == nil {
				continue
			}
			data := make([]byte, len(codeChange.Bytecode))
			copy(data, codeChange.Bytecode)
			balAccount.CodeChanges = append(balAccount.CodeChanges, &typesproto.BlockAccessListCodeChange{
				Index: uint32(codeChange.Index),
				Data:  data,
			})
		}
		out = append(out, balAccount)
	}
	return out
}

func ConvertBlockAccessListFromExecutionProto(protoList []*executionproto.BlockAccessListAccount) *hexutil.Bytes {
	if protoList == nil {
		return nil
	}
	bal, err := ConvertExecutionProtoToBlockAccessList(protoList)
	if err != nil {
		return nil
	}
	encoded, err := rlp.EncodeToBytes(bal)
	if err != nil {
		return nil
	}
	res := hexutil.Bytes(encoded)
	return &res
}

func ConvertBlockAccessListToExecutionProto(bal BlockAccessList) []*executionproto.BlockAccessListAccount {
	if len(bal) == 0 {
		return nil
	}
	out := make([]*executionproto.BlockAccessListAccount, 0, len(bal))
	for _, account := range bal {
		if account == nil {
			continue
		}
		rpcAccount := &executionproto.BlockAccessListAccount{
			Address: gointerfaces.ConvertAddressToH160(account.Address.Value()),
		}
		for _, storageChange := range account.StorageChanges {
			if storageChange == nil {
				continue
			}
			slotChanges := &executionproto.BlockAccessListSlotChanges{
				Slot: gointerfaces.ConvertHashToH256(storageChange.Slot.Value()),
			}
			for _, change := range storageChange.Changes {
				if change == nil {
					continue
				}
				slotChanges.Changes = append(slotChanges.Changes, &executionproto.BlockAccessListStorageChange{
					Index: uint32(change.Index),
					Value: gointerfaces.ConvertUint256IntToH256(&change.Value),
				})
			}
			rpcAccount.StorageChanges = append(rpcAccount.StorageChanges, slotChanges)
		}
		for _, read := range account.StorageReads {
			rpcAccount.StorageReads = append(rpcAccount.StorageReads, gointerfaces.ConvertHashToH256(read.Value()))
		}
		for _, balanceChange := range account.BalanceChanges {
			if balanceChange == nil {
				continue
			}
			val := balanceChange.Value
			rpcAccount.BalanceChanges = append(rpcAccount.BalanceChanges, &executionproto.BlockAccessListBalanceChange{
				Index: uint32(balanceChange.Index),
				Value: gointerfaces.ConvertUint256IntToH256(&val),
			})
		}
		for _, nonceChange := range account.NonceChanges {
			if nonceChange == nil {
				continue
			}
			rpcAccount.NonceChanges = append(rpcAccount.NonceChanges, &executionproto.BlockAccessListNonceChange{
				Index: uint32(nonceChange.Index),
				Value: nonceChange.Value,
			})
		}
		for _, codeChange := range account.CodeChanges {
			if codeChange == nil {
				continue
			}
			data := make([]byte, len(codeChange.Bytecode))
			copy(data, codeChange.Bytecode)
			rpcAccount.CodeChanges = append(rpcAccount.CodeChanges, &executionproto.BlockAccessListCodeChange{
				Index: uint32(codeChange.Index),
				Data:  data,
			})
		}
		out = append(out, rpcAccount)
	}
	return out
}

func ConvertExecutionProtoToBlockAccessList(protoList []*executionproto.BlockAccessListAccount) (BlockAccessList, error) {
	if len(protoList) == 0 {
		return nil, nil
	}
	out := make(BlockAccessList, 0, len(protoList))
	for accountIdx, account := range protoList {
		if account == nil {
			return nil, fmt.Errorf("blockAccessList account %d is nil", accountIdx)
		}
		if account.Address == nil {
			return nil, fmt.Errorf("blockAccessList account %d missing address", accountIdx)
		}
		accountChanges := &AccountChanges{
			Address: accounts.InternAddress(gointerfaces.ConvertH160toAddress(account.Address)),
		}
		for slotIdx, storageChange := range account.StorageChanges {
			if storageChange == nil {
				return nil, fmt.Errorf("blockAccessList account %d storageChanges[%d] is nil", accountIdx, slotIdx)
			}
			if storageChange.Slot == nil {
				return nil, fmt.Errorf("blockAccessList account %d storageChanges[%d] missing slot", accountIdx, slotIdx)
			}
			slotChanges := &SlotChanges{Slot: accounts.InternKey(gointerfaces.ConvertH256ToHash(storageChange.Slot))}
			for changeIdx, change := range storageChange.Changes {
				if change == nil {
					return nil, fmt.Errorf("blockAccessList account %d storageChanges[%d].changes[%d] is nil", accountIdx, slotIdx, changeIdx)
				}
				if change.Index > math.MaxUint16 {
					return nil, fmt.Errorf("blockAccessList account %d storageChanges[%d].changes[%d] index overflow: %d", accountIdx, slotIdx, changeIdx, change.Index)
				}
				if change.Value == nil {
					return nil, fmt.Errorf("blockAccessList account %d storageChanges[%d].changes[%d] missing value", accountIdx, slotIdx, changeIdx)
				}
				val := gointerfaces.ConvertH256ToUint256Int(change.Value)
				slotChanges.Changes = append(slotChanges.Changes, &StorageChange{
					Index: uint16(change.Index),
					Value: *val,
				})
			}
			accountChanges.StorageChanges = append(accountChanges.StorageChanges, slotChanges)
		}
		for readIdx, read := range account.StorageReads {
			if read == nil {
				return nil, fmt.Errorf("blockAccessList account %d storageReads[%d] is nil", accountIdx, readIdx)
			}
			accountChanges.StorageReads = append(accountChanges.StorageReads, accounts.InternKey(gointerfaces.ConvertH256ToHash(read)))
		}
		for balanceIdx, balanceChange := range account.BalanceChanges {
			if balanceChange == nil {
				return nil, fmt.Errorf("blockAccessList account %d balanceChanges[%d] is nil", accountIdx, balanceIdx)
			}
			if balanceChange.Index > math.MaxUint16 {
				return nil, fmt.Errorf("blockAccessList account %d balanceChanges[%d] index overflow: %d", accountIdx, balanceIdx, balanceChange.Index)
			}
			if balanceChange.Value == nil {
				return nil, fmt.Errorf("blockAccessList account %d balanceChanges[%d] missing value", accountIdx, balanceIdx)
			}
			val := gointerfaces.ConvertH256ToUint256Int(balanceChange.Value)
			accountChanges.BalanceChanges = append(accountChanges.BalanceChanges, &BalanceChange{
				Index: uint16(balanceChange.Index),
				Value: *val,
			})
		}
		for nonceIdx, nonceChange := range account.NonceChanges {
			if nonceChange == nil {
				return nil, fmt.Errorf("blockAccessList account %d nonceChanges[%d] is nil", accountIdx, nonceIdx)
			}
			if nonceChange.Index > math.MaxUint16 {
				return nil, fmt.Errorf("blockAccessList account %d nonceChanges[%d] index overflow: %d", accountIdx, nonceIdx, nonceChange.Index)
			}
			accountChanges.NonceChanges = append(accountChanges.NonceChanges, &NonceChange{
				Index: uint16(nonceChange.Index),
				Value: nonceChange.Value,
			})
		}
		for codeIdx, codeChange := range account.CodeChanges {
			if codeChange == nil {
				return nil, fmt.Errorf("blockAccessList account %d codeChanges[%d] is nil", accountIdx, codeIdx)
			}
			if codeChange.Index > math.MaxUint16 {
				return nil, fmt.Errorf("blockAccessList account %d codeChanges[%d] index overflow: %d", accountIdx, codeIdx, codeChange.Index)
			}
			data := make([]byte, len(codeChange.Data))
			copy(data, codeChange.Data)
			accountChanges.CodeChanges = append(accountChanges.CodeChanges, &CodeChange{
				Index:    uint16(codeChange.Index),
				Bytecode: data,
			})
		}
		out = append(out, accountChanges)
	}
	if err := out.Validate(); err != nil {
		return nil, fmt.Errorf("blockAccessList validate: %w", err)
	}
	return out, nil
}
