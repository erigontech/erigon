package types

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/executionproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

type BlockAccessList []AccountChanges

// BlockAccessListSidecar carries immutable decoded and encoded BAL representations.
type BlockAccessListSidecar struct {
	bal               BlockAccessList
	raw               atomic.Pointer[[]byte]
	hash              atomic.Pointer[common.Hash]
	validatedGasLimit atomic.Pointer[uint64]
	validatedTxCount  atomic.Pointer[int]
}

// NewBlockAccessListSidecar wraps a decoded BAL that must not be mutated afterwards.
func NewBlockAccessListSidecar(bal BlockAccessList) *BlockAccessListSidecar {
	if bal == nil {
		return nil
	}
	return &BlockAccessListSidecar{bal: bal}
}

// DecodeBlockAccessListSidecar decodes a BAL and copies its canonical RLP bytes.
func DecodeBlockAccessListSidecar(data []byte) (*BlockAccessListSidecar, error) {
	return DecodeBlockAccessListSidecarOwned(bytes.Clone(data))
}

// DecodeBlockAccessListSidecarOwned decodes a BAL and takes ownership of its canonical RLP bytes.
// The caller must not mutate data after the call.
func DecodeBlockAccessListSidecarOwned(data []byte) (*BlockAccessListSidecar, error) {
	return decodeBlockAccessListSidecar(data, nil)
}

// DecodeBlockAccessListSidecarForHeader takes ownership of data and bounds decoded items by the block gas limit.
func DecodeBlockAccessListSidecarForHeader(data []byte, gasLimit uint64) (*BlockAccessListSidecar, error) {
	return decodeBlockAccessListSidecar(data, &blockAccessListDecodeBudget{
		remaining: gasLimit / BalItemCost,
		maxIndex:  math.MaxUint32,
	})
}

// DecodeBlockAccessListSidecarForBlock also bounds change indices by the transaction count and takes ownership of data.
func DecodeBlockAccessListSidecarForBlock(data []byte, gasLimit uint64, txCount int) (*BlockAccessListSidecar, error) {
	return decodeBlockAccessListSidecar(data, &blockAccessListDecodeBudget{
		remaining: gasLimit / BalItemCost,
		maxIndex:  uint64(txCount) + 1,
	})
}

func decodeBlockAccessListSidecar(data []byte, budget *blockAccessListDecodeBudget) (*BlockAccessListSidecar, error) {
	bal, err := decodeBlockAccessListBytes(data, budget)
	if err != nil {
		return nil, err
	}
	sidecar := &BlockAccessListSidecar{bal: bal}
	sidecar.raw.Store(&data)
	return sidecar, nil
}

// BlockAccessList returns the immutable decoded BAL.
func (s *BlockAccessListSidecar) BlockAccessList() BlockAccessList {
	if s == nil {
		return nil
	}
	return s.bal
}

// Bytes returns the immutable canonical RLP bytes.
func (s *BlockAccessListSidecar) Bytes() ([]byte, error) {
	if s == nil {
		return nil, nil
	}
	if raw := s.raw.Load(); raw != nil {
		return *raw, nil
	}
	raw, err := EncodeBlockAccessListBytes(s.bal)
	if err != nil {
		return nil, err
	}
	if s.raw.CompareAndSwap(nil, &raw) {
		return raw, nil
	}
	return *s.raw.Load(), nil
}

// Hash returns the memoized Keccak-256 hash of the encoded BAL.
func (s *BlockAccessListSidecar) Hash() (common.Hash, error) {
	if s == nil {
		return common.Hash{}, nil
	}
	if hash := s.hash.Load(); hash != nil {
		return *hash, nil
	}
	raw, err := s.Bytes()
	if err != nil {
		return common.Hash{}, err
	}
	return s.cacheHash(raw), nil
}

func (s *BlockAccessListSidecar) cacheHash(raw []byte) common.Hash {
	hash := crypto.Keccak256Hash(raw)
	if s.hash.CompareAndSwap(nil, &hash) {
		return hash
	}
	return *s.hash.Load()
}

// ValidateForHeader validates once for each distinct gas limit.
func (s *BlockAccessListSidecar) ValidateForHeader(gasLimit uint64) error {
	if s == nil {
		return nil
	}
	if validated := s.validatedGasLimit.Load(); validated != nil && *validated == gasLimit {
		return nil
	}
	if err := s.bal.ValidateForHeader(gasLimit); err != nil {
		return err
	}
	s.validatedGasLimit.Store(&gasLimit)
	return nil
}

// ValidateForBlock also rejects block access indices beyond len(transactions)+1, the
// post-execution index, so a spurious change fails before the block executes. The
// index pass is memoized for each distinct transaction count.
func (s *BlockAccessListSidecar) ValidateForBlock(gasLimit uint64, txCount int) error {
	if s == nil {
		return nil
	}
	if err := s.ValidateForHeader(gasLimit); err != nil {
		return err
	}
	if validated := s.validatedTxCount.Load(); validated != nil && *validated == txCount {
		return nil
	}
	if err := s.bal.validateBlockAccessIndices(txCount); err != nil {
		return err
	}
	s.validatedTxCount.Store(&txCount)
	return nil
}

func (s *BlockAccessListSidecar) copy() *BlockAccessListSidecar {
	if s == nil {
		return nil
	}
	cpy := &BlockAccessListSidecar{bal: s.bal.Copy()}
	if raw := s.raw.Load(); raw != nil {
		rawCopy := bytes.Clone(*raw)
		cpy.raw.Store(&rawCopy)
	}
	if hash := s.hash.Load(); hash != nil {
		hashCopy := *hash
		cpy.hash.Store(&hashCopy)
	}
	if validated := s.validatedGasLimit.Load(); validated != nil {
		gasLimit := *validated
		cpy.validatedGasLimit.Store(&gasLimit)
	}
	if validated := s.validatedTxCount.Load(); validated != nil {
		txCount := *validated
		cpy.validatedTxCount.Store(&txCount)
	}
	return cpy
}

const maxBlockAccessListBytes = 10 * 1024 * 1024

// EIP-7928: bal_items <= block_gas_limit / BalItemCost
const BalItemCost = 2000

type blockAccessListDecodeBudget struct {
	remaining uint64
	maxIndex  uint64
}

func (budget *blockAccessListDecodeBudget) takeItem() error {
	if budget == nil {
		return nil
	}
	if budget.remaining == 0 {
		return fmt.Errorf("%w: block access list too large", ErrInvalidBlockAccessList)
	}
	budget.remaining--
	return nil
}

type AccountChanges struct {
	Address        common.Address
	StorageChanges []SlotChanges
	StorageReads   []common.Hash
	BalanceChanges []*BalanceChange
	NonceChanges   []*NonceChange
	CodeChanges    []*CodeChange
}

type SlotChanges struct {
	Slot    accounts.StorageKey
	Changes []*StorageChange
}

type StorageChange struct {
	Index uint32
	Value uint256.Int
}

type BalanceChange struct {
	Index uint32
	Value uint256.Int
}

type NonceChange struct {
	Index uint32
	Value uint64
}

type CodeChange struct {
	Index    uint32
	Bytecode []byte
}

// Copy returns a deep copy of bal.
func (bal BlockAccessList) Copy() BlockAccessList {
	if bal == nil {
		return nil
	}
	cpy := make(BlockAccessList, len(bal))
	for i := range bal {
		account := &bal[i]
		accountCopy := *account
		accountCopy.StorageChanges = slices.Clone(account.StorageChanges)
		for j := range account.StorageChanges {
			slot := &account.StorageChanges[j]
			changes := make([]*StorageChange, len(slot.Changes))
			for k, change := range slot.Changes {
				if change != nil {
					changeCopy := *change
					changes[k] = &changeCopy
				}
			}
			accountCopy.StorageChanges[j].Changes = changes
		}
		accountCopy.StorageReads = slices.Clone(account.StorageReads)
		accountCopy.BalanceChanges = slices.Clone(account.BalanceChanges)
		for j, change := range account.BalanceChanges {
			if change != nil {
				changeCopy := *change
				accountCopy.BalanceChanges[j] = &changeCopy
			}
		}
		accountCopy.NonceChanges = slices.Clone(account.NonceChanges)
		for j, change := range account.NonceChanges {
			if change != nil {
				changeCopy := *change
				accountCopy.NonceChanges[j] = &changeCopy
			}
		}
		accountCopy.CodeChanges = slices.Clone(account.CodeChanges)
		for j, change := range account.CodeChanges {
			if change != nil {
				changeCopy := *change
				changeCopy.Bytecode = bytes.Clone(change.Bytecode)
				accountCopy.CodeChanges[j] = &changeCopy
			}
		}
		cpy[i] = accountCopy
	}
	return cpy
}

// indexedChange interface for generic validation of change types with indices
type indexedChange interface {
	*StorageChange | *BalanceChange | *NonceChange | *CodeChange
	GetIndex() uint32
}

// GetIndex methods for indexedChange interface
func (bc *BalanceChange) GetIndex() uint32 { return bc.Index }
func (nc *NonceChange) GetIndex() uint32   { return nc.Index }
func (cc *CodeChange) GetIndex() uint32    { return cc.Index }
func (sc *StorageChange) GetIndex() uint32 { return sc.Index }

func encodingSizeSlotChanges(list []SlotChanges) int {
	size := 0
	for i := range list {
		s := list[i].EncodingSize()
		size += rlp.ListPrefixLen(s) + s
	}
	return size
}

func encodeSlotChanges(list []SlotChanges, w io.Writer, b []byte) error {
	total := encodingSizeSlotChanges(list)
	if err := rlp.EncodeListPrefix(total, w, b); err != nil {
		return err
	}
	for i := range list {
		if err := list[i].EncodeRLP(w); err != nil {
			return err
		}
	}
	return nil
}

func (ac *AccountChanges) EncodingSize() int {
	size := 21 // address (1 prefix + 20 bytes)
	storageChangesLen := encodingSizeSlotChanges(ac.StorageChanges)
	size += rlp.ListPrefixLen(storageChangesLen) + storageChangesLen
	storageReadsLen := encodingSizeHashList(ac.StorageReads)
	balanceChangesLen := encodingSizeBlockAccessList(ac.BalanceChanges)
	nonceChangesLen := encodingSizeBlockAccessList(ac.NonceChanges)
	codeChangesLen := encodingSizeBlockAccessList(ac.CodeChanges)
	size += storageReadsLen
	size += rlp.ListPrefixLen(balanceChangesLen) + balanceChangesLen
	size += rlp.ListPrefixLen(nonceChangesLen) + nonceChangesLen
	size += rlp.ListPrefixLen(codeChangesLen) + codeChangesLen
	return size
}

func (ac *AccountChanges) EncodeRLP(w io.Writer) error {
	encodingSize := ac.EncodingSize()
	b := rlp.NewEncodingBuf()
	defer b.Release()

	if err := rlp.EncodeListPrefix(encodingSize, w, b[:]); err != nil {
		return err
	}

	b[0] = 128 + 20
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	address := ac.Address
	if _, err := w.Write(address[:]); err != nil {
		return err
	}

	if err := encodeSlotChanges(ac.StorageChanges, w, b[:]); err != nil {
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
	return ac.decodeRLP(s, nil)
}

func (ac *AccountChanges) decodeRLP(s *rlp.Stream, budget *blockAccessListDecodeBudget) error {
	if _, err := s.List(); err != nil {
		return err
	}

	address, err := s.Addr()
	if err != nil {
		return fmt.Errorf("read Address: %w", err)
	}
	ac.Address = address
	list, err := decodeSlotChangesList(s, budget)
	if err != nil {
		return fmt.Errorf("read StorageChanges: %w", err)
	}
	ac.StorageChanges = nil
	if len(list) > 0 {
		ac.StorageChanges = make([]SlotChanges, len(list))
		for i, sc := range list {
			if sc != nil {
				ac.StorageChanges[i] = *sc
			}
		}
	}

	reads, err := decodeStorageKeys(s, budget)
	if err != nil {
		return fmt.Errorf("read StorageReads: %w", err)
	}
	ac.StorageReads = reads

	tmpBalances, err := decodeChangeList[BalanceChange](s, budget)
	if err != nil {
		return fmt.Errorf("read BalanceChanges: %w", err)
	}
	ac.BalanceChanges = tmpBalances

	nonces, err := decodeChangeList[NonceChange](s, budget)
	if err != nil {
		return fmt.Errorf("read NonceChanges: %w", err)
	}
	ac.NonceChanges = nonces

	codes, err := decodeChangeList[CodeChange](s, budget)
	if err != nil {
		return fmt.Errorf("read CodeChanges: %w", err)
	}
	ac.CodeChanges = codes

	return s.ListEnd()
}

func (ac *AccountChanges) Normalize() {
	if len(ac.StorageChanges) > 1 {
		slices.SortFunc(ac.StorageChanges, func(a, b SlotChanges) int {
			return a.Slot.Cmp(b.Slot)
		})
	}

	for i := range ac.StorageChanges {
		if len(ac.StorageChanges[i].Changes) > 1 {
			sortByIndex(ac.StorageChanges[i].Changes)
			ac.StorageChanges[i].Changes = dedupByIndex(ac.StorageChanges[i].Changes)
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
	size := rlp.Uint256Len(hashToUint256(sc.Slot.Value())) // minimal slot key
	changesLen := encodingSizeBlockAccessList(sc.Changes)
	size += rlp.ListPrefixLen(changesLen) + changesLen
	return size
}

func (sc *SlotChanges) EncodeRLP(w io.Writer) error {
	b := rlp.NewEncodingBuf()
	defer b.Release()

	encodingSize := sc.EncodingSize()
	if err := rlp.EncodeListPrefix(encodingSize, w, b[:]); err != nil {
		return err
	}
	if err := rlp.EncodeUint256(hashToUint256(sc.Slot.Value()), w, b[:]); err != nil {
		return err
	}

	return encodeBlockAccessList(sc.Changes, w, b[:])
}

func (sc *SlotChanges) DecodeRLP(s *rlp.Stream) error {
	return sc.decodeRLP(s, nil)
}

func (sc *SlotChanges) decodeRLP(s *rlp.Stream, budget *blockAccessListDecodeBudget) error {
	if _, err := s.List(); err != nil {
		return err
	}
	slot, err := decodeMinimalHash(s)
	if err != nil {
		return fmt.Errorf("read Slot: %w", err)
	}
	sc.Slot = accounts.InternKey(slot)
	changes, err := decodeChangeList[StorageChange](s, budget)
	if err != nil {
		return fmt.Errorf("read Changes: %w", err)
	}
	sc.Changes = changes

	return s.ListEnd()
}

func (sc *StorageChange) EncodingSize() int {
	size := rlp.U64Len(uint64(sc.Index))
	size += rlp.Uint256Len(sc.Value) // minimal storage value
	return size
}

func (sc *StorageChange) EncodeRLP(w io.Writer) error {
	b := rlp.NewEncodingBuf()
	defer b.Release()

	encodingSize := sc.EncodingSize()
	if err := rlp.EncodeListPrefix(encodingSize, w, b[:]); err != nil {
		return err
	}
	if err := rlp.EncodeU64(uint64(sc.Index), w, b[:]); err != nil {
		return err
	}
	return rlp.EncodeUint256(sc.Value, w, b[:])
}

func (sc *StorageChange) DecodeRLP(s *rlp.Stream) error {
	if _, err := s.List(); err != nil {
		return err
	}
	idx, err := s.Uint64()
	if err != nil {
		return fmt.Errorf("read Index: %w", err)
	}
	if idx > math.MaxUint32 {
		return fmt.Errorf("read Index: value %d exceeds uint32", idx)
	}
	sc.Index = uint32(idx)
	// ReadUint256 enforces canonical (minimal) integer encoding, matching the
	// encoder and rejecting non-canonical values (leading zero bytes).
	if err := s.ReadUint256(&sc.Value); err != nil {
		return fmt.Errorf("read Value: %w", err)
	}
	return s.ListEnd()
}

func (bc *BalanceChange) EncodingSize() int {
	size := rlp.U64Len(uint64(bc.Index))
	size += rlp.Uint256Len(bc.Value) // minimal balance value
	return size
}

func (bc *BalanceChange) EncodeRLP(w io.Writer) error {
	b := rlp.NewEncodingBuf()
	defer b.Release()

	encodingSize := bc.EncodingSize()
	if err := rlp.EncodeListPrefix(encodingSize, w, b[:]); err != nil {
		return err
	}
	if err := rlp.EncodeU64(uint64(bc.Index), w, b[:]); err != nil {
		return err
	}
	return rlp.EncodeUint256(bc.Value, w, b[:])
}

func (bc *BalanceChange) DecodeRLP(s *rlp.Stream) error {
	if _, err := s.List(); err != nil {
		return err
	}
	idx, err := s.Uint64()
	if err != nil {
		return fmt.Errorf("read Index: %w", err)
	}
	if idx > math.MaxUint32 {
		return fmt.Errorf("read Index: value %d exceeds uint32", idx)
	}
	bc.Index = uint32(idx)
	// ReadUint256 enforces canonical (minimal) integer encoding, matching the
	// encoder and rejecting non-canonical values (leading zero bytes).
	if err := s.ReadUint256(&bc.Value); err != nil {
		return fmt.Errorf("read Value: %w", err)
	}
	return s.ListEnd()
}

func (nc *NonceChange) EncodingSize() int {
	size := rlp.U64Len(uint64(nc.Index))
	size += rlp.U64Len(nc.Value)
	return size
}

func (nc *NonceChange) EncodeRLP(w io.Writer) error {
	b := rlp.NewEncodingBuf()
	defer b.Release()

	encodingSize := nc.EncodingSize()
	if err := rlp.EncodeListPrefix(encodingSize, w, b[:]); err != nil {
		return err
	}
	if err := rlp.EncodeU64(uint64(nc.Index), w, b[:]); err != nil {
		return err
	}
	return rlp.EncodeU64(nc.Value, w, b[:])
}

func (nc *NonceChange) DecodeRLP(s *rlp.Stream) error {
	if _, err := s.List(); err != nil {
		return err
	}
	idx, err := s.Uint64()
	if err != nil {
		return fmt.Errorf("read Index: %w", err)
	}
	if idx > math.MaxUint32 {
		return fmt.Errorf("read Index: value %d exceeds uint32", idx)
	}
	nc.Index = uint32(idx)
	value, err := s.Uint64()
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
	b := rlp.NewEncodingBuf()
	defer b.Release()

	encodingSize := cc.EncodingSize()
	if err := rlp.EncodeListPrefix(encodingSize, w, b[:]); err != nil {
		return err
	}
	if err := rlp.EncodeU64(uint64(cc.Index), w, b[:]); err != nil {
		return err
	}
	return rlp.EncodeString(cc.Bytecode, w, b[:])
}

func (cc *CodeChange) DecodeRLP(s *rlp.Stream) error {
	if _, err := s.List(); err != nil {
		return err
	}
	idx, err := s.Uint64()
	if err != nil {
		return fmt.Errorf("read Index: %w", err)
	}
	if idx > math.MaxUint32 {
		return fmt.Errorf("read Index: value %d exceeds uint32", idx)
	}
	cc.Index = uint32(idx)
	data, err := s.Bytes()
	if err != nil {
		return fmt.Errorf("read Bytecode: %w", err)
	}
	cc.Bytecode = data
	return s.ListEnd()
}

func dedupByIndex[T interface{ GetIndex() uint32 }](changes []T) []T {
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

func sortByIndex[T interface{ GetIndex() uint32 }](changes []T) {
	slices.SortFunc(changes, func(a, b T) int {
		return cmp.Compare(a.GetIndex(), b.GetIndex())
	})
}

func sortHashes(hashes []common.Hash) {
	slices.SortFunc(hashes, common.Hash.Cmp)
}

type blockAccessListRLPItem interface {
	rlpEncodable
	*AccountChanges | *SlotChanges | *StorageChange | *BalanceChange | *NonceChange | *CodeChange
}

func encodingSizeBlockAccessList[T blockAccessListRLPItem](items []T) int {
	var total int
	for _, item := range items {
		if item == nil {
			total += rlp.ListPrefixLen(0)
			continue
		}
		size := item.EncodingSize()
		total += rlp.ListPrefixLen(size) + size
	}
	return total
}

func encodeBlockAccessList[T blockAccessListRLPItem](items []T, w io.Writer, buf []byte) error {
	var total int
	for i, item := range items {
		if item == nil {
			return fmt.Errorf("nil block access list item at index %d", i)
		}
		size := item.EncodingSize()
		total += rlp.ListPrefixLen(size) + size
	}
	if err := rlp.EncodeListPrefix(total, w, buf); err != nil {
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
	total := 0
	for i := range hashes {
		total += rlp.Uint256Len(hashToUint256(hashes[i]))
	}
	if err := rlp.EncodeListPrefix(total, w, buf); err != nil {
		return err
	}
	for i := range hashes {
		if err := rlp.EncodeUint256(hashToUint256(hashes[i]), w, buf); err != nil {
			return err
		}
	}
	return nil
}

func encodingSizeHashList(hashes []common.Hash) int {
	size := 0
	for i := range hashes {
		size += rlp.Uint256Len(hashToUint256(hashes[i]))
	}
	return rlp.ListPrefixLen(size) + size
}

// ErrInvalidBlockAccessList marks a well-formed but semantically invalid block access list.
var ErrInvalidBlockAccessList = errors.New("invalid block access list")

func decodeBlockAccessList(out *BlockAccessList, s *rlp.Stream, budget *blockAccessListDecodeBudget) error {
	if _, err := s.List(); err != nil {
		return err
	}
	var changes []*AccountChanges

	for s.MoreDataInList() {
		if err := budget.takeItem(); err != nil {
			return err
		}
		var ac AccountChanges
		if err := ac.decodeRLP(s, budget); err != nil {
			return err
		}
		if budget != nil && len(changes) > 0 && bytes.Compare(changes[len(changes)-1].Address[:], ac.Address[:]) >= 0 {
			return fmt.Errorf("%w: account addresses must be strictly increasing", ErrInvalidBlockAccessList)
		}
		acCopy := ac
		changes = append(changes, &acCopy)
	}
	if err := s.ListEnd(); err != nil {
		return err
	}
	if len(changes) == 0 {
		// EIP-7928: a genuine empty list (0xc0) was successfully decoded.
		// Return an initialized empty slice, not nil.
		*out = make(BlockAccessList, 0)
		return nil
	}
	bal := make(BlockAccessList, len(changes))
	for i, ac := range changes {
		if ac != nil {
			bal[i] = *ac
		}
	}
	*out = bal
	return nil
}

// DecodeBlockAccessListBytes decodes a syntactically valid RLP block access list.
func DecodeBlockAccessListBytes(data []byte) (BlockAccessList, error) {
	return decodeBlockAccessListBytes(data, nil)
}

func decodeBlockAccessListBytes(data []byte, budget *blockAccessListDecodeBudget) (BlockAccessList, error) {
	if len(data) > maxBlockAccessListBytes {
		return nil, fmt.Errorf("block access list RLP exceeds maximum size (%d bytes)", len(data))
	}
	stream := rlp.NewStream(bytes.NewReader(data), 0)
	var bal BlockAccessList
	if err := decodeBlockAccessList(&bal, stream, budget); err != nil {
		return nil, err
	}
	// The payload must be exactly one RLP list; trailing bytes (e.g. 0xc000) are
	// malformed input, not a valid empty list. Keep the error unwrapped so callers
	// classify it as undecodable rather than an EIP-7928 rule violation.
	if _, _, err := stream.Kind(); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing bytes after block access list")
	}
	return bal, nil
}

// EncodeBlockAccessListBytes encodes a block access list without semantic validation.
func EncodeBlockAccessListBytes(bal BlockAccessList) ([]byte, error) {
	var buf bytes.Buffer
	encBuf := rlp.NewEncodingBuf()
	defer encBuf.Release()
	if err := encodeAccountChanges(bal, &buf, encBuf[:]); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeAccountChanges(list []AccountChanges, w io.Writer, b []byte) error {
	var total int
	for i := range list {
		size := list[i].EncodingSize()
		total += rlp.ListPrefixLen(size) + size
	}
	if err := rlp.EncodeListPrefix(total, w, b); err != nil {
		return err
	}
	for i := range list {
		if err := list[i].EncodeRLP(w); err != nil {
			return err
		}
	}
	return nil
}

func decodeSlotChangesList(s *rlp.Stream, budget *blockAccessListDecodeBudget) ([]*SlotChanges, error) {
	if _, err := s.List(); err != nil {
		return nil, err
	}
	var out []*SlotChanges
	for s.MoreDataInList() {
		if err := budget.takeItem(); err != nil {
			return nil, err
		}
		sc := new(SlotChanges)
		if err := sc.decodeRLP(s, budget); err != nil {
			return nil, err
		}
		if budget != nil && len(sc.Changes) == 0 {
			return nil, fmt.Errorf("%w: empty slot changes", ErrInvalidBlockAccessList)
		}
		if budget != nil && len(out) > 0 && out[len(out)-1].Slot.Cmp(sc.Slot) >= 0 {
			return nil, fmt.Errorf("%w: slots must be strictly increasing", ErrInvalidBlockAccessList)
		}
		out = append(out, sc)
	}
	if err := s.ListEnd(); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeChangeList[T any, P interface {
	*T
	DecodeRLP(*rlp.Stream) error
	GetIndex() uint32
}](s *rlp.Stream, budget *blockAccessListDecodeBudget) ([]*T, error) {
	if _, err := s.List(); err != nil {
		return nil, err
	}
	var out []*T
	var lastIndex uint32
	for s.MoreDataInList() {
		change := new(T)
		if err := P(change).DecodeRLP(s); err != nil {
			return nil, err
		}
		if budget != nil {
			index := P(change).GetIndex()
			if uint64(index) > budget.maxIndex {
				return nil, fmt.Errorf("%w: block access index %d exceeds len(transactions)+1 = %d", ErrInvalidBlockAccessList, index, budget.maxIndex)
			}
			if len(out) > 0 && index <= lastIndex {
				return nil, fmt.Errorf("%w: indices must be strictly increasing", ErrInvalidBlockAccessList)
			}
			lastIndex = index
		}
		out = append(out, change)
	}
	if err := s.ListEnd(); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeStorageKeys(s *rlp.Stream, budget *blockAccessListDecodeBudget) ([]common.Hash, error) {
	if _, err := s.List(); err != nil {
		return nil, err
	}
	var hashes []common.Hash
	for s.MoreDataInList() {
		if err := budget.takeItem(); err != nil {
			return nil, err
		}
		h, err := decodeMinimalHash(s)
		if err != nil {
			return nil, err
		}
		if budget != nil && len(hashes) > 0 && hashes[len(hashes)-1].Cmp(h) >= 0 {
			return nil, fmt.Errorf("%w: storage reads must be strictly increasing", ErrInvalidBlockAccessList)
		}
		hashes = append(hashes, h)
	}
	if err := s.ListEnd(); err != nil {
		return nil, err
	}
	return hashes, nil
}

// hashToUint256 converts a common.Hash to a uint256.Int for minimal RLP encoding.
// EIP-7928 encodes slot keys, storage values, and balance values using standard
// RLP integer encoding (minimal big-endian, leading zeros stripped).
func hashToUint256(h common.Hash) uint256.Int {
	var v uint256.Int
	v.SetBytes(h[:])
	return v
}

// decodeMinimalHash reads a slot/key into a 32-byte hash; ReadUint256 enforces
// canonical (minimal) encoding, matching the encoder and preventing key malleability.
func decodeMinimalHash(s *rlp.Stream) (common.Hash, error) {
	var v uint256.Int
	if err := s.ReadUint256(&v); err != nil {
		return common.Hash{}, err
	}
	return common.Hash(v.Bytes32()), nil
}

func (bal BlockAccessList) Hash() common.Hash {
	if len(bal) == 0 {
		return empty.BlockAccessListHash
	}
	return RlpHash(bal)
}

func (bal BlockAccessList) Validate() error {
	if len(bal) == 0 {
		return nil
	}
	var prev common.Address
	var hasPrev bool
	for i := range bal {
		account := &bal[i]
		address := account.Address
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

// ValidateForHeader validates a block access list against its block gas limit.
func (bal BlockAccessList) ValidateForHeader(gasLimit uint64) error {
	if err := bal.ValidateMaxItems(gasLimit); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidBlockAccessList, err)
	}
	if err := bal.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidBlockAccessList, err)
	}
	return nil
}

func (bal BlockAccessList) validateBlockAccessIndices(txCount int) error {
	maxIndex := uint64(txCount) + 1
	for i := range bal {
		if err := bal[i].validateBlockAccessIndices(maxIndex); err != nil {
			return fmt.Errorf("%w: account %s: %w", ErrInvalidBlockAccessList, bal[i].Address.Hex(), err)
		}
	}
	return nil
}

func (ac *AccountChanges) validateBlockAccessIndices(maxIndex uint64) error {
	for i := range ac.StorageChanges {
		if err := validateMaxIndex(ac.StorageChanges[i].Changes, maxIndex); err != nil {
			return fmt.Errorf("storage_changes: %w", err)
		}
	}
	if err := validateMaxIndex(ac.BalanceChanges, maxIndex); err != nil {
		return fmt.Errorf("balance_changes: %w", err)
	}
	if err := validateMaxIndex(ac.NonceChanges, maxIndex); err != nil {
		return fmt.Errorf("nonce_changes: %w", err)
	}
	if err := validateMaxIndex(ac.CodeChanges, maxIndex); err != nil {
		return fmt.Errorf("code_changes: %w", err)
	}
	return nil
}

func validateMaxIndex[T indexedChange](changes []T, maxIndex uint64) error {
	for _, change := range changes {
		if index := uint64(change.GetIndex()); index > maxIndex {
			return fmt.Errorf("block access index %d exceeds len(transactions)+1 = %d", index, maxIndex)
		}
	}
	return nil
}

// ValidateMaxItems checks the EIP-7928 constraint: bal_items <= blockGasLimit / BalItemCost
// where bal_items = count(addresses) + count(storage keys across all accounts).
func (bal BlockAccessList) ValidateMaxItems(blockGasLimit uint64) error {
	maxItems := blockGasLimit / BalItemCost
	var items uint64
	for i := range bal {
		items++ // each address counts as 1 item
		items += uint64(len(bal[i].StorageChanges))
		items += uint64(len(bal[i].StorageReads))
	}
	if items > maxItems {
		return fmt.Errorf("block access list too large: %d items > %d max (gas limit %d / %d)", items, maxItems, blockGasLimit, BalItemCost)
	}
	return nil
}

func (ac *AccountChanges) validate() error {
	if ac == nil {
		return errors.New("nil account changes")
	}
	if err := validateSlotChanges(ac.StorageChanges); err != nil {
		return fmt.Errorf("storage_changes: %w", err)
	}
	if err := validateStorageReads(ac.StorageReads); err != nil {
		return fmt.Errorf("storage_reads: %w", err)
	}
	// EIP-7928: a slot must not appear in both storage_changes and storage_reads.
	if len(ac.StorageChanges) > 0 && len(ac.StorageReads) > 0 {
		changeSlots := make(map[common.Hash]struct{}, len(ac.StorageChanges))
		for _, sc := range ac.StorageChanges {
			changeSlots[sc.Slot.Value()] = struct{}{}
		}
		for _, key := range ac.StorageReads {
			if _, exists := changeSlots[key]; exists {
				return fmt.Errorf("storage key %s in both changes and reads", key.Hex())
			}
		}
	}
	if err := validateIndexedChanges(ac.BalanceChanges); err != nil {
		return fmt.Errorf("balance_changes: %w", err)
	}
	if err := validateIndexedChanges(ac.NonceChanges); err != nil {
		return fmt.Errorf("nonce_changes: %w", err)
	}
	if err := validateIndexedChanges(ac.CodeChanges); err != nil {
		return fmt.Errorf("code_changes: %w", err)
	}
	return nil
}

func validateStorageChangeEntries(changes []*StorageChange) error {
	// Each SlotChanges entry MUST contain at least one StorageChange.
	if len(changes) == 0 {
		return errors.New("empty slot changes")
	}
	return validateIndexedChanges(changes)
}

func validateStorageReads(reads []common.Hash) error {
	for i := 1; i < len(reads); i++ {
		if reads[i-1].Cmp(reads[i]) >= 0 {
			return fmt.Errorf("storage reads must be strictly increasing (index %d)", i)
		}
	}
	return nil
}

// validateIndexedChanges validates that indices are strictly increasing
func validateIndexedChanges[T indexedChange](changes []T) error {
	if len(changes) == 0 {
		return nil
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

func validateSlotChanges(slots []SlotChanges) error {
	if len(slots) == 0 {
		return nil
	}
	for i := range slots {
		if err := validateStorageChangeEntries(slots[i].Changes); err != nil {
			return fmt.Errorf("entry %d: %w", i, err)
		}
	}

	// Each storage key MUST appear at most once in storage_changes per account.
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
	for i := range bal {
		account := &bal[i]
		fmt.Fprintf(w, "\n[%d] addr=%s", i, account.Address.Hex())
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
				fmt.Fprint(w, read.Hex())
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
		bal[i] = AccountChanges{
			Address: gointerfaces.ConvertH160toAddress(acc.Address),
		}
		if acc.StorageChanges != nil {
			bal[i].StorageChanges = make([]SlotChanges, len(acc.StorageChanges))
			for j, sc := range acc.StorageChanges {
				bal[i].StorageChanges[j] = SlotChanges{
					Slot: accounts.InternKey(gointerfaces.ConvertH256ToHash(sc.Slot)),
				}
				if sc.Changes != nil {
					bal[i].StorageChanges[j].Changes = make([]*StorageChange, len(sc.Changes))
					for k, c := range sc.Changes {
						val := gointerfaces.ConvertH256ToUint256Int(c.Value)
						bal[i].StorageChanges[j].Changes[k] = &StorageChange{
							Index: uint32(c.Index),
							Value: *val,
						}
					}
				}
			}
		}
		if acc.StorageReads != nil {
			bal[i].StorageReads = make([]common.Hash, len(acc.StorageReads))
			for j, r := range acc.StorageReads {
				bal[i].StorageReads[j] = gointerfaces.ConvertH256ToHash(r)
			}
		}
		if acc.BalanceChanges != nil {
			bal[i].BalanceChanges = make([]*BalanceChange, len(acc.BalanceChanges))
			for j, bc := range acc.BalanceChanges {
				val := gointerfaces.ConvertH256ToUint256Int(bc.Value)
				bal[i].BalanceChanges[j] = &BalanceChange{
					Index: uint32(bc.Index),
					Value: *val,
				}
			}
		}
		if acc.NonceChanges != nil {
			bal[i].NonceChanges = make([]*NonceChange, len(acc.NonceChanges))
			for j, nc := range acc.NonceChanges {
				bal[i].NonceChanges[j] = &NonceChange{
					Index: uint32(nc.Index),
					Value: nc.Value,
				}
			}
		}
		if acc.CodeChanges != nil {
			bal[i].CodeChanges = make([]*CodeChange, len(acc.CodeChanges))
			for j, cc := range acc.CodeChanges {
				bal[i].CodeChanges[j] = &CodeChange{
					Index:    uint32(cc.Index),
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
	for ai := range bal {
		account := &bal[ai]
		balAccount := &typesproto.BlockAccessListAccount{
			Address: gointerfaces.ConvertAddressToH160(account.Address),
		}
		for si := range account.StorageChanges {
			storageChange := &account.StorageChanges[si]
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
			balAccount.StorageReads = append(balAccount.StorageReads, gointerfaces.ConvertHashToH256(read))
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
	for ai := range bal {
		account := &bal[ai]
		rpcAccount := &executionproto.BlockAccessListAccount{
			Address: gointerfaces.ConvertAddressToH160(account.Address),
		}
		for si := range account.StorageChanges {
			storageChange := &account.StorageChanges[si]
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
			rpcAccount.StorageReads = append(rpcAccount.StorageReads, gointerfaces.ConvertHashToH256(read))
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
			Address: gointerfaces.ConvertH160toAddress(account.Address),
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

				if change.Value == nil {
					return nil, fmt.Errorf("blockAccessList account %d storageChanges[%d].changes[%d] missing value", accountIdx, slotIdx, changeIdx)
				}
				val := gointerfaces.ConvertH256ToUint256Int(change.Value)
				slotChanges.Changes = append(slotChanges.Changes, &StorageChange{
					Index: uint32(change.Index),
					Value: *val,
				})
			}
			accountChanges.StorageChanges = append(accountChanges.StorageChanges, *slotChanges)
		}
		for readIdx, read := range account.StorageReads {
			if read == nil {
				return nil, fmt.Errorf("blockAccessList account %d storageReads[%d] is nil", accountIdx, readIdx)
			}
			accountChanges.StorageReads = append(accountChanges.StorageReads, gointerfaces.ConvertH256ToHash(read))
		}
		for balanceIdx, balanceChange := range account.BalanceChanges {
			if balanceChange == nil {
				return nil, fmt.Errorf("blockAccessList account %d balanceChanges[%d] is nil", accountIdx, balanceIdx)
			}

			if balanceChange.Value == nil {
				return nil, fmt.Errorf("blockAccessList account %d balanceChanges[%d] missing value", accountIdx, balanceIdx)
			}
			val := gointerfaces.ConvertH256ToUint256Int(balanceChange.Value)
			accountChanges.BalanceChanges = append(accountChanges.BalanceChanges, &BalanceChange{
				Index: uint32(balanceChange.Index),
				Value: *val,
			})
		}
		for nonceIdx, nonceChange := range account.NonceChanges {
			if nonceChange == nil {
				return nil, fmt.Errorf("blockAccessList account %d nonceChanges[%d] is nil", accountIdx, nonceIdx)
			}

			accountChanges.NonceChanges = append(accountChanges.NonceChanges, &NonceChange{
				Index: uint32(nonceChange.Index),
				Value: nonceChange.Value,
			})
		}
		for codeIdx, codeChange := range account.CodeChanges {
			if codeChange == nil {
				return nil, fmt.Errorf("blockAccessList account %d codeChanges[%d] is nil", accountIdx, codeIdx)
			}

			data := make([]byte, len(codeChange.Data))
			copy(data, codeChange.Data)
			accountChanges.CodeChanges = append(accountChanges.CodeChanges, &CodeChange{
				Index:    uint32(codeChange.Index),
				Bytecode: data,
			})
		}
		out = append(out, *accountChanges)
	}
	if err := out.Validate(); err != nil {
		return nil, fmt.Errorf("blockAccessList validate: %w", err)
	}
	return out, nil
}
