package state

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strconv"

	"github.com/heimdalr/dag"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type ReadSource int

func (s ReadSource) String() string {
	switch s {
	case MapRead:
		return "version-map"
	case StorageRead:
		return "storage"
	case WriteSetRead:
		return "tx-writes"
	case ReadSetRead:
		return "tx-reads"
	default:
		return "unknown"
	}
}

func (s ReadSource) VersionedString(version Version) string {
	switch s {
	case MapRead:
		return fmt.Sprintf("version-map:%d.%d", version.TxIndex, version.Incarnation)
	case StorageRead:
		return "storage"
	case WriteSetRead:
		return "tx-writes"
	case ReadSetRead:
		return "tx-reads"
	default:
		return "unknown"
	}
}

const (
	UnknownSource ReadSource = iota
	MapRead
	StorageRead
	WriteSetRead
	ReadSetRead
)

type ReadSet map[accounts.Address]map[AccountKey]*VersionedRead

func (a ReadSet) Merge(b ReadSet) ReadSet {
	if a == nil && b == nil {
		return nil
	}
	out := make(ReadSet)
	if a != nil {
		a.Scan(func(vr *VersionedRead) bool {
			out.Set(*vr)
			return true
		})
	}
	if b != nil {
		b.Scan(func(vr *VersionedRead) bool {
			out.Set(*vr)
			return true
		})
	}
	return out
}

func (rs ReadSet) Set(v VersionedRead) {
	reads, ok := rs[v.Address]

	if !ok {
		rs[v.Address] = map[AccountKey]*VersionedRead{
			{v.Path, v.Key}: &v,
		}
	} else {
		if read, ok := reads[AccountKey{v.Path, v.Key}]; ok {
			*read = v
		} else {
			reads[AccountKey{v.Path, v.Key}] = &v
		}
	}
}

func (s ReadSet) Scan(yield func(input *VersionedRead) bool) {
	for _, reads := range s {
		for _, v := range reads {
			if !yield(v) {
				return
			}
		}
	}
}

func (s ReadSet) Len() int {
	var l int
	for _, p := range s {
		l += len(p)
	}
	return l
}

func (s ReadSet) Delete(addr accounts.Address, key AccountKey) {
	if reads, ok := s[addr]; ok {
		delete(reads, key)
		if len(reads) == 0 {
			delete(s, addr)
		}
	}
}

type WriteSet map[accounts.Address]map[AccountKey]*VersionedWrite

func (s WriteSet) Set(v VersionedWrite) {
	writes, ok := s[v.Address]

	if !ok {
		s[v.Address] = map[AccountKey]*VersionedWrite{
			{v.Path, v.Key}: &v,
		}
	} else {
		if write, ok := writes[AccountKey{v.Path, v.Key}]; ok {
			*write = v
		} else {
			writes[AccountKey{v.Path, v.Key}] = &v
		}
	}
}

func (s WriteSet) Delete(addr accounts.Address, key AccountKey) {
	if writes, ok := s[addr]; ok {
		delete(writes, key)
		if len(writes) == 0 {
			delete(s, addr)
		}
	}
}

func (s WriteSet) Len() int {
	var l int
	for _, p := range s {
		l += len(p)
	}
	return l
}

func (s WriteSet) Scan(yield func(input *VersionedWrite) bool) {
	for _, writes := range s {
		for _, v := range writes {
			if !yield(v) {
				return
			}
		}
	}
}

type VersionedRead struct {
	Address accounts.Address
	Path    AccountPath
	Key     accounts.StorageKey
	Source  ReadSource
	Version Version
	Val     any
}

func (vr VersionedRead) String() string {
	return fmt.Sprintf("(%s) %x %s: %s", vr.Source.VersionedString(vr.Version), vr.Address, AccountKey{Path: vr.Path, Key: vr.Key}, valueString(vr.Path, vr.Val))
}

type VersionedWrite struct {
	Address accounts.Address
	Path    AccountPath
	Key     accounts.StorageKey
	Version Version
	Val     any
	Reason  tracing.BalanceChangeReason
}

func (vr VersionedWrite) String() string {
	return fmt.Sprintf("%x %s: %s (%d.%d)", vr.Address, AccountKey{Path: vr.Path, Key: vr.Key}, valueString(vr.Path, vr.Val), vr.Version.TxIndex, vr.Version.Incarnation)
}

func valueString(path AccountPath, value any) string {
	if value == nil {
		return "<nil>"
	}
	switch path {
	case AddressPath:
		return fmt.Sprintf("%+v", value)
	case BalancePath:
		num := value.(uint256.Int)
		return (&num).String()
	case StoragePath:
		num := value.(uint256.Int)
		return fmt.Sprintf("%x", &num)
	case NoncePath, IncarnationPath:
		return strconv.FormatUint(value.(uint64), 10)
	case CodePath:
		l := min(len(value.([]byte)), 40)
		return hex.EncodeToString(value.([]byte)[0:l])
	}

	return fmt.Sprint(value)
}

var ErrDependency = errors.New("found dependency")

type versionedStateReader struct {
	txIndex     int
	reads       ReadSet
	versionMap  *VersionMap
	stateReader StateReader
}

func NewVersionedStateReader(txIndex int, reads ReadSet, versionMap *VersionMap, stateReader StateReader) *versionedStateReader {
	return &versionedStateReader{txIndex, reads, versionMap, stateReader}
}

func (vr *versionedStateReader) SetTrace(trace bool, tracePrefix string) {
	vr.stateReader.SetTrace(trace, tracePrefix)
}

func (vr *versionedStateReader) Trace() bool {
	return vr.stateReader.Trace()
}

func (vr *versionedStateReader) TracePrefix() string {
	return vr.stateReader.TracePrefix()
}

func (vr *versionedStateReader) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	if r, ok := vr.reads[address][AccountKey{Path: AddressPath}]; ok && r.Val != nil {
		if account, ok := r.Val.(*accounts.Account); ok && account != nil {
			updated := vr.applyVersionedUpdates(address, *account)
			return &updated, nil
		}
	}

	// Check version map for AddressPath — handles accounts created by
	// prior transactions in the same block that aren't in the read set.
	if vr.versionMap != nil {
		if acc, ok := versionedUpdate[*accounts.Account](vr.versionMap, address, AddressPath, accounts.NilKey, vr.txIndex); ok && acc != nil {
			updated := vr.applyVersionedUpdates(address, *acc)
			return &updated, nil
		}
	}

	if vr.stateReader != nil {
		account, err := vr.stateReader.ReadAccountData(address)

		if err != nil {
			return nil, err
		}

		if account != nil {
			updated := vr.applyVersionedUpdates(address, *account)
			return &updated, nil
		}
	}

	return nil, nil
}

func versionedUpdate[T any](versionMap *VersionMap, addr accounts.Address, path AccountPath, key accounts.StorageKey, txIndex int) (T, bool) {
	if res := versionMap.Read(addr, path, key, txIndex); res.Status() == MVReadResultDone {
		return res.Value().(T), true
	}
	var v T
	return v, false
}

// applyVersionedUpdates applies updated from the version map to the account before returning it, this is necessary
// for the account obkect becuase the state reader/.writer api's treat the subfileds as a group and this
// may lead to updated from pervious transactions being missed where we only update a subset of the fiels as these won't
// be recored as reads and hence the varification process will miss them.  We don't want to creat a fail but
// we do  want to capture the updates
func (vr versionedStateReader) applyVersionedUpdates(address accounts.Address, account accounts.Account) accounts.Account {
	if update, ok := versionedUpdate[uint256.Int](vr.versionMap, address, BalancePath, accounts.NilKey, vr.txIndex); ok {
		account.Balance = update
	}
	if update, ok := versionedUpdate[uint64](vr.versionMap, address, NoncePath, accounts.NilKey, vr.txIndex); ok {
		account.Nonce = update
	}
	if update, ok := versionedUpdate[uint64](vr.versionMap, address, IncarnationPath, accounts.NilKey, vr.txIndex); ok {
		account.Incarnation = update
	}
	if update, ok := versionedUpdate[accounts.CodeHash](vr.versionMap, address, CodeHashPath, accounts.NilKey, vr.txIndex); ok {
		account.CodeHash = update
	}
	return account
}

func (vr versionedStateReader) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	if r, ok := vr.reads[address][AccountKey{Path: AddressPath}]; ok && r.Val != nil {
		if account, ok := r.Val.(*accounts.Account); ok {
			updated := vr.applyVersionedUpdates(address, *account)
			return &updated, nil
		}
	}

	if vr.stateReader != nil {
		account, err := vr.stateReader.ReadAccountDataForDebug(address)

		if err != nil {
			return nil, err
		}

		updated := vr.applyVersionedUpdates(address, *account)
		return &updated, nil
	}

	return nil, nil
}

func (vr versionedStateReader) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	if r, ok := vr.reads[address][AccountKey{Path: StoragePath, Key: key}]; ok && r.Val != nil {
		val := r.Val.(uint256.Int)
		return val, true, nil
	}

	// Check version map for storage written by prior transactions.
	if vr.versionMap != nil {
		if val, ok := versionedUpdate[uint256.Int](vr.versionMap, address, StoragePath, key, vr.txIndex); ok {
			return val, true, nil
		}
	}

	if vr.stateReader != nil {
		return vr.stateReader.ReadAccountStorage(address, key)
	}

	return uint256.Int{}, false, nil
}

func (vr versionedStateReader) HasStorage(address accounts.Address) (bool, error) {
	if r, ok := vr.reads[address]; ok {
		for k := range r {
			if k.Path == StoragePath {
				return true, nil
			}
		}
	}

	if vr.stateReader != nil {
		return vr.stateReader.HasStorage(address)
	}

	return false, nil
}

func (vr versionedStateReader) ReadAccountCode(address accounts.Address) ([]byte, error) {
	if r, ok := vr.reads[address][AccountKey{Path: CodePath}]; ok && r.Val != nil {
		if code, ok := r.Val.([]byte); ok {
			return code, nil
		}
	}

	// Check version map for CodePath entries written by prior transactions
	// (e.g. EIP-7702 delegation set by an earlier tx in the same block).
	if vr.versionMap != nil {
		if code, ok := versionedUpdate[[]byte](vr.versionMap, address, CodePath, accounts.NilKey, vr.txIndex); ok {
			return code, nil
		}
	}

	if vr.stateReader != nil {
		return vr.stateReader.ReadAccountCode(address)
	}

	return nil, nil
}

func (vr versionedStateReader) ReadAccountCodeSize(address accounts.Address) (int, error) {
	if r, ok := vr.reads[address][AccountKey{Path: CodePath}]; ok && r.Val != nil {
		if code, ok := r.Val.([]byte); ok {
			return len(code), nil
		}
	}

	if vr.versionMap != nil {
		if code, ok := versionedUpdate[[]byte](vr.versionMap, address, CodePath, accounts.NilKey, vr.txIndex); ok {
			return len(code), nil
		}
	}

	if vr.stateReader != nil {
		return vr.stateReader.ReadAccountCodeSize(address)
	}

	return 0, nil
}

func (vr versionedStateReader) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
	if r, ok := vr.reads[address][AccountKey{Path: AddressPath}]; ok && r.Val != nil {
		return r.Val.(*accounts.Account).Incarnation, nil
	}

	if vr.stateReader != nil {
		return vr.stateReader.ReadAccountIncarnation(address)
	}

	return 0, nil
}

type VersionedWrites []*VersionedWrite

func (prev VersionedWrites) Merge(next VersionedWrites) VersionedWrites {
	if len(prev) == 0 {
		return next
	}
	if len(next) == 0 {
		return prev
	}
	merged := WriteSet{}
	for _, v := range prev {
		merged.Set(*v)
	}
	for _, v := range next {
		merged.Set(*v)
	}
	out := make(VersionedWrites, 0, merged.Len())
	merged.Scan(func(v *VersionedWrite) bool {
		out = append(out, v)
		return true
	})
	return out
}

// hasNewWrite: returns true if the current set has a new write compared to the input
func (writes VersionedWrites) HasNewWrite(cmpSet []*VersionedWrite) bool {
	if len(writes) == 0 {
		return false
	} else if len(cmpSet) == 0 || len(writes) > len(cmpSet) {
		return true
	}

	cmpMap := map[accounts.Address]map[AccountKey]struct{}{}

	for _, vw := range cmpSet {
		keys, ok := cmpMap[vw.Address]
		if !ok {
			keys = map[AccountKey]struct{}{}
			cmpMap[vw.Address] = keys
		}
		keys[AccountKey{vw.Path, vw.Key}] = struct{}{}
	}

	for _, v := range writes {
		if _, ok := cmpMap[v.Address][AccountKey{v.Path, v.Key}]; !ok {
			return true
		}
	}

	return false
}

// StripBalanceWrite removes the BalancePath write for addr from the write set
// and computes the TX's net balance delta by comparing the stale write with
// the stale read from readSet. This is used in finalize to prevent stale
// speculative coinbase/burnt-contract balance writes from being applied via
// ApplyVersionedWrites. The delta is returned so it can be applied separately
// on top of the correct base balance from the VersionedStateReader.
//
// Returns:
//   - stripped: the write set with the balance write removed
//   - delta: the absolute difference between stale write and stale read
//   - increase: true if the TX increased the balance, false if decreased
//   - found: true if both a stale read and write were found and a non-zero delta computed
func (writes VersionedWrites) StripBalanceWrite(addr accounts.Address, readSet ReadSet) (stripped VersionedWrites, delta uint256.Int, increase bool, found bool) {
	stripped = writes
	if addr.IsNil() {
		return
	}

	reads, ok := readSet[addr]
	if !ok {
		// TX didn't read this address — no delta to compute.
		// Still strip the write to prevent stale cache pollution.
		for i, w := range stripped {
			if w.Address == addr && w.Path == BalancePath {
				stripped = append(stripped[:i], stripped[i+1:]...)
				return
			}
		}
		return
	}

	balKey := AccountKey{Path: BalancePath, Key: accounts.NilKey}
	balRead, ok := reads[balKey]
	if !ok {
		return
	}
	staleRead, ok := balRead.Val.(uint256.Int)
	if !ok {
		return
	}

	for i, w := range stripped {
		if w.Address == addr && w.Path == BalancePath {
			staleWrite, ok := w.Val.(uint256.Int)
			if !ok {
				break
			}
			// Remove the stale absolute write
			stripped = append(stripped[:i], stripped[i+1:]...)
			// Compute the TX's net effect on this balance
			if staleWrite.Gt(&staleRead) {
				delta.Sub(&staleWrite, &staleRead)
				increase = true
				found = true
			} else if staleRead.Gt(&staleWrite) {
				delta.Sub(&staleRead, &staleWrite)
				increase = false
				found = true
			}
			return
		}
	}

	return
}

func versionedRead[T any](s *IntraBlockState, addr accounts.Address, path AccountPath, key accounts.StorageKey, commited bool, defaultV T, copyV func(T) T, readStorage func(sdb *stateObject) (T, error)) (T, ReadSource, Version, error) {
	if s.versionMap == nil {
		so, err := s.getStateObject(addr, true)

		if err != nil || readStorage == nil {
			return defaultV, StorageRead, UnknownVersion, err
		}
		val, err := readStorage(so)
		return val, StorageRead, UnknownVersion, err
	}

	var destrcutedVersion Version
	if so, ok := s.stateObjects[addr]; ok && so.deleted {
		return defaultV, StorageRead, UnknownVersion, nil
	} else if res := s.versionMap.Read(addr, SelfDestructPath, accounts.NilKey, s.txIndex); res.Status() == MVReadResultDone && res.value.(bool) {
		if path != CodePath {
			// A prior tx self-destructed this account — all state reads must
			// return the zero value of the type, not the caller-supplied default.
			// refreshVersionedAccount passes the account's pre-destruction field
			// values as defaultV, so using defaultV here would return stale data.
			var zero T
			sdVersion := Version{TxIndex: res.DepIdx(), Incarnation: res.Incarnation()}
			if commited {
				return zero, MapRead, sdVersion, nil
			}
			if vw, ok := s.versionedWrite(addr, SelfDestructPath, key); !ok || vw.Val.(bool) {
				// Record the SelfDestructPath dependency so that
				// ValidateVersion can verify the destruct is still
				// valid.  Without this entry the readSet would be
				// empty for the affected address, and validation
				// would have nothing to cross-check — allowing
				// skipCheck to commit stale results.
				if s.versionedReads == nil {
					s.versionedReads = ReadSet{}
				}
				s.versionedReads.Set(VersionedRead{
					Address: addr,
					Path:    SelfDestructPath,
					Key:     accounts.NilKey,
					Source:  MapRead,
					Version: sdVersion,
					Val:     true,
				})
				return zero, MapRead, sdVersion, nil
			}
			destrcutedVersion = Version{
				TxIndex: res.DepIdx(),
			}
		}
	}

	res := s.versionMap.Read(addr, path, key, s.txIndex)

	var v T
	var vr = VersionedRead{
		Address: addr,
		Path:    path,
		Key:     key,
		Version: Version{
			TxIndex:     res.DepIdx(),
			Incarnation: res.Incarnation(),
		},
	}

	if !commited {
		if vw, ok := s.versionedWrite(addr, path, key); ok {
			if res.Status() == MVReadResultDone {
				if pr, ok := s.versionedReads[addr][AccountKey{Path: path, Key: key}]; ok {
					if vr.Version.TxIndex > destrcutedVersion.TxIndex && vr.Version != pr.Version {
						if vr.Version.TxIndex > s.dep {
							s.dep = vr.Version.TxIndex
						}

						if dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(addr.Handle())) {
							fmt.Printf("%d (%d.%d) WR DEP (%d.%d)!=(%d.%d) %x %s: %s\n", s.blockNum, s.txIndex, s.version, pr.Version.TxIndex, pr.Version.Incarnation, vr.Version.TxIndex, vr.Version.Incarnation, addr, AccountKey{path, key}, valueString(path, pr.Val))
						}

						if s.versionedReads == nil {
							s.versionedReads = ReadSet{}
						}
						s.versionedReads.Set(vr)
						panic(ErrDependency)
					}
				}
			}

			if dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(addr.Handle())) {
				fmt.Printf("%d (%d.%d) RD (%s) %x %s: %s\n", s.blockNum, s.txIndex, s.version, WriteSetRead, addr, AccountKey{path, key}, valueString(path, vw.Val))
			}

			val := vw.Val.(T)
			return val, WriteSetRead, Version{TxIndex: s.txIndex, Incarnation: s.version}, nil
		}
	}

	switch res.Status() {
	case MVReadResultDone:
		vr.Source = MapRead

		if pr, ok := s.versionedReads[addr][AccountKey{Path: path, Key: key}]; ok {
			if pr.Version == vr.Version {
				if dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(addr.Handle())) {
					fmt.Printf("%d (%d.%d) RD (%s:%s) %x %s: %s\n", s.blockNum, s.txIndex, s.version, MapRead, res.DepString(), addr, AccountKey{path, key}, valueString(path, pr.Val))
				}

				return pr.Val.(T), vr.Source, vr.Version, nil
			}

			if vr.Version.TxIndex > s.dep {
				s.dep = vr.Version.TxIndex
			}

			if dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(addr.Handle())) {
				fmt.Printf("%d (%d.%d) RD DEP (%d.%d)!=(%d.%d) %x %s\n", s.blockNum, s.txIndex, s.version, pr.Version.TxIndex, pr.Version.Incarnation, vr.Version.TxIndex, vr.Version.Incarnation, addr, AccountKey{path, key})
			}

			if s.versionedReads == nil {
				s.versionedReads = ReadSet{}
			}

			s.versionedReads.Set(vr)

			panic(ErrDependency)
		}

		var ok bool
		if v, ok = res.Value().(T); !ok {
			return defaultV, UnknownSource, vr.Version, fmt.Errorf("unexpected type: got: %T, expected %v", res.Value(), reflect.TypeFor[T]())
		}

		if path == CodePath {
			sdres := s.versionMap.Read(addr, SelfDestructPath, accounts.NilKey, s.txIndex)
			if sdres.Status() == MVReadResultDone && sdres.Value().(bool) && sdres.DepIdx() >= res.DepIdx() {
				return defaultV, MapRead, Version{TxIndex: res.DepIdx(), Incarnation: res.Incarnation()}, nil
			}
		}

		if dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(addr.Handle())) {
			fmt.Printf("%d (%d.%d) RD (%s:%s) %x %s: %s\n", s.blockNum, s.txIndex, s.version, MapRead, res.DepString(), addr, AccountKey{path, key}, valueString(path, v))
		}

		if copyV == nil {
			return v, MapRead, vr.Version, nil
		}

		vr.Val = copyV(v)

	case MVReadResultDependency:
		if dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(addr.Handle())) {
			fmt.Printf("%d (%d.%d) MP DEP (%d.%d) %x %s\n", s.blockNum, s.txIndex, s.version, res.DepIdx(), res.Incarnation(), addr, AccountKey{path, key})
		}

		if res.DepIdx() > s.dep {
			s.dep = res.DepIdx()
		}
		vr.Source = MapRead
		if s.versionedReads == nil {
			s.versionedReads = ReadSet{}
		}
		s.versionedReads.Set(vr)
		panic(ErrDependency)

	case MVReadResultNone:
		if versionedReads := s.versionedReads; !commited && versionedReads != nil {
			if pr, ok := versionedReads[addr][AccountKey{Path: path, Key: key}]; ok {
				if pr.Version == vr.Version {
					if dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(addr.Handle())) {
						fmt.Printf("%d (%d.%d) RD (%s) %x %s: %s\n", s.blockNum, s.txIndex, s.version, ReadSetRead, addr, AccountKey{path, key}, valueString(path, pr.Val))
					}

					return pr.Val.(T), ReadSetRead, pr.Version, nil
				}

				if pr.Source == MapRead {
					if path == BalancePath || path == NoncePath || path == IncarnationPath || path == CodeHashPath {
						if _, source, version, _ := versionedRead(s, addr, AddressPath, accounts.NilKey, false, nil,
							func(v *accounts.Account) *accounts.Account { return v }, nil); source == pr.Source && version == pr.Version {
							return pr.Val.(T), ReadSetRead, pr.Version, nil
						}
					}

					// a previous dependency has been removed from the map
					if dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(addr.Handle())) {
						fmt.Printf("%d (%d.%d) RM DEP (%d.%d)!=(%d.%d) %x %s\n", s.blockNum, s.txIndex, s.version, pr.Version.TxIndex, pr.Version.Incarnation, vr.Version.TxIndex, vr.Version.Incarnation, addr, AccountKey{path, key})
					}

					if pr.Version.TxIndex > s.dep {
						s.dep = pr.Version.TxIndex
					}

					panic(ErrDependency)
				}
			}
		}

		if readStorage == nil {
			// Record AddressPath reads so that ValidateVersion can detect
			// when a prior transaction later creates this account.
			// For example, if Tx1 looks up an account that Tx0 has not yet
			// created, we must record the "nothing here" read so that once
			// Tx0 creates the account, validation invalidates Tx1.
			if !commited && path == AddressPath {
				vr.Source = StorageRead
				vr.Val = defaultV
				if s.versionedReads == nil {
					s.versionedReads = ReadSet{}
				}
				s.versionedReads.Set(vr)
			}
			return defaultV, UnknownSource, UnknownVersion, nil
		}

		var so *stateObject
		var err error

		// For StoragePath, detect contract creation/destruction by a prior tx.
		// IncarnationPath is written ONLY by CreateAccount (contract creation) and
		// Selfdestruct — both operations that clear all storage.  When no prior tx
		// wrote this specific storage slot (MVReadResultNone), but a prior tx DID
		// write IncarnationPath, the account was created or destroyed in this block
		// and all unwritten storage slots must be zero.
		//
		// Without this check, the read falls through to StorageDomain which may
		// contain stale data from before a prior block's SELFDESTRUCT (because
		// Writer.DeleteAccount clears AccountsDomain but NOT StorageDomain).
		if path == StoragePath {
			incRes := s.versionMap.Read(addr, IncarnationPath, accounts.NilKey, s.txIndex)
			if incRes.Status() == MVReadResultDone {
				var zero T
				vr.Source = StorageRead
				vr.Val = zero

				if dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(addr.Handle())) {
					fmt.Printf("%d (%d.%d) RD (%s) %x %s: zero (IncarnationPath written by tx %d)\n",
						s.blockNum, s.txIndex, s.version, StorageRead, addr, AccountKey{path, key}, incRes.DepIdx())
				}

				if s.versionedReads == nil {
					s.versionedReads = ReadSet{}
				}
				s.versionedReads.Set(vr)
				// Record dependency on IncarnationPath so that ValidateVersion
				// detects if the creation/destruction is reverted by a re-execution.
				incVersion := Version{TxIndex: incRes.DepIdx(), Incarnation: incRes.Incarnation()}
				s.versionedReads.Set(VersionedRead{
					Address: addr,
					Path:    IncarnationPath,
					Key:     accounts.NilKey,
					Source:  MapRead,
					Version: incVersion,
					Val:     incRes.Value(),
				})
				return zero, StorageRead, UnknownVersion, nil
			}
		}

		if path == BalancePath || path == NoncePath || path == IncarnationPath || path == CodeHashPath {
			readAccount, source, version, err := versionedRead(s, addr, AddressPath, accounts.NilKey, false, nil,
				func(v *accounts.Account) *accounts.Account { return v }, nil)

			if err != nil {
				return defaultV, source, UnknownVersion, err
			}

			if readAccount != nil {
				vr.Source = source
				vr.Version = version
				so = newObject(s, addr, readAccount, readAccount)
			}
		}

		if so == nil {
			vr.Source = StorageRead
			so, err = s.getStateObject(addr, true)
			if err != nil {
				return defaultV, StorageRead, UnknownVersion, err
			}
		}

		if v, err = readStorage(so); err != nil {
			return defaultV, StorageRead, UnknownVersion, err
		}

		if dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(addr.Handle())) {
			fmt.Printf("%d (%d.%d) RD (%s:%d.%d) %x %s: %s\n", s.blockNum, s.txIndex, s.version, vr.Source, vr.Version.TxIndex, vr.Version.Incarnation, addr, AccountKey{path, key}, valueString(path, v))
		}

		vr.Val = copyV(v)

	default:
		return defaultV, UnknownSource, UnknownVersion, nil
	}

	if s.versionedReads == nil {
		s.versionedReads = ReadSet{}
	}
	s.versionedReads.Set(vr)

	return v, vr.Source, vr.Version, nil
}

// note that TxIndex starts at -1 (the begin system tx)
type VersionedIO struct {
	inputs   []versionedReadSet
	outputs  []VersionedWrites // write sets that should be checked during validation
	accessed []AccessSet
}

func NewVersionedIO(numTx int) *VersionedIO {
	return &VersionedIO{
		inputs:   make([]versionedReadSet, numTx+1),
		outputs:  make([]VersionedWrites, numTx+1),
		accessed: make([]AccessSet, numTx+1),
	}
}

func (io *VersionedIO) Len() int {
	if io == nil {
		return 0
	}
	return max(len(io.inputs), max(len(io.outputs), len(io.accessed)))
}

func (io *VersionedIO) Inputs() []versionedReadSet {
	return io.inputs
}

func (io *VersionedIO) Outputs() []VersionedWrites {
	return io.outputs
}

func (io *VersionedIO) ReadSet(txnIdx int) ReadSet {
	if len(io.inputs) <= txnIdx+1 {
		return nil
	}
	return io.inputs[txnIdx+1].readSet
}

func (io *VersionedIO) ReadSetIncarnation(txnIdx int) int {
	if len(io.inputs) <= txnIdx+1 {
		return -1
	}

	if io.inputs[txnIdx+1].readSet != nil {
		return io.inputs[txnIdx+1].incarnation
	}

	return 0
}

func (io *VersionedIO) WriteSet(txnIdx int) VersionedWrites {
	if len(io.outputs) <= txnIdx+1 {
		return nil
	}
	return io.outputs[txnIdx+1]
}

func (io *VersionedIO) WriteCount() (count int64) {
	for _, output := range io.outputs {
		count += int64(len(output))
	}

	return count
}

func (io *VersionedIO) ReadCount() (count int64) {
	for _, input := range io.inputs {
		if input.readSet != nil {
			count += int64(input.readSet.Len())
		}
	}

	return count
}

func (io *VersionedIO) HasReads(txnIdx int) bool {
	if len(io.inputs) <= txnIdx+1 {
		return false
	}
	return len(io.inputs[txnIdx+1].readSet) > 0
}

func (io *VersionedIO) RecordReads(txVersion Version, input ReadSet) {
	if len(io.inputs) <= txVersion.TxIndex+1 {
		io.inputs = append(io.inputs, make([]versionedReadSet, txVersion.TxIndex+2-len(io.inputs))...)
	}
	io.inputs[txVersion.TxIndex+1] = versionedReadSet{txVersion.Incarnation, input}
}

func (io *VersionedIO) RecordWrites(txVersion Version, output VersionedWrites) {
	txId := txVersion.TxIndex

	if len(io.outputs) <= txId+1 {
		io.outputs = append(io.outputs, make([]VersionedWrites, txId+2-len(io.outputs))...)
	}
	io.outputs[txId+1] = output
}

func (io *VersionedIO) RecordAccesses(txVersion Version, addresses AccessSet) {
	if len(addresses) == 0 {
		return
	}
	if len(io.accessed) <= txVersion.TxIndex+1 {
		io.accessed = append(io.accessed, make([]AccessSet, txVersion.TxIndex+2-len(io.accessed))...)
	}
	dest := make(AccessSet, len(addresses))
	for addr, opt := range addresses {
		dest[addr] = opt
	}
	io.accessed[txVersion.TxIndex+1] = dest
}

func (io *VersionedIO) AccessedAddresses(txIndex int) AccessSet {
	if len(io.accessed) <= txIndex+1 {
		return nil
	}
	return io.accessed[txIndex+1]
}

func (io *VersionedIO) Merge(other *VersionedIO) *VersionedIO {
	mergedLen := max(io.Len(), other.Len())
	merged := NewVersionedIO(mergedLen - 1)

	for i := 0; i < mergedLen; i++ {
		if i < len(io.inputs) {
			if i < len(other.inputs) {
				merged.inputs[i] = io.inputs[i].Merge(other.inputs[i])
			} else {
				merged.inputs[i] = io.inputs[i].Merge(versionedReadSet{})
			}
		} else if i < len(other.inputs) {
			merged.inputs[i] = other.inputs[i].Merge(versionedReadSet{})
		}
		if i < len(io.outputs) {
			if i < len(other.outputs) {
				merged.outputs[i] = io.outputs[i].Merge(other.outputs[i])
			} else {
				merged.outputs[i] = io.outputs[i].Merge(nil)
			}
		} else if i < len(other.outputs) {
			merged.outputs[i] = other.outputs[i].Merge(nil)
		}
		if i < len(io.accessed) {
			if i < len(other.accessed) {
				merged.accessed[i] = io.accessed[i].Merge(other.accessed[i])
			} else {
				merged.accessed[i] = io.accessed[i].Merge(nil)
			}
		} else if i < len(other.accessed) {
			merged.accessed[i] = other.accessed[i].Merge(nil)
		}
	}
	return merged
}

func (io *VersionedIO) AsBlockAccessList() types.BlockAccessList {
	if io == nil {
		return nil
	}

	ac := make(map[accounts.Address]*accountState)
	maxTxIndex := io.Len() - 1

	for txIndex := -1; txIndex <= maxTxIndex; txIndex++ {
		io.ReadSet(txIndex).Scan(func(vr *VersionedRead) bool {
			if vr.Address.IsNil() {
				return true
			}
			// Skip validation-only reads for non-existent accounts.
			// These are recorded by versionedRead when the version map
			// has no entry (MVReadResultNone) so that conflict detection
			// works across transactions, but they should not appear in
			// the block access list.
			if vr.Path == AddressPath {
				if val, ok := vr.Val.(*accounts.Account); ok && val == nil {
					return true
				}
			}
			account := ensureAccountState(ac, vr.Address)
			account.updateRead(vr)
			return true
		})

		for _, vw := range io.WriteSet(txIndex) {
			if vw.Address.IsNil() || params.IsSystemAddress(vw.Address) {
				continue
			}
			account := ensureAccountState(ac, vw.Address)
			accessIndex := vw.Version.blockAccessIndex()
			account.updateWrite(vw, accessIndex)
		}

		for addr := range io.AccessedAddresses(txIndex) {
			if addr.IsNil() || params.IsSystemAddress(addr) {
				continue
			}

			ensureAccountState(ac, addr)
		}
	}

	bal := make([]*types.AccountChanges, 0, len(ac))
	for _, account := range ac {
		account.finalize()
		account.changes.Normalize()
		// The system address is touched during system calls (EIP-4788 beacon root)
		// because it is msg.sender. Exclude it when it has no actual state changes,
		// but keep it when a user tx sends real ETH to it (e.g. SELFDESTRUCT to
		// the system address or a plain value transfer).
		if isSystemBALAddress(account.changes.Address) && !hasAccountChanges(account.changes) {
			continue
		}
		bal = append(bal, account.changes)
	}

	sort.Slice(bal, func(i, j int) bool {
		return bal[i].Address.Cmp(bal[j].Address) < 0
	})

	return bal
}

type accountState struct {
	changes        *types.AccountChanges
	balance        *fieldTracker[uint256.Int]
	nonce          *fieldTracker[uint64]
	code           *fieldTracker[[]byte]
	balanceValue   *uint256.Int // tracks latest seen balance
	selfDestructed bool
}

// check pre- and post-values, add to BAL if different
func (a *accountState) finalize() {
	applyToBalance(a.balance, a.changes)
	applyToNonce(a.nonce, a.changes)
	applyToCode(a.code, a.changes)
}

type fieldTracker[T any] struct {
	changes changeTracker[T]
}

func (ft *fieldTracker[T]) recordWrite(idx uint16, value T, copyFn func(T) T, equal func(T, T) bool) {
	ft.changes.recordWrite(idx, value, copyFn, equal)
}

func newBalanceTracker() *fieldTracker[uint256.Int] {
	return &fieldTracker[uint256.Int]{}
}

func applyToBalance(bt *fieldTracker[uint256.Int], ac *types.AccountChanges) {
	bt.changes.apply(func(idx uint16, value uint256.Int) {
		ac.BalanceChanges = append(ac.BalanceChanges, &types.BalanceChange{
			Index: idx,
			Value: value,
		})
	})
}

func newNonceTracker() *fieldTracker[uint64] {
	return &fieldTracker[uint64]{}
}

func applyToNonce(nt *fieldTracker[uint64], ac *types.AccountChanges) {
	nt.changes.apply(func(idx uint16, value uint64) {
		ac.NonceChanges = append(ac.NonceChanges, &types.NonceChange{
			Index: idx,
			Value: value,
		})
	})
}

func newCodeTracker() *fieldTracker[[]byte] {
	return &fieldTracker[[]byte]{}
}

func applyToCode(ct *fieldTracker[[]byte], ac *types.AccountChanges) {
	ct.changes.apply(func(idx uint16, value []byte) {
		ac.CodeChanges = append(ac.CodeChanges, &types.CodeChange{
			Index:    idx,
			Bytecode: bytes.Clone(value),
		})
	})
}

type changeTracker[T any] struct {
	entries map[uint16]T
	equal   func(T, T) bool
}

func (ct *changeTracker[T]) recordWrite(idx uint16, value T, copyFn func(T) T, equal func(T, T) bool) {
	if ct.entries == nil {
		ct.entries = make(map[uint16]T)
		ct.equal = equal
	}
	ct.entries[idx] = copyFn(value)
}

func (ct *changeTracker[T]) apply(applyFn func(uint16, T)) {
	if len(ct.entries) == 0 {
		return
	}

	indices := make([]uint16, 0, len(ct.entries))
	for idx := range ct.entries {
		indices = append(indices, idx)
	}
	slices.Sort(indices)

	for _, idx := range indices {
		applyFn(idx, ct.entries[idx])
	}
}

func (a *accountState) setBalanceValue(v uint256.Int) {
	if a.balanceValue == nil {
		a.balanceValue = &uint256.Int{}
	}
	*a.balanceValue = v
}

func ensureAccountState(accounts map[accounts.Address]*accountState, addr accounts.Address) *accountState {
	if account, ok := accounts[addr]; ok {
		return account
	}
	account := &accountState{
		changes: &types.AccountChanges{Address: addr},
		balance: newBalanceTracker(),
		nonce:   newNonceTracker(),
		code:    newCodeTracker(),
	}
	accounts[addr] = account
	return account
}

func (account *accountState) updateWrite(vw *VersionedWrite, accessIndex uint16) {
	switch vw.Path {
	case StoragePath:
		addStorageUpdate(account.changes, vw, accessIndex)
	case BalancePath:
		val, ok := vw.Val.(uint256.Int)
		if !ok {
			return
		}
		// Skip non-zero balance writes for selfdestructed accounts.
		// Post-selfdestruct ETH (e.g. priority fee applied during finalize) must
		// not appear in the BAL per EIP-7928 — only the zero-balance write from
		// the selfdestruct itself belongs there.
		if account.selfDestructed && !val.IsZero() {
			return
		}
		// If we haven't seen a balance and the first write is zero, treat it as a touch only.
		if account.balanceValue == nil && val.IsZero() {
			account.setBalanceValue(val)
			return
		}
		// Skip no-op writes.
		if account.balanceValue != nil && val.Eq(account.balanceValue) {
			account.setBalanceValue(val)
			return
		}
		account.setBalanceValue(val)
		account.balance.recordWrite(accessIndex, val, func(v uint256.Int) uint256.Int { return v }, func(a, b uint256.Int) bool {
			return a.Eq(&b)
		})
	case NoncePath:
		if val, ok := vw.Val.(uint64); ok {
			account.nonce.recordWrite(accessIndex, val, func(v uint64) uint64 { return v }, func(a, b uint64) bool {
				return a == b
			})
		}
	case CodePath:
		if val, ok := vw.Val.([]byte); ok {
			account.code.recordWrite(accessIndex, val, bytes.Clone, bytes.Equal)
		}
	case SelfDestructPath:
		if val, ok := vw.Val.(bool); ok && val {
			account.selfDestructed = true
		}
	default:
	}
}

func (account *accountState) updateRead(vr *VersionedRead) {
	if vr != nil {
		switch vr.Path {
		case StoragePath:
			if hasStorageWrite(account.changes, vr.Key) {
				return
			}
			account.changes.StorageReads = append(account.changes.StorageReads, vr.Key)
		case BalancePath:
			if val, ok := vr.Val.(uint256.Int); ok {
				account.setBalanceValue(val)
			}
		default:
			// Only track storage reads for BAL. Balance/nonce/code changes are tracked via writes, others are ignored
		}
	}
}

func addStorageUpdate(ac *types.AccountChanges, vw *VersionedWrite, txIndex uint16) {
	// If we already recorded a read for this slot, drop it because a write takes precedence.
	removeStorageRead(ac, vw.Key)

	if ac.StorageChanges == nil {
		ac.StorageChanges = []*types.SlotChanges{{
			Slot:    vw.Key,
			Changes: []*types.StorageChange{{Index: txIndex, Value: vw.Val.(uint256.Int)}},
		}}
		return
	}

	for _, slotChange := range ac.StorageChanges {
		if slotChange.Slot == vw.Key {
			slotChange.Changes = append(slotChange.Changes, &types.StorageChange{Index: txIndex, Value: vw.Val.(uint256.Int)})
			return
		}
	}

	ac.StorageChanges = append(ac.StorageChanges, &types.SlotChanges{
		Slot:    vw.Key,
		Changes: []*types.StorageChange{{Index: txIndex, Value: vw.Val.(uint256.Int)}},
	})
}

func hasStorageWrite(ac *types.AccountChanges, slot accounts.StorageKey) bool {
	for _, sc := range ac.StorageChanges {
		if sc != nil && sc.Slot == slot {
			return true
		}
	}
	return false
}

func removeStorageRead(ac *types.AccountChanges, slot accounts.StorageKey) {
	if len(ac.StorageReads) == 0 {
		return
	}
	out := ac.StorageReads[:0]
	for _, s := range ac.StorageReads {
		if s != slot {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		ac.StorageReads = nil
	} else {
		ac.StorageReads = out
	}
}

func isSystemBALAddress(addr accounts.Address) bool {
	return params.IsSystemAddress(addr)
}

func hasAccountChanges(ac *types.AccountChanges) bool {
	return len(ac.BalanceChanges) > 0 || len(ac.NonceChanges) > 0 ||
		len(ac.CodeChanges) > 0 || len(ac.StorageChanges) > 0
}

type versionedReadSet struct {
	incarnation int
	readSet     ReadSet
}

func (s versionedReadSet) Scan(yield func(input *VersionedRead) bool) {
	if s.readSet != nil {
		s.readSet.Scan(yield)
	}
}

func (s versionedReadSet) Merge(o versionedReadSet) versionedReadSet {
	if s.incarnation > o.incarnation {
		return versionedReadSet{
			incarnation: s.incarnation,
			readSet:     o.readSet.Merge(s.readSet),
		}
	}
	return versionedReadSet{
		incarnation: o.incarnation,
		readSet:     s.readSet.Merge(o.readSet),
	}
}

type DAG struct {
	*dag.DAG
}

type TxDep struct {
	Index         int
	Reads         ReadSet
	FullWriteList []VersionedWrites
}

func HasReadDep(txFrom VersionedWrites, txTo ReadSet) bool {
	for _, rd := range txFrom {
		if _, ok := txTo[rd.Address][AccountKey{Path: rd.Path, Key: rd.Key}]; ok {
			return true
		}
	}
	return false
}

func BuildDAG(deps *VersionedIO, logger log.Logger) (d DAG) {
	d = DAG{dag.NewDAG()}
	ids := make(map[int]string)

	for i := len(deps.inputs) - 1; i > 0; i-- {
		txTo := deps.inputs[i]

		var txToId string

		if _, ok := ids[i]; ok {
			txToId = ids[i]
		} else {
			txToId, _ = d.AddVertex(i)
			ids[i] = txToId
		}

		for j := i - 1; j >= 0; j-- {
			txFrom := deps.outputs[j]

			if HasReadDep(txFrom, txTo.readSet) {
				var txFromId string
				if _, ok := ids[j]; ok {
					txFromId = ids[j]
				} else {
					txFromId, _ = d.AddVertex(j)
					ids[j] = txFromId
				}

				err := d.AddEdge(txFromId, txToId)
				if err != nil {
					logger.Warn("Failed to add edge", "from", txFromId, "to", txToId, "err", err)
				}
			}
		}
	}

	return
}

func depsHelper(dependencies map[int]map[int]bool, txFrom VersionedWrites, txTo ReadSet, i int, j int) map[int]map[int]bool {
	if HasReadDep(txFrom, txTo) {
		dependencies[i][j] = true

		for k := range dependencies[i] {
			_, foundDep := dependencies[j][k]

			if foundDep {
				delete(dependencies[i], k)
			}
		}
	}

	return dependencies
}

func UpdateDeps(deps map[int]map[int]bool, t TxDep) map[int]map[int]bool {
	txTo := t.Reads

	deps[t.Index] = map[int]bool{}

	for j := 0; j <= t.Index-1; j++ {
		txFrom := t.FullWriteList[j]

		deps = depsHelper(deps, txFrom, txTo, t.Index, j)
	}

	return deps
}

func GetDep(deps *VersionedIO) map[int]map[int]bool {
	newDependencies := map[int]map[int]bool{}

	for i := 1; i < len(deps.inputs); i++ {
		txTo := deps.inputs[i]

		newDependencies[i] = map[int]bool{}

		for j := 0; j <= i-1; j++ {
			txFrom := deps.outputs[j]

			newDependencies = depsHelper(newDependencies, txFrom, txTo.readSet, i, j)
		}
	}

	return newDependencies
}
