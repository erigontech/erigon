package state

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types/accounts"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/heimdalr/dag"
	"github.com/holiman/uint256"
	"github.com/tidwall/btree"
)

const (
	ReadKindMap     = 0
	ReadKindStorage = 1
)

type VersionedRead struct {
	Path    *VersionKey
	Kind    int
	Version Version
	Val     interface{}
}

type VersionedWrite struct {
	Path    *VersionKey
	Version Version
	Val     interface{}
	Reason  tracing.BalanceChangeReason
}

var ErrDependency = errors.New("found dependency")

func VersionedReadLess(a, b *VersionedRead) bool {
	return VersionKeyLess(a.Path, b.Path)
}

func VersionedWriteLess(a, b *VersionedWrite) bool {
	return VersionKeyLess(a.Path, b.Path)
}

type versionedStateReader struct {
	txIndex     int
	reads       *btree.BTreeG[*VersionedRead]
	versionMap  *VersionMap
	stateReader StateReader
}

func NewVersionedStateReader(txIndex int, reads *btree.BTreeG[*VersionedRead], versionMap *VersionMap) *versionedStateReader {
	return &versionedStateReader{txIndex, reads, versionMap, nil}
}

func (vr *versionedStateReader) SetStateReader(stateReader StateReader) {
	vr.stateReader = stateReader
}

func (vr *versionedStateReader) ReadAccountData(address common.Address) (*accounts.Account, error) {
	key := AddressKey(&address)
	if r, ok := vr.reads.Get(&VersionedRead{Path: &key}); ok && r.Val != nil {
		if account, ok := r.Val.(accounts.Account); ok {
			updated := vr.applyVersionedUpdates(address, account)
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

func versionedUpdate[T any](versionMap *VersionMap, k VersionKey, txIndex int) (T, bool) {
	if res := versionMap.Read(&k, txIndex); res.Status() == MVReadResultDone {
		v, ok := res.Value().(T)
		return v, ok
	}
	var v T
	return v, false
}

// applyVersionedUpdates applies updated from the version map to the account before returning it, this is necessary
// for the account obkect becuase the state reader/.writer api's treat the subfileds as a group and this
// may lead to updated from pervious transactions being missed where we only update a subset of the fiels as these won't
// be recored as reads and hence the varification process will miss them.  We don't want to creat a fail but
// we do  want to capture the updates
func (vr versionedStateReader) applyVersionedUpdates(address common.Address, account accounts.Account) accounts.Account {
	if update, ok := versionedUpdate[*uint256.Int](vr.versionMap, SubpathKey(&address, BalancePath), vr.txIndex); ok {
		account.Balance = *update
	}
	if update, ok := versionedUpdate[uint64](vr.versionMap, SubpathKey(&address, NoncePath), vr.txIndex); ok {
		account.Nonce = update
	}
	if update, ok := versionedUpdate[common.Hash](vr.versionMap, SubpathKey(&address, CodeHashPath), vr.txIndex); ok {
		account.CodeHash = update
	}

	return account
}

func (vr versionedStateReader) ReadAccountDataForDebug(address common.Address) (*accounts.Account, error) {
	key := AddressKey(&address)
	if r, ok := vr.reads.Get(&VersionedRead{Path: &key}); ok && r.Val != nil {
		if account, ok := r.Val.(accounts.Account); ok {
			updated := vr.applyVersionedUpdates(address, account)
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

func (vr versionedStateReader) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	path := StateKey(&address, key)
	if r, ok := vr.reads.Get(&VersionedRead{Path: &path}); ok && r.Val != nil {
		return r.Val.(*uint256.Int).Bytes(), nil
	}

	if vr.stateReader != nil {
		return vr.stateReader.ReadAccountStorage(address, incarnation, key)
	}

	return nil, nil
}

func (vr versionedStateReader) ReadAccountCode(address common.Address, incarnation uint64) ([]byte, error) {
	key := SubpathKey(&address, CodePath)
	if r, ok := vr.reads.Get(&VersionedRead{Path: &key}); ok && r.Val != nil {
		if code, ok := r.Val.([]byte); ok {
			return code, nil
		}
	}

	if vr.stateReader != nil {
		return vr.stateReader.ReadAccountCode(address, incarnation)
	}

	return nil, nil
}

func (vr versionedStateReader) ReadAccountCodeSize(address common.Address, incarnation uint64) (int, error) {
	key := SubpathKey(&address, CodePath)
	if r, ok := vr.reads.Get(&VersionedRead{Path: &key}); ok && r.Val != nil {
		if code, ok := r.Val.([]byte); ok {
			return len(code), nil
		}
	}

	if vr.stateReader != nil {
		return vr.stateReader.ReadAccountCodeSize(address, incarnation)
	}

	return 0, nil
}

func (vr versionedStateReader) ReadAccountIncarnation(address common.Address) (uint64, error) {
	key := AddressKey(&address)
	if r, ok := vr.reads.Get(&VersionedRead{Path: &key}); ok && r.Val != nil {
		return r.Val.(accounts.Account).Incarnation, nil
	}

	if vr.stateReader != nil {
		return vr.stateReader.ReadAccountIncarnation(address)
	}

	return 0, nil
}

type VersionedWrites []*VersionedWrite

// hasNewWrite: returns true if the current set has a new write compared to the input
func (writes VersionedWrites) HasNewWrite(cmpSet []*VersionedWrite) bool {
	if len(writes) == 0 {
		return false
	} else if len(cmpSet) == 0 || len(writes) > len(cmpSet) {
		return true
	}

	cmpMap := *btree.NewBTreeG[*VersionKey](VersionKeyLess)

	for i := 0; i < len(cmpSet); i++ {
		cmpMap.Set(cmpSet[i].Path)
	}

	for _, v := range writes {
		if _, ok := cmpMap.Get(v.Path); !ok {
			return true
		}
	}

	return false
}

func (writes VersionedWrites) stateObjects() (map[libcommon.Address][]*stateObject, error) {
	stateObjects := map[libcommon.Address][]*stateObject{}

	for i := range writes {
		path := writes[i].Path
		so := writes[i].Val.(*stateObject)
		addr := path.GetAddress()

		if so != nil {
			prevs, ok := stateObjects[addr]

			if ok {
				isPrev := false

				for _, prev := range prevs {
					if prev == so {
						isPrev = true
						break
					}
				}
				if isPrev {
					continue
				}
			} else {
				stateObjects[addr] = []*stateObject{so}
			}

			if path.IsState() {
				stateKey := path.GetStateKey()
				var state uint256.Int
				so.GetState(stateKey, &state)
				if len(prevs) > 0 {
					var prevState uint256.Int
					prevs[len(prevs)-1].GetState(stateKey, &state)
					if prevState.Eq(&state) {
						continue
					}
				}
			} else if path.IsAddress() {
				continue
			} else {
				switch path.GetSubpath() {
				case BalancePath:
					b := so.Balance()
					if len(prevs) > 0 {
						prev := prevs[len(prevs)-1].Balance()
						if prev.Eq(&b) {
							continue
						}
					}
				case NoncePath:
					n := so.Nonce()
					if len(prevs) > 0 {
						prev := prevs[len(prevs)-1].Nonce()
						if prev == n {
							continue
						}
					}
				case CodePath:
					c, err := so.Code()
					if err != nil {
						return nil, err
					}
					if len(prevs) > 0 {
						prev, err := prevs[len(prevs)-1].Code()
						if err != nil {
							return nil, err
						}
						if bytes.Equal(prev, c) {
							continue
						}
					}
				case SelfDestructPath:
					if len(prevs) > 0 {
						if so.deleted == prevs[len(prevs)-1].deleted {
							continue
						}
					}
				default:
					panic(fmt.Errorf("unknown key type: %d", path.GetSubpath()))
				}

				stateObjects[addr] = append(prevs, so)
			}
		}

	}
	return stateObjects, nil
}

func versionedRead[T any](s *IntraBlockState, k VersionKey, commited bool, defaultV T, copyV func(T) T, readStorage func(sdb *stateObject) (T, error)) (T, error) {
	if s.versionMap == nil {
		so, err := s.getStateObject(k.GetAddress())

		if err != nil || readStorage==nil {
			return defaultV, err
		}

		return readStorage(so)
	}

	if !commited {
		if vw, ok := s.versionedWrite(k); ok {
			return vw.Val.(T), nil
		}
	}

	res := s.versionMap.Read(&k, s.txIndex)

	var v T
	var vr = VersionedRead{
		Version: Version{
			TxIndex:     res.DepIdx(),
			Incarnation: res.Incarnation(),
		},
		Path: &k,
	}

	switch res.Status() {
	case MVReadResultDone:
		if versionedReads := s.versionedReads; versionedReads != nil {
			if pr, ok := versionedReads.Get(&VersionedRead{Path: &k}); ok {
				if pr.Version == vr.Version {
					return pr.Val.(T), nil
				}

				if pr.Version.Incarnation < vr.Version.Incarnation {
					if res.DepIdx() > s.dep {
						s.dep = res.DepIdx()
					}
					panic(ErrDependency)
				}
			}
		}

		var ok bool
		vr.Kind = ReadKindMap
		if v, ok = res.Value().(T); !ok {
			return defaultV, nil
		}

		if copyV == nil {
			return v, nil
		}

		vr.Val = copyV(v)

	case MVReadResultDependency:
		s.dep = res.DepIdx()
		panic(ErrDependency)

	case MVReadResultNone:
		if versionedReads := s.versionedReads; versionedReads != nil {
			if pr, ok := versionedReads.Get(&VersionedRead{Path: &k}); ok {
				if pr.Version == vr.Version {
					return pr.Val.(T), nil
				}
			}
		}

		if readStorage == nil {
			return defaultV, nil
		}

		vr.Kind = ReadKindStorage
		so, err := s.getStateObject(k.GetAddress())

		if err != nil {
			return defaultV, nil
		}

		if v, err = readStorage(so); err != nil {
			return defaultV, nil
		}

		vr.Val = copyV(v)

	default:
		return defaultV, nil
	}

	if s.versionedReads == nil {
		s.versionedReads = btree.NewBTreeGOptions(VersionedReadLess, btree.Options{NoLocks: true})
	}

	s.versionedReads.Set(&vr)

	return v, nil
}

func ApplyVersionedWrites(chainRules *chain.Rules, writes VersionedWrites, stateWriter StateWriter) error {
	stateObjects, err := writes.stateObjects()

	if err != nil {
		return err
	}

	for addr, sos := range stateObjects {
		for _, so := range sos {
			if err := updateAccount(chainRules.IsSpuriousDragon, chainRules.IsAura, stateWriter, addr, so, so.IsDirty(), nil); err != nil {
				return err
			}
		}
	}

	return nil
}

// note that TxIndex starts at -1 (the begin system tx)
type VersionedIO struct {
	inputs     []*btree.BTreeG[*VersionedRead]
	outputs    []VersionedWrites // write sets that should be checked during validation
	outputsSet []*btree.BTreeG[*VersionKey]
	allOutputs []VersionedWrites // entire write sets in MVHashMap. allOutputs should always be a parent set of outputs
}

func (io *VersionedIO) Inputs() []*btree.BTreeG[*VersionedRead] {
	return io.inputs
}

func (io *VersionedIO) ReadSet(txnIdx int) *btree.BTreeG[*VersionedRead] {
	if len(io.inputs) <= txnIdx+1 {
		return nil
	}
	return io.inputs[txnIdx+1]
}

func (io *VersionedIO) WriteSet(txnIdx int) VersionedWrites {
	if len(io.outputs) <= txnIdx+1 {
		return nil
	}
	return io.outputs[txnIdx+1]
}

func (io *VersionedIO) AllWriteSet(txnIdx int) VersionedWrites {
	if len(io.allOutputs) <= txnIdx+1 {
		return nil
	}
	return io.allOutputs[txnIdx+1]
}

func (io *VersionedIO) WriteCount() (count int64) {
	for _, output := range io.outputs {
		count += int64(len(output))
	}

	return count
}

func (io *VersionedIO) ReadCount() (count int64) {
	for _, input := range io.inputs {
		if input != nil {
			count += int64(input.Len())
		}
	}

	return count
}

func (io *VersionedIO) HasWritten(txnIdx int, k VersionKey) bool {
	if len(io.outputsSet) <= txnIdx+1 {
		return false
	}
	_, ok := io.outputsSet[txnIdx+1].Get(&k)
	return ok
}

func NewVersionedIO(numTx int) *VersionedIO {
	return &VersionedIO{
		inputs:     make([]*btree.BTreeG[*VersionedRead], numTx+1),
		outputs:    make([]VersionedWrites, numTx+1),
		outputsSet: make([]*btree.BTreeG[*VersionKey], numTx+1),
		allOutputs: make([]VersionedWrites, numTx+1),
	}
}

func (io *VersionedIO) RecordReads(txId int, input *btree.BTreeG[*VersionedRead]) {
	if len(io.inputs) <= txId+1 {
		io.inputs = append(io.inputs, make([]*btree.BTreeG[*VersionedRead], txId+2-len(io.inputs))...)
	}
	io.inputs[txId+1] = input
}

func (io *VersionedIO) RecordWrites(txId int, output VersionedWrites) {
	if len(io.outputs) <= txId+1 {
		io.outputs = append(io.outputs, make([]VersionedWrites, txId+2-len(io.outputs))...)
	}
	io.outputs[txId+1] = output

	if len(io.outputsSet) <= txId+1 {
		io.outputsSet = append(io.outputsSet, make([]*btree.BTreeG[*VersionKey], txId+2-len(io.outputsSet))...)
	}
	io.outputsSet[txId+1] = btree.NewBTreeGOptions(VersionKeyLess, btree.Options{NoLocks: true})

	for _, v := range output {
		io.outputsSet[txId+1].Set(v.Path)
	}
}

func (io *VersionedIO) RecordAllWrites(txId int, output VersionedWrites) {
	if len(io.allOutputs) <= txId+1 {
		io.allOutputs = append(io.allOutputs, make([]VersionedWrites, txId+2-len(io.allOutputs))...)
	}
	io.allOutputs[txId+1] = output
}

type DAG struct {
	*dag.DAG
}

type TxDep struct {
	Index         int
	Reads         *btree.BTreeG[*VersionedRead]
	FullWriteList []VersionedWrites
}

func HasReadDep(txFrom VersionedWrites, txTo *btree.BTreeG[*VersionedRead]) bool {
	for _, rd := range txFrom {
		if _, ok := txTo.Get(&VersionedRead{Path: rd.Path}); ok {
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
			txFrom := deps.allOutputs[j]

			if HasReadDep(txFrom, txTo) {
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

func depsHelper(dependencies map[int]map[int]bool, txFrom VersionedWrites, txTo *btree.BTreeG[*VersionedRead], i int, j int) map[int]map[int]bool {
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
			txFrom := deps.allOutputs[j]

			newDependencies = depsHelper(newDependencies, txFrom, txTo, i, j)
		}
	}

	return newDependencies
}
