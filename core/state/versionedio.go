package state

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"

	"github.com/heimdalr/dag"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/tracing"
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

type ReadSet map[common.Address]map[AccountKey]*VersionedRead

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

type WriteSet map[common.Address]map[AccountKey]*VersionedWrite

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

func (s WriteSet) Delete(addr common.Address, key AccountKey) {
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
	Address common.Address
	Path    AccountPath
	Key     common.Hash
	Source  ReadSource
	Version Version
	Val     interface{}
}

func (vr VersionedRead) String() string {
	return fmt.Sprintf("%x %s (%s): %s", vr.Address, AccountKey{Path: vr.Path, Key: vr.Key}, vr.Source.VersionedString(vr.Version), valueString(vr.Path, vr.Val))
}

type VersionedWrite struct {
	Address common.Address
	Path    AccountPath
	Key     common.Hash
	Version Version
	Val     interface{}
	Reason  tracing.BalanceChangeReason
}

func (vr VersionedWrite) String() string {
	return fmt.Sprintf("%x %s: %s", vr.Address, AccountKey{Path: vr.Path, Key: vr.Key}, valueString(vr.Path, vr.Val))
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
	case StatePath:
		num := value.(uint256.Int)
		return fmt.Sprintf("%x", &num)
	case NoncePath:
		return strconv.FormatUint(value.(uint64), 10)
	case CodePath:
		l := len(value.([]byte))
		if l > 40 {
			l = 40
		}
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

func (vr *versionedStateReader) ReadAccountData(address common.Address) (*accounts.Account, error) {
	if r, ok := vr.reads[address][AccountKey{Path: AddressPath}]; ok && r.Val != nil {
		if account, ok := r.Val.(*accounts.Account); ok {
			if addr := fmt.Sprintf("%x", address); addr == "9ead03f7136fc6b4bdb0780b00a1c14ae5a8b6d0" {
				fmt.Println(addr, "ReadAccountData - reads", account.Nonce)
			}
			updated := vr.applyVersionedUpdates(address, *account)
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

func versionedUpdate[T any](versionMap *VersionMap, addr common.Address, path AccountPath, key common.Hash, txIndex int) (T, bool) {
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
func (vr versionedStateReader) applyVersionedUpdates(address common.Address, account accounts.Account) accounts.Account {
	if update, ok := versionedUpdate[uint256.Int](vr.versionMap, address, BalancePath, common.Hash{}, vr.txIndex); ok {
		account.Balance = update
	}
	if update, ok := versionedUpdate[uint64](vr.versionMap, address, NoncePath, common.Hash{}, vr.txIndex); ok {
		account.Nonce = update
	}
	if update, ok := versionedUpdate[common.Hash](vr.versionMap, address, CodeHashPath, common.Hash{}, vr.txIndex); ok {
		account.CodeHash = update
	}
	return account
}

func (vr versionedStateReader) ReadAccountDataForDebug(address common.Address) (*accounts.Account, error) {
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

func (vr versionedStateReader) ReadAccountStorage(address common.Address, key common.Hash) (uint256.Int, bool, error) {
	if r, ok := vr.reads[address][AccountKey{Path: StatePath, Key: key}]; ok && r.Val != nil {
		val := r.Val.(uint256.Int)
		return val, true, nil
	}

	if vr.stateReader != nil {
		return vr.stateReader.ReadAccountStorage(address, key)
	}

	return uint256.Int{}, false, nil
}

func (vr versionedStateReader) HasStorage(address common.Address) (bool, error) {
	if r, ok := vr.reads[address]; ok {
		for k := range r {
			if k.Path == StatePath {
				return true, nil
			}
		}
	}

	if vr.stateReader != nil {
		return vr.stateReader.HasStorage(address)
	}

	return false, nil
}

func (vr versionedStateReader) ReadAccountCode(address common.Address) ([]byte, error) {
	if r, ok := vr.reads[address][AccountKey{Path: CodePath}]; ok && r.Val != nil {
		if code, ok := r.Val.([]byte); ok {
			return code, nil
		}
	}

	if vr.stateReader != nil {
		return vr.stateReader.ReadAccountCode(address)
	}

	return nil, nil
}

func (vr versionedStateReader) ReadAccountCodeSize(address common.Address) (int, error) {
	if r, ok := vr.reads[address][AccountKey{Path: CodePath}]; ok && r.Val != nil {
		if code, ok := r.Val.([]byte); ok {
			return len(code), nil
		}
	}

	if vr.stateReader != nil {
		return vr.stateReader.ReadAccountCodeSize(address)
	}

	return 0, nil
}

func (vr versionedStateReader) ReadAccountIncarnation(address common.Address) (uint64, error) {
	if r, ok := vr.reads[address][AccountKey{Path: AddressPath}]; ok && r.Val != nil {
		return r.Val.(*accounts.Account).Incarnation, nil
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

	cmpMap := map[common.Address]map[AccountKey]struct{}{}

	for i := 0; i < len(cmpSet); i++ {
		vw := cmpSet[i]
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

func versionedRead[T any](s *IntraBlockState, addr common.Address, path AccountPath, key common.Hash, commited bool, defaultV T, copyV func(T) T, readStorage func(sdb *stateObject) (T, error)) (T, ReadSource, error) {
	if s.versionMap == nil {
		so, err := s.getStateObject(addr)

		if err != nil || readStorage == nil {
			return defaultV, StorageRead, err
		}
		val, err := readStorage(so)
		return val, StorageRead, err
	}

	if so, ok := s.stateObjects[addr]; ok && so.deleted {
		return defaultV, StorageRead, nil
	} else if dres := s.versionMap.Read(addr, SelfDestructPath, common.Hash{}, s.txIndex); dres.Status() == MVReadResultDone {
		return defaultV, MapRead, nil
	}

	if !commited {
		if vw, ok := s.versionedWrite(addr, path, key); ok {
			if dbg.TraceTransactionIO && (s.trace || traceAccount(addr)) {
				fmt.Printf("%d (%d.%d) RD %s %x %s: %s\n", s.blockNum, s.txIndex, s.version, WriteSetRead, addr, AccountKey{path, key}, valueString(path, vw.Val))
			}

			val := vw.Val.(T)
			return val, WriteSetRead, nil
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

	switch res.Status() {
	case MVReadResultDone:
		vr.Source = MapRead

		if pr, ok := s.versionedReads[addr][AccountKey{Path: path, Key: key}]; ok {
			if pr.Version == vr.Version {
				if dbg.TraceTransactionIO && (s.trace || traceAccount(addr)) {
					fmt.Printf("%d (%d.%d) RD %s (%d.%d) %x %s: %s\n", s.blockNum, s.txIndex, s.version, MapRead, res.DepIdx(), res.Incarnation(), addr, AccountKey{path, key}, valueString(path, pr.Val))
				}

				return pr.Val.(T), MapRead, nil
			}

			if vr.Version.TxIndex > pr.Version.TxIndex || vr.Version.Incarnation > pr.Version.Incarnation {
				if vr.Version.TxIndex > s.dep {
					s.dep = vr.Version.TxIndex
				}

				if dbg.TraceTransactionIO && (s.trace || traceAccount(addr)) {
					fmt.Printf("%d (%d.%d) DEP (%d.%d) %x %s\n", s.blockNum, s.txIndex, s.version, vr.Version.TxIndex, vr.Version.Incarnation, addr, AccountKey{path, key})
				}

				if s.versionedReads == nil {
					s.versionedReads = ReadSet{}
				}
				s.versionedReads.Set(vr)

				panic(ErrDependency)
			}
		}

		var ok bool
		if v, ok = res.Value().(T); !ok {
			return defaultV, MapRead, fmt.Errorf("unexpected type: %T", res.Value())
		}

		if dbg.TraceTransactionIO && (s.trace || traceAccount(addr)) {
			fmt.Printf("%d (%d.%d) RD %s (%d.%d) %x %s: %s\n", s.blockNum, s.txIndex, s.version, MapRead, res.DepIdx(), res.Incarnation(), addr, AccountKey{path, key}, valueString(path, v))
		}

		if copyV == nil {
			return v, MapRead, nil
		}

		vr.Val = copyV(v)

	case MVReadResultDependency:
		if dbg.TraceTransactionIO && (s.trace || traceAccount(addr)) {
			fmt.Printf("%d (%d.%d) DEP (%d.%d) %x %s\n", s.blockNum, s.txIndex, s.version, res.DepIdx(), res.Incarnation(), addr, AccountKey{path, key})
		}

		s.dep = res.DepIdx()
		vr.Source = MapRead

		if s.versionedReads == nil {
			s.versionedReads = ReadSet{}
		}
		s.versionedReads.Set(vr)

		panic(ErrDependency)

	case MVReadResultNone:
		if versionedReads := s.versionedReads; versionedReads != nil {
			if pr, ok := versionedReads[addr][AccountKey{Path: path, Key: key}]; ok {
				if pr.Version == vr.Version {
					if dbg.TraceTransactionIO && (s.trace || traceAccount(addr)) {
						fmt.Printf("%d (%d.%d) RD %s %x %s: %s\n", s.blockNum, s.txIndex, s.version, ReadSetRead, addr, AccountKey{path, key}, valueString(path, pr.Val))
					}

					return pr.Val.(T), ReadSetRead, nil
				}
			}
		}

		if readStorage == nil {
			return defaultV, UnknownSource, nil
		}

		vr.Source = StorageRead
		so, err := s.getStateObject(addr)

		if err != nil {
			return defaultV, StorageRead, nil
		}

		if v, err = readStorage(so); err != nil {
			return defaultV, StorageRead, nil
		}

		if dbg.TraceTransactionIO && (s.trace || traceAccount(addr)) {
			fmt.Printf("%d (%d.%d) RD %s %x %s: %s\n", s.blockNum, s.txIndex, s.version, StorageRead, addr, AccountKey{path, key}, valueString(path, v))
		}

		vr.Val = copyV(v)

	default:
		return defaultV, UnknownSource, nil
	}

	if s.versionedReads == nil {
		s.versionedReads = ReadSet{}
	}
	s.versionedReads.Set(vr)

	return v, vr.Source, nil
}

// note that TxIndex starts at -1 (the begin system tx)
type VersionedIO struct {
	inputs     []ReadSet
	outputs    []VersionedWrites // write sets that should be checked during validation
	outputsSet []map[common.Address]map[AccountKey]struct{}
	allOutputs []VersionedWrites // entire write sets in MVHashMap. allOutputs should always be a parent set of outputs
}

func (io *VersionedIO) Inputs() []ReadSet {
	return io.inputs
}

func (io *VersionedIO) ReadSet(txnIdx int) ReadSet {
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

func (io *VersionedIO) HasReads(txnIdx int) bool {
	if len(io.outputsSet) <= txnIdx+1 {
		return false
	}
	return len(io.outputsSet[txnIdx+1]) > 0
}

func (io *VersionedIO) HasWritten(txnIdx int, addr common.Address, path AccountPath, key common.Hash) bool {
	if len(io.outputsSet) <= txnIdx+1 {
		return false
	}
	_, ok := io.outputsSet[txnIdx+1][addr][AccountKey{path, key}]
	return ok
}

func NewVersionedIO(numTx int) *VersionedIO {
	return &VersionedIO{
		inputs:     make([]ReadSet, numTx+1),
		outputs:    make([]VersionedWrites, numTx+1),
		outputsSet: make([]map[common.Address]map[AccountKey]struct{}, numTx+1),
		allOutputs: make([]VersionedWrites, numTx+1),
	}
}

func (io *VersionedIO) RecordReads(txId int, input ReadSet) {
	if len(io.inputs) <= txId+1 {
		io.inputs = append(io.inputs, make([]ReadSet, txId+2-len(io.inputs))...)
	}
	io.inputs[txId+1] = input
}

func (io *VersionedIO) RecordWrites(txId int, output VersionedWrites) {
	if len(io.outputs) <= txId+1 {
		io.outputs = append(io.outputs, make([]VersionedWrites, txId+2-len(io.outputs))...)
	}
	io.outputs[txId+1] = output

	if len(io.outputsSet) <= txId+1 {
		io.outputsSet = append(io.outputsSet, make([]map[common.Address]map[AccountKey]struct{}, txId+2-len(io.outputsSet))...)
	}
	io.outputsSet[txId+1] = map[common.Address]map[AccountKey]struct{}{}

	for _, v := range output {
		keys, ok := io.outputsSet[txId+1][v.Address]
		if !ok {
			keys = map[AccountKey]struct{}{}
			io.outputsSet[txId+1][v.Address] = keys
		}
		keys[AccountKey{v.Path, v.Key}] = struct{}{}
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
			txFrom := deps.allOutputs[j]

			newDependencies = depsHelper(newDependencies, txFrom, txTo, i, j)
		}
	}

	return newDependencies
}
