package state

import "github.com/erigontech/erigon/core/tracing"

const (
	ReadKindMap     = 0
	ReadKindStorage = 1
)

type VersionedRead struct {
	Path VersionKey
	Kind int
	V    Version
}

type VersionedWrite struct {
	Path   VersionKey
	V      Version
	Val    interface{}
	Reason tracing.BalanceChangeReason
}

type VersionedReads []VersionedRead
type VersionedWrites []VersionedWrite

// hasNewWrite: returns true if the current set has a new write compared to the input
func (txo VersionedWrites) hasNewWrite(cmpSet []VersionedWrite) bool {
	if len(txo) == 0 {
		return false
	} else if len(cmpSet) == 0 || len(txo) > len(cmpSet) {
		return true
	}

	cmpMap := map[VersionKey]bool{cmpSet[0].Path: true}

	for i := 1; i < len(cmpSet); i++ {
		cmpMap[cmpSet[i].Path] = true
	}

	for _, v := range txo {
		if !cmpMap[v.Path] {
			return true
		}
	}

	return false
}

func versionedRead[T any](s *IntraBlockState, k VersionKey, defaultV T, readStorage func(sdb *IntraBlockState) (T, error)) (T, error) {
	if s.versionMap == nil {
		return readStorage(s)
	}

	if s.versionWritten(k) {
		return readStorage(s)
	}

	if !k.IsAddress() {
		// If we are reading subpath from a deleted account, return default value instead of reading from MVHashmap
		addr := k.GetAddress()
		stateObject, err := s.getStateObject(addr)
		if err != nil {
			return defaultV, err
		}
		if stateObject == nil || stateObject.deleted {
			readStorage(s)
			return defaultV, nil
		}
	}

	res := s.versionMap.Read(k, s.txIndex)

	var v T
	var err error
	var vr = VersionedRead{
		V: Version{
			TxnIndex:    res.DepIdx(),
			Incarnation: res.Incarnation(),
		},
		Path: k,
	}

	switch res.Status() {
	case MVReadResultDone:
		{
			v, err = readStorage(res.Value().(*IntraBlockState))
			vr.Kind = ReadKindMap
		}
	case MVReadResultDependency:
		{
			s.dep = res.DepIdx()
			panic("Found denpendency")
		}
	case MVReadResultNone:
		{
			v, err = readStorage(s)
			vr.Kind = ReadKindStorage
		}
	default:
		return defaultV, nil
	}

	if err != nil {
		return defaultV, err
	}

	if s.versionedReads == nil {
		s.versionedReads = map[VersionKey]VersionedRead{}
	}

	// TODO: I assume we don't want to overwrite an existing read because this could - for example - change a storage
	//  read to map if the same value is read multiple times.
	if _, ok := s.versionedReads[k]; !ok {
		s.versionedReads[k] = vr
	}

	return v, nil
}

type VersionedIO struct {
	inputs     []VersionedReads
	outputs    []VersionedWrites // write sets that should be checked during validation
	outputsSet []map[VersionKey]struct{}
	allOutputs []VersionedWrites // entire write sets in MVHashMap. allOutputs should always be a parent set of outputs
}

func (io *VersionedIO) ReadSet(txnIdx int) []VersionedRead {
	return io.inputs[txnIdx]
}

func (io *VersionedIO) WriteSet(txnIdx int) []VersionedWrite {
	return io.outputs[txnIdx]
}

func (io *VersionedIO) AllWriteSet(txnIdx int) []VersionedWrite {
	return io.allOutputs[txnIdx]
}

func (io *VersionedIO) HasWritten(txnIdx int, k VersionKey) bool {
	_, ok := io.outputsSet[txnIdx][k]
	return ok
}

func NewVersionedIO(numTx int) *VersionedIO {
	return &VersionedIO{
		inputs:     make([]VersionedReads, numTx),
		outputs:    make([]VersionedWrites, numTx),
		outputsSet: make([]map[VersionKey]struct{}, numTx),
		allOutputs: make([]VersionedWrites, numTx),
	}
}

func (io *VersionedIO) recordRead(txId int, input []VersionedRead) {
	io.inputs[txId] = input
}

func (io *VersionedIO) recordWrite(txId int, output []VersionedWrite) {
	io.outputs[txId] = output
	io.outputsSet[txId] = make(map[VersionKey]struct{}, len(output))

	for _, v := range output {
		io.outputsSet[txId][v.Path] = struct{}{}
	}
}

func (io *VersionedIO) recordAllWrite(txId int, output []VersionedWrite) {
	io.allOutputs[txId] = output
}

func (io *VersionedIO) RecordReadAtOnce(inputs [][]VersionedRead) {
	for ind, val := range inputs {
		io.inputs[ind] = val
	}
}

func (io *VersionedIO) RecordAllWriteAtOnce(outputs [][]VersionedWrite) {
	for ind, val := range outputs {
		io.allOutputs[ind] = val
	}
}
