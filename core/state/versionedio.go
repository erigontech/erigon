package state

import (
	"bytes"
	"fmt"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/heimdalr/dag"
	"github.com/holiman/uint256"
)

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
func (txo VersionedWrites) HasNewWrite(cmpSet []VersionedWrite) bool {
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

	if s.hasVersionedWrite(k) {
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
			TxIndex:     res.DepIdx(),
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

func ApplyVersionedWrites(chainRules *chain.Rules, writes []VersionedWrite, stateWriter StateWriter) error {
	stateObjects := map[libcommon.Address][]*stateObject{}

	for i := range writes {
		path := writes[i].Path
		sr := writes[i].Val.(*IntraBlockState)

		addr := path.GetAddress()

		so, err := sr.getStateObject(addr)

		if err != nil {
			return err
		}

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
				so.GetState(&stateKey, &state)
				if len(prevs) > 0 {
					var prevState uint256.Int
					prevs[len(prevs)-1].GetState(&stateKey, &state)
					if prevState.Eq(&state) {
						continue
					}
				}
				fmt.Println("STATE", fmt.Sprintf("%x %x", stateKey, state.Bytes()))
			} else if path.IsAddress() {
				continue
			} else {
				switch path.GetSubpath() {
				case BalancePath:
					b := so.Balance()
					if len(prevs) > 0 {
						prev := prevs[len(prevs)-1].Balance()
						if prev.Eq(b) {
							continue
						}
					}
					fmt.Println("BAL", b)
				case NoncePath:
					n := so.Nonce()
					if len(prevs) > 0 {
						prev := prevs[len(prevs)-1].Nonce()
						if prev == n {
							continue
						}
					}
					fmt.Println("NONCE", n)
				case CodePath:
					c, err := so.Code()
					if err != nil {
						return err
					}
					if len(prevs) > 0 {
						prev, err := prevs[len(prevs)-1].Code()
						if err != nil {
							return err
						}
						if bytes.Equal(prev, c) {
							continue
						}
					}
					fmt.Println("CODE", c)
					//s.SetCode(addr, c)
				case SelfDestructPath:
					if so.deleted {
						//s.Selfdestruct(addr)
					}
				default:
					panic(fmt.Errorf("unknown key type: %d", path.GetSubpath()))
				}

				stateObjects[addr] = append(prevs, so)
			}
		}
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
	inputs     []VersionedReads
	outputs    []VersionedWrites // write sets that should be checked during validation
	outputsSet []map[VersionKey]struct{}
	allOutputs []VersionedWrites // entire write sets in MVHashMap. allOutputs should always be a parent set of outputs
}

func (io *VersionedIO) Inputs() []VersionedReads {
	return io.inputs
}

func (io *VersionedIO) ReadSet(txnIdx int) []VersionedRead {
	if len(io.inputs) <= txnIdx+1 {
		return nil
	}
	return io.inputs[txnIdx+1]
}

func (io *VersionedIO) WriteSet(txnIdx int) []VersionedWrite {
	if len(io.outputs) <= txnIdx+1 {
		return nil
	}
	return io.outputs[txnIdx+1]
}

func (io *VersionedIO) AllWriteSet(txnIdx int) []VersionedWrite {
	if len(io.allOutputs) <= txnIdx+1 {
		return nil
	}
	return io.allOutputs[txnIdx+1]
}

func (io *VersionedIO) HasWritten(txnIdx int, k VersionKey) bool {
	if len(io.outputsSet) <= txnIdx+1 {
		return false
	}
	_, ok := io.outputsSet[txnIdx+1][k]
	return ok
}

func NewVersionedIO(numTx int) *VersionedIO {
	return &VersionedIO{
		inputs:     make([]VersionedReads, numTx+1),
		outputs:    make([]VersionedWrites, numTx+1),
		outputsSet: make([]map[VersionKey]struct{}, numTx+1),
		allOutputs: make([]VersionedWrites, numTx+1),
	}
}

func (io *VersionedIO) RecordReads(txId int, input []VersionedRead) {
	if len(io.inputs) <= txId+1 {
		io.inputs = append(io.inputs, make([]VersionedReads, txId+2-len(io.inputs))...)
	}
	io.inputs[txId+1] = input
}

func (io *VersionedIO) RecordWrites(txId int, output []VersionedWrite) {
	if len(io.outputs) <= txId+1 {
		io.outputs = append(io.outputs, make([]VersionedWrites, txId+2-len(io.outputs))...)
	}
	io.outputs[txId+1] = output

	if len(io.outputsSet) <= txId+1 {
		io.outputsSet = append(io.outputsSet, make([]map[VersionKey]struct{}, txId+2-len(io.outputsSet))...)
	}
	io.outputsSet[txId+1] = make(map[VersionKey]struct{}, len(output))

	for _, v := range output {
		io.outputsSet[txId+1][v.Path] = struct{}{}
	}
}

func (io *VersionedIO) RecordAllWrites(txId int, output []VersionedWrite) {
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
	ReadList      []VersionedRead
	FullWriteList [][]VersionedWrite
}

func HasReadDep(txFrom VersionedWrites, txTo VersionedReads) bool {
	reads := make(map[VersionKey]bool)

	for _, v := range txTo {
		reads[v.Path] = true
	}

	for _, rd := range txFrom {
		if _, ok := reads[rd.Path]; ok {
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

func depsHelper(dependencies map[int]map[int]bool, txFrom VersionedWrites, txTo VersionedReads, i int, j int) map[int]map[int]bool {
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
	txTo := t.ReadList

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
