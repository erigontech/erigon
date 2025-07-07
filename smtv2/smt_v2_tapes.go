package smtv2

import (
	"errors"
	"fmt"
	"slices"

	"github.com/erigontech/mdbx-go/mdbx"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
)

var (
	ErrNoHandlerForChange = errors.New("no handler for CHANGE operation")
	ErrNoHandlerForAdd    = errors.New("no handler for ADD operation - IH path matched branch node")
	ErrNoHandlerForDelete = errors.New("no handler for DELETE operation - the IH was not found in the DB")
	ErrOvershoot          = errors.New("overshoot detected for change operation - IH tape moved beyond the change path")
)

type ChangeSetEntry struct {
	Type          ChangeSetEntryType
	Key           SmtKey
	Value         SmtValue8
	OriginalValue SmtValue8
	keyPath       []int
}

func (c *ChangeSetEntry) GetKeyPath() []int {
	if c.keyPath == nil {
		c.keyPath = c.Key.GetPath()
	}
	return c.keyPath
}

type ChangeSetEntryType uint8

const (
	ChangeSetEntryType_Add ChangeSetEntryType = iota
	ChangeSetEntryType_Change
	ChangeSetEntryType_Delete
	ChangeSetEntryType_Terminator
)

func (c *ChangeSetEntry) String(keyTruncate int) string {
	typ := "Add"
	switch c.Type {
	case ChangeSetEntryType_Change:
		typ = "Change"
	case ChangeSetEntryType_Delete:
		typ = "Delete"
	case ChangeSetEntryType_Terminator:
		typ = "Terminator"
	}

	fullPath := c.Key.GetPath()[:keyTruncate]

	return fmt.Sprintf("ChangeSetEntry{Type: %v, Key: %v, KeyPath(Trunc): %v, Value: %v, OriginalValue: %v}", typ, c.Key, fullPath, c.Value, c.OriginalValue)
}

type ChangeSetTape struct {
	index   int
	entries []ChangeSetEntry
}

func NewChangeSetTape() *ChangeSetTape {
	return &ChangeSetTape{
		index:   0,
		entries: []ChangeSetEntry{},
	}
}

func (c *ChangeSetTape) Size() int {
	return len(c.entries)
}

func (c *ChangeSetTape) Add(typ ChangeSetEntryType, key SmtKey, value SmtValue8, originalValue SmtValue8) {
	c.entries = append(c.entries, ChangeSetEntry{
		Type:          typ,
		Key:           key,
		Value:         value,
		OriginalValue: originalValue,
	})
}

func (c *ChangeSetTape) Next() ChangeSetEntry {
	toReturn := c.entries[c.index]
	c.index++
	return toReturn
}

func (c *ChangeSetTape) Current() ChangeSetEntry {
	return c.entries[c.index]
}

func (c *ChangeSetTape) Processed() int {
	return c.index
}

func LexoSortChangeSet(changeSet []ChangeSetEntry) []ChangeSetEntry {
	slices.SortFunc(changeSet, func(a, b ChangeSetEntry) int {
		p1 := a.Key.GetPath()
		p2 := b.Key.GetPath()
		return lexographhicalCheckPaths(p1, p2)
	})
	return changeSet
}

type InterhashTape interface {
	Next() (IntermediateHash, error)
	Current() IntermediateHash
	Size() (uint64, error)
	FastForward(prefix []byte) (bool, error)
}

type DbInterhashTape struct {
	cursor  kv.Cursor
	current IntermediateHash
}

func NewDbInterhashTape(tx kv.Tx) (*DbInterhashTape, error) {
	c, err := tx.Cursor(kv.TableSmtIntermediateHashes)
	if err != nil {
		return nil, err
	}

	_, value, err := c.First()
	if err != nil {
		return nil, err
	}

	ih := DeserialiseIntermediateHash(value)

	return &DbInterhashTape{
		cursor:  c,
		current: *ih,
	}, nil
}

func (i *DbInterhashTape) Next() (IntermediateHash, error) {
	key, value, err := i.cursor.Next()
	if err != nil {
		if mdbx.IsNotFound(err) {
			terminator := IntermediateHash{
				HashType: IntermediateHashType_Terminator,
			}
			i.current = terminator
			return terminator, nil
		}
		return IntermediateHash{}, err
	}

	if len(key) == 0 {
		terminator := IntermediateHash{
			HashType: IntermediateHashType_Terminator,
		}
		i.current = terminator
		return terminator, nil
	}

	ih := DeserialiseIntermediateHash(value)

	i.current = *ih

	return *ih, nil
}

func (i *DbInterhashTape) Current() IntermediateHash {
	return i.current
}

func (i *DbInterhashTape) Size() (uint64, error) {
	count, err := i.cursor.Count()
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (i *DbInterhashTape) Close() {
	i.cursor.Close()
}

func (i *DbInterhashTape) FastForward(prefix []byte) (bool, error) {
	// Use Seek to position cursor at first key >= prefix
	key, value, err := i.cursor.Seek(prefix)
	if err != nil {
		return false, err
	}

	// If we found a valid value, update current
	if value != nil {
		i.current = *DeserialiseIntermediateHash(value)
	}

	return len(key) > 0, nil
}

type OutputTape struct {
	first   InputTapeItem
	second  InputTapeItem
	size    int // tracks number of items currently stored
	debug   bool
	history []InputTapeItem
}

func NewOutputTape(debug bool) *OutputTape {
	return &OutputTape{
		debug:   debug,
		history: []InputTapeItem{},
	}
}

func (i *OutputTape) Push(item InputTapeItem) {
	if i.size == 0 {
		i.first = item
	} else if i.size == 1 {
		i.second = item
	} else {
		panic("OutputTape cannot hold more than 2 items")
	}
	i.size++

	if i.debug {
		i.history = append(i.history, item)
	}
}

func (i *OutputTape) Dequeue() InputTapeItem {
	if i.size == 0 {
		return InputTapeItem{}
	}

	toReturn := i.first
	i.first = i.second         // shift second item to first position
	i.second = InputTapeItem{} // clear second position
	i.size--

	if i.debug {
		i.history = i.history[1:]
	}

	return toReturn
}

func (i *OutputTape) Pop() InputTapeItem {
	if i.size == 0 {
		return InputTapeItem{}
	}

	var toReturn InputTapeItem
	if i.size == 2 {
		toReturn = i.second
		i.second = InputTapeItem{}
	} else { // size == 1
		toReturn = i.first
		i.first = InputTapeItem{}
	}
	i.size--

	if i.debug {
		i.history = i.history[:i.size]
	}

	return toReturn
}

func (i *OutputTape) Next() (InputTapeItem, error) {
	if i.size == 0 {
		return InputTapeItem{}, ErrNothingFound
	}
	return i.Dequeue(), nil
}

func (i *OutputTape) LastAdded() InputTapeItem {
	if i.size == 0 {
		return InputTapeItem{}
	}
	if i.size == 2 {
		return i.second
	}
	return i.first
}

type TapesProcessor struct {
	changeSetTape *ChangeSetTape
	interhashTape InterhashTape
	outputTape    *OutputTape
	debug         bool
	stoppingEarly bool
	isDone        bool
	fastForwards  int
	adds          int
	changes       int
	deletes       int
	terminators   int
}

func NewTapesProcessor(changeSetTape *ChangeSetTape, interhashTape InterhashTape, debug bool) (*TapesProcessor, error) {
	return &TapesProcessor{
		changeSetTape: changeSetTape,
		interhashTape: interhashTape,
		outputTape:    NewOutputTape(debug),
		debug:         debug,
	}, nil
}

func (i *TapesProcessor) Size() int {
	return i.changeSetTape.Size()
}

func (i *TapesProcessor) Processed() int {
	return i.changeSetTape.Processed()
}

func (i *TapesProcessor) Next() (InputTapeItem, error) {
	// here we need to return not the most recently written thing on the tape because it could be
	// replaced by some change set in the next call to process but the one prior to this one
	// which we can consider safe on the tape.  So we iterate calling processNext until we know there is
	// something we can return and return it
	// first we check if we are done and drain the output tape in this case, then we move on to processing more
	// if we're not in a done state

	if i.isDone {
		if i.outputTape.size > 0 {
			return i.outputTape.Dequeue(), nil
		} else {
			return InputTapeItem{}, ErrNothingFound
		}
	}

	// we only want to buffer up at most 2 things on the tape as we can only return one at a time
	for i.outputTape.size < 2 {
		done, err := i.processNext()
		if err != nil {
			return InputTapeItem{}, err
		}
		if done {
			i.isDone = true
			break
		}
	}

	return i.outputTape.Dequeue(), nil
}

func (i *TapesProcessor) processNext() (bool, error) {
	var changeSetEntry ChangeSetEntry
	if i.stoppingEarly {
		// if we found an early termination in the algo the we jump straight to the terminator
		changeSetEntry = ChangeSetEntry{
			Type: ChangeSetEntryType_Terminator,
		}
	} else {
		changeSetEntry = i.changeSetTape.Current()
	}

	// now we can switch on the type of change we have as each will need handling differently
	switch changeSetEntry.Type {
	case ChangeSetEntryType_Add:
		i.Debug("start add %v\n", changeSetEntry.Key)
		i.adds++
		err := i.processAdd(changeSetEntry)
		if err != nil {
			fmt.Printf("error processing add: %v\n", changeSetEntry)
			return true, err
		}
	case ChangeSetEntryType_Change:
		i.Debug("start change %v\n", changeSetEntry.Key)
		i.changes++
		err := i.processChange(changeSetEntry)
		if err != nil {
			fmt.Printf("error processing change: %v\n", changeSetEntry)
			return true, err
		}
	case ChangeSetEntryType_Delete:
		i.Debug("start delete %v\n", changeSetEntry.Key)
		i.deletes++
		err := i.processDelete(changeSetEntry)
		if err != nil {
			fmt.Printf("error processing delete: %v\n", changeSetEntry)
			return true, err
		}
	case ChangeSetEntryType_Terminator:
		i.Debug("start terminator\n")
		i.terminators++
		done, err := i.processTerminator()
		if err != nil {
			fmt.Printf("error processing terminator: %v\n", changeSetEntry)
			return true, err
		}
		if done {
			log.Info(fmt.Sprintf("timing: tapes: adds: %d, changes: %d, deletes: %d, terminators: %d, fastForwards: %d", i.adds, i.changes, i.deletes, i.terminators, i.fastForwards))
			return true, nil
		}
	default:
		panic("unknown change set entry type")
	}

	i.Debug("------------\n")

	return false, nil
}

// fastForwardAndCheckForTerminator returns true if the terminator is found i.e. nothing
// to fast forward to.  It returns false if there was data to jump forward to.
func (i *TapesProcessor) fastForwardAndCheckForTerminator(path []int) (bool, error) {
	i.fastForwards++
	nextJump, shouldStop := NextFastForwardPath(path)
	if shouldStop {
		i.stoppingEarly = true
		return true, nil
	}

	// always 0 length here because we want to find the exact match or next lexographically greater
	// record in the db
	prefix := PathToKeyBytes(nextJump, 0)

	found, err := i.interhashTape.FastForward(prefix)
	if err != nil {
		return false, err
	}

	return !found, nil
}

func (i *TapesProcessor) processAdd(currentCS ChangeSetEntry) error {
	currentOutput := i.outputTape.LastAdded()
	currentOutputPath := currentOutput.Path[:currentOutput.Path[256]]
	currentCSPath := currentCS.GetKeyPath()
	currentIH := i.interhashTape.Current()
	currentIHPath := currentIH.Path[:currentIH.Path[256]]
	if currentIH.HashType == IntermediateHashType_Value {
		currentIHPath = currentIH.GetKeyPath()
	}

	i.Debug("ih type: %v, ih path: %v, cs key: %v\n", currentIH.HashType, currentIHPath, currentCS.Key)

	// if there are no more interhashes left to process we can just add the value to the output tape
	if currentIH.HashType == IntermediateHashType_Terminator {
		i.outputTape.Push(InputTapeItem{Type: IntersInputType_Value, Key: currentCS.Key, Value: currentCS.Value})
		i.changeSetTape.Next()
		return nil
	}

	if isPrefix(currentIHPath, currentCSPath) {
		// skip this interhash as it is a subtree of the current changeset
		i.interhashTape.Next()
		return nil
	}

	var err error

	// rule 1.0
	if i.outputTape.size > 0 {
		// we have something on the tape to check
		if currentOutput.Type == IntersInputType_Value {
			// compare the two paths lexicographically
			lexicalResult := lexographicalCompareKeys(currentCS.Key, currentOutput.Key)

			// now write the entries back to the tape in the lexographical order and assert they aren't the same
			if lexicalResult == 0 {
				// this operation is not an add operation it should be a change
				return fmt.Errorf("add op for path %v which already exists", currentCS.Key.GetPath())
			} else if lexicalResult == -1 {
				// remove the current output from the tape as we're going to re-insert it
				i.outputTape.Pop()
				// change is on the left so it is written back first
				i.outputTape.Push(InputTapeItem{Type: IntersInputType_Value, Key: currentCS.Key, Value: currentCS.Value})
				i.outputTape.Push(InputTapeItem{Type: IntersInputType_Value, Key: currentOutput.Key, Value: currentOutput.Value})

				i.Debug("rule 1.0 - re-order and write latest output and CS in lexo order\n")

				i.changeSetTape.Next()
				return nil
			}
		}
	}

	// rule 1.1
	if currentIH.HashType == IntermediateHashType_Value {
		if isPrefix(currentIHPath, currentCSPath) {
			// now we need to find the sort order of the two paths and insert them to the output tape in that order
			lexResult := lexographhicalCheckPaths(currentCSPath, currentIHPath)
			if lexResult == 0 {
				// this operation is not an add operation it should be a change
				return fmt.Errorf("add op for path %v which already exists", currentCS.Key.GetPath())
			} else if lexResult == -1 {
				i.outputTape.Push(InputTapeItem{Type: IntersInputType_Value, Key: currentCS.Key, Value: currentCS.Value})
				i.outputTape.Push(InputTapeItem{Type: IntersInputType_Value, Key: currentIH.LeafKey, Value: currentIH.LeafValue})
			} else {
				i.outputTape.Push(InputTapeItem{Type: IntersInputType_Value, Key: currentIH.LeafKey, Value: currentIH.LeafValue})
				i.outputTape.Push(InputTapeItem{Type: IntersInputType_Value, Key: currentCS.Key, Value: currentCS.Value})
			}

			i.Debug("rule 1.1 - write value IH and CS in lexo order\n")

			i.changeSetTape.Next()
			_, err = i.interhashTape.Next()
			return err
		}
	}

	// rule 1.2
	if currentOutput.Type == IntersInputType_IntermediateHashBranch {
		if isPrefix(currentIHPath, currentOutputPath) {
			i.Debug("rule 1.2 - branch IH matching path skip IH\n")
			_, err = i.interhashTape.Next()
			return err
		}
	}

	// rule 1.3 - inter hash is to the left
	lexo := lexographhicalCheckPaths(currentIHPath, currentCSPath)
	pathShared := isPrefix(currentIHPath, currentCSPath)
	if pathShared && currentIH.HashType == IntermediateHashType_Branch {
		i.Debug("rule 1.3 - branch IH with shared path skip\n")
		i.interhashTape.Next()
		return nil
	} else if lexo == -1 {
		i.Debug("rule 1.3 - branch IH not sharing path but to left - write IH \n")
		if currentIH.HashType == IntermediateHashType_Branch {
			i.outputTape.Push(InputTapeItem{Type: IntersInputType_IntermediateHashBranch, Path: currentIH.Path, Hash: currentIH.Hash})
			_, err := i.fastForwardAndCheckForTerminator(currentIH.Path[:currentIH.Path[256]])
			return err
		} else if currentIH.HashType == IntermediateHashType_Value {
			i.outputTape.Push(InputTapeItem{Type: IntersInputType_Value, Key: currentIH.LeafKey, Value: currentIH.LeafValue})
			_, err := i.interhashTape.Next()
			return err
		}
	}

	// rule 1.4
	if lexo == 1 {
		i.Debug("rule 1.4 - CS to right - write CS and move to next CS\n")
		i.outputTape.Push(InputTapeItem{Type: IntersInputType_Value, Key: currentCS.Key, Value: currentCS.Value})
		i.changeSetTape.Next()
		return nil
	}

	return ErrNoHandlerForAdd
}

func (i *TapesProcessor) processChange(currentCS ChangeSetEntry) error {
	currentOutput := i.outputTape.LastAdded()
	currentOutputPath := currentOutput.Path[:currentOutput.Path[256]]
	currentCSPath := currentCS.GetKeyPath()
	currentIH := i.interhashTape.Current()
	currentIHPath := currentIH.Path[:currentIH.Path[256]]

	i.Debug("ih type: %v, ih path: %v, cs key: %v\n", currentIH.HashType, currentIHPath, currentCS.Key)

	// rule 2.1
	if i.outputTape.size > 0 {
		if currentOutput.Type == IntersInputType_IntermediateHashBranch {
			if isPrefix(currentIHPath, currentOutputPath) {
				i.Debug("rule 2.1 - shared path - skip IH \n")
				// skip if we are at interhash of a subtree, root of which is already on the tape
				i.interhashTape.Next()
				return nil
			}
		}
	}

	// rule 2.2
	if currentIH.HashType == IntermediateHashType_Value {
		if isPrefix(currentIHPath, currentCSPath) {
			// add the change set to the output tape and move on the change set tape and interhash tape
			i.Debug("rule 2.2 - value IH matching CS - write CS and move to next CS and IH\n")
			i.outputTape.Push(InputTapeItem{Type: IntersInputType_Value, Key: currentCS.Key, Value: currentCS.Value})
			i.changeSetTape.Next()
			_, err := i.interhashTape.Next()
			return err
		}
	}

	// rule 2.3
	lexo := lexographhicalCheckPaths(currentIHPath, currentCSPath)
	pathShared := isPrefix(currentIHPath, currentCSPath)
	if pathShared && currentIH.HashType == IntermediateHashType_Branch {
		i.Debug("rule 2.3 branch IH with shared path skip\n")
		i.interhashTape.Next()
		return nil
	} else if lexo == -1 {
		i.Debug("rule 2.3 - branch IH not sharing path but to left - write IH \n")
		if currentIH.HashType == IntermediateHashType_Branch {
			i.outputTape.Push(InputTapeItem{Type: IntersInputType_IntermediateHashBranch, Path: currentIH.Path, Hash: currentIH.Hash})
			_, err := i.fastForwardAndCheckForTerminator(currentIH.Path[:currentIH.Path[256]])
			return err
		} else if currentIH.HashType == IntermediateHashType_Value {
			i.outputTape.Push(InputTapeItem{Type: IntersInputType_Value, Key: currentIH.LeafKey, Value: currentIH.LeafValue})
			_, err := i.interhashTape.Next()
			return err
		}
	}

	// rule 2.5 - overshoot detection
	if lexo == 1 {
		// IH is on the right, we cannot be in this position here for a change operation as we should have already hit the change itself
		// so assert and quit
		i.Debug("rule 2.5 - overshoot\n")
		return ErrOvershoot
	}

	// should never be reached we either found the IH or were to the left or right of it, we should have handled all cases
	// by this point
	return ErrNoHandlerForChange
}

func (i *TapesProcessor) processDelete(currentCS ChangeSetEntry) error {
	currentOutput := i.outputTape.LastAdded()
	currentOutputPath := currentOutput.Path[:currentOutput.Path[256]]
	currentCSPath := currentCS.GetKeyPath()
	currentIH := i.interhashTape.Current()
	currentIHPath := currentIH.Path[:currentIH.Path[256]]
	if currentIH.HashType == IntermediateHashType_Value {
		currentIHPath = currentIH.GetKeyPath()
	}

	i.Debug("ih type: %v, ih path: %v, cs key: %v\n", currentIH.HashType, currentIHPath, currentCS.Key)

	// rule 3.1
	if i.outputTape.size > 0 {
		if currentOutput.Type == IntersInputType_IntermediateHashBranch {
			if isPrefix(currentIHPath, currentOutputPath) {
				i.Debug("rule 3.1 - shared path - skip IH \n")
				i.interhashTape.Next()
				return nil
			}
		}
	}

	// rule 3.2
	if currentIH.HashType == IntermediateHashType_Value {
		if isPrefix(currentIHPath, currentCSPath) {
			i.Debug("rule 3.2 - value IH matching CS - move to next CS and IH\n")
			// this is a special case where we know we need to delete a leaf node specifically and nothing else is going to handle this deletion.
			// the interhash here will never be added to the DB or come from it, but it will be serialised into the ETL collector by the stack algorithm and
			// be treated like it was from the DB at ETL Load time.
			i.outputTape.Push(InputTapeItem{Type: IntersInputType_DeletedIntermediateHash, Path: currentIH.Path, Hash: currentIH.Hash, Key: currentIH.LeafKey, Value: currentIH.LeafValue})
			i.changeSetTape.Next()
			i.interhashTape.Next()
			return nil
		}
	}

	// rule 3.3
	lexo := lexographhicalCheckPaths(currentIHPath, currentCSPath)
	pathShared := isPrefix(currentIHPath, currentCSPath)
	if pathShared && currentIH.HashType == IntermediateHashType_Branch {
		i.Debug("rule 3.3 - branch IH with shared path skip\n")
		i.interhashTape.Next()
		return nil
	} else if lexo == -1 {
		i.Debug("rule 3.3 - branch IH not sharing path but to left - write IH \n")
		if currentIH.HashType == IntermediateHashType_Branch {
			i.outputTape.Push(InputTapeItem{Type: IntersInputType_IntermediateHashBranch, Path: currentIH.Path, Hash: currentIH.Hash})
			_, err := i.fastForwardAndCheckForTerminator(currentIH.Path[:currentIH.Path[256]])
			return err
		} else if currentIH.HashType == IntermediateHashType_Value {
			i.outputTape.Push(InputTapeItem{Type: IntersInputType_Value, Key: currentIH.LeafKey, Value: currentIH.LeafValue})
			_, err := i.interhashTape.Next()
			return err
		}
	}

	return ErrNoHandlerForDelete
}

func (i *TapesProcessor) processTerminator() (bool, error) {
	currentOutput := i.outputTape.LastAdded()
	currentIH := i.interhashTape.Current()
	currentOutputPath := currentOutput.Path[:currentOutput.Path[256]]
	currentIHPath := currentIH.Path[:currentIH.Path[256]]

	i.Debug("ih type: %v, ih path: %v\n", currentIH.HashType, currentIHPath)

	// rule 4.0
	if currentIH.HashType == IntermediateHashType_Terminator {
		i.Debug("rule 4.0 - terminator IH - done\n")
		return true, nil
	}

	// rule 4.1
	if currentOutput.Type == IntersInputType_IntermediateHashBranch {
		if isPrefix(currentIHPath, currentOutputPath) {
			i.Debug("rule 4.1 - shared path - skip IH \n")

			stop, err := i.fastForwardAndCheckForTerminator(currentIH.Path[:currentIH.Path[256]])
			if err != nil {
				return true, err
			}

			return stop, nil

		}
	}

	// rule 4.2
	if currentIH.HashType != IntermediateHashType_Terminator {
		i.Debug("rule 4.2 - write IH \n")
		if currentIH.HashType == IntermediateHashType_Branch {
			// todo: this block of code should really be handled in the call to fastForwardAndCheckForTerminator and
			// that should put a terminator on the tape or some flag that we're done
			if currentIH.Path[256] == 0 {
				// this special case means mdbx was asked to jump to a record that doesn't exist so we need to treat this as a terminator
				i.Debug("rule 4.2 - special case - terminator IH - done\n")
				return true, nil
			}
			i.outputTape.Push(InputTapeItem{Type: IntersInputType_IntermediateHashBranch, Path: currentIH.Path, Hash: currentIH.Hash})
			_, err := i.fastForwardAndCheckForTerminator(currentIH.Path[:currentIH.Path[256]])
			return false, err
		} else if currentIH.HashType == IntermediateHashType_Value {
			i.outputTape.Push(InputTapeItem{Type: IntersInputType_Value, Key: currentIH.LeafKey, Value: currentIH.LeafValue})
			_, err := i.interhashTape.Next()
			return false, err
		}
	}

	// rule 4.3
	if currentIH.HashType == IntermediateHashType_Terminator {
		i.Debug("rule 4.3 - terminator IH - done\n")
		// we have reached the end of the interhash tape
		return true, nil
	}

	return false, nil
}

func (i *TapesProcessor) SetDebug(debug bool) {
	i.debug = debug
}

func (i *TapesProcessor) Debug(format string, args ...any) {
	if i.debug {
		fmt.Printf(format, args...)
	}
}
