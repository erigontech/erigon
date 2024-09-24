package smt

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"github.com/ledgerwatch/erigon/zk"
	"github.com/ledgerwatch/log/v3"
)

//////////////////////////////////////////////////////////////////////////////
//	Since we have all the kv pairs and they are all unique, we can create a tree under the assumption
//	that there will be only inserts (no updates and deletes) in the tree.
//
//	With that assumption we sort the keys and build a binary tree representing the SMT. This way we
//	don't have to calculate and save to the db the hashes of all the nodes that are reordered because
//	of an insert somewhere below them. This saves a lot of time, because those hash recalculations are
//	growing exponentially with the tree size / amount of values.
//
//	Another thing we have optimized is that, since our keys are sorted ASC, we can be sure that the
//	"left" part of the tree is at its final place. Because of that, every time we insert a node on the
//	right side of something, we can hash, save and delete it from the temp binary tree. We can also
//	delete the values from the KV map, so we actually get the same or smaller memory usage.
//////////////////////////////////////////////////////////////////////////////

// sorts the keys and builds a binary tree from left
// this makes it so the left part of a node can be deleted once it's right part is inserted
// this is because the left part is at its final spot
// when deleting nodes, go down to the leaf and create and save hashes in the SMT
func (s *SMT) GenerateFromKVBulk(ctx context.Context, logPrefix string, nodeKeys []utils.NodeKey) ([4]uint64, error) {
	s.clearUpMutex.Lock()
	defer s.clearUpMutex.Unlock()

	log.Info(fmt.Sprintf("[%s] Building temp binary tree started", logPrefix))

	totalKeysCount := len(nodeKeys)

	log.Info(fmt.Sprintf("[%s] Total values to insert: %d", logPrefix, totalKeysCount))

	log.Info(fmt.Sprintf("[%s] Sorting keys...", logPrefix))
	sortStartTime := time.Now()

	//TODO: can sort without converting
	utils.SortNodeKeysBitwiseAsc(nodeKeys)

	sortTotalTime := time.Since(sortStartTime)
	log.Info(fmt.Sprintf("[%s] Keys sorted in %v", logPrefix, sortTotalTime))

	rootNode := SmtNode{
		leftHash: [4]uint64{},
		node0:    nil,
		node1:    nil,
	}

	//start a progress checker
	progressChan, stopProgressPrinter := zk.ProgressPrinterWithoutValues(fmt.Sprintf("[%s] SMT regenerate progress", logPrefix), uint64(totalKeysCount)*2)
	defer stopProgressPrinter()
	progressChan <- uint64(totalKeysCount)

	insertedKeysCount := uint64(0)

	maxReachedLevel := 0

	deletesWorker := utils.NewWorker(ctx, "smt_save_finished", 1000)

	// start a worker to delete finished parts of the tree and return values to save to the db
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		deletesWorker.DoWork()
		wg.Done()
	}()

	tempTreeBuildStart := time.Now()
	leafValueMap := sync.Map{}

	var err error
	for _, k := range nodeKeys {
		// split the key
		keys := k.GetPath()
		v, err := s.Db.GetAccountValue(k)
		if err != nil {
			return [4]uint64{}, err
		}
		leafValueMap.Store(k, v)

		// find last node
		siblings, level := rootNode.findLastNode(keys)

		//if last found node is leaf
		//1. create nodes till the different bit
		//2. insert both leafs there
		//3. save left leaf with hash
		if len(siblings) > 0 && siblings[level].isLeaf() {
			//take the leaf to insert it later where it needs to be
			leaf0 := siblings[len(siblings)-1]
			bit := keys[level]

			//add node, this should always occur in this scenario

			///take the node above the leaf, so we can set its left/right and continue the tree
			var upperNode *SmtNode
			if level == 0 {
				upperNode = &rootNode
			} else {
				upperNode = siblings[len(siblings)-2]
			}

			///set the new node depending on the bit and insert it into siblings
			///now it is the last sibling
			newNode := &SmtNode{}
			if bit == 0 {
				upperNode.node0 = newNode
			} else {
				upperNode.node1 = newNode
			}
			siblings = append(siblings, newNode)
			level++

			//create nodes till the different bit
			level2 := 0
			for leaf0.rKey[level2] == keys[level+level2] {
				newNode := SmtNode{}
				if keys[level+level2] == 0 {
					siblings[len(siblings)-1].node0 = &newNode
				} else {
					siblings[len(siblings)-1].node1 = &newNode
				}
				siblings = append(siblings, &newNode)
				level2++
			}

			//sanity check - new leaf should be on the right side
			//otherwise something went wrong
			if leaf0.rKey[level2] != 0 || keys[level2+level] != 1 {
				return [4]uint64{}, fmt.Errorf(
					"leaf insert error. new leaf should be on the right of the old, oldLeaf: %v, newLeaf: %v",
					append(keys[:level+1], leaf0.rKey[level2:]...),
					keys,
				)
			}

			//insert both leaf nodes in last node
			// r key is reduced by how many nodes were added
			leaf0.rKey = leaf0.rKey[level2+1:]
			siblings[len(siblings)-1].node0 = leaf0
			siblings[len(siblings)-1].node1 = &SmtNode{
				rKey: keys[level+level2+1:],
			}

			nodeToDelFrom := siblings[len(siblings)-1]
			pathToDeleteFrom := make([]int, level+level2+1, 256)
			copy(pathToDeleteFrom, keys[:level+level2])
			pathToDeleteFrom[level+level2] = 0

			jobResult := utils.NewCalcAndPrepareJobResult(s.Db)
			//hash, save and delete left leaf
			deleteFunc := func() utils.JobResult {
				leftHash, err := nodeToDelFrom.node0.deleteTreeNoSave(pathToDeleteFrom, &leafValueMap, jobResult.KvMap, jobResult.LeafsKvMap)
				if err != nil {
					jobResult.Err = err
					return jobResult
				}

				nodeToDelFrom.leftHash = leftHash
				nodeToDelFrom.node0 = nil

				return jobResult
			}
			deletesWorker.AddJob(deleteFunc)

			if maxReachedLevel < level+level2+1 {
				maxReachedLevel = level + level2 + 1
			}
		} else
		// if it is not leaf
		// insert the new leaf on the right side
		// save left side
		{
			var upperNode *SmtNode
			//upper node is root node
			if len(siblings) == 0 {
				upperNode = &rootNode
			} else {
				//root is not counted as level, so inserting under it will always be zero
				//in other cases increment level, so it corresponds to the new step down
				level++
				upperNode = siblings[len(siblings)-1]
			}

			newNode := &SmtNode{
				rKey: keys[level+1:],
			}

			// this is case for 1 leaf inserted to the left of the root node
			if len(siblings) == 0 && keys[0] == 0 {
				if upperNode.node0 != nil {
					return [4]uint64{}, fmt.Errorf("tried to override left node")
				}
				upperNode.node0 = newNode
			} else {
				//sanity check
				//found node should be on the left side
				//the new leaf should be on the right side
				//otherwise something went wrong
				if upperNode.node1 != nil || keys[level] != 1 {
					return [4]uint64{}, fmt.Errorf(
						"leaf insert error. new should be on the right of the found node, foundNode: %v, newLeafKey: %v",
						upperNode.node1,
						keys,
					)
				}

				upperNode.node1 = newNode

				//hash, save and delete left leaf
				if upperNode.node0 != nil {
					nodeToDelFrom := upperNode
					pathToDeleteFrom := make([]int, level+1, 256)
					copy(pathToDeleteFrom, keys[:level])
					pathToDeleteFrom[level] = 0

					jobResult := utils.NewCalcAndPrepareJobResult(s.Db)

					// get all leaf keys so we can then get all needed values and pass them
					// this is needed because w can't read from the db in another routine
					deleteFunc := func() utils.JobResult {
						leftHash, err := nodeToDelFrom.node0.deleteTreeNoSave(pathToDeleteFrom, &leafValueMap, jobResult.KvMap, jobResult.LeafsKvMap)

						if err != nil {
							jobResult.Err = err
							return jobResult
						}
						nodeToDelFrom.leftHash = leftHash
						nodeToDelFrom.node0 = nil
						return jobResult
					}

					deletesWorker.AddJob(deleteFunc)
				}
			}

			if maxReachedLevel < level+1 {
				maxReachedLevel = level + 1
			}
		}

		if err := runSaveLoop(deletesWorker.GetJobResultsChannel()); err != nil {
			return [4]uint64{}, err
		}

		insertedKeysCount++
		progressChan <- uint64(totalKeysCount) + insertedKeysCount
	}
	deletesWorker.Stop()

	wg.Wait()

	// wait and save all jobs
	if err := runSaveLoop(deletesWorker.GetJobResultsChannel()); err != nil {
		return [4]uint64{}, err
	}

	s.updateDepth(maxReachedLevel)

	tempTreeBuildTime := time.Since(tempTreeBuildStart)

	log.Info(fmt.Sprintf("[%s] Finished the temp tree build in %v, hashing and saving the result...", logPrefix, tempTreeBuildTime))

	//special case where no values were inserted
	if rootNode.isLeaf() {
		return [4]uint64{}, nil
	}

	//if the root node has only one branch, that branch should become the root node
	var pathToDeleteFrom []int
	if len(nodeKeys) == 1 {
		if rootNode.node1 == nil {
			rootNode = *rootNode.node0
			pathToDeleteFrom = append(pathToDeleteFrom, 0)
		} else if rootNode.node0 == nil && utils.IsArrayUint64Empty(rootNode.leftHash[:]) {
			rootNode = *rootNode.node1
			pathToDeleteFrom = append(pathToDeleteFrom, 1)
		}
	}

	//if the branch is a leaf, the rkey is the whole key
	if rootNode.isLeaf() {
		newRkey := []int{pathToDeleteFrom[0]}
		pathToDeleteFrom = []int{}
		newRkey = append(newRkey, rootNode.rKey...)
		rootNode.rKey = newRkey
	}

	finalRoot, err := rootNode.deleteTree(pathToDeleteFrom, s, &leafValueMap)
	if err != nil {
		return [4]uint64{}, err
	}

	if err := s.setLastRoot(finalRoot); err != nil {
		return [4]uint64{}, err
	}

	return finalRoot, nil
}

func runSaveLoop(jobResultsChannel chan utils.JobResult) error {
	for {
		select {
		case result := <-jobResultsChannel:
			if result.GetError() != nil {
				return result.GetError()
			}

			if err := result.Save(); err != nil {
				return err
			}
		default:
			return nil
		}
	}
}

type SmtNode struct {
	rKey     []int
	leftHash [4]uint64
	node0    *SmtNode
	node1    *SmtNode
}

func (n *SmtNode) isLeaf() bool {
	return n.node0 == nil && n.node1 == nil && utils.IsArrayUint64Empty(n.leftHash[:])
}

// go down the tree and return last matching node and it's level
// returns level 0 and empty siblings if the last node is the root node
func (n *SmtNode) findLastNode(keys []int) ([]*SmtNode, int) {
	var siblings []*SmtNode
	level := 0
	currentNode := n

	for {
		bit := keys[level]
		if bit == 0 {
			currentNode = currentNode.node0
		} else {
			currentNode = currentNode.node1
		}

		if currentNode == nil {
			if level > 0 {
				level--
			}
			break
		}

		siblings = append(siblings, currentNode)
		if currentNode.isLeaf() {
			break
		}

		level++

	}

	return siblings, level
}

func (n *SmtNode) deleteTreeNoSave(keyPath []int, leafValueMap *sync.Map, kvMapOfValuesToSave map[[4]uint64]utils.NodeValue12, kvMapOfLeafValuesToSave map[[4]uint64][4]uint64) ([4]uint64, error) {
	if n.isLeaf() {
		fullKey := append(keyPath, n.rKey...)
		k, err := utils.NodeKeyFromPath(fullKey)
		if err != nil {
			return [4]uint64{}, err
		}

		v, ok := leafValueMap.LoadAndDelete(k)
		if !ok {
			return [4]uint64{}, fmt.Errorf("value not found for key %v", k)
		}
		accoutnValue := v.(utils.NodeValue8)

		newKey := utils.RemoveKeyBits(k, len(keyPath))
		//hash and save leaf
		newValH, newValHV, newLeafHash, newLeafHashV := createNewLeafNoSave(&newKey, &accoutnValue)
		kvMapOfValuesToSave[*newValH] = *newValHV
		kvMapOfValuesToSave[*newLeafHash] = *newLeafHashV
		kvMapOfLeafValuesToSave[*newLeafHash] = k

		return *newLeafHash, nil
	}

	var totalHash utils.NodeValue8

	if n.node0 != nil {
		if !utils.IsArrayUint64Empty(n.leftHash[:]) {
			return [4]uint64{}, fmt.Errorf("node has previously deleted left part")
		}
		localKeyPath := append(keyPath, 0)
		leftHash, err := n.node0.deleteTreeNoSave(localKeyPath, leafValueMap, kvMapOfValuesToSave, kvMapOfLeafValuesToSave)
		if err != nil {
			return [4]uint64{}, err
		}

		n.leftHash = leftHash
		n.node0 = nil
	}

	if n.node1 != nil {
		localKeyPath := append(keyPath, 1)
		rightHash, err := n.node1.deleteTreeNoSave(localKeyPath, leafValueMap, kvMapOfValuesToSave, kvMapOfLeafValuesToSave)
		if err != nil {
			return [4]uint64{}, err
		}
		totalHash.SetHalfValue(rightHash, 1)

		n.node1 = nil
	}

	totalHash.SetHalfValue(n.leftHash, 0)

	newRoot, v := utils.HashKeyAndValueByPointers(totalHash.ToUintArrayByPointer(), &utils.BranchCapacity)
	kvMapOfValuesToSave[*newRoot] = *v

	return *newRoot, nil
}

func (n *SmtNode) deleteTree(keyPath []int, s *SMT, leafValueMap *sync.Map) (newRoot [4]uint64, err error) {
	jobResult := utils.NewCalcAndPrepareJobResult(s.Db)

	if newRoot, err = n.deleteTreeNoSave(keyPath, leafValueMap, jobResult.KvMap, jobResult.LeafsKvMap); err != nil {
		return [4]uint64{}, err
	}

	jobResult.Save()

	return newRoot, nil
}

func createNewLeafNoSave(rkey *utils.NodeKey, v *utils.NodeValue8) (newValH *[4]uint64, newValHV *utils.NodeValue12, newLeafHash *[4]uint64, newLeafHashV *utils.NodeValue12) {
	//hash and save leaf
	newValH, newValHV = utils.HashKeyAndValueByPointers(v.ToUintArrayByPointer(), &utils.BranchCapacity)
	newLeafHash, newLeafHashV = utils.HashKeyAndValueByPointers(utils.ConcatArrays4ByPointers(rkey.AsUint64Pointer(), newValH), &utils.LeafCapacity)
	return newValH, newValHV, newLeafHash, newLeafHashV
}
