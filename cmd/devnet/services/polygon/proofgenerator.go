// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package polygon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon-lib/trie"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/erigontech/erigon/cmd/devnet/devnet"
	"github.com/erigontech/erigon/execution/abi/bind"
	bortypes "github.com/erigontech/erigon/polygon/bor/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/requests"
)

var ErrTokenIndexOutOfRange = errors.New("index is grater than the number of tokens in transaction")

type ProofGenerator struct {
	heimdall *Heimdall
}

func NewProofGenerator() *ProofGenerator {
	return &ProofGenerator{}
}

func (pg *ProofGenerator) NodeCreated(ctx context.Context, node devnet.Node) {

	if pg.heimdall == nil {
		if strings.HasPrefix(node.GetName(), "bor") {
			if network := devnet.CurrentNetwork(ctx); network != nil {
				for _, service := range network.Services {
					if heimdall, ok := service.(*Heimdall); ok {
						pg.heimdall = heimdall
					}
				}
			}
		}
	}
}

func (pg *ProofGenerator) NodeStarted(ctx context.Context, node devnet.Node) {
}

func (pg *ProofGenerator) Start(ctx context.Context) error {
	return nil
}

func (pg *ProofGenerator) Stop() {
}

func (pg *ProofGenerator) GenerateExitPayload(ctx context.Context, burnTxHash common.Hash, eventSignature common.Hash, tokenIndex int) ([]byte, error) {
	logger := devnet.Logger(ctx)

	if pg.heimdall == nil || pg.heimdall.rootChainBinding == nil {
		return nil, errors.New("ProofGenerator not initialized")
	}

	logger.Info("Checking for checkpoint status", "hash", burnTxHash)

	isCheckpointed, err := pg.isCheckPointed(ctx, burnTxHash)

	if err != nil {
		return nil, fmt.Errorf("error getting burn transaction: %w", err)
	}

	if !isCheckpointed {
		return nil, errors.New("eurn transaction has not been checkpointed yet")
	}

	// build payload for exit
	result, err := pg.buildPayloadForExit(ctx, burnTxHash, eventSignature, tokenIndex)

	if err != nil {
		if errors.Is(err, ErrTokenIndexOutOfRange) {
			return nil, fmt.Errorf("block not included: %w", err)
		}

		return nil, errors.New("null receipt received")
	}

	if len(result) == 0 {
		return nil, errors.New("null result received")
	}

	return result, nil
}

func (pg *ProofGenerator) getChainBlockInfo(ctx context.Context, burnTxHash common.Hash) (uint64, uint64, error) {
	childNode := devnet.SelectBlockProducer(devnet.WithCurrentNetwork(ctx, networkname.BorDevnet))

	var wg sync.WaitGroup

	var lastChild *big.Int
	var burnTransaction *ethapi.RPCTransaction
	var err [2]error

	// err group
	wg.Add(1)
	go func() {
		defer wg.Done()
		lastChild, err[0] = pg.heimdall.rootChainBinding.GetLastChildBlock(&bind.CallOpts{})
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		burnTransaction, err[1] = childNode.GetTransactionByHash(burnTxHash)
	}()

	wg.Wait()

	for _, err := range err {
		if err != nil {
			return 0, 0, err
		}
	}

	return lastChild.Uint64(), burnTransaction.BlockNumber.Uint64(), nil
}

// lastchild block is greater equal to transacton block number;
func (pg *ProofGenerator) isCheckPointed(ctx context.Context, burnTxHash common.Hash) (bool, error) {
	lastChildBlockNum, burnTxBlockNum, err := pg.getChainBlockInfo(ctx, burnTxHash)

	if err != nil {
		return false, err
	}

	return lastChildBlockNum >= burnTxBlockNum, nil
}

func (pg *ProofGenerator) buildPayloadForExit(ctx context.Context, burnTxHash common.Hash, logEventSig common.Hash, index int) ([]byte, error) {

	node := devnet.SelectBlockProducer(ctx)

	if node == nil {
		return nil, errors.New("no node available")
	}

	if index < 0 {
		return nil, errors.New("index must not negative")
	}

	var receipt *types.Receipt
	var block *requests.Block

	// step 1 - Get Block number from transaction hash
	lastChildBlockNum, txBlockNum, err := pg.getChainBlockInfo(ctx, burnTxHash)

	if err != nil {
		return nil, err
	}

	if lastChildBlockNum < txBlockNum {
		return nil, errors.New("burn transaction has not been checkpointed as yet")
	}

	// step 2-  get transaction receipt from txhash and
	// block information from block number

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(2)

	g.Go(func() error {
		var err error
		receipt, err = node.GetTransactionReceipt(gctx, burnTxHash)
		return err
	})

	g.Go(func() error {
		var err error
		block, err = node.GetBlockByNumber(gctx, rpc.AsBlockNumber(txBlockNum), true)
		return err
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// step 3 - get information about block saved in parent chain
	// step 4 - build block proof
	var rootBlockNumber uint64
	var start, end uint64

	rootBlockNumber, start, end, err = pg.getRootBlockInfo(txBlockNum)

	if err != nil {
		return nil, err
	}

	blockProofs, err := getBlockProofs(ctx, node, txBlockNum, start, end)

	if err != nil {
		return nil, err
	}

	// step 5- create receipt proof
	receiptProof, err := getReceiptProof(ctx, node, receipt, block, nil)

	if err != nil {
		return nil, err
	}

	// step 6 - encode payload, convert into hex
	var logIndex int

	if index > 0 {
		logIndices := getAllLogIndices(logEventSig, receipt)

		if index >= len(logIndices) {
			return nil, ErrTokenIndexOutOfRange
		}

		logIndex = logIndices[index]
	} else {
		logIndex = getLogIndex(logEventSig, receipt)
	}

	if logIndex < 0 {
		return nil, errors.New("log not found in receipt")
	}

	parentNodesBytes, err := rlp.EncodeToBytes(receiptProof.parentNodes)

	if err != nil {
		return nil, err
	}

	return rlp.EncodeToBytes(
		[]interface{}{
			rootBlockNumber,
			hexutil.Encode(bytes.Join(blockProofs, []byte{})),
			block.Number.Uint64(),
			block.Time,
			hexutil.Encode(block.TxHash[:]),
			hexutil.Encode(block.ReceiptHash[:]),
			hexutil.Encode(getReceiptBytes(receipt)), //rpl encoded
			hexutil.Encode(parentNodesBytes),
			hexutil.Encode(append([]byte{0}, receiptProof.path...)),
			logIndex,
		})
}

type receiptProof struct {
	blockHash   common.Hash
	parentNodes [][]byte
	root        []byte
	path        []byte
	value       interface{}
}

func getReceiptProof(ctx context.Context, node requests.RequestGenerator, receipt *types.Receipt, block *requests.Block, receipts []*types.Receipt) (*receiptProof, error) {
	stateSyncTxHash := bortypes.ComputeBorTxHash(block.Number.Uint64(), block.Hash)
	receiptsTrie := trie.New(trie.EmptyRoot)

	if len(receipts) == 0 {
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(len(block.Transactions))

		var lock sync.Mutex

		for _, transaction := range block.Transactions {
			if transaction.Hash == stateSyncTxHash {
				// ignore if txn hash is bor state-sync tx
				continue
			}

			hash := transaction.Hash
			g.Go(func() error {
				receipt, err := node.GetTransactionReceipt(gctx, hash)

				if err != nil {
					return err
				}

				path, _ := rlp.EncodeToBytes(receipt.TransactionIndex)
				rawReceipt := getReceiptBytes(receipt)
				lock.Lock()
				defer lock.Unlock()
				receiptsTrie.Update(path, rawReceipt)

				return nil
			})
		}

		if err := g.Wait(); err != nil {
			return nil, err
		}
	} else {
		for _, receipt := range receipts {
			path, _ := rlp.EncodeToBytes(receipt.TransactionIndex)
			rawReceipt := getReceiptBytes(receipt)
			receiptsTrie.Update(path, rawReceipt)
		}
	}

	path, _ := rlp.EncodeToBytes(receipt.TransactionIndex)
	result, parents, ok := receiptsTrie.FindPath(path)

	if !ok {
		return nil, errors.New("node does not contain the key")
	}

	var nodeValue any

	if isTypedReceipt(receipt) {
		nodeValue = result
	} else {
		rlp.DecodeBytes(result, nodeValue)
	}

	return &receiptProof{
		blockHash:   receipt.BlockHash,
		parentNodes: parents,
		root:        block.ReceiptHash[:],
		path:        path,
		value:       nodeValue,
	}, nil
}

func getBlockProofs(ctx context.Context, node requests.RequestGenerator, blockNumber, startBlock, endBlock uint64) ([][]byte, error) {
	merkleTreeDepth := int(math.Ceil(math.Log2(float64(endBlock - startBlock + 1))))

	// We generate the proof root down, whereas we need from leaf up
	var reversedProof [][]byte

	offset := startBlock
	targetIndex := blockNumber - offset
	leftBound := uint64(0)
	rightBound := endBlock - offset

	//   console.log("Searching for", targetIndex);
	for depth := 0; depth < merkleTreeDepth; depth++ {
		nLeaves := uint64(2) << (merkleTreeDepth - depth)

		// The pivot leaf is the last leaf which is included in the left subtree
		pivotLeaf := leftBound + nLeaves/2 - 1

		if targetIndex > pivotLeaf {
			// Get the root hash to the merkle subtree to the left
			newLeftBound := pivotLeaf + 1
			subTreeMerkleRoot, err := node.GetRootHash(ctx, offset+leftBound, offset+pivotLeaf)

			if err != nil {
				return nil, err
			}

			reversedProof = append(reversedProof, subTreeMerkleRoot[:])
			leftBound = newLeftBound
		} else {
			// Things are more complex when querying to the right.
			// Root hash may come some layers down so we need to build a full tree by padding with zeros
			// Some trees may be completely empty

			var newRightBound uint64

			if rightBound <= pivotLeaf {
				newRightBound = rightBound
			} else {
				newRightBound = pivotLeaf
			}

			// Expect the merkle tree to have a height one less than the current layer
			expectedHeight := merkleTreeDepth - (depth + 1)
			if rightBound <= pivotLeaf {
				// Tree is empty so we repeatedly hash zero to correct height
				subTreeMerkleRoot := recursiveZeroHash(expectedHeight)
				reversedProof = append(reversedProof, subTreeMerkleRoot[:])
			} else {
				// Height of tree given by RPC node
				subTreeHeight := int(math.Ceil(math.Log2(float64(rightBound - pivotLeaf))))

				// Find the difference in height between this and the subtree we want
				heightDifference := expectedHeight - subTreeHeight

				// For every extra layer we need to fill 2*n leaves filled with the merkle root of a zero-filled Merkle tree
				// We need to build a tree which has heightDifference layers

				// The first leaf will hold the root hash as returned by the RPC
				remainingNodesHash, err := node.GetRootHash(ctx, offset+pivotLeaf+1, offset+rightBound)

				if err != nil {
					return nil, err
				}

				// The remaining leaves will hold the merkle root of a zero-filled tree of height subTreeHeight
				leafRoots := recursiveZeroHash(subTreeHeight)

				// Build a merkle tree of correct size for the subtree using these merkle roots
				var leafCount int

				if heightDifference > 0 {
					leafCount = 2 << heightDifference
				} else {
					leafCount = 1
				}

				leaves := make([]interface{}, leafCount)

				leaves[0] = remainingNodesHash[:]

				for i := 1; i < len(leaves); i++ {
					leaves[i] = leafRoots[:]
				}

				subTreeMerkleRoot, err := merkle_tree.HashTreeRoot(leaves...)

				if err != nil {
					return nil, err
				}

				reversedProof = append(reversedProof, subTreeMerkleRoot[:])
			}

			rightBound = newRightBound
		}
	}

	for i, j := 0, len(reversedProof)-1; i < j; i, j = i+1, j-1 {
		reversedProof[i], reversedProof[j] = reversedProof[j], reversedProof[i]
	}

	return reversedProof, nil
}

func recursiveZeroHash(n int) common.Hash {
	if n == 0 {
		return common.Hash{}
	}

	subHash := recursiveZeroHash(n - 1)
	bytes, _ := rlp.EncodeToBytes([]common.Hash{subHash, subHash})
	return crypto.Keccak256Hash(bytes)
}

func getAllLogIndices(logEventSig common.Hash, receipt *types.Receipt) []int {
	var logIndices []int

	switch logEventSig.Hex() {
	case "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef":
	case "0xf94915c6d1fd521cee85359239227480c7e8776d7caf1fc3bacad5c269b66a14":
		for index, log := range receipt.Logs {
			if log.Topics[0] == logEventSig &&
				log.Topics[2] == zeroHash {
				logIndices = append(logIndices, index)
			}
		}
	case "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62":
	case "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb":
		for index, log := range receipt.Logs {
			if log.Topics[0] == logEventSig &&
				log.Topics[3] == zeroHash {
				logIndices = append(logIndices, index)
			}
		}

	case "0xf871896b17e9cb7a64941c62c188a4f5c621b86800e3d15452ece01ce56073df":
		for index, log := range receipt.Logs {
			if strings.EqualFold(hexutil.Encode(log.Topics[0][:]), "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef") &&
				log.Topics[2] == zeroHash {
				logIndices = append(logIndices, index)
			}
		}

	default:
		for index, log := range receipt.Logs {
			if log.Topics[0] == logEventSig {
				logIndices = append(logIndices, index)
			}
		}
	}

	return logIndices
}

func getLogIndex(logEventSig common.Hash, receipt *types.Receipt) int {
	switch logEventSig.Hex() {
	case "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef":
	case "0xf94915c6d1fd521cee85359239227480c7e8776d7caf1fc3bacad5c269b66a14":
		for index, log := range receipt.Logs {
			if log.Topics[0] == logEventSig &&
				log.Topics[2] == zeroHash {
				return index
			}
		}

	case "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62":
	case "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb":
		for index, log := range receipt.Logs {
			if log.Topics[0] == logEventSig &&
				log.Topics[3] == zeroHash {
				return index
			}
		}

	default:
		for index, log := range receipt.Logs {
			if log.Topics[0] == logEventSig {
				return index
			}
		}
	}

	return -1
}

func (pg *ProofGenerator) getRootBlockInfo(txBlockNumber uint64) (rootBlockNumber uint64, start uint64, end uint64, err error) {
	// find in which block child was included in parent
	rootBlockNumber, err = pg.findRootBlockFromChild(txBlockNumber)

	if err != nil {
		return 0, 0, 0, err
	}

	headerBlock, err := pg.heimdall.rootChainBinding.HeaderBlocks(&bind.CallOpts{}, new(big.Int).SetUint64(rootBlockNumber))

	if err != nil {
		return 0, 0, 0, err
	}

	return rootBlockNumber, headerBlock.Start.Uint64(), headerBlock.End.Uint64(), nil
}

const checkPointInterval = uint64(10000)

func (pg *ProofGenerator) findRootBlockFromChild(childBlockNumber uint64) (uint64, error) {
	// first checkpoint id = start * 10000
	start := uint64(1)

	currentHeaderBlock, err := pg.heimdall.rootChainBinding.CurrentHeaderBlock(&bind.CallOpts{})

	if err != nil {
		return 0, err
	}

	end := currentHeaderBlock.Uint64() / checkPointInterval

	// binary search on all the checkpoints to find the checkpoint that contains the childBlockNumber
	var ans uint64

	for start <= end {
		if start == end {
			ans = start
			break
		}

		mid := (start + end) / 2
		headerBlock, err := pg.heimdall.rootChainBinding.HeaderBlocks(&bind.CallOpts{}, new(big.Int).SetUint64(mid*checkPointInterval))

		if err != nil {
			return 0, err
		}
		headerStart := headerBlock.Start.Uint64()
		headerEnd := headerBlock.End.Uint64()

		if headerStart <= childBlockNumber && childBlockNumber <= headerEnd {
			// if childBlockNumber is between the upper and lower bounds of the headerBlock, we found our answer
			ans = mid
			break
		} else if headerStart > childBlockNumber {
			// childBlockNumber was checkpointed before this header
			end = mid - 1
		} else if headerEnd < childBlockNumber {
			// childBlockNumber was checkpointed after this header
			start = mid + 1
		}
	}

	return ans * checkPointInterval, nil
}

func isTypedReceipt(receipt *types.Receipt) bool {
	return receipt.Status != 0 && receipt.Type != 0
}

func getReceiptBytes(receipt *types.Receipt) []byte {
	buffer := &bytes.Buffer{}
	receipt.EncodeRLP(buffer)
	return buffer.Bytes()
}
