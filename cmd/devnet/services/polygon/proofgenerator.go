package polygon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"math"
	"math/big"
	"strings"
	"sync"

	"github.com/ledgerwatch/erigon-lib/chain/networkname"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/jsonrpc"
	"github.com/ledgerwatch/erigon/turbo/trie"
)

var ErrTokenIndexOutOfRange = errors.New("Index is grater than the number of tokens in transaction")

type ProofGenerator struct {
	heimdall *Heimdall
}

func NewProofGenerator() *ProofGenerator {
	return &ProofGenerator{}
}

func (pg *ProofGenerator) NodeCreated(ctx context.Context, node devnet.Node) {

	if pg.heimdall == nil {
		if strings.HasPrefix(node.Name(), "bor") {
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

func (pg *ProofGenerator) GenerateExitPayload(ctx context.Context, burnTxHash libcommon.Hash, eventSignature libcommon.Hash, tokenIndex int) ([]byte, error) {
	logger := devnet.Logger(ctx)

	if pg.heimdall == nil || pg.heimdall.rootChainBinding == nil {
		return nil, fmt.Errorf("ProofGenerator not initialized")
	}

	logger.Info("Checking for checkpoint status", "hash", burnTxHash)

	isCheckpointed, err := pg.isCheckPointed(ctx, burnTxHash)

	if err != nil {
		return nil, fmt.Errorf("Error getting burn transaction: %w", err)
	}

	if !isCheckpointed {
		return nil, fmt.Errorf("Burn transaction has not been checkpointed yet")
	}

	// build payload for exit
	result, err := pg.buildPayloadForExit(ctx, burnTxHash, eventSignature, tokenIndex)

	if err != nil {
		if errors.Is(err, ErrTokenIndexOutOfRange) {
			return nil, fmt.Errorf("Block not included: %w", err)
		}

		return nil, fmt.Errorf("Null receipt received")
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("Null result received")
	}

	return result, nil
}

func (pg *ProofGenerator) getChainBlockInfo(ctx context.Context, burnTxHash libcommon.Hash) (uint64, uint64, error) {
	childNode := devnet.SelectBlockProducer(devnet.WithCurrentNetwork(ctx, networkname.BorDevnetChainName))

	var wg sync.WaitGroup

	var lastChild *big.Int
	var burnTransaction *jsonrpc.RPCTransaction
	var err [2]error

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
func (pg *ProofGenerator) isCheckPointed(ctx context.Context, burnTxHash libcommon.Hash) (bool, error) {
	lastChildBlockNum, burnTxBlockNum, err := pg.getChainBlockInfo(ctx, burnTxHash)

	if err != nil {
		return false, err
	}

	return lastChildBlockNum >= burnTxBlockNum, nil
}

func (pg *ProofGenerator) buildPayloadForExit(ctx context.Context, burnTxHash libcommon.Hash, logEventSig libcommon.Hash, index int) ([]byte, error) {

	node := devnet.SelectBlockProducer(ctx)

	if node == nil {
		return nil, fmt.Errorf("No node available")
	}

	if index < 0 {
		return nil, fmt.Errorf("Index must not negative")
	}

	var receipt *types.Receipt
	var block *requests.Block

	// step 1 - Get Block number from transaction hash
	lastChildBlockNum, txBlockNum, err := pg.getChainBlockInfo(ctx, burnTxHash)

	if err != nil {
		return nil, err
	}

	if lastChildBlockNum < txBlockNum {
		return nil, fmt.Errorf("Burn transaction has not been checkpointed as yet")
	}

	// step 2-  get transaction receipt from txhash and
	// block information from block number

	var wg sync.WaitGroup
	var errs [2]error

	wg.Add(1)
	go func() {
		defer wg.Done()
		receipt, errs[0] = node.GetTransactionReceipt(burnTxHash)
	}()

	go func() {
		defer wg.Done()
		block, errs[1] = node.GetBlockByNumber(rpc.AsBlockNumber(txBlockNum), true)
	}()

	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}

	// step 3 - get information about block saved in parent chain
	// step 4 - build block proof
	var rootBlockNumber uint64
	var start, end uint64

	rootBlockNumber, start, end, err = pg.getRootBlockInfo(txBlockNum)

	if err != nil {
		return nil, err
	}

	blockProof, err := getBlockProof(node, txBlockNum, start, end)

	if err != nil {
		return nil, err
	}

	// step 5- create receipt proof
	receiptProof, err := getReceiptProof(receipt, block, node, nil)

	if err != nil {
		return nil, err
	}

	// step 6 - encode payload, convert into hex
	if index > 0 {
		logIndices := getAllLogIndices(logEventSig, receipt)

		if index >= len(logIndices) {
			return nil, ErrTokenIndexOutOfRange
		}

		return encodePayload(
			rootBlockNumber,
			blockProof,
			txBlockNum,
			block.Time,
			block.TxHash,
			block.ReceiptHash,
			getReceiptBytes(receipt), // rlp encoded
			receiptProof.parentNodes,
			receiptProof.path,
			logIndices[index]), nil
	}

	logIndex := getLogIndex(logEventSig, receipt)

	if logIndex < 0 {
		return nil, fmt.Errorf("Log not found in receipt")
	}

	return encodePayload(
		rootBlockNumber,
		blockProof,
		txBlockNum,
		block.Time,
		block.TxHash,
		block.ReceiptHash,
		getReceiptBytes(receipt), // rlp encoded
		receiptProof.parentNodes,
		receiptProof.path,
		logIndex), nil
}

func encodePayload(headerNumber uint64, buildBlockProof string, blockNumber uint64, timestamp uint64, transactionsRoot libcommon.Hash, receiptsRoot libcommon.Hash, receipt []byte, receiptParentNodes [][]byte, path []byte, logIndex int) []byte {
	parentNodesBytes, _ := rlp.EncodeToBytes(receiptParentNodes)

	bytes, _ := rlp.EncodeToBytes(
		[]interface{}{
			headerNumber,
			buildBlockProof,
			blockNumber,
			timestamp,
			hexutility.Encode(transactionsRoot[:]),
			hexutility.Encode(receiptsRoot[:]),
			hexutility.Encode(receipt),
			hexutility.Encode(parentNodesBytes),
			hexutility.Encode(append([]byte{0}, path...)),
			logIndex,
		})

	return bytes
}

type receiptProof struct {
	blockHash   libcommon.Hash
	parentNodes [][]byte
	root        []byte
	path        []byte
	value       interface{}
}

func getReceiptProof(receipt *types.Receipt, block *requests.Block, node devnet.Node, receipts []*types.Receipt) (*receiptProof, error) {
	stateSyncTxHash := types.ComputeBorTxHash(block.Number.Uint64(), block.Hash)
	receiptsTrie := trie.New(trie.EmptyRoot)

	if len(receipts) == 0 {
		var wg sync.WaitGroup
		var lock sync.Mutex

		errs := make([]error, len(block.Transactions))
		for i, transaction := range block.Transactions {
			if transaction.Hash == stateSyncTxHash {
				// ignore if tx hash is bor state-sync tx
				continue
			}

			hash := transaction.Hash
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				receipt, errs[i] = node.GetTransactionReceipt(hash)
				path, _ := rlp.EncodeToBytes(receipt.TransactionIndex)
				rawReceipt := getReceiptBytes(receipt)
				lock.Lock()
				defer lock.Unlock()
				receiptsTrie.Update(path, rawReceipt)
			}(i)
		}

		wg.Wait()

		for _, err := range errs {
			if err != nil {
				return nil, err
			}
		}
	} else {
		for _, receipt := range receipts {
			path, _ := rlp.EncodeToBytes(receipt.TransactionIndex)
			rawReceipt := getReceiptBytes(receipt)
			receiptsTrie.Update(path, rawReceipt)
		}
	}

	path, _ := rlp.EncodeToBytes(receipt.TransactionIndex)
	result, ok := receiptsTrie.Get(path)

	if !ok {
		return nil, fmt.Errorf("Node does not contain the key")
	}

	var nodeValue any

	if isTypedReceipt(receipt) {
		nodeValue = result
	} else {
		rlp.DecodeBytes(result, nodeValue)
	}

	return &receiptProof{
		blockHash:   receipt.BlockHash,
		parentNodes: nil, //TODO - not sure how to get this result.stack.map(s => s.raw()),
		root:        block.ReceiptHash[:],
		path:        path,
		value:       nodeValue,
	}, nil
}

func getBlockProof(node devnet.Node, txBlockNum, startBlock, endBlock uint64) (string, error) {
	proofs, err := getFastMerkleProof(node, txBlockNum, startBlock, endBlock)

	if err != nil {
		return "", err
	}

	return hexutility.Encode(bytes.Join(proofs, []byte{})), nil
}

func getFastMerkleProof(node devnet.Node, blockNumber, startBlock, endBlock uint64) ([][]byte, error) {
	merkleTreeDepth := int(math.Ceil(math.Log2(float64(endBlock - startBlock + 1))))

	// We generate the proof root down, whereas we need from leaf up
	var reversedProof [][]byte

	offset := startBlock
	targetIndex := blockNumber - offset
	leftBound := uint64(0)
	rightBound := endBlock - offset

	//   console.log("Searching for", targetIndex);
	for depth := 0; depth < merkleTreeDepth; depth += 1 {
		nLeaves := uint64(2) << (merkleTreeDepth - depth)

		// The pivot leaf is the last leaf which is included in the left subtree
		pivotLeaf := leftBound + nLeaves/2 - 1

		if targetIndex > pivotLeaf {
			// Get the root hash to the merkle subtree to the left
			newLeftBound := pivotLeaf + 1
			// eslint-disable-next-line no-await-in-loop
			subTreeMerkleRoot, err := node.GetRootHash(offset+leftBound, offset+pivotLeaf)

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
				remainingNodesHash, err := node.GetRootHash(offset+pivotLeaf+1, offset+rightBound)

				if err != nil {
					return nil, err
				}

				// The remaining leaves will hold the merkle root of a zero-filled tree of height subTreeHeight
				leafRoots := recursiveZeroHash(subTreeHeight)

				// Build a merkle tree of correct size for the subtree using these merkle roots
				leaves := make([][]byte, 2<<heightDifference)
				leaves[0] = remainingNodesHash[:]

				for i := 1; i < len(leaves); i++ {
					leaves[i] = leafRoots[:]
				}

				subTreeMerkleRoot, err := merkle_tree.HashTreeRoot(leaves)

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

func recursiveZeroHash(n int) libcommon.Hash {
	if n == 0 {
		return libcommon.Hash{}
	}

	subHash := recursiveZeroHash(n - 1)
	bytes, _ := rlp.EncodeToBytes([]libcommon.Hash{subHash, subHash})
	return crypto.Keccak256Hash(bytes)
}

func getAllLogIndices(logEventSig libcommon.Hash, receipt *types.Receipt) []int {
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
			if strings.EqualFold(hexutility.Encode(log.Topics[0][:]), "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef") &&
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

func getLogIndex(logEventSig libcommon.Hash, receipt *types.Receipt) int {
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

	headerBlock, err := pg.heimdall.rootChainBinding.HeaderBlocks(&bind.CallOpts{}, big.NewInt(int64(rootBlockNumber)))

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
		headerBlock, err := pg.heimdall.rootChainBinding.HeaderBlocks(&bind.CallOpts{}, big.NewInt(int64(mid*checkPointInterval)))

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
