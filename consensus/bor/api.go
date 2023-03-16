package bor

import (
	"encoding/hex"
	"math"
	"math/big"
	"sort"
	"strconv"
	"sync"

	lru2 "github.com/hashicorp/golang-lru/v2"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/xsleonard/go-merkle"
	"golang.org/x/crypto/sha3"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/bor/valset"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/rpc"
)

var (
	// MaxCheckpointLength is the maximum number of blocks that can be requested for constructing a checkpoint root hash
	MaxCheckpointLength = uint64(math.Pow(2, 15))
)

// API is a user facing RPC API to allow controlling the signer and voting
// mechanisms of the proof-of-authority scheme.
type API struct {
	chain         consensus.ChainHeaderReader
	bor           *Bor
	rootHashCache *lru2.ARCCache[string, string]
}

// GetSnapshot retrieves the state snapshot at a given block.
func (api *API) GetSnapshot(number *rpc.BlockNumber) (*Snapshot, error) {
	// Retrieve the requested block number (or current if none requested)
	var header *types.Header
	if number == nil || *number == rpc.LatestBlockNumber {
		header = api.chain.CurrentHeader()
	} else {
		header = api.chain.GetHeaderByNumber(uint64(number.Int64()))
	}
	// Ensure we have an actually valid block and return its snapshot
	if header == nil {
		return nil, errUnknownBlock
	}

	return api.bor.snapshot(api.chain, header.Number.Uint64(), header.Hash(), nil)
}

type BlockSigners struct {
	Signers []difficultiesKV
	Diff    int
	Author  libcommon.Address
}

type difficultiesKV struct {
	Signer     libcommon.Address
	Difficulty uint64
}

func rankMapDifficulties(values map[libcommon.Address]uint64) []difficultiesKV {
	ss := make([]difficultiesKV, 0, len(values))
	for k, v := range values {
		ss = append(ss, difficultiesKV{k, v})
	}

	sort.Slice(ss, func(i, j int) bool {
		return ss[i].Difficulty > ss[j].Difficulty
	})

	return ss
}

// GetSnapshotProposerSequence retrieves the in-turn signers of all sprints in a span
func (api *API) GetSnapshotProposerSequence(number *rpc.BlockNumber) (BlockSigners, error) {
	snapNumber := *number - 1

	var difficulties = make(map[libcommon.Address]uint64)

	snap, err := api.GetSnapshot(&snapNumber)

	if err != nil {
		return BlockSigners{}, err
	}

	proposer := snap.ValidatorSet.GetProposer().Address
	proposerIndex, _ := snap.ValidatorSet.GetByAddress(proposer)

	signers := snap.signers()
	for i := 0; i < len(signers); i++ {
		tempIndex := i
		if tempIndex < proposerIndex {
			tempIndex = tempIndex + len(signers)
		}

		difficulties[signers[i]] = uint64(len(signers) - (tempIndex - proposerIndex))
	}

	rankedDifficulties := rankMapDifficulties(difficulties)

	author, err := api.GetAuthor(number)
	if err != nil {
		return BlockSigners{}, err
	}

	diff := int(difficulties[*author])
	blockSigners := &BlockSigners{
		Signers: rankedDifficulties,
		Diff:    diff,
		Author:  *author,
	}

	return *blockSigners, nil
}

// GetSnapshotProposer retrieves the in-turn signer at a given block.
func (api *API) GetSnapshotProposer(number *rpc.BlockNumber) (libcommon.Address, error) {
	*number -= 1
	snap, err := api.GetSnapshot(number)

	if err != nil {
		return libcommon.Address{}, err
	}

	return snap.ValidatorSet.GetProposer().Address, nil
}

// GetAuthor retrieves the author a block.
func (api *API) GetAuthor(number *rpc.BlockNumber) (*libcommon.Address, error) {
	// Retrieve the requested block number (or current if none requested)
	var header *types.Header
	if number == nil || *number == rpc.LatestBlockNumber {
		header = api.chain.CurrentHeader()
	} else {
		header = api.chain.GetHeaderByNumber(uint64(number.Int64()))
	}
	// Ensure we have an actually valid block and return its snapshot
	if header == nil {
		return nil, errUnknownBlock
	}

	author, err := api.bor.Author(header)

	return &author, err
}

// GetSnapshotAtHash retrieves the state snapshot at a given block.
func (api *API) GetSnapshotAtHash(hash libcommon.Hash) (*Snapshot, error) {
	header := api.chain.GetHeaderByHash(hash)
	if header == nil {
		return nil, errUnknownBlock
	}

	return api.bor.snapshot(api.chain, header.Number.Uint64(), header.Hash(), nil)
}

// GetSigners retrieves the list of authorized signers at the specified block.
func (api *API) GetSigners(number *rpc.BlockNumber) ([]libcommon.Address, error) {
	// Retrieve the requested block number (or current if none requested)
	var header *types.Header
	if number == nil || *number == rpc.LatestBlockNumber {
		header = api.chain.CurrentHeader()
	} else {
		header = api.chain.GetHeaderByNumber(uint64(number.Int64()))
	}
	// Ensure we have an actually valid block and return the signers from its snapshot
	if header == nil {
		return nil, errUnknownBlock
	}

	snap, err := api.bor.snapshot(api.chain, header.Number.Uint64(), header.Hash(), nil)

	if err != nil {
		return nil, err
	}

	return snap.signers(), nil
}

// GetSignersAtHash retrieves the list of authorized signers at the specified block.
func (api *API) GetSignersAtHash(hash libcommon.Hash) ([]libcommon.Address, error) {
	header := api.chain.GetHeaderByHash(hash)
	if header == nil {
		return nil, errUnknownBlock
	}

	snap, err := api.bor.snapshot(api.chain, header.Number.Uint64(), header.Hash(), nil)

	if err != nil {
		return nil, err
	}

	return snap.signers(), nil
}

// GetCurrentProposer gets the current proposer
func (api *API) GetCurrentProposer() (libcommon.Address, error) {
	snap, err := api.GetSnapshot(nil)
	if err != nil {
		return libcommon.Address{}, err
	}

	return snap.ValidatorSet.GetProposer().Address, nil
}

// GetCurrentValidators gets the current validators
func (api *API) GetCurrentValidators() ([]*valset.Validator, error) {
	snap, err := api.GetSnapshot(nil)
	if err != nil {
		return make([]*valset.Validator, 0), err
	}

	return snap.ValidatorSet.Validators, nil
}

// GetRootHash returns the merkle root of the start to end block headers
func (api *API) GetRootHash(start uint64, end uint64) (string, error) {
	if err := api.initializeRootHashCache(); err != nil {
		return "", err
	}

	key := getRootHashKey(start, end)

	if root, known := api.rootHashCache.Get(key); known {
		return root, nil
	}

	length := end - start + 1

	if length > MaxCheckpointLength {
		return "", &MaxCheckpointLengthExceededError{start, end}
	}

	currentHeaderNumber := api.chain.CurrentHeader().Number.Uint64()

	if start > end || end > currentHeaderNumber {
		return "", &valset.InvalidStartEndBlockError{Start: start, End: end, CurrentHeader: currentHeaderNumber}
	}

	blockHeaders := make([]*types.Header, end-start+1)
	wg := new(sync.WaitGroup)
	concurrent := make(chan bool, 20)

	for i := start; i <= end; i++ {
		wg.Add(1)
		concurrent <- true

		go func(number uint64) {
			blockHeaders[number-start] = api.chain.GetHeaderByNumber(number)

			<-concurrent
			wg.Done()
		}(i)
	}
	wg.Wait()
	close(concurrent)

	headers := make([][32]byte, NextPowerOfTwo(length))

	for i := 0; i < len(blockHeaders); i++ {
		blockHeader := blockHeaders[i]
		header := crypto.Keccak256(AppendBytes32(
			blockHeader.Number.Bytes(),
			new(big.Int).SetUint64(blockHeader.Time).Bytes(),
			blockHeader.TxHash.Bytes(),
			blockHeader.ReceiptHash.Bytes(),
		))

		var arr [32]byte

		copy(arr[:], header)
		headers[i] = arr
	}

	tree := merkle.NewTreeWithOpts(merkle.TreeOptions{EnableHashSorting: false, DisableHashLeaves: true})
	if err := tree.Generate(Convert(headers), sha3.NewLegacyKeccak256()); err != nil {
		return "", err
	}

	root := hex.EncodeToString(tree.Root().Hash)
	api.rootHashCache.Add(key, root)

	return root, nil
}

func (api *API) initializeRootHashCache() error {
	var err error
	if api.rootHashCache == nil {
		api.rootHashCache, err = lru2.NewARC[string, string](10)
	}

	return err
}

func getRootHashKey(start uint64, end uint64) string {
	return strconv.FormatUint(start, 10) + "-" + strconv.FormatUint(end, 10)
}
