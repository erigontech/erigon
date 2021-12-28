package commands

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sync"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/bor"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/xsleonard/go-merkle"
	"golang.org/x/crypto/sha3"
)

var (
	// errUnknownBlock is returned when the list of signers is requested for a block
	// that is not part of the local blockchain.
	errUnknownBlock = errors.New("unknown block")

	// errMissingSignature is returned if a block's extra-data section doesn't seem
	// to contain a 65 byte secp256k1 signature.
	errMissingSignature = errors.New("extra-data 65 byte signature suffix missing")
)

var (
	extraSeal = 65 // Fixed number of extra-data suffix bytes reserved for signer seal
)

type Snapshot struct {
	config *params.BorConfig // Consensus engine parameters to fine tune behavior

	Number       uint64                    `json:"number"`       // Block number where the snapshot was created
	Hash         common.Hash               `json:"hash"`         // Block hash where the snapshot was created
	ValidatorSet *ValidatorSet             `json:"validatorSet"` // Validator set at this moment
	Recents      map[uint64]common.Address `json:"recents"`      // Set of recent signers for spam protections
}

type ValidatorSet struct {
	// NOTE: persisted via reflect, must be exported.
	Validators []*bor.Validator `json:"validators"`
	Proposer   *bor.Validator   `json:"proposer"`

	// cached (unexported)
	totalVotingPower int64
}

// GetSnapshot retrieves the state snapshot at a given block.
func (api *BorImpl) GetSnapshot(number *rpc.BlockNumber) (*Snapshot, error) {
	ctx := context.Background()
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Retrieve the requested block number (or current if none requested)
	var header *types.Header
	if number == nil || *number == rpc.LatestBlockNumber {
		header = rawdb.ReadCurrentHeader(tx)
	} else {
		header, _ = getHeaderByNumber(*number, api, tx)
	}
	// Ensure we have an actually valid block and return its snapshot
	if header == nil {
		return nil, errUnknownBlock
	}
	return snapshot(api, tx, header.Number.Uint64(), header.Hash())
}

// GetAuthor retrieves the author a block.
func (api *BorImpl) GetAuthor(number *rpc.BlockNumber) (*common.Address, error) {
	ctx := context.Background()
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Retrieve the requested block number (or current if none requested)
	var header *types.Header
	if number == nil || *number == rpc.LatestBlockNumber {
		header = rawdb.ReadCurrentHeader(tx)
	} else {
		header, _ = getHeaderByNumber(*number, api, tx)
	}
	// Ensure we have an actually valid block
	if header == nil {
		return nil, errUnknownBlock
	}
	author, err := author(api, tx, header)
	return &author, err
}

// GetSnapshotAtHash retrieves the state snapshot at a given block.
func (api *BorImpl) GetSnapshotAtHash(hash common.Hash) (*Snapshot, error) {
	ctx := context.Background()
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	header, _ := getHeaderByHash(tx, hash)
	if header == nil {
		return nil, errUnknownBlock
	}
	return snapshot(api, tx, header.Number.Uint64(), header.Hash())
}

// GetSigners retrieves the list of authorized signers at the specified block.
func (api *BorImpl) GetSigners(number *rpc.BlockNumber) ([]common.Address, error) {
	ctx := context.Background()
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Retrieve the requested block number (or current if none requested)
	var header *types.Header
	if number == nil || *number == rpc.LatestBlockNumber {
		header = rawdb.ReadCurrentHeader(tx)
	} else {
		header, _ = getHeaderByNumber(*number, api, tx)
	}
	// Ensure we have an actually valid block and return its snapshot
	if header == nil {
		return nil, errUnknownBlock
	}
	snap, err := snapshot(api, tx, header.Number.Uint64(), header.Hash())
	return signers(snap.ValidatorSet), err
}

// GetSignersAtHash retrieves the list of authorized signers at the specified block.
func (api *BorImpl) GetSignersAtHash(hash common.Hash) ([]common.Address, error) {
	ctx := context.Background()
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	header, _ := getHeaderByHash(tx, hash)
	if header == nil {
		return nil, errUnknownBlock
	}
	snap, err := snapshot(api, tx, header.Number.Uint64(), header.Hash())
	if err != nil {
		return nil, err
	}
	return signers(snap.ValidatorSet), nil
}

// GetCurrentProposer gets the current proposer
func (api *BorImpl) GetCurrentProposer() (common.Address, error) {
	snap, err := api.GetSnapshot(nil)
	if err != nil {
		return common.Address{}, err
	}
	return getProposer(snap.ValidatorSet).Address, nil
}

// GetCurrentValidators gets the current validators
func (api *BorImpl) GetCurrentValidators() ([]*bor.Validator, error) {
	snap, err := api.GetSnapshot(nil)
	if err != nil {
		return make([]*bor.Validator, 0), err
	}
	return snap.ValidatorSet.Validators, nil
}

// GetRootHash returns the merkle root of the start to end block headers
func (api *BorImpl) GetRootHash(start, end uint64) (string, error) {
	length := uint64(end - start + 1)
	if length > bor.MaxCheckpointLength {
		return "", &bor.MaxCheckpointLengthExceededError{start, end}
	}
	ctx := context.Background()
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	currentHeaderNumber := rawdb.ReadCurrentHeader(tx).Number.Uint64()
	if start > end || end > currentHeaderNumber {
		return "", &bor.InvalidStartEndBlockError{start, end, currentHeaderNumber}
	}
	blockHeaders := make([]*types.Header, end-start+1)
	wg := new(sync.WaitGroup)
	concurrent := make(chan bool, 20)
	for i := start; i <= end; i++ {
		wg.Add(1)
		concurrent <- true
		go func(number uint64) {
			blockHeaders[number-start], _ = getHeaderByNumber(rpc.BlockNumber(number), api, tx)
			<-concurrent
			wg.Done()
		}(i)
	}
	wg.Wait()
	close(concurrent)

	headers := make([][32]byte, nextPowerOfTwo(length))
	for i := 0; i < len(blockHeaders); i++ {
		blockHeader := blockHeaders[i]
		header := crypto.Keccak256(appendBytes32(
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
	if err := tree.Generate(convert(headers), sha3.NewLegacyKeccak256()); err != nil {
		return "", err
	}
	root := hex.EncodeToString(tree.Root().Hash)
	return root, nil
}

func (api *BorImpl) Test() (string, error) {
	return "Hello World", nil
}

// helper functions

// getHeaderByNumber returns a block's header given a block number ignoring the block's transaction and uncle list (may be faster).
// derived from erigon_getHeaderByNumber implementation (see ./erigon_block.go)
func getHeaderByNumber(number rpc.BlockNumber, api *BorImpl, tx kv.Tx) (*types.Header, error) {
	// Pending block is only known by the miner
	if number == rpc.PendingBlockNumber {
		block := api.pendingBlock()
		if block == nil {
			return nil, nil
		}
		return block.Header(), nil
	}

	blockNum, err := getBlockNumber(number, tx)
	if err != nil {
		return nil, err
	}

	header := rawdb.ReadHeaderByNumber(tx, blockNum)
	if header == nil {
		return nil, fmt.Errorf("block header not found: %d", blockNum)
	}

	return header, nil
}

// snapshot retrieves the authorization snapshot at a given point in time.
func snapshot(api *BorImpl, tx kv.Tx, number uint64, hash common.Hash) (*Snapshot, error) {
	var snap *Snapshot
	// load on-disk checkpoints
	if s, err := loadSnapshot(api, tx, hash); err == nil {
		snap = s
		return snap, nil
	} else {
		return nil, fmt.Errorf("unknown error while retrieving snapshot at block number %v", number)
	}
}

// loadSnapshot loads an existing snapshot from the database.
func loadSnapshot(api *BorImpl, tx kv.Tx, hash common.Hash) (*Snapshot, error) {
	blob, err := tx.GetOne(kv.CliqueSeparate, append([]byte("bor-"), hash[:]...))
	if err != nil {
		return nil, err
	}
	snap := new(Snapshot)
	if err := json.Unmarshal(blob, snap); err != nil {
		return nil, err
	}
	config, _ := api.BaseAPI.chainConfig(tx)
	snap.config = config.Bor

	// update total voting power
	if err := updateTotalVotingPower(snap.ValidatorSet); err != nil {
		return nil, err
	}

	return snap, nil
}

// signers retrieves the list of authorized signers in ascending order.
func signers(vals *ValidatorSet) []common.Address {
	sigs := make([]common.Address, 0, len(vals.Validators))
	for _, sig := range vals.Validators {
		sigs = append(sigs, sig.Address)
	}
	return sigs
}

// Force recalculation of the set's total voting power.
func updateTotalVotingPower(vals *ValidatorSet) error {

	sum := int64(0)
	for _, val := range vals.Validators {
		// mind overflow
		sum = safeAddClip(sum, val.VotingPower)
		if sum > bor.MaxTotalVotingPower {
			return &bor.TotalVotingPowerExceededError{sum, vals.Validators}
		}
	}
	vals.totalVotingPower = sum
	return nil
}

// getHeaderByHash returns a block's header given a block's hash.
// derived from erigon_getHeaderByHash implementation (see ./erigon_block.go)
func getHeaderByHash(tx kv.Tx, hash common.Hash) (*types.Header, error) {
	header, err := rawdb.ReadHeaderByHash(tx, hash)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, fmt.Errorf("block header not found: %s", hash.String())
	}

	return header, nil
}

// getProposer returns the current proposer.
// If the validator set is empty, nil is returned.
func getProposer(vals *ValidatorSet) (proposer *bor.Validator) {
	if len(vals.Validators) == 0 {
		return nil
	}
	if vals.Proposer == nil {
		vals.Proposer = findProposer(vals)
	}
	return vals.Proposer.Copy()
}

func findProposer(vals *ValidatorSet) *bor.Validator {
	var proposer *bor.Validator
	for _, val := range vals.Validators {
		if proposer == nil || !bytes.Equal(val.Address.Bytes(), proposer.Address.Bytes()) {
			proposer = proposer.Cmp(val)
		}
	}
	return proposer
}

// author returns the Ethereum address recovered
// from the signature in the header's extra-data section.
func author(api *BorImpl, tx kv.Tx, header *types.Header) (common.Address, error) {
	config, _ := api.BaseAPI.chainConfig(tx)
	return ecrecover(header, config.Bor)
}

// ecrecover extracts the Ethereum account address from a signed header.
func ecrecover(header *types.Header, c *params.BorConfig) (common.Address, error) {
	// Retrieve the signature from the header extra-data
	if len(header.Extra) < extraSeal {
		return common.Address{}, errMissingSignature
	}
	signature := header.Extra[len(header.Extra)-extraSeal:]

	// Recover the public key and the Ethereum address
	pubkey, err := crypto.Ecrecover(bor.SealHash(header, c).Bytes(), signature)
	if err != nil {
		return common.Address{}, err
	}
	var signer common.Address
	copy(signer[:], crypto.Keccak256(pubkey[1:])[12:])

	return signer, nil
}

// safe addition
func safeAdd(a, b int64) (int64, bool) {
	if b > 0 && a > math.MaxInt64-b {
		return -1, true
	} else if b < 0 && a < math.MinInt64-b {
		return -1, true
	}
	return a + b, false
}

func safeAddClip(a, b int64) int64 {
	c, overflow := safeAdd(a, b)
	if overflow {
		if b < 0 {
			return math.MinInt64
		}
		return math.MaxInt64
	}
	return c
}
