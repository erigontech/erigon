// Copyright 2017 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

// Package clique implements the proof-of-authority rules engine.
package clique

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"

	"github.com/erigontech/erigon/execution/chain"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/rpc"
)

const (
	epochLength          = uint64(30000)          // Default number of blocks after which to checkpoint and reset the pending votes
	ExtraVanity          = 32                     // Fixed number of extra-data prefix bytes reserved for signer vanity
	ExtraSeal            = crypto.SignatureLength // Fixed number of extra-data suffix bytes reserved for signer seal
	warmupCacheSnapshots = 20

	wiggleTime = 500 * time.Millisecond // Random delay (per signer) to allow concurrent signers
)

var (
	NonceAuthVote = hexutil.MustDecode("0xffffffffffffffff") // Magic nonce number to vote on adding a new signer
	nonceDropVote = hexutil.MustDecode("0x0000000000000000") // Magic nonce number to vote on removing a signer.

	DiffInTurn = uint64(2) // Block difficulty for in-turn signatures
	diffNoTurn = uint64(1) // Block difficulty for out-of-turn signatures
)

var (
	errUnknownBlock = errors.New("unknown block")

	errInvalidCheckpointBeneficiary = errors.New("beneficiary in checkpoint block non-zero")

	errInvalidVote = errors.New("vote nonce not 0x00..0 or 0xff..f")

	errInvalidCheckpointVote = errors.New("vote nonce in checkpoint block non-zero")

	errMissingVanity = errors.New("extra-data 32 byte vanity prefix missing")

	errMissingSignature = errors.New("extra-data 65 byte signature suffix missing")

	errExtraSigners = errors.New("non-checkpoint block contains extra signer list")

	errInvalidCheckpointSigners = errors.New("invalid signer list on checkpoint block")

	errMismatchingCheckpointSigners = errors.New("mismatching signer list on checkpoint block")

	errInvalidMixDigest = errors.New("non-zero mix digest")

	errInvalidUncleHash = errors.New("non empty uncle hash")

	errInvalidDifficulty = errors.New("invalid difficulty")

	errWrongDifficulty = errors.New("wrong difficulty")

	errInvalidTimestamp = errors.New("invalid timestamp")

	errInvalidVotingChain = errors.New("invalid voting chain")

	ErrUnauthorizedSigner = errors.New("unauthorized signer")

	ErrRecentlySigned = errors.New("recently signed")
)

type SignerFn func(signer common.Address, mimeType string, message []byte) ([]byte, error)

func ecrecover(header *types.Header, sigcache *lru.Cache[common.Hash, accounts.Address]) (accounts.Address, error) {
	hash := header.Hash()

	if address, known := sigcache.Peek(hash); known {
		return address, nil
	}

	if len(header.Extra) < ExtraSeal {
		return accounts.NilAddress, errMissingSignature
	}
	signature := header.Extra[len(header.Extra)-ExtraSeal:]

	sealHash := SealHash(header)
	pubkey, err := crypto.Ecrecover(sealHash[:], signature)
	if err != nil {
		return accounts.NilAddress, err
	}

	signer := accounts.InternAddress(common.BytesToAddress(crypto.Keccak256(pubkey[1:])[12:]))
	sigcache.Add(hash, signer)
	return signer, nil
}

type Clique struct {
	ChainConfig    *chain.Config
	config         *chain.CliqueConfig                // Rules engine configuration parameters
	snapshotConfig *chainspec.ConsensusSnapshotConfig // Rules engine configuration parameters
	DB             kv.RwDB                            // Database to store and retrieve snapshot checkpoints

	signatures *lru.Cache[common.Hash, accounts.Address] // Signatures of recent blocks to speed up mining
	recents    *lru.Cache[common.Hash, *Snapshot]        // Snapshots for recent block to speed up reorgs

	proposals map[common.Address]bool // Current list of proposals we are pushing

	signer common.Address // Ethereum address of the signing key
	signFn SignerFn       // Signer function to authorize hashes with
	lock   sync.RWMutex   // Protects the signer and proposals fields

	FakeDiff bool // Skip difficulty verifications

	exitCh chan struct{}
	logger log.Logger
}

func New(cfg *chain.Config, snapshotConfig *chainspec.ConsensusSnapshotConfig, cliqueDB kv.RwDB, logger log.Logger) *Clique {
	config := cfg.Clique

	conf := *config
	if conf.Epoch == 0 {
		conf.Epoch = epochLength
	}
	recents, _ := lru.New[common.Hash, *Snapshot](snapshotConfig.InmemorySnapshots)
	signatures, _ := lru.New[common.Hash, accounts.Address](snapshotConfig.InmemorySignatures)

	exitCh := make(chan struct{})

	c := &Clique{
		ChainConfig:    cfg,
		config:         &conf,
		snapshotConfig: snapshotConfig,
		DB:             cliqueDB,
		recents:        recents,
		signatures:     signatures,
		proposals:      make(map[common.Address]bool),
		exitCh:         exitCh,
		logger:         logger,
	}

	snapNum, err := lastSnapshot(cliqueDB, logger)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			logger.Error("on Clique init while getting latest snapshot", "err", err)
		}
	} else {
		snaps, err := c.snapshots(snapNum, warmupCacheSnapshots)
		if err != nil {
			logger.Error("on Clique init", "err", err)
		}

		for _, sn := range snaps {
			c.recentsAdd(sn.Number, sn.Hash, sn)
		}
	}

	return c
}

func (c *Clique) Type() chain.RulesName {
	return chain.CliqueRules
}

func (c *Clique) Author(header *types.Header) (accounts.Address, error) {
	return ecrecover(header, c.signatures)
}

func (c *Clique) VerifyHeader(chain rules.ChainHeaderReader, header *types.Header, _ bool) error {
	return c.verifyHeader(chain, header, nil)
}

type VerifyHeaderResponse struct {
	Results chan error
	Cancel  func()
}

func (c *Clique) recentsAdd(num uint64, hash common.Hash, s *Snapshot) {
	c.recents.Add(hash, s.copy())
}

func (c *Clique) VerifyUncles(chain rules.ChainReader, header *types.Header, uncles []*types.Header) error {
	if len(uncles) > 0 {
		return errors.New("uncles not allowed")
	}
	return nil
}

func (c *Clique) VerifySeal(chain rules.ChainHeaderReader, header *types.Header) error {

	snap, err := c.Snapshot(chain, header.Number.Uint64(), header.Hash(), nil)
	if err != nil {
		return err
	}
	return c.verifySeal(chain, header, snap)
}

func (c *Clique) Prepare(chain rules.ChainHeaderReader, header *types.Header, state *state.IntraBlockState) error {

	header.Coinbase = common.Address{}
	header.Nonce = types.BlockNonce{}

	number := header.Number.Uint64()
	snap, err := c.Snapshot(chain, number-1, header.ParentHash, nil)
	if err != nil {
		return err
	}
	c.lock.RLock()
	if number%c.config.Epoch != 0 {
		addresses := make([]common.Address, 0, len(c.proposals))
		for address, authorize := range c.proposals {
			if snap.validVote(accounts.InternAddress(address), authorize) {
				addresses = append(addresses, address)
			}
		}
		if len(addresses) > 0 {
			header.Coinbase = addresses[rand.Intn(len(addresses))] // nolint: gosec
			if c.proposals[header.Coinbase] {
				copy(header.Nonce[:], NonceAuthVote)
			} else {
				copy(header.Nonce[:], nonceDropVote)
			}
		}
	}

	signer := c.signer
	c.lock.RUnlock()

	header.Difficulty.SetUint64(calcDifficulty(snap, signer))

	if len(header.Extra) < ExtraVanity {
		header.Extra = append(header.Extra, bytes.Repeat([]byte{0x00}, ExtraVanity-len(header.Extra))...) //nolint: gocritic
	}
	header.Extra = header.Extra[:ExtraVanity]

	if number%c.config.Epoch == 0 {
		for _, signer := range snap.GetSigners() {
			header.Extra = append(header.Extra, signer[:]...)
		}
	}
	header.Extra = append(header.Extra, make([]byte, ExtraSeal)...)

	header.MixDigest = common.Hash{}

	parent := chain.GetHeader(header.ParentHash, number-1)
	if parent == nil {
		return rules.ErrUnknownAncestor
	}
	header.Time = parent.Time + c.config.Period

	now := uint64(time.Now().Unix())
	if header.Time < now {
		header.Time = now
	}

	return nil
}

func (c *Clique) Initialize(config *chain.Config, chain rules.ChainHeaderReader, header *types.Header,
	state *state.IntraBlockState, syscall rules.SysCallCustom, logger log.Logger, tracer *tracing.Hooks) error {
	return nil
}

func (c *Clique) CalculateRewards(config *chain.Config, header *types.Header, uncles []*types.Header, syscall rules.SystemCall,
) ([]rules.Reward, error) {
	return []rules.Reward{}, nil
}

func (c *Clique) Finalize(config *chain.Config, header *types.Header, state *state.IntraBlockState,
	uncles []*types.Header, r types.Receipts, withdrawals []*types.Withdrawal,
	chain rules.ChainReader, syscall rules.SystemCall, skipReceiptsEval bool, logger log.Logger,
) (types.FlatRequests, error) {
	return nil, nil
}

func (c *Clique) FinalizeAndAssemble(chainConfig *chain.Config, header *types.Header, state *state.IntraBlockState,
	txs types.Transactions, uncles []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal, chain rules.ChainReader, syscall rules.SystemCall, call rules.Call, logger log.Logger,
) (*types.Block, types.FlatRequests, error) {
	return types.NewBlockForAsembling(header, txs, nil, receipts, withdrawals, nil), nil, nil
}

func (c *Clique) Authorize(signer common.Address, signFn SignerFn) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.signer = signer
	c.signFn = signFn
}

func (c *Clique) Seal(chain rules.ChainHeaderReader, blockWithReceipts *types.BlockWithReceipts, results chan<- *types.BlockWithReceipts, stop <-chan struct{}) error {
	block := blockWithReceipts.Block
	receipts := blockWithReceipts.Receipts
	header := block.Header()

	number := header.Number.Uint64()
	if number == 0 {
		return errUnknownBlock
	}
	if c.config.Period == 0 && len(block.Transactions()) == 0 {
		c.logger.Info("Sealing paused, waiting for transactions")
		results <- nil

		return nil
	}
	c.lock.RLock()
	signer, signFn := accounts.InternAddress(c.signer), c.signFn
	c.lock.RUnlock()

	snap, err := c.Snapshot(chain, number-1, header.ParentHash, nil)
	if err != nil {
		return err
	}
	if _, authorized := snap.Signers[signer]; !authorized {
		return fmt.Errorf("Clique.Seal: %w", ErrUnauthorizedSigner)
	}
	for seen, recent := range snap.Recents {
		if recent == signer {
			if limit := uint64(len(snap.Signers)/2 + 1); number < limit || seen > number-limit {
				c.logger.Info("Signed recently, must wait for others")
				return nil
			}
		}
	}
	delay := time.Unix(int64(header.Time), 0).Sub(time.Now()) //nolint:staticcheck
	if header.Difficulty.CmpUint64(diffNoTurn) == 0 {
		wiggle := time.Duration(len(snap.Signers)/2+1) * wiggleTime
		delay += time.Duration(rand.Int63n(int64(wiggle))) // nolint: gosec

		c.logger.Trace("Out-of-turn signing requested", "wiggle", common.PrettyDuration(wiggle))
	}
	sighash, err := signFn(signer.Value(), accounts.MimetypeClique, CliqueRLP(header))
	if err != nil {
		return err
	}
	copy(header.Extra[len(header.Extra)-ExtraSeal:], sighash)
	c.logger.Trace("Waiting for slot to sign and propagate", "delay", common.PrettyDuration(delay))
	go func() {
		defer dbg.LogPanic()
		select {
		case <-stop:
			return
		case <-time.After(delay):
		}

		select {
		case results <- &types.BlockWithReceipts{Block: block.WithSeal(header), Receipts: receipts}:
		default:
			c.logger.Warn("Sealing result is not read by miner", "sealhash", SealHash(header))
		}
	}()

	return nil
}

func (c *Clique) CalcDifficulty(chain rules.ChainHeaderReader, _, _ uint64, _ uint256.Int, parentNumber uint64, parentHash, _ common.Hash, _ uint64) uint256.Int {
	snap, err := c.Snapshot(chain, parentNumber, parentHash, nil)
	if err != nil {
		return uint256.Int{}
	}
	c.lock.RLock()
	signer := c.signer
	c.lock.RUnlock()
	return *uint256.NewInt(calcDifficulty(snap, signer))
}

func calcDifficulty(snap *Snapshot, signer common.Address) uint64 {
	if snap.inturn(snap.Number+1, signer) {
		return DiffInTurn
	}
	return diffNoTurn
}

func (c *Clique) SealHash(header *types.Header) common.Hash {
	return SealHash(header)
}

func (c *Clique) IsServiceTransaction(sender accounts.Address, syscall rules.SystemCall) bool {
	return false
}

func (c *Clique) Close() error {
	c.DB.Close()
	common.SafeClose(c.exitCh)
	return nil
}

func (c *Clique) APIs(chain rules.ChainHeaderReader) []rpc.API {
	return []rpc.API{}
}

func (c *Clique) TxDependencies(h *types.Header) [][]int {
	return nil
}

func SealHash(header *types.Header) (hash common.Hash) {
	hasher := crypto.NewKeccakState()
	defer crypto.ReturnToPool(hasher)

	encodeSigHeader(hasher, header)
	hasher.Sum(hash[:0])
	return hash
}

func CliqueRLP(header *types.Header) []byte {
	b := new(bytes.Buffer)
	encodeSigHeader(b, header)
	return b.Bytes()
}

func encodeSigHeader(w io.Writer, header *types.Header) {
	enc := []any{
		header.ParentHash,
		header.UncleHash,
		header.Coinbase,
		header.Root,
		header.TxHash,
		header.ReceiptHash,
		header.Bloom,
		header.Difficulty,
		header.Number,
		header.GasLimit,
		header.GasUsed,
		header.Time,
		header.Extra[:len(header.Extra)-crypto.SignatureLength], // Yes, this will panic if extra is too short
		header.MixDigest,
		header.Nonce,
	}
	if header.BaseFee != nil {
		enc = append(enc, header.BaseFee)
	}
	if err := rlp.Encode(w, enc); err != nil {
		panic("can't encode: " + err.Error())
	}
}

func (c *Clique) snapshots(latest uint64, total int) ([]*Snapshot, error) {
	if total <= 0 {
		return nil, nil
	}

	blockEncoded := dbutils.EncodeBlockNumber(latest)

	tx, err := c.DB.BeginRo(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	cur, err1 := tx.Cursor(kv.CliqueSeparate)
	if err1 != nil {
		return nil, err1
	}
	defer cur.Close()

	res := make([]*Snapshot, 0, total)
	for k, v, err := cur.Seek(blockEncoded); k != nil; k, v, err = cur.Prev() {
		if err != nil {
			return nil, err
		}

		s := new(Snapshot)
		err = json.Unmarshal(v, s)
		if err != nil {
			return nil, err
		}

		s.config = c.config

		res = append(res, s)

		total--
		if total == 0 {
			break
		}
	}

	return res, nil
}

func (c *Clique) GetTransferFunc() evmtypes.TransferFunc {
	return misc.Transfer
}

func (c *Clique) GetPostApplyMessageFunc() evmtypes.PostApplyMessageFunc {
	return nil
}

func (c *Clique) ValidateBlockPostExecution(config *chain.Config, header *types.Header, gasUsed, blobGasUsed uint64, checkReceipts, checkBloom bool, receipts types.Receipts, txns types.Transactions, logger log.Logger) error {
	return rules.DefaultBlockPostValidation(config, header, gasUsed, blobGasUsed, checkReceipts, checkBloom, receipts, txns, logger)
}
