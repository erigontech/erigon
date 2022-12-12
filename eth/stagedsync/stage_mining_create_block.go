package stagedsync

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"time"

	mapset "github.com/deckarep/golang-set"
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon-lib/txpool"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/eth/ethutils"
	"github.com/ledgerwatch/erigon/params"
)

type MiningBlock struct {
	Header      *types.Header
	Uncles      []*types.Header
	Txs         types.Transactions
	Receipts    types.Receipts
	PreparedTxs types.TransactionsStream
	Withdrawals []*types.Withdrawal

	LocalTxs  types.TransactionsStream
	RemoteTxs types.TransactionsStream
}

type MiningState struct {
	MiningConfig      *params.MiningConfig
	PendingResultCh   chan *types.Block
	MiningResultCh    chan *types.Block
	MiningResultPOSCh chan *types.Block
	MiningBlock       *MiningBlock
}

func NewMiningState(cfg *params.MiningConfig) MiningState {
	return MiningState{
		MiningConfig:    cfg,
		PendingResultCh: make(chan *types.Block, 1),
		MiningResultCh:  make(chan *types.Block, 1),
		MiningBlock:     &MiningBlock{},
	}
}

func NewProposingState(cfg *params.MiningConfig) MiningState {
	return MiningState{
		MiningConfig:      cfg,
		PendingResultCh:   make(chan *types.Block, 1),
		MiningResultCh:    make(chan *types.Block, 1),
		MiningResultPOSCh: make(chan *types.Block, 1),
		MiningBlock:       &MiningBlock{},
	}
}

type MiningCreateBlockCfg struct {
	db                     kv.RwDB
	miner                  MiningState
	chainConfig            params.ChainConfig
	engine                 consensus.Engine
	txPool2                *txpool.TxPool
	txPool2DB              kv.RoDB
	tmpdir                 string
	blockBuilderParameters *core.BlockBuilderParameters
}

func StageMiningCreateBlockCfg(db kv.RwDB, miner MiningState, chainConfig params.ChainConfig, engine consensus.Engine, txPool2 *txpool.TxPool, txPool2DB kv.RoDB, blockBuilderParameters *core.BlockBuilderParameters, tmpdir string) MiningCreateBlockCfg {
	return MiningCreateBlockCfg{
		db:                     db,
		miner:                  miner,
		chainConfig:            chainConfig,
		engine:                 engine,
		txPool2:                txPool2,
		txPool2DB:              txPool2DB,
		tmpdir:                 tmpdir,
		blockBuilderParameters: blockBuilderParameters,
	}
}

var maxTransactions uint16 = 1000

// SpawnMiningCreateBlockStage
// TODO:
// - resubmitAdjustCh - variable is not implemented
func SpawnMiningCreateBlockStage(s *StageState, tx kv.RwTx, cfg MiningCreateBlockCfg, quit <-chan struct{}) (err error) {
	current := cfg.miner.MiningBlock
	txPoolLocals := []common.Address{} //txPoolV2 has no concept of local addresses (yet?)
	coinbase := cfg.miner.MiningConfig.Etherbase

	const (
		// staleThreshold is the maximum depth of the acceptable stale block.
		staleThreshold = 7
	)

	logPrefix := s.LogPrefix()
	executionAt, err := s.ExecutionAt(tx)
	if err != nil {
		return fmt.Errorf("getting last executed block: %w", err)
	}

	parent := rawdb.ReadHeaderByNumber(tx, executionAt)
	if parent == nil { // todo: how to return error and don't stop Erigon?
		return fmt.Errorf("empty block %d", executionAt)
	}

	if cfg.blockBuilderParameters != nil && cfg.blockBuilderParameters.ParentHash != parent.Hash() {
		return fmt.Errorf("wrong head block: %x (current) vs %x (requested)", parent.Hash(), cfg.blockBuilderParameters.ParentHash)
	}

	if cfg.miner.MiningConfig.Etherbase == (common.Address{}) {
		if cfg.blockBuilderParameters == nil {
			return fmt.Errorf("refusing to mine without etherbase")
		}
		// If we do not have an etherbase, let's use the suggested one
		coinbase = cfg.blockBuilderParameters.SuggestedFeeRecipient
	}

	blockNum := executionAt + 1
	localUncles, remoteUncles, err := readNonCanonicalHeaders(tx, blockNum, cfg.engine, coinbase, txPoolLocals)
	if err != nil {
		return err
	}
	chain := ChainReader{Cfg: cfg.chainConfig, Db: tx}
	var GetBlocksFromHash = func(hash common.Hash, n int) (blocks []*types.Block) {
		number := rawdb.ReadHeaderNumber(tx, hash)
		if number == nil {
			return nil
		}
		for i := 0; i < n; i++ {
			block := rawdb.ReadBlock(tx, hash, *number)
			if block == nil {
				break
			}
			blocks = append(blocks, block)
			hash = block.ParentHash()
			*number--
		}
		return
	}

	type envT struct {
		signer    *types.Signer
		ancestors mapset.Set // ancestor set (used for checking uncle parent validity)
		family    mapset.Set // family set (used for checking uncle invalidity)
		uncles    mapset.Set // uncle set
	}
	env := &envT{
		signer:    types.MakeSigner(&cfg.chainConfig, blockNum),
		ancestors: mapset.NewSet(),
		family:    mapset.NewSet(),
		uncles:    mapset.NewSet(),
	}

	// re-written miner/worker.go:commitNewWork
	var timestamp uint64
	if cfg.blockBuilderParameters == nil {
		timestamp = uint64(time.Now().Unix())
		if parent.Time >= timestamp {
			timestamp = parent.Time + 1
		}
	} else {
		// If we are on proof-of-stake timestamp should be already set for us
		timestamp = cfg.blockBuilderParameters.Timestamp
	}

	header := core.MakeEmptyHeader(parent, &cfg.chainConfig, timestamp, &cfg.miner.MiningConfig.GasLimit)
	header.Coinbase = coinbase
	header.Extra = cfg.miner.MiningConfig.ExtraData

	log.Info(fmt.Sprintf("[%s] Start mine", logPrefix), "block", executionAt+1, "baseFee", header.BaseFee, "gasLimit", header.GasLimit)

	stateReader := state.NewPlainStateReader(tx)
	ibs := state.New(stateReader)

	if err = cfg.engine.Prepare(chain, header, ibs); err != nil {
		log.Error("Failed to prepare header for mining",
			"err", err,
			"headerNumber", header.Number.Uint64(),
			"headerRoot", header.Root.String(),
			"headerParentHash", header.ParentHash.String(),
			"parentNumber", parent.Number.Uint64(),
			"parentHash", parent.Hash().String(),
			"callers", debug.Callers(10))
		return err
	}

	if cfg.blockBuilderParameters != nil {
		header.MixDigest = cfg.blockBuilderParameters.PrevRandao

		current.Header = header
		current.Uncles = nil
		current.Withdrawals = cfg.blockBuilderParameters.Withdrawals
		return nil
	}

	// If we are care about TheDAO hard-fork check whether to override the extra-data or not
	if daoBlock := cfg.chainConfig.DAOForkBlock; daoBlock != nil {
		// Check whether the block is among the fork extra-override range
		limit := new(big.Int).Add(daoBlock, params.DAOForkExtraRange)
		if header.Number.Cmp(daoBlock) >= 0 && header.Number.Cmp(limit) < 0 {
			// Depending whether we support or oppose the fork, override differently
			if cfg.chainConfig.DAOForkSupport {
				header.Extra = libcommon.Copy(params.DAOForkBlockExtra)
			} else if bytes.Equal(header.Extra, params.DAOForkBlockExtra) {
				header.Extra = []byte{} // If miner opposes, don't let it use the reserved extra-data
			}
		}
	}

	// analog of miner.Worker.updateSnapshot
	var makeUncles = func(proposedUncles mapset.Set) []*types.Header {
		var uncles []*types.Header
		proposedUncles.Each(func(item interface{}) bool {
			hash, ok := item.(common.Hash)
			if !ok {
				return false
			}

			uncle, exist := localUncles[hash]
			if !exist {
				uncle, exist = remoteUncles[hash]
			}
			if !exist {
				return false
			}
			uncles = append(uncles, uncle)
			return false
		})
		return uncles
	}

	// when 08 is processed ancestors contain 07 (quick block)
	for _, ancestor := range GetBlocksFromHash(parent.Hash(), 7) {
		for _, uncle := range ancestor.Uncles() {
			env.family.Add(uncle.Hash())
		}
		env.family.Add(ancestor.Hash())
		env.ancestors.Add(ancestor.Hash())
	}
	commitUncle := func(env *envT, uncle *types.Header) error {
		hash := uncle.Hash()
		if env.uncles.Contains(hash) {
			return errors.New("uncle not unique")
		}
		if parent.Hash() == uncle.ParentHash {
			return errors.New("uncle is sibling")
		}
		if !env.ancestors.Contains(uncle.ParentHash) {
			return errors.New("uncle's parent unknown")
		}
		if env.family.Contains(hash) {
			return errors.New("uncle already included")
		}
		env.uncles.Add(uncle.Hash())
		return nil
	}

	// Accumulate the miningUncles for the env block
	// Prefer to locally generated uncle
	uncles := make([]*types.Header, 0, 2)
	for _, blocks := range []map[common.Hash]*types.Header{localUncles, remoteUncles} {
		// Clean up stale uncle blocks first
		for hash, uncle := range blocks {
			if uncle.Number.Uint64()+staleThreshold <= header.Number.Uint64() {
				delete(blocks, hash)
			}
		}
		for hash, uncle := range blocks {
			if len(uncles) == 2 {
				break
			}
			if err = commitUncle(env, uncle); err != nil {
				log.Trace("Possible uncle rejected", "hash", hash, "reason", err)
			} else {
				log.Trace("Committing new uncle to block", "hash", hash)
				uncles = append(uncles, uncle)
			}
		}
	}

	current.Header = header
	current.Uncles = makeUncles(env.uncles)
	current.Withdrawals = nil
	return nil
}

func readNonCanonicalHeaders(tx kv.Tx, blockNum uint64, engine consensus.Engine, coinbase common.Address, txPoolLocals []common.Address) (localUncles, remoteUncles map[common.Hash]*types.Header, err error) {
	localUncles, remoteUncles = map[common.Hash]*types.Header{}, map[common.Hash]*types.Header{}
	nonCanonicalBlocks, err := rawdb.ReadHeadersByNumber(tx, blockNum)
	if err != nil {
		return
	}
	for _, u := range nonCanonicalBlocks {
		if ethutils.IsLocalBlock(engine, coinbase, txPoolLocals, u) {
			localUncles[u.Hash()] = u
		} else {
			remoteUncles[u.Hash()] = u
		}

	}
	return
}

func filterBadTransactions(transactions []types.Transaction, config params.ChainConfig, blockNumber uint64, baseFee *big.Int, simulationTx *memdb.MemoryMutation) ([]types.Transaction, error) {
	initialCnt := len(transactions)
	var filtered []types.Transaction
	gasBailout := config.Consensus == params.ParliaConsensus

	missedTxs := 0
	noSenderCnt := 0
	noAccountCnt := 0
	nonceTooLowCnt := 0
	notEOACnt := 0
	feeTooLowCnt := 0
	balanceTooLowCnt := 0
	overflowCnt := 0
	for len(transactions) > 0 && missedTxs != len(transactions) {
		transaction := transactions[0]
		sender, ok := transaction.GetSender()
		if !ok {
			transactions = transactions[1:]
			noSenderCnt++
			continue
		}
		var account accounts.Account
		ok, err := rawdb.ReadAccount(simulationTx, sender, &account)
		if err != nil {
			return nil, err
		}
		if !ok {
			transactions = transactions[1:]
			noAccountCnt++
			continue
		}
		// Check transaction nonce
		if account.Nonce > transaction.GetNonce() {
			transactions = transactions[1:]
			nonceTooLowCnt++
			continue
		}
		if account.Nonce < transaction.GetNonce() {
			missedTxs++
			transactions = append(transactions[1:], transaction)
			continue
		}
		missedTxs = 0

		// Make sure the sender is an EOA (EIP-3607)
		if !account.IsEmptyCodeHash() {
			transactions = transactions[1:]
			notEOACnt++
			continue
		}

		if config.IsLondon(blockNumber) {
			baseFee256 := uint256.NewInt(0)
			if overflow := baseFee256.SetFromBig(baseFee); overflow {
				return nil, fmt.Errorf("bad baseFee %s", baseFee)
			}
			// Make sure the transaction gasFeeCap is greater than the block's baseFee.
			if !transaction.GetFeeCap().IsZero() || !transaction.GetTip().IsZero() {
				if err := core.CheckEip1559TxGasFeeCap(sender, transaction.GetFeeCap(), transaction.GetTip(), baseFee256, false /* isFree */); err != nil {
					transactions = transactions[1:]
					feeTooLowCnt++
					continue
				}
			}
		}
		txnGas := transaction.GetGas()
		txnPrice := transaction.GetPrice()
		value := transaction.GetValue()
		accountBalance := account.Balance

		want := uint256.NewInt(0)
		want.SetUint64(txnGas)
		want, overflow := want.MulOverflow(want, txnPrice)
		if overflow {
			transactions = transactions[1:]
			overflowCnt++
			continue
		}

		if transaction.GetFeeCap() != nil {
			want.SetUint64(txnGas)
			want, overflow = want.MulOverflow(want, transaction.GetFeeCap())
			if overflow {
				transactions = transactions[1:]
				overflowCnt++
				continue
			}
			want, overflow = want.AddOverflow(want, value)
			if overflow {
				transactions = transactions[1:]
				overflowCnt++
				continue
			}
		}

		if accountBalance.Cmp(want) < 0 {
			if !gasBailout {
				transactions = transactions[1:]
				balanceTooLowCnt++
				continue
			}
		}
		// Updates account in the simulation
		account.Nonce++
		account.Balance.Sub(&account.Balance, want)
		accountBuffer := make([]byte, account.EncodingLengthForStorage())
		account.EncodeForStorage(accountBuffer)
		if err := simulationTx.Put(kv.PlainState, sender[:], accountBuffer); err != nil {
			return nil, err
		}
		// Mark transaction as valid
		filtered = append(filtered, transaction)
		transactions = transactions[1:]
	}
	log.Debug("Filtration", "initial", initialCnt, "no sender", noSenderCnt, "no account", noAccountCnt, "nonce too low", nonceTooLowCnt, "nonceTooHigh", missedTxs, "sender not EOA", notEOACnt, "fee too low", feeTooLowCnt, "overflow", overflowCnt, "balance too low", balanceTooLowCnt, "filtered", len(filtered))
	return filtered, nil
}
