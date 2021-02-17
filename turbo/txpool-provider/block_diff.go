package txpoolprovider

import (
	"fmt"
	"math"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/turbo/adapter"
	"github.com/ledgerwatch/turbo-geth/turbo/txpool-provider/pb"
)

func buildBlockDiff(oldHead, newHead *types.Header, chainId *big.Int, db ethdb.Database) (*pb.BlockDiff, error) {
	if newHead == nil {
		return buildLatestBlockDiff(chainId, db)
	}
	if newHead.ParentHash == oldHead.Hash() {
		return buildAppliedBlockDiff(oldHead, newHead, chainId, db)

	} else {
		return buildRevertedBlockDiff(oldHead, newHead, chainId, db)
	}
}

func buildLatestBlockDiff(chainId *big.Int, db ethdb.Database) (*pb.BlockDiff, error) {
	latest, err := rawdb.ReadHeaderByHash(db, rawdb.ReadHeadBlockHash(db))
	if err != nil {
		return nil, err
	}
	parent, err := rawdb.ReadHeaderByHash(db, latest.ParentHash)
	if err != nil {
		return nil, err
	}
	return buildAppliedBlockDiff(parent, latest, chainId, db)
}

func buildAppliedBlockDiff(oldHead, newHead *types.Header, chainId *big.Int, db ethdb.Database) (*pb.BlockDiff, error) {
	included, discarded := cmpTxsAcrossFork(oldHead, newHead, db)
	reverted := types.TxDifference(discarded, included)
	diff := pb.BlockDiff_Applied{
		Applied: &pb.AppliedBlock{
			Hash:         newHead.Hash().Bytes(),
			ParentHash:   oldHead.Hash().Bytes(),
			AccountDiffs: buildAccountDiff(append(included, reverted...), chainId, db),
		},
	}
	return &pb.BlockDiff{Diff: &diff}, nil
}

func buildRevertedBlockDiff(oldHead, newHead *types.Header, chainId *big.Int, db ethdb.Database) (*pb.BlockDiff, error) {
	included, discarded := cmpTxsAcrossFork(oldHead, newHead, db)
	reverted := types.TxDifference(discarded, included)
	encoded := make([][]byte, len(reverted))
	for i, tx := range reverted {
		b, err := rlp.EncodeToBytes(tx)
		if err != nil {
			return nil, err
		}
		encoded[i] = b
	}
	diff := pb.BlockDiff_Reverted{
		Reverted: &pb.RevertedBlock{
			RevertedHash:         oldHead.Hash().Bytes(),
			NewHash:              newHead.Hash().Bytes(),
			NewParent:            newHead.ParentHash.Bytes(),
			RevertedTransactions: encoded,
			AccountDiffs:         buildAccountDiff(append(included, reverted...), chainId, db),
		},
	}
	return &pb.BlockDiff{Diff: &diff}, nil
}

func cmpTxsAcrossFork(oldHead, newHead *types.Header, db ethdb.Database) (types.Transactions, types.Transactions) {
	var discarded, included types.Transactions

	if oldHead != nil {
		getter := adapter.NewBlockGetter(db)
		oldNum := oldHead.Number.Uint64()
		newNum := newHead.Number.Uint64()

		// If the reorg is too deep, avoid doing it (will happen during fast sync)
		if depth := uint64(math.Abs(float64(oldNum) - float64(newNum))); depth > 64 {
			log.Debug("Skipping deep transaction reorg", "depth", depth)
		} else {
			// Reorg seems shallow enough to pull in all transactions into memory
			rem := getter.GetBlock(oldHead.Hash(), oldHead.Number.Uint64())
			add := getter.GetBlock(newHead.Hash(), newHead.Number.Uint64())

			if rem == nil {
				// This can happen if a setHead is performed, where we simply discard the old
				// head from the chain.
				// If that is the case, we don't have the lost transactions any more, and
				// there's nothing to add
				if newNum < oldNum {
					// If the reorg ended up on a lower number, it's indicative of setHead being the cause
					log.Debug("Skipping transaction reset caused by setHead",
						"old", oldHead.Hash(), "oldnum", oldNum, "new", newHead.Hash(), "newnum", newNum)
				} else {
					// If we reorged to a same or higher number, then it's not a case of setHead
					log.Warn("Transaction pool reset with missing oldhead",
						"old", oldHead.Hash(), "oldnum", oldNum, "new", newHead.Hash(), "newnum", newNum)
				}
				return types.Transactions{}, types.Transactions{}
			}
			for rem.NumberU64() > add.NumberU64() {
				discarded = append(discarded, rem.Transactions()...)
				if rem = getter.GetBlock(rem.ParentHash(), rem.NumberU64()-1); rem == nil {
					log.Error("Unrooted old chain seen by tx pool", "block", oldHead.Number, "hash", oldHead.Hash())
					return types.Transactions{}, types.Transactions{}
				}
			}
			for add.NumberU64() > rem.NumberU64() {
				included = append(included, add.Transactions()...)
				if add = getter.GetBlock(add.ParentHash(), add.NumberU64()-1); add == nil {
					log.Error("Unrooted new chain seen by tx pool", "block", newHead.Number, "hash", newHead.Hash())
					fmt.Println(included)
					return types.Transactions{}, types.Transactions{}
				}
			}
			for rem.Hash() != add.Hash() {
				discarded = append(discarded, rem.Transactions()...)
				if rem = getter.GetBlock(rem.ParentHash(), rem.NumberU64()-1); rem == nil {
					log.Error("Unrooted old chain seen by tx pool", "block", oldHead.Number, "hash", oldHead.Hash())
					return types.Transactions{}, types.Transactions{}
				}
				included = append(included, add.Transactions()...)
				if add = getter.GetBlock(add.ParentHash(), add.NumberU64()-1); add == nil {
					log.Error("Unrooted new chain seen by tx pool", "block", newHead.Number, "hash", newHead.Hash())
					return types.Transactions{}, types.Transactions{}
				}
			}

		}
	}

	return included, discarded
}

func buildAccountDiff(txs types.Transactions, chainId *big.Int, db ethdb.Database) []*pb.AccountInfo {
	addrs, nonces, balances := touchedAccounts(txs, chainId, db)
	diffs := make([]*pb.AccountInfo, len(addrs))

	for i, addr := range addrs {
		diff := pb.AccountInfo{
			Address: addr.Bytes(),
			Nonce:   i64tob(nonces[i]),
			Balance: balances[i].Bytes(),
		}

		diffs[i] = &diff
	}

	return diffs
}

func touchedAccounts(txs types.Transactions, chainId *big.Int, db ethdb.Database) ([]common.Address, []uint64, []*big.Int) {
	m := make(map[common.Address]bool)
	addrs := []common.Address{}
	signer := types.NewEIP155Signer(chainId)

	for _, tx := range txs {
		from, _ := types.Sender(signer, tx)
		if _, value := m[from]; !value {
			m[from] = true
			addrs = append(addrs, from)
		}
	}

	nonces := make([]uint64, len(addrs))
	balances := make([]*big.Int, len(addrs))

	for i, addr := range addrs {
		acc := new(accounts.Account)
		rawdb.PlainReadAccount(db, addr, acc)
		nonces[i] = acc.Nonce
		balances[i] = acc.Balance.ToBig()
	}

	return addrs, nonces, balances
}
