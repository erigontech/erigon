package stagedsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"slices"

	"github.com/ethereum/go-verkle"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/dbutils"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cmd/verkle/verkletrie"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/ledgerwatch/erigon/turbo/trie/vkutils"
)

func int256ToVerkleFormat(x *uint256.Int, buffer []byte) {
	bbytes := x.ToBig().Bytes()
	if len(bbytes) > 0 {
		for i, b := range bbytes {
			buffer[len(bbytes)-i-1] = b
		}
	}
}

func SpawnVerkleTrieStage(s *StageState, u Unwinder, tx kv.RwTx, cfg TrieCfg, ctx context.Context, logger log.Logger) (libcommon.Hash, error) {
	var err error
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			// TODO @somnathb1 - check empty root instead of libcommon.Hash{}
			return libcommon.Hash{}, err
		}
		defer tx.Rollback()
	}
	to, err := s.ExecutionAt(tx)
	if err != nil {
		return libcommon.Hash{}, err
	}

	if s.BlockNumber > to { // Erigon will self-heal (download missed blocks) eventually
		return trie.EmptyRoot, nil
	}

	if s.BlockNumber == to {
		// we already did hash check for this block
		// we don't do the obvious `if s.BlockNumber > to` to support reorgs more naturally
		return trie.EmptyRoot, nil
	}

	from := uint64(0)
	if s.BlockNumber >= 0 {
		from = s.BlockNumber + 1
	}

	var (
	// rootVerkleNode verkle.VerkleNode
	)

	logPrefix := s.LogPrefix()
	// if to > s.BlockNumber+16 {
	logger.Info(fmt.Sprintf("[%s] Computing Verkle Root", logPrefix), "from", s.BlockNumber, "to", to)
	// }

	if s.BlockNumber == 0 {
		GenerateGenesisTree(tx, core.VerkleGenDevnet6GenesisBlock())
	}

	rootHash, err := rawdb.ReadVerkleRoot(tx, s.BlockNumber)
	if err != nil {
		return libcommon.Hash{}, err
	}

	var newRoot libcommon.Hash

	vTrie, err := trie.OpenVKTrie(rootHash, tx)
	if err != nil {
		return libcommon.Hash{}, err
	}
	accChangesCursor, err := tx.CursorDupSort(kv.AccountChangeSet)
	if err != nil {
		return libcommon.Hash{}, err
	}
	defer accChangesCursor.Close()

	for k, v, err := accChangesCursor.Seek(hexutility.EncodeTs(from)); k != nil; k, v, err = accChangesCursor.Next() {
		// for k, v, err := accChangesCursor.Seek(hexutility.EncodeTs(from)); k != nil; k, v, err = accChangesCursor.Next() {
		if err != nil {
			return libcommon.Hash{}, err
		}
		blockNumber, addressBytes, _, err := historyv2.DecodeAccounts(k, v)
		if err != nil {
			return libcommon.Hash{}, err
		}
		if blockNumber > to {
			break
		}

		encodedAccount, err := tx.GetOne(kv.PlainState, addressBytes)
		if err != nil {
			return libcommon.Hash{}, err
		}

		incarnationBytes, err := tx.GetOne(kv.IncarnationMap, addressBytes)
		if err != nil {
			return libcommon.Hash{}, err
		}
		isContract := len(incarnationBytes) > 0 && binary.BigEndian.Uint64(incarnationBytes) != 0

		accountAddr := libcommon.BytesToAddress(addressBytes)
		if len(encodedAccount) == 0 {
			vTrie.DeleteAccount(accountAddr)
		} else {
			acc := &accounts.Account{}
			if err := acc.DecodeForStorage(encodedAccount); err != nil {
				return libcommon.Hash{}, err
			}
			tryCodeHash, err := tx.GetOne(kv.PlainContractCode, dbutils.PlainGenerateStoragePrefix(addressBytes[:], acc.Incarnation))
			log.Debug("Lo behold", "anotherway", tryCodeHash)
			if err := vTrie.UpdateAccount(accountAddr, acc); err != nil {
				return libcommon.Hash{}, err
			}
			code, err := tx.GetOne(kv.Code, acc.CodeHash[:])
			if err != nil {
				return libcommon.Hash{}, err
			}
			if len(code) != 0 && isContract {
				vTrie.UpdateContractCode(accountAddr, acc.CodeHash, code)
			}
		}
	}

	storageCursor, err := tx.CursorDupSort(kv.StorageChangeSet)

	// for k, v, err := storageCursor.Seek(hexutility.EncodeTs(from)); k != nil; k, v, err = storageCursor.Next() {
	for k, v, err := storageCursor.Seek(hexutility.EncodeTs(from)); k != nil; k, v, err = storageCursor.Next() {
		if err != nil {
			return libcommon.Hash{}, err
		}
		blockNumber, changesetKey, _, err := historyv2.DecodeStorage(k, v)
		if err != nil {
			return libcommon.Hash{}, err
		}

		if blockNumber > to {
			break
		}

		address := libcommon.BytesToAddress(changesetKey[:20])

		storageValue, err := tx.GetOne(kv.PlainState, changesetKey)
		if err != nil {
			return libcommon.Hash{}, err
		}
		// var storageValueFormatted []byte

		// if len(storageValue) > 0 {
		// 	storageValueFormatted = make([]byte, 32)
		// 	int256ToVerkleFormat(new(uint256.Int).SetBytes(storageValue), storageValueFormatted)
		// }

		// vTrie.UpdateStorage(address, changesetKey[28:], storageValueFormatted)
		vTrie.UpdateStorage(address, changesetKey[28:], storageValue)
	}

	newRoot, err = vTrie.Commit(true)
	if err != nil {
		return libcommon.Hash{}, err
	}

	if cfg.checkRoot {
		header := rawdb.ReadHeaderByNumber(tx, to)
		if header.Root != newRoot {
			return libcommon.Hash{}, fmt.Errorf("invalid verkle root for block %d header has %x, computed: %x", header.Number.Uint64(), header.Root, newRoot)
		}
	}

	if err := s.Update(tx, to); err != nil {
		return libcommon.Hash{}, err
	}

	// TODO @somnathb1
	if err := stages.SaveStageProgress(tx, stages.VerkleTrie, to); err != nil {
		return libcommon.Hash{}, err
	}

	if err := stages.SaveStageProgress(tx, stages.IntermediateHashes, to); err != nil {
		return libcommon.Hash{}, err
	}
	if !useExternalTx {
		return newRoot, tx.Commit()
	}
	rawdb.WriteVerkleRoot(tx, to, newRoot)

	logger.Info(fmt.Sprintf("[%s] Completed on", logPrefix), "block", to, "Verkle Root", newRoot)
	return newRoot, nil
}

func sortedAllocKeys(m types.GenesisAlloc) []string {
	keys := make([]string, len(m))
	i := 0
	for k := range m {
		keys[i] = string(k.Bytes())
		i++
	}
	slices.Sort(keys)
	return keys
}


func GenerateGenesisTree(tx kv.RwTx, g *types.Genesis) {
	// defer tx.Rollback()

		// Construct header
		head := &types.Header{
			Number:        new(big.Int).SetUint64(g.Number),
			Nonce:         types.EncodeNonce(g.Nonce),
			Time:          g.Timestamp,
			ParentHash:    g.ParentHash,
			Extra:         g.ExtraData,
			GasLimit:      g.GasLimit,
			GasUsed:       g.GasUsed,
			Difficulty:    g.Difficulty,
			MixDigest:     g.Mixhash,
			Coinbase:      g.Coinbase,
			BaseFee:       g.BaseFee,
			BlobGasUsed:   g.BlobGasUsed,
			ExcessBlobGas: g.ExcessBlobGas,
			AuRaStep:      g.AuRaStep,
			AuRaSeal:      g.AuRaSeal,
		}
		if g.GasLimit == 0 {
			head.GasLimit = params.GenesisGasLimit
		}
		if g.Difficulty == nil {
			head.Difficulty = params.GenesisDifficulty
		}
		if g.Config != nil && g.Config.IsLondon(0) {
			if g.BaseFee != nil {
				head.BaseFee = g.BaseFee
			} else {
				head.BaseFee = new(big.Int).SetUint64(params.InitialBaseFee)
			}
		}
		if g.Config != nil && g.Config.IsCancun(g.Timestamp) {
			if g.BlobGasUsed != nil {
				head.BlobGasUsed = g.BlobGasUsed
			} else {
				head.BlobGasUsed = new(uint64)
			}
			if g.ExcessBlobGas != nil {
				head.ExcessBlobGas = g.ExcessBlobGas
			} else {
				head.ExcessBlobGas = new(uint64)
			}
			if g.ParentBeaconBlockRoot != nil {
				head.ParentBeaconBlockRoot = g.ParentBeaconBlockRoot
			} else {
				head.ParentBeaconBlockRoot = &libcommon.Hash{}
			}
		}


	r, w := state.NewDbStateReader(tx), state.NewDbStateWriter(tx, 0)
	statedb := state.New(r)

	//Create in-memory instance of verkle trie
	vTrie := trie.NewVerkleTrie(verkle.New(), nil, tx, vkutils.NewPointCache(), true)

	// Loop through alloc keys
	keys := sortedAllocKeys(g.Alloc)
	for _, key := range keys {
		addr := libcommon.BytesToAddress([]byte(key))
		account := g.Alloc[addr]

		coreAcc := &accounts.Account{
			Nonce:    account.Nonce,
			Balance:  *uint256.MustFromBig(account.Balance),
			CodeHash: crypto.Keccak256Hash(account.Code),
		}

		// Update account in verkle trie
		vTrie.UpdateAccount(addr, coreAcc)
		if account.Code != nil && len(account.Code) > 0 {
			vTrie.UpdateContractCode(addr, crypto.Keccak256Hash(account.Code), account.Code)
		}

		//Update account bits to (temp) statedb
		statedb.AddBalance(addr, &coreAcc.Balance)
		statedb.SetCode(addr, account.Code)
		statedb.SetNonce(addr, account.Nonce)

		// Add storage bits to verkle trie and statedb
		for storageKey, storageItem := range account.Storage {
			vTrie.UpdateStorage(addr, storageKey.Bytes(), storageItem.Bytes())
			statedb.SetState(addr, &storageKey, *uint256.NewInt(0).SetBytes(storageItem.Bytes()))
		}

		if len(account.Constructor) > 0 {
			if _, err := core.SysCreate(addr, account.Constructor, *g.Config, statedb, head); err != nil {
				return
			}
		}
		if len(account.Code) > 0 || len(account.Storage) > 0 || len(account.Constructor) > 0 {
			statedb.SetIncarnation(addr, state.FirstContractIncarnation)
		}

	}

	if err := statedb.FinalizeTx(&chain.Rules{}, w); err != nil {
		return
	}
	var err error
	head.Root, err = vTrie.Commit(true)
	if err != nil {
		log.Error("Error calculating verkle trie root", "Msg", err)
		return
	}
	rawdb.WriteVerkleRoot(tx, 0, head.Root)
}

// TODO @somnathb1

// DONT USE
// func SpawnVerkleTrie(s *StageState, u Unwinder, tx kv.RwTx, cfg TrieCfg, ctx context.Context, logger log.Logger) (libcommon.Hash, error) {
// 	var err error
// 	useExternalTx := tx != nil
// 	if !useExternalTx {
// 		tx, err = cfg.db.BeginRw(ctx)
// 		if err != nil {
// 			return libcommon.Hash{}, err
// 		}
// 		defer tx.Rollback()
// 	}
// 	from := uint64(0)
// 	if s.BlockNumber > 0 {
// 		from = s.BlockNumber + 1
// 	}
// 	to, err := s.ExecutionAt(tx)
// 	if err != nil {
// 		return libcommon.Hash{}, err
// 	}
// 	verkleWriter := verkletrie.NewVerkleTreeWriter(tx, cfg.tmpDir, logger)
// 	if err := verkletrie.IncrementAccount(tx, tx, 10, verkleWriter, from, to, cfg.tmpDir); err != nil {
// 		return libcommon.Hash{}, err
// 	}
// 	var newRoot libcommon.Hash
// 	if newRoot, err = verkletrie.IncrementStorage(tx, tx, 10, verkleWriter, from, to, cfg.tmpDir); err != nil {
// 		return libcommon.Hash{}, err
// 	}
// 	if cfg.checkRoot {
// 		header := rawdb.ReadHeaderByNumber(tx, to)
// 		if header.Root != newRoot {
// 			return libcommon.Hash{}, fmt.Errorf("invalid verkle root, header has %x, computed: %x", header.Root, newRoot)
// 		}
// 	}
// 	if err := s.Update(tx, to); err != nil {
// 		return libcommon.Hash{}, err
// 	}
// 	if err := stages.SaveStageProgress(tx, stages.VerkleTrie, to); err != nil {
// 		return libcommon.Hash{}, err
// 	}
// 	if !useExternalTx {
// 		return newRoot, tx.Commit()
// 	}
// 	return newRoot, nil
// }

func UnwindVerkleTrie(u *UnwindState, s *StageState, tx kv.RwTx, cfg TrieCfg, ctx context.Context, logger log.Logger) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	from := u.UnwindPoint + 1
	to, err := s.ExecutionAt(tx)
	if err != nil {
		return err
	}
	verkleWriter := verkletrie.NewVerkleTreeWriter(tx, cfg.tmpDir, logger)
	if err := verkletrie.IncrementAccount(tx, tx, 10, verkleWriter, from, to, cfg.tmpDir); err != nil {
		return err
	}
	if _, err = verkletrie.IncrementStorage(tx, tx, 10, verkleWriter, from, to, cfg.tmpDir); err != nil {
		return err
	}
	if err := s.Update(tx, from); err != nil {
		return err
	}

	//TODO @somnathb1
	if err := stages.SaveStageProgress(tx, stages.VerkleTrie, from); err != nil {
		return err
	}

	if err := stages.SaveStageProgress(tx, stages.VerkleTrie, from); err != nil {
		return err
	}
	if !useExternalTx {
		return tx.Commit()
	}
	return nil
}

func PruneVerkleTries(s *PruneState, tx kv.RwTx, cfg TrieCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	s.Done(tx)

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
