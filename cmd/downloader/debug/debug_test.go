package debug

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/snapshotdb"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"
)

const (
	AccountDiff  = "accdiff"
	StorageDiff  = "stdiff"
	ContractDiff = "contractdiff"
	Deleted      = "it is deleted"
)

func WithBlock(block uint64, key []byte) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, block)
	return append(b, key...)
}
func TestMatreshkaStream(t *testing.T) {
	t.Skip()
	chaindataDir := "/media/b00ris/nvme/fresh_sync/tg/chaindata"
	tmpDbDir := "/home/b00ris/event_stream"

	chaindata, err := mdbx.Open(chaindataDir, log.New(), true)
	if err != nil {
		t.Fatal(err)
	}
	//tmpDb:=ethdb.NewMemDatabase()
	os.RemoveAll(tmpDbDir)

	db, err := mdbx.NewMDBX(log.New()).Path(tmpDbDir).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		defaultBuckets[AccountDiff] = kv.TableCfgItem{}
		defaultBuckets[StorageDiff] = kv.TableCfgItem{}
		defaultBuckets[ContractDiff] = kv.TableCfgItem{}
		return defaultBuckets
	}).Open()
	if err != nil {
		t.Fatal(err)
	}

	chainConfig, _, genesisErr := core.CommitGenesisBlock(db, core.DefaultGenesisBlock())
	if genesisErr != nil {
		t.Fatal(err)
	}
	if err := db.Update(context.Background(), func(tx kv.RwTx) error {
		return tx.ClearBucket(kv.HeadHeaderKey)
	}); err != nil {
		t.Fatal(err)
	}

	snkv := snapshotdb.NewSnapshotKV().DB(db).
		//broken
		//SnapshotDB([]string{dbutils.Headers, dbutils.HeaderCanonical, dbutils.HeaderTD, dbutils.HeaderNumber, dbutils.BlockBody, dbutils.HeadHeaderKey, dbutils.Senders}, chaindata.RwDB()).
		Open()
	_ = chaindata
	defer snkv.Close()

	tx, err := snkv.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	//
	//tx, err := db.Begin(context.Background(), ethdb.RW)
	//if err != nil {
	//	t.Fatal(err)
	//}
	psCursor, err := tx.Cursor(kv.PlainState)
	if err != nil {
		t.Fatal(err)
	}

	i := 5
	err = ethdb.Walk(psCursor, []byte{}, 0, func(k, v []byte) (bool, error) {
		fmt.Println(common.Bytes2Hex(k))
		i--
		if i == 0 {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	currentBlock := rawdb.ReadCurrentHeader(tx)
	fmt.Println("currentBlock", currentBlock.Number.Uint64())
	blockNum := uint64(1)
	limit := currentBlock.Number.Uint64()
	getHeader := func(hash common.Hash, number uint64) *types.Header { return rawdb.ReadHeader(tx, hash, number) }

	stateReaderWriter := NewDebugReaderWriter(state.NewPlainStateReader(tx), state.NewPlainStateWriter(tx, tx, blockNum))
	tt := time.Now()
	ttt := time.Now()
	for currentBlock := blockNum; currentBlock < blockNum+limit; currentBlock++ {
		stateReaderWriter.UpdateWriter(state.NewPlainStateWriter(tx, tx, currentBlock))
		block, err := rawdb.ReadBlockByNumber(tx, currentBlock)
		if err != nil {
			t.Fatal(err, currentBlock)
		}

		contractHasTEVM := ethdb.GetHasTEVM(tx)

		_, _, err = core.ExecuteBlockEphemerally(chainConfig, &vm.Config{NoReceipts: true}, getHeader, ethash.NewFaker(), block, stateReaderWriter, stateReaderWriter, nil, nil, contractHasTEVM)
		if err != nil {
			t.Fatal(err, currentBlock)
		}
		cs := stateReaderWriter.UpdatedAccouts()
		accDiffLen := len(cs)
		for i := range cs {
			if len(cs[i].Value) == 0 {
				cs[i].Value = []byte(Deleted)
			}
			err = tx.Put(AccountDiff, WithBlock(currentBlock, cs[i].Key), cs[i].Value)
			if err != nil {
				t.Fatal(err, cs[i].Key, currentBlock)
			}
		}
		cs = stateReaderWriter.UpdatedStorage()
		stDiffLen := len(cs)
		for i := range cs {
			if len(cs[i].Value) == 0 {
				cs[i].Value = []byte(Deleted)
			}
			err = tx.Put(StorageDiff, WithBlock(currentBlock, cs[i].Key), cs[i].Value)
			if err != nil {
				t.Fatal(err, cs[i].Key, currentBlock)
			}
		}
		cs = stateReaderWriter.UpdatedCodes()
		codesDiffLen := len(cs)
		for i := range cs {
			if len(cs[i].Value) == 0 {
				cs[i].Value = []byte(Deleted)
			}
			err = tx.Put(ContractDiff, WithBlock(currentBlock, cs[i].Key), cs[i].Value)
			if err != nil {
				t.Fatal(err, cs[i].Key, currentBlock)
			}
		}

		stateReaderWriter.Reset()
		if currentBlock%10000 == 0 {
			err = tx.Commit()
			if err != nil {
				t.Fatal(err, currentBlock)
			}
			tx, err = snkv.BeginRw(context.Background())
			if err != nil {
				t.Fatal(err, currentBlock)
			}
			defer tx.Rollback()

			dr := time.Since(ttt)
			fmt.Println(currentBlock, "finished", "acc-", accDiffLen, "st-", stDiffLen, "codes - ", codesDiffLen, "all -", time.Since(tt), "chunk - ", dr, "blocks/s", 10000/dr.Seconds())
			ttt = time.Now()
		}
	}
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("End")
	//spew.Dump("readAcc",len(stateReaderWriter.readAcc))
	//spew.Dump("readStr",len(stateReaderWriter.readStorage))
	//spew.Dump("createdContracts", len(stateReaderWriter.createdContracts))
	//spew.Dump("deleted",len(stateReaderWriter.deletedAcc))

}

var _ state.StateReader = &DebugReaderWriter{}
var _ state.WriterWithChangeSets = &DebugReaderWriter{}

func NewDebugReaderWriter(r state.StateReader, w state.WriterWithChangeSets) *DebugReaderWriter {
	return &DebugReaderWriter{
		r: r,
		w: w,
		//readAcc: make(map[common.Address]struct{}),
		//readStorage: make(map[string]struct{}),
		//readCodes: make(map[common.Hash]struct{}),
		//readIncarnations: make(map[common.Address]struct{}),

		updatedAcc:     make(map[common.Address][]byte),
		updatedStorage: make(map[string][]byte),
		updatedCodes:   make(map[common.Hash][]byte),
		//deletedAcc: make(map[common.Address]struct{}),
		//createdContracts: make(map[common.Address]struct{}),

	}
}

type DebugReaderWriter struct {
	r state.StateReader
	w state.WriterWithChangeSets
	//readAcc map[common.Address]struct{}
	//readStorage map[string]struct{}
	//readCodes map[common.Hash] struct{}
	//readIncarnations map[common.Address] struct{}
	updatedAcc     map[common.Address][]byte
	updatedStorage map[string][]byte
	updatedCodes   map[common.Hash][]byte
	//deletedAcc map[common.Address]struct{}
	//createdContracts map[common.Address]struct{}
}

func (d *DebugReaderWriter) Reset() {
	d.updatedAcc = map[common.Address][]byte{}
	d.updatedStorage = map[string][]byte{}
	d.updatedCodes = map[common.Hash][]byte{}
}
func (d *DebugReaderWriter) UpdateWriter(w state.WriterWithChangeSets) {
	d.w = w
}

func (d *DebugReaderWriter) ReadAccountData(address common.Address) (*accounts.Account, error) {
	//d.readAcc[address] = struct{}{}
	return d.r.ReadAccountData(address)
}

func (d *DebugReaderWriter) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	//d.readStorage[string(dbutils.PlainGenerateCompositeStorageKey(address.Bytes(),incarnation, key.Bytes()))] = struct{}{}
	return d.r.ReadAccountStorage(address, incarnation, key)
}

func (d *DebugReaderWriter) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	//d.readCodes[codeHash] = struct{}{}
	return d.r.ReadAccountCode(address, incarnation, codeHash)
}

func (d *DebugReaderWriter) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	return d.r.ReadAccountCodeSize(address, incarnation, codeHash)
}

func (d *DebugReaderWriter) ReadAccountIncarnation(address common.Address) (uint64, error) {
	//d.readIncarnations[address] = struct{}{}
	return d.r.ReadAccountIncarnation(address)
}

func (d *DebugReaderWriter) WriteChangeSets() error {
	return d.w.WriteChangeSets()
}

func (d *DebugReaderWriter) WriteHistory() error {
	return d.w.WriteHistory()
}

func (d *DebugReaderWriter) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	b, err := rlp.EncodeToBytes(account)
	if err != nil {
		return err
	}
	d.updatedAcc[address] = b
	return d.w.UpdateAccountData(address, original, account)
}

func (d *DebugReaderWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	d.updatedCodes[codeHash] = code
	return d.w.UpdateAccountCode(address, incarnation, codeHash, code)
}

func (d *DebugReaderWriter) DeleteAccount(address common.Address, original *accounts.Account) error {
	d.updatedAcc[address] = nil
	//d.deletedAcc[address]= struct{}{}
	return d.w.DeleteAccount(address, original)
}

func (d *DebugReaderWriter) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	d.updatedStorage[string(dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), incarnation, key.Bytes()))] = value.Bytes()
	return d.w.WriteAccountStorage(address, incarnation, key, original, value)
}

func (d *DebugReaderWriter) CreateContract(address common.Address) error {
	//d.createdContracts[address] = struct{}{}
	return d.w.CreateContract(address)
}

type Change struct {
	Key   []byte
	Value []byte
}

func (d *DebugReaderWriter) UpdatedAccouts() []Change {
	ch := make([]Change, 0, len(d.updatedAcc))
	for k, v := range d.updatedAcc {
		ch = append(ch, Change{
			Key:   common.CopyBytes(k.Bytes()),
			Value: common.CopyBytes(v),
		})
	}
	return ch
}
func (d *DebugReaderWriter) UpdatedStorage() []Change {
	ch := make([]Change, 0, len(d.updatedStorage))
	for k, v := range d.updatedStorage {
		ch = append(ch, Change{
			Key:   common.CopyBytes([]byte(k)),
			Value: common.CopyBytes(v),
		})
	}
	return ch

}
func (d *DebugReaderWriter) UpdatedCodes() []Change {
	ch := make([]Change, 0, len(d.updatedCodes))
	for k, v := range d.updatedCodes {
		ch = append(ch, Change{
			Key:   common.CopyBytes(k.Bytes()),
			Value: common.CopyBytes(v),
		})
	}
	return ch
}

//func (d *DebugReaderWriter) AllAccounts() map[common.Address]struct{}  {
//	accs:=make(map[common.Address]struct{})
//	for i:=range d.readAcc {
//		accs[i]=struct{}{}
//	}
//	for i:=range d.updatedAcc {
//		accs[i]=struct{}{}
//	}
//	for i:=range d.readIncarnations {
//		accs[i]=struct{}{}
//	}
//	for i:=range d.deletedAcc {
//		accs[i]=struct{}{}
//	}
//	for i:=range d.createdContracts {
//		accs[i]=struct{}{}
//	}
//	return accs
//}
//func (d *DebugReaderWriter) AllStorage() map[string]struct{}  {
//	st:=make(map[string]struct{})
//	for i:=range d.readStorage {
//		st[i]=struct{}{}
//	}
//	for i:=range d.updatedStorage {
//		st[i]=struct{}{}
//	}
//	return st
//}
//func (d *DebugReaderWriter) AllCodes() map[common.Hash]struct{}  {
//	c:=make(map[common.Hash]struct{})
//	for i:=range d.readCodes {
//		c[i]=struct{}{}
//	}
//	for i:=range d.updatedCodes {
//		c[i]=struct{}{}
//	}
//	return c
//}
