package debug

import (
	"context"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"os"
	"testing"
)

func TestName(t *testing.T) {
	chaindataDir:="/media/b00ris/nvme/fresh_sync/tg/chaindata"
	tmpDbDir:="/media/b00ris/nvme/tmp/debug2"
	tmpDbDir2:="/media/b00ris/nvme/tmp/debug3"

	chaindata,err:=ethdb.Open(chaindataDir, true)
	if err!=nil {
		t.Fatal(err)
	}
	//tmpDb:=ethdb.NewMemDatabase()
	os.RemoveAll(tmpDbDir)
	os.RemoveAll(tmpDbDir2)
	tmpDb,err:=ethdb.Open(tmpDbDir, false)
	if err!=nil {
		t.Fatal(err)
	}
	snkv:=ethdb.NewSnapshotKV().DB(tmpDb.RwKV()).SnapshotDB([]string{dbutils.HeadersBucket, dbutils.HeaderCanonicalBucket, dbutils.HeaderTDBucket, dbutils.HeaderNumberBucket, dbutils.BlockBodyPrefix, dbutils.HeadHeaderKey, dbutils.Senders}, chaindata.RwKV()).Open()
	db:=ethdb.NewObjectDatabase(snkv)

	tx,err:=db.Begin(context.Background(), ethdb.RW)
	if err!=nil {
		t.Fatal(err)
	}
	blockNum:=uint64(0)
	limit:=uint64(10)
	blockchain, err := core.NewBlockChain(tx, nil, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{
		NoReceipts: true,
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	stateReaderWriter := NewDebugReaderWriter(state.NewPlainStateReader(tx), state.NewPlainStateWriter(tx, tx,blockNum))
	for i:=blockNum; i<blockNum+limit; i++ {
		fmt.Println("exsecuted", i)
		stateReaderWriter.UpdateWriter(state.NewPlainStateWriter(tx, tx, i))

		block, err:=rawdb.ReadBlockByNumber(chaindata, i)
		if err!=nil {
			t.Fatal(err)
		}
		_, err = core.ExecuteBlockEphemerally(blockchain.Config(), blockchain.GetVMConfig(), nil, ethash.NewFaker(),  block, stateReaderWriter, stateReaderWriter)
		if err != nil {
			t.Fatal(err)
		}

	}
	tx.Rollback()
	fmt.Println("End")
	spew.Dump("readAcc",len(stateReaderWriter.readAcc))
	spew.Dump("readStr",len(stateReaderWriter.readStorage))
	spew.Dump("createdContracts", len(stateReaderWriter.createdContracts))
	spew.Dump("deleted",len(stateReaderWriter.deletedAcc))

}


var _ state.StateReader = &DebugReaderWriter{}
var _ state.WriterWithChangeSets = &DebugReaderWriter{}

func NewDebugReaderWriter(r state.StateReader, w state.WriterWithChangeSets) *DebugReaderWriter {
	return &DebugReaderWriter{
		r:   r,
		w: w,
		readAcc: make(map[common.Address]struct{}),
		readStorage: make(map[string]struct{}),
		readCodes: make(map[common.Hash]struct{}),
		readIncarnations: make(map[common.Address]struct{}),

		updatedAcc: make(map[common.Address]struct{}),
		updatedStorage:make(map[string]struct{}),
		updatedCodes: make(map[common.Hash]struct{}),
		deletedAcc: make(map[common.Address]struct{}),
		createdContracts: make(map[common.Address]struct{}),


	}
}
type DebugReaderWriter struct {
	r state.StateReader
	w state.WriterWithChangeSets
	readAcc map[common.Address]struct{}
	readStorage map[string]struct{}
	readCodes map[common.Hash] struct{}
	readIncarnations map[common.Address] struct{}
	updatedAcc map[common.Address]struct{}
	updatedStorage map[string]struct{}
	updatedCodes map[common.Hash]struct{}
	deletedAcc map[common.Address]struct{}
	createdContracts map[common.Address]struct{}
}
func (d *DebugReaderWriter) UpdateWriter(w state.WriterWithChangeSets) {
	d.w = w
}

func (d *DebugReaderWriter) ReadAccountData(address common.Address) (*accounts.Account, error) {
	d.readAcc[address] = struct{}{}
	return d.r.ReadAccountData(address)
}

func (d *DebugReaderWriter) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	d.readStorage[string(dbutils.PlainGenerateCompositeStorageKey(address.Bytes(),incarnation, key.Bytes()))] = struct{}{}
	return d.r.ReadAccountStorage(address, incarnation, key)
}

func (d *DebugReaderWriter) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	d.readCodes[codeHash] = struct{}{}
	return d.r.ReadAccountCode(address, incarnation, codeHash)
}

func (d *DebugReaderWriter) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	return d.r.ReadAccountCodeSize(address, incarnation, codeHash)
}

func (d *DebugReaderWriter) ReadAccountIncarnation(address common.Address) (uint64, error) {
	d.readIncarnations[address] = struct{}{}
	return d.r.ReadAccountIncarnation(address)
}

func (d *DebugReaderWriter) WriteChangeSets() error {
	return d.w.WriteChangeSets()
}

func (d *DebugReaderWriter) WriteHistory() error {
	return d.w.WriteHistory()
}

func (d *DebugReaderWriter) UpdateAccountData(ctx context.Context, address common.Address, original, account *accounts.Account) error {
	d.updatedAcc[address] = struct{}{}
	return d.w.UpdateAccountData(ctx, address, original, account)
}

func (d *DebugReaderWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	d.updatedCodes[codeHash] = struct{}{}
	return d.w.UpdateAccountCode(address, incarnation, codeHash, code)
}

func (d *DebugReaderWriter) DeleteAccount(ctx context.Context, address common.Address, original *accounts.Account) error {
	d.deletedAcc[address]= struct{}{}
	return d.w.DeleteAccount(ctx, address, original)
}

func (d *DebugReaderWriter) WriteAccountStorage(ctx context.Context, address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	d.updatedStorage[string(dbutils.PlainGenerateCompositeStorageKey(address.Bytes(),incarnation, key.Bytes()))] = struct{}{}
	return d.w.WriteAccountStorage(ctx, address, incarnation, key, original, value)
}

func (d *DebugReaderWriter) CreateContract(address common.Address) error {
	d.createdContracts[address] = struct{}{}
	return d.w.CreateContract(address)
}

func (d *DebugReaderWriter) AllAccounts() map[common.Address]struct{}  {
	accs:=make(map[common.Address]struct{})
	for i:=range d.readAcc {
		accs[i]=struct{}{}
	}
	for i:=range d.updatedAcc {
		accs[i]=struct{}{}
	}
	for i:=range d.readIncarnations {
		accs[i]=struct{}{}
	}
	for i:=range d.deletedAcc {
		accs[i]=struct{}{}
	}
	for i:=range d.createdContracts {
		accs[i]=struct{}{}
	}
	return accs
}
func (d *DebugReaderWriter) AllStorage() map[string]struct{}  {
	st:=make(map[string]struct{})
	for i:=range d.readStorage {
		st[i]=struct{}{}
	}
	for i:=range d.updatedStorage {
		st[i]=struct{}{}
	}
	return st
}
func (d *DebugReaderWriter) AllCodes() map[common.Hash]struct{}  {
	c:=make(map[common.Hash]struct{})
	for i:=range d.readCodes {
		c[i]=struct{}{}
	}
	for i:=range d.updatedCodes {
		c[i]=struct{}{}
	}
	return c
}