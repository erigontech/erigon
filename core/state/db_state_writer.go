package state

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/turbo/trie"
)

// This type is now used in GenerateChain to generate blockchains for the tests (core/chain_makers.go)
// Main mode of operation uses PlainDbStateWriter

var _ WriterWithChangeSets = (*DbStateWriter)(nil)

func NewDbStateWriter(db putDel, blockNr uint64) *DbStateWriter {
	return &DbStateWriter{
		db:      db,
		blockNr: blockNr,
		csw:     NewChangeSetWriter(),
	}
}

type DbStateWriter struct {
	db      putDel
	blockNr uint64
	csw     *ChangeSetWriter
}

func (dsw *DbStateWriter) ChangeSetWriter() *ChangeSetWriter {
	return dsw.csw
}

func originalAccountData(original *accounts.Account, omitHashes bool) []byte {
	var originalData []byte
	if !original.Initialised {
		originalData = []byte{}
	} else if omitHashes {
		testAcc := original.SelfCopy()
		copy(testAcc.CodeHash[:], emptyCodeHash)
		testAcc.Root = trie.EmptyRoot
		originalDataLen := testAcc.EncodingLengthForStorage()
		originalData = make([]byte, originalDataLen)
		testAcc.EncodeForStorage(originalData)
	} else {
		originalDataLen := original.EncodingLengthForStorage()
		originalData = make([]byte, originalDataLen)
		original.EncodeForStorage(originalData)
	}
	return originalData
}

func (dsw *DbStateWriter) UpdateAccountData(address libcommon.Address, original, account *accounts.Account) error {
	if err := dsw.csw.UpdateAccountData(address, original, account); err != nil {
		return err
	}
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return err
	}
	value := make([]byte, account.EncodingLengthForStorage())
	account.EncodeForStorage(value)
	if err := dsw.db.Put(kv.HashedAccounts, addrHash[:], value); err != nil {
		return err
	}
	return nil
}

func (dsw *DbStateWriter) DeleteAccount(address libcommon.Address, original *accounts.Account) error {
	if err := dsw.csw.DeleteAccount(address, original); err != nil {
		return err
	}
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return err
	}
	if err := dsw.db.Delete(kv.HashedAccounts, addrHash[:]); err != nil {
		return err
	}
	if original.Incarnation > 0 {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], original.Incarnation)
		if err := dsw.db.Put(kv.IncarnationMap, address[:], b[:]); err != nil {
			return err
		}
	}
	return nil
}

func (dsw *DbStateWriter) UpdateAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash, code []byte) error {
	if err := dsw.csw.UpdateAccountCode(address, incarnation, codeHash, code); err != nil {
		return err
	}
	//save contract code mapping
	if err := dsw.db.Put(kv.Code, codeHash[:], code); err != nil {
		return err
	}
	addrHash, err := common.HashData(address.Bytes())
	if err != nil {
		return err
	}
	//save contract to codeHash mapping
	if err := dsw.db.Put(kv.ContractCode, dbutils.GenerateStoragePrefix(addrHash[:], incarnation), codeHash[:]); err != nil {
		return err
	}
	return nil
}

func (dsw *DbStateWriter) WriteAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash, original, value *uint256.Int) error {
	// We delegate here first to let the changeSetWrite make its own decision on whether to proceed in case *original == *value
	if err := dsw.csw.WriteAccountStorage(address, incarnation, key, original, value); err != nil {
		return err
	}
	if *original == *value {
		return nil
	}
	seckey, err := common.HashData(key[:])
	if err != nil {
		return err
	}
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return err
	}
	compositeKey := dbutils.GenerateCompositeStorageKey(addrHash, incarnation, seckey)

	v := value.Bytes()
	if len(v) == 0 {
		return dsw.db.Delete(kv.HashedStorage, compositeKey)
	}
	return dsw.db.Put(kv.HashedStorage, compositeKey, v)
}

func (dsw *DbStateWriter) CreateContract(address libcommon.Address) error {
	if err := dsw.csw.CreateContract(address); err != nil {
		return err
	}
	return nil
}

// WriteChangeSets causes accumulated change sets to be written into
// the database (or batch) associated with the `dsw`
func (dsw *DbStateWriter) WriteChangeSets() error {
	return nil
}

func (dsw *DbStateWriter) WriteHistory() error {
	accountChanges, err := dsw.csw.GetAccountChanges()
	if err != nil {
		return err
	}
	err = writeIndex(dsw.blockNr, accountChanges, kv.E2AccountsHistory, dsw.db.(ethdb.HasTx).Tx().(kv.RwTx))
	if err != nil {
		return err
	}

	storageChanges, err := dsw.csw.GetStorageChanges()
	if err != nil {
		return err
	}
	err = writeIndex(dsw.blockNr, storageChanges, kv.E2StorageHistory, dsw.db.(ethdb.HasTx).Tx().(kv.RwTx))
	if err != nil {
		return err
	}

	return nil
}

func writeIndex(blocknum uint64, changes *historyv2.ChangeSet, bucket string, changeDb kv.RwTx) error {
	buf := bytes.NewBuffer(nil)
	for _, change := range changes.Changes {
		k := dbutils.CompositeKeyWithoutIncarnation(change.Key)

		index, err := bitmapdb.Get64(changeDb, bucket, k, 0, math.MaxUint32)
		if err != nil {
			return fmt.Errorf("find chunk failed: %w", err)
		}
		index.Add(blocknum)
		if err = bitmapdb.WalkChunkWithKeys64(k, index, bitmapdb.ChunkLimit, func(chunkKey []byte, chunk *roaring64.Bitmap) error {
			buf.Reset()
			if _, err = chunk.WriteTo(buf); err != nil {
				return err
			}
			return changeDb.Put(bucket, chunkKey, common.CopyBytes(buf.Bytes()))
		}); err != nil {
			return err
		}
	}

	return nil
}
