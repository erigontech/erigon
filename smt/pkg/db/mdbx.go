package db

import (
	"math/big"

	"fmt"
	"strings"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/olddb"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"github.com/ledgerwatch/log/v3"
)

type SmtDbTx interface {
	GetOne(bucket string, key []byte) ([]byte, error)
	Put(bucket string, key []byte, value []byte) error
	Has(bucket string, key []byte) (bool, error)
	Delete(bucket string, key []byte) error
	ForEach(bucket string, start []byte, fn func(k, v []byte) error) error
	ForPrefix(bucket string, prefix []byte, fn func(k, v []byte) error) error
	Commit() error
	Rollback()
}

const TableSmt = "HermezSmt"
const TableLastRoot = "HermezSmtLastRoot"
const TableAccountValues = "HermezSmtAccountValues"
const TableMetadata = "HermezSmtMetadata"
const TableHashKey = "HermezSmtHashKey"

type EriDb struct {
	kvTx kv.RwTx
	tx   SmtDbTx
}

func CreateEriDbBuckets(tx kv.RwTx) error {
	err := tx.CreateBucket(TableSmt)
	if err != nil {
		return err
	}

	err = tx.CreateBucket(TableLastRoot)
	if err != nil {
		return err
	}

	err = tx.CreateBucket(TableAccountValues)
	if err != nil {
		return err
	}

	err = tx.CreateBucket(TableMetadata)
	if err != nil {
		return err
	}

	err = tx.CreateBucket(TableHashKey)
	if err != nil {
		return err
	}

	return nil
}

func NewEriDb(tx kv.RwTx) *EriDb {
	return &EriDb{
		kvTx: tx,
		tx:   tx,
	}
}

func (m *EriDb) OpenBatch(quitCh <-chan struct{}) {
	var batch ethdb.DbWithPendingMutations
	batch = olddb.NewHashBatch(m.kvTx, quitCh, "./tempdb")
	defer func() {
		batch.Rollback()
	}()
	m.tx = batch
}

func (m *EriDb) CommitBatch() error {
	if _, ok := m.tx.(ethdb.DbWithPendingMutations); !ok {
		return nil // don't roll back a kvRw tx
	}
	err := m.tx.Commit()
	if err != nil {
		m.tx.Rollback()
		return err
	}
	m.tx = m.kvTx
	return nil
}

func (m *EriDb) RollbackBatch() {
	if _, ok := m.tx.(ethdb.DbWithPendingMutations); !ok {
		return // don't roll back a kvRw tx
	}
	m.tx.Rollback()
	m.tx = m.kvTx
}

func (m *EriDb) GetLastRoot() (*big.Int, error) {
	data, err := m.tx.GetOne(TableLastRoot, []byte("lastRoot"))
	if err != nil {
		return big.NewInt(0), err
	}

	if data == nil {
		return big.NewInt(0), nil
	}

	return utils.ConvertHexToBigInt(string(data)), nil
}

func (m *EriDb) SetLastRoot(r *big.Int) error {
	v := utils.ConvertBigIntToHex(r)
	return m.tx.Put(TableLastRoot, []byte("lastRoot"), []byte(v))
}

func (m *EriDb) Get(key utils.NodeKey) (utils.NodeValue12, error) {
	keyConc := utils.ArrayToScalar(key[:])
	k := utils.ConvertBigIntToHex(keyConc)

	data, err := m.tx.GetOne(TableSmt, []byte(k))
	if err != nil {
		return utils.NodeValue12{}, err
	}

	if data == nil {
		return utils.NodeValue12{}, nil
	}

	vConc := utils.ConvertHexToBigInt(string(data))
	val := utils.ScalarToNodeValue(vConc)

	return val, nil
}

func (m *EriDb) Insert(key utils.NodeKey, value utils.NodeValue12) error {
	keyConc := utils.ArrayToScalar(key[:])
	k := utils.ConvertBigIntToHex(keyConc)

	vals := make([]*big.Int, 12)
	for i, v := range value {
		vals[i] = v
	}

	vConc := utils.ArrayToScalarBig(vals[:])
	v := utils.ConvertBigIntToHex(vConc)

	return m.tx.Put(TableSmt, []byte(k), []byte(v))
}

func (m *EriDb) GetAccountValue(key utils.NodeKey) (utils.NodeValue8, error) {
	keyConc := utils.ArrayToScalar(key[:])
	k := utils.ConvertBigIntToHex(keyConc)

	data, err := m.tx.GetOne(TableAccountValues, []byte(k))
	if err != nil {
		return utils.NodeValue8{}, err
	}

	if data == nil {
		return utils.NodeValue8{}, nil
	}

	vConc := utils.ConvertHexToBigInt(string(data))
	val := utils.ScalarToNodeValue8(vConc)

	return val, nil
}

func (m *EriDb) InsertAccountValue(key utils.NodeKey, value utils.NodeValue8) error {
	keyConc := utils.ArrayToScalar(key[:])
	k := utils.ConvertBigIntToHex(keyConc)

	vals := make([]*big.Int, 8)
	for i, v := range value {
		vals[i] = v
	}

	vConc := utils.ArrayToScalarBig(vals[:])
	v := utils.ConvertBigIntToHex(vConc)

	return m.tx.Put(TableAccountValues, []byte(k), []byte(v))
}

func (m *EriDb) InsertKeySource(key utils.NodeKey, value []byte) error {
	keyConc := utils.ArrayToScalar(key[:])

	return m.tx.Put(TableMetadata, keyConc.Bytes(), value)
}

func (m *EriDb) GetKeySource(key utils.NodeKey) ([]byte, error) {
	keyConc := utils.ArrayToScalar(key[:])

	data, err := m.tx.GetOne(TableMetadata, keyConc.Bytes())
	if err != nil {
		return nil, err
	}

	if data == nil {
		return nil, fmt.Errorf("key %x not found", keyConc.Bytes())
	}

	return data, nil
}

func (m *EriDb) InsertHashKey(key utils.NodeKey, value utils.NodeKey) error {
	keyConc := utils.ArrayToScalar(key[:])

	valConc := utils.ArrayToScalar(value[:])

	return m.tx.Put(TableHashKey, keyConc.Bytes(), valConc.Bytes())
}

func (m *EriDb) GetHashKey(key utils.NodeKey) (utils.NodeKey, error) {
	keyConc := utils.ArrayToScalar(key[:])

	data, err := m.tx.GetOne(TableHashKey, keyConc.Bytes())
	if err != nil {
		return utils.NodeKey{}, err
	}

	if data == nil {
		return utils.NodeKey{}, fmt.Errorf("hash key %x not found", keyConc.Bytes())
	}

	nv := big.NewInt(0).SetBytes(data)

	na := utils.ScalarToArray(nv)

	return utils.NodeKey{na[0], na[1], na[2], na[3]}, nil
}

func (m *EriDb) Delete(key string) error {
	return m.tx.Delete(TableSmt, []byte(key))
}

func (m *EriDb) PrintDb() {
	err := m.tx.ForEach(TableSmt, []byte{}, func(k, v []byte) error {
		println(string(k), string(v))
		return nil
	})
	if err != nil {
		println(err)
	}
}

func (m *EriDb) GetDb() map[string][]string {
	transformedDb := make(map[string][]string)

	err := m.tx.ForEach(TableSmt, []byte{}, func(k, v []byte) error {
		hk := string(k)

		vConc := utils.ConvertHexToBigInt(string(v))
		val := utils.ScalarToNodeValue(vConc)

		truncationLength := 12

		allFirst8PaddedWithZeros := true
		for i := 0; i < 8; i++ {
			if !strings.HasPrefix(fmt.Sprintf("%016s", val[i].Text(16)), "00000000") {
				allFirst8PaddedWithZeros = false
				break
			}
		}

		if allFirst8PaddedWithZeros {
			truncationLength = 8
		}

		outputArr := make([]string, truncationLength)
		for i := 0; i < truncationLength; i++ {
			if i < len(val) {
				outputArr[i] = fmt.Sprintf("%016s", val[i].Text(16))
			} else {
				outputArr[i] = "0000000000000000"
			}
		}

		transformedDb[hk] = outputArr
		return nil
	})

	if err != nil {
		log.Error(err.Error())
	}

	return transformedDb
}
