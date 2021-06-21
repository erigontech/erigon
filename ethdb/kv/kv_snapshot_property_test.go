package kv

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func TestGetAndPut(t *testing.T) {
	t.Skip("remove when it become stable for 200 rounds")
	rapid.Check(t, rapid.Run(&getPutkvMachine{}))
}

type getPutkvMachine struct {
	bucket       string
	snKV         ethdb.RwKV
	modelKV      ethdb.RwKV
	snapshotKeys [][20]byte
	newKeys      [][20]byte
	allKeys      [][20]byte

	snTX    ethdb.RwTx
	modelTX ethdb.RwTx
}

func (m *getPutkvMachine) Init(t *rapid.T) {
	m.bucket = dbutils.PlainStateBucket
	m.snKV = NewMemKV()
	m.modelKV = NewMemKV()
	m.snapshotKeys = rapid.SliceOf(rapid.ArrayOf(20, rapid.Byte())).Filter(func(_v [][20]byte) bool {
		return len(_v) > 0
	}).Draw(t, "generate keys").([][20]byte)
	m.newKeys = rapid.SliceOf(rapid.ArrayOf(20, rapid.Byte())).Filter(func(_v [][20]byte) bool {
		return len(_v) > 0
	}).Draw(t, "generate new keys").([][20]byte)
	notExistingKeys := rapid.SliceOf(rapid.ArrayOf(20, rapid.Byte())).Filter(func(_v [][20]byte) bool {
		return len(_v) > 0
	}).Draw(t, "generate not excisting keys").([][20]byte)
	m.allKeys = append(m.snapshotKeys, notExistingKeys...)

	txSn, err := m.snKV.BeginRw(context.Background())
	require.NoError(t, err)

	txModel, err := m.modelKV.BeginRw(context.Background())
	require.NoError(t, err)
	defer txModel.Rollback()
	for _, key := range m.snapshotKeys {
		innerErr := txSn.Put(m.bucket, key[:], []byte("sn_"+common.Bytes2Hex(key[:])))
		require.NoError(t, innerErr)
		innerErr = txModel.Put(m.bucket, key[:], []byte("sn_"+common.Bytes2Hex(key[:])))
		require.NoError(t, innerErr)
	}

	//save snapshot and wrap new write db
	err = txSn.Commit()
	require.NoError(t, err)
	err = txModel.Commit()
	require.NoError(t, err)
	m.snKV = NewSnapshotKV().SnapshotDB([]string{m.bucket}, m.snKV).DB(NewMemKV()).Open()
}

func (m *getPutkvMachine) Cleanup() {
	if m.snTX != nil {
		m.snTX.Rollback()
	}
	if m.modelTX != nil {
		m.modelTX.Rollback()
	}
	m.snKV.Close()
	m.modelKV.Close()
}

func (m *getPutkvMachine) Check(t *rapid.T) {
}

func (m *getPutkvMachine) Get(t *rapid.T) {
	if m.snTX == nil && m.modelTX == nil {
		return
	}
	key := rapid.SampledFrom(m.allKeys).Draw(t, "get a key").([20]byte)
	var (
		v1, v2     []byte
		err1, err2 error
	)

	v1, err1 = m.snTX.GetOne(m.bucket, key[:])
	v2, err2 = m.modelTX.GetOne(m.bucket, key[:])

	require.Equal(t, err1, err2)
	require.Equal(t, v1, v2)
}

func (m *getPutkvMachine) Put(t *rapid.T) {
	if len(m.newKeys) == 0 {
		return
	}
	if m.snTX == nil && m.modelTX == nil {
		return
	}
	key := rapid.SampledFrom(m.newKeys).Draw(t, "put a key").([20]byte)
	m.allKeys = append(m.allKeys, key)
	for i, v := range m.newKeys {
		if v == key {
			m.newKeys = append(m.newKeys[:i], m.newKeys[i+1:]...)
		}
	}
	err := m.snTX.Put(m.bucket, key[:], []byte("put"+common.Bytes2Hex(key[:])))
	require.NoError(t, err)

	err = m.modelTX.Put(m.bucket, key[:], []byte("put"+common.Bytes2Hex(key[:])))
	require.NoError(t, err)
}

func (m *getPutkvMachine) Delete(t *rapid.T) {
	if m.snTX == nil && m.modelTX == nil {
		return
	}
	key := rapid.SampledFrom(m.allKeys).Draw(t, "delete a key").([20]byte)

	err := m.snTX.Delete(m.bucket, key[:], nil)
	require.NoError(t, err)

	err = m.modelTX.Put(m.bucket, key[:], nil)
	require.NoError(t, err)
}

func (m *getPutkvMachine) Begin(t *rapid.T) {
	if m.modelTX != nil && m.snTX != nil {
		return
	}
	mtx, err := m.modelKV.BeginRw(context.Background())
	require.NoError(t, err)
	sntx, err := m.snKV.BeginRw(context.Background())
	require.NoError(t, err)
	m.modelTX = mtx
	m.snTX = sntx
}

func (m *getPutkvMachine) Rollback(t *rapid.T) {
	if m.modelTX == nil && m.snTX == nil {
		return
	}
	m.snTX.Rollback()
	m.modelTX.Rollback()
	m.snTX = nil
	m.modelTX = nil
}

func (m *getPutkvMachine) Commit(t *rapid.T) {
	if m.modelTX == nil && m.snTX == nil {
		return
	}
	err := m.modelTX.Commit()
	require.NoError(t, err)
	err = m.snTX.Commit()
	require.NoError(t, err)
	m.snTX = nil
	m.modelTX = nil
}

type getKVMachine struct {
	bucket        string
	snKV          ethdb.RwKV
	modelKV       ethdb.RwKV
	overWriteKeys [][20]byte
	snKeys        [][20]byte
	newKeys       [][20]byte
	allKeys       [][20]byte
}

func (m *getKVMachine) Init(t *rapid.T) {
	m.bucket = dbutils.PlainStateBucket
	m.snKV = NewMemKV()
	m.modelKV = NewMemKV()
	m.snKeys = rapid.SliceOf(rapid.ArrayOf(20, rapid.Byte())).Filter(func(_v [][20]byte) bool {
		return len(_v) > 0
	}).Draw(t, "generate keys").([][20]byte)
	m.overWriteKeys = rapid.SliceOf(rapid.SampledFrom(m.snKeys)).Draw(t, "get snKeys").([][20]byte)
	m.newKeys = rapid.SliceOf(rapid.ArrayOf(20, rapid.Byte())).Draw(t, "generate new keys").([][20]byte)
	m.allKeys = append(m.snKeys, m.overWriteKeys...)
	m.allKeys = append(m.allKeys, m.newKeys...)
	notExistingKeys := rapid.SliceOf(rapid.ArrayOf(20, rapid.Byte())).Draw(t, "generate not excisting keys").([][20]byte)
	m.allKeys = append(m.allKeys, notExistingKeys...)

	txSn, err := m.snKV.BeginRw(context.Background())
	require.NoError(t, err)

	txModel, err := m.modelKV.BeginRw(context.Background())
	require.NoError(t, err)
	defer txModel.Rollback()
	for _, key := range m.snKeys {
		innerErr := txSn.Put(m.bucket, key[:], []byte("sn_"+common.Bytes2Hex(key[:])))
		require.NoError(t, innerErr)
		innerErr = txModel.Put(m.bucket, key[:], []byte("sn_"+common.Bytes2Hex(key[:])))
		require.NoError(t, innerErr)
	}

	//save snapshot and wrap new write db
	err = txSn.Commit()
	require.NoError(t, err)
	m.snKV = NewSnapshotKV().SnapshotDB([]string{m.bucket}, m.snKV).DB(NewMemKV()).Open()
	txSn, err = m.snKV.BeginRw(context.Background())
	require.NoError(t, err)
	defer txSn.Rollback()

	for _, key := range m.overWriteKeys {
		innerErr := txSn.Put(m.bucket, key[:], []byte("overwrite_"+common.Bytes2Hex(key[:])))
		require.NoError(t, innerErr)
		innerErr = txModel.Put(m.bucket, key[:], []byte("overwrite_"+common.Bytes2Hex(key[:])))
		require.NoError(t, innerErr)
	}
	for _, key := range m.newKeys {
		innerErr := txSn.Put(m.bucket, key[:], []byte("new_"+common.Bytes2Hex(key[:])))
		require.NoError(t, innerErr)
		innerErr = txModel.Put(m.bucket, key[:], []byte("new_"+common.Bytes2Hex(key[:])))
		require.NoError(t, innerErr)
	}
	err = txSn.Commit()
	require.NoError(t, err)
	err = txModel.Commit()
	require.NoError(t, err)
}

func (m *getKVMachine) Cleanup() {
	m.snKV.Close()
	m.modelKV.Close()
}

func (m *getKVMachine) Check(t *rapid.T) {
}

func (m *getKVMachine) Get(t *rapid.T) {
	key := rapid.SampledFrom(m.allKeys).Draw(t, "get a key").([20]byte)
	var (
		v1, v2     []byte
		err1, err2 error
	)
	err := m.snKV.View(context.Background(), func(tx ethdb.Tx) error {
		v1, err1 = tx.GetOne(m.bucket, key[:])
		return nil
	})
	require.NoError(t, err)
	err = m.modelKV.View(context.Background(), func(tx ethdb.Tx) error {
		v2, err2 = tx.GetOne(m.bucket, key[:])
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, err1, err2)
	require.Equal(t, v1, v2)
}

func TestGet(t *testing.T) {
	t.Skip("remove when it become stable for 200 rounds")
	rapid.Check(t, rapid.Run(&getKVMachine{}))
}

func TestCursorWithTX(t *testing.T) {
	t.Skip("remove when it become stable for 200 rounds")
	rapid.Check(t, rapid.Run(&cursorKVMachine{}))
}

type cursorKVMachine struct {
	bucket  string
	snKV    ethdb.RwKV
	modelKV ethdb.RwKV

	snTX    ethdb.RwTx
	modelTX ethdb.RwTx

	snCursor    ethdb.RwCursor
	modelCursor ethdb.RwCursor

	snapshotKeys [][20]byte
	newKeys      [][20]byte
	allKeys      [][20]byte
}

func (m *cursorKVMachine) Init(t *rapid.T) {
	m.bucket = dbutils.PlainStateBucket
	m.snKV = NewMemKV()
	m.modelKV = NewMemKV()
	m.snapshotKeys = rapid.SliceOf(rapid.ArrayOf(20, rapid.Byte())).Filter(func(_v [][20]byte) bool {
		return len(_v) > 0
	}).Draw(t, "generate keys").([][20]byte)
	m.newKeys = rapid.SliceOf(rapid.ArrayOf(20, rapid.Byte())).Filter(func(_v [][20]byte) bool {
		return len(_v) > 0
	}).Draw(t, "generate new keys").([][20]byte)
	notExistingKeys := rapid.SliceOf(rapid.ArrayOf(20, rapid.Byte())).Filter(func(_v [][20]byte) bool {
		return len(_v) > 0
	}).Draw(t, "generate not excisting keys").([][20]byte)
	m.allKeys = append(m.snapshotKeys, notExistingKeys...)

	defer func() {
		m.snTX = nil
		m.modelTX = nil
	}()
	txSn, err := m.snKV.BeginRw(context.Background())
	require.NoError(t, err)
	defer txSn.Rollback()

	txModel, err := m.modelKV.BeginRw(context.Background())
	require.NoError(t, err)
	defer txModel.Rollback()
	for _, key := range m.snapshotKeys {
		innerErr := txSn.Put(m.bucket, key[:], []byte("sn_"+common.Bytes2Hex(key[:])))
		require.NoError(t, innerErr)
		innerErr = txModel.Put(m.bucket, key[:], []byte("sn_"+common.Bytes2Hex(key[:])))
		require.NoError(t, innerErr)
	}

	//save snapshot and wrap new write db
	err = txSn.Commit()
	require.NoError(t, err)
	err = txModel.Commit()
	require.NoError(t, err)
	m.snKV = NewSnapshotKV().SnapshotDB([]string{m.bucket}, m.snKV).DB(NewMemKV()).Open()
}

func (m *cursorKVMachine) Check(t *rapid.T) {
}

func (m *cursorKVMachine) Cleanup() {
	if m.snTX != nil {
		m.snTX.Rollback()
	}
	if m.modelTX != nil {
		m.modelTX.Rollback()
	}

	m.snKV.Close()
	m.snKV = nil
	m.modelKV.Close()
	m.modelKV = nil
}

func (m *cursorKVMachine) Begin(t *rapid.T) {
	if m.modelTX != nil && m.snTX != nil {
		return
	}

	mtx, err := m.modelKV.BeginRw(context.Background())
	require.NoError(t, err)
	sntx, err := m.snKV.BeginRw(context.Background())
	require.NoError(t, err)
	m.modelTX = mtx
	m.snTX = sntx
}

func (m *cursorKVMachine) Rollback(t *rapid.T) {
	if m.modelTX == nil && m.snTX == nil {
		return
	}
	m.snTX.Rollback()
	m.modelTX.Rollback()
	m.snTX = nil
	m.modelTX = nil
	m.snCursor = nil
	m.modelCursor = nil
}

func (m *cursorKVMachine) Commit(t *rapid.T) {
	if m.modelTX == nil && m.snTX == nil {
		return
	}
	err := m.modelTX.Commit()
	require.NoError(t, err)
	err = m.snTX.Commit()
	require.NoError(t, err)
	m.snTX = nil
	m.modelTX = nil
	m.snCursor = nil
	m.modelCursor = nil
}

func (m *cursorKVMachine) Cursor(t *rapid.T) {
	if m.modelTX == nil && m.snTX == nil {
		return
	}
	if m.modelCursor != nil && m.snCursor != nil {
		return
	}
	var err error
	m.modelCursor, err = m.modelTX.RwCursor(m.bucket)
	require.NoError(t, err)
	m.snCursor, err = m.snTX.RwCursor(m.bucket)
	require.NoError(t, err)
}

func (m *cursorKVMachine) CloseCursor(t *rapid.T) {
	if m.modelTX == nil && m.snTX == nil {
		return
	}
	if m.modelCursor == nil && m.snCursor == nil {
		return
	}
	m.modelCursor.Close()
	m.snCursor.Close()
	m.modelCursor = nil
	m.snCursor = nil
}

func (m *cursorKVMachine) First(t *rapid.T) {
	if m.modelCursor == nil && m.snCursor == nil {
		return
	}
	k1, v1, err1 := m.modelCursor.First()
	k2, v2, err2 := m.snCursor.First()
	require.Equal(t, k1, k2)
	require.Equal(t, v1, v2)
	require.Equal(t, err1, err2)
}

func (m *cursorKVMachine) Last(t *rapid.T) {
	if m.modelCursor == nil && m.snCursor == nil {
		return
	}
	k1, v1, err1 := m.modelCursor.Last()
	k2, v2, err2 := m.snCursor.Last()
	require.Equal(t, k1, k2)
	require.Equal(t, v1, v2)
	require.Equal(t, err1, err2)
}

func (m *cursorKVMachine) Seek(t *rapid.T) {
	if m.modelCursor == nil && m.snCursor == nil {
		return
	}
	key := rapid.SampledFrom(m.allKeys).Draw(t, "get random key").([20]byte)
	k1, v1, err1 := m.modelCursor.Seek(key[:])
	k2, v2, err2 := m.snCursor.Seek(key[:])
	require.Equal(t, k1, k2)
	require.Equal(t, v1, v2)
	require.Equal(t, err1, err2)
}

func (m *cursorKVMachine) SeekExact(t *rapid.T) {
	if m.modelCursor == nil && m.snCursor == nil {
		return
	}

	key := rapid.SampledFrom(m.allKeys).Draw(t, "get random key").([20]byte)
	k1, v1, err1 := m.modelCursor.SeekExact(key[:])
	k2, v2, err2 := m.snCursor.SeekExact(key[:])
	require.Equal(t, k1, k2)
	require.Equal(t, v1, v2)
	require.Equal(t, err1, err2)
}

func (m *cursorKVMachine) Next(t *rapid.T) {
	if m.modelCursor == nil && m.snCursor == nil {
		return
	}
	k1, v1, err1 := m.modelCursor.Next()
	k2, v2, err2 := m.snCursor.Next()

	require.Equal(t, k1, k2)
	require.Equal(t, v1, v2)
	require.Equal(t, err1, err2)
}
