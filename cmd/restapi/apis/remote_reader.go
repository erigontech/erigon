package apis

import (
	"bytes"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote/remotechain"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"golang.org/x/net/context"
)

// ChangeSetReader is a mock StateWriter that accumulates changes in-memory into ChangeSets.
type RemoteReader struct {
	accountReads map[common.Address]bool
	storageReads map[common.Hash]bool
	blockNr      uint64
	db           ethdb.KV
}

type RemoteContext struct {
	db ethdb.KV
}

type powEngine struct {
}

func (c *powEngine) VerifyHeader(chain consensus.ChainReader, header *types.Header, seal bool) error {

	panic("must not be called")
}
func (c *powEngine) VerifyHeaders(chain consensus.ChainReader, headers []*types.Header, seals []bool) (chan<- struct{}, <-chan error) {
	panic("must not be called")
}
func (c *powEngine) VerifyUncles(chain consensus.ChainReader, block *types.Block) error {
	panic("must not be called")
}
func (c *powEngine) VerifySeal(chain consensus.ChainReader, header *types.Header) error {
	panic("must not be called")
}
func (c *powEngine) Prepare(chain consensus.ChainReader, header *types.Header) error {
	panic("must not be called")
}
func (c *powEngine) Finalize(chainConfig *params.ChainConfig, header *types.Header, state *state.IntraBlockState, txs []*types.Transaction, uncles []*types.Header) {
	panic("must not be called")
}
func (c *powEngine) FinalizeAndAssemble(chainConfig *params.ChainConfig, header *types.Header, state *state.IntraBlockState, txs []*types.Transaction,
	uncles []*types.Header, receipts []*types.Receipt) (*types.Block, error) {
	panic("must not be called")
}
func (c *powEngine) Seal(_ consensus.Cancel, chain consensus.ChainReader, block *types.Block, results chan<- consensus.ResultWithContext, stop <-chan struct{}) error {
	panic("must not be called")
}
func (c *powEngine) SealHash(header *types.Header) common.Hash {
	panic("must not be called")
}
func (c *powEngine) CalcDifficulty(chain consensus.ChainReader, time uint64, parent *types.Header) *big.Int {
	panic("must not be called")
}
func (c *powEngine) APIs(chain consensus.ChainReader) []rpc.API {
	panic("must not be called")
}

func (c *powEngine) Close() error {
	panic("must not be called")
}

func (c *powEngine) Author(header *types.Header) (common.Address, error) {
	return header.Coinbase, nil
}

func NewRemoteReader(db ethdb.KV, blockNr uint64) *RemoteReader {
	return &RemoteReader{
		accountReads: make(map[common.Address]bool),
		storageReads: make(map[common.Hash]bool),
		db:           db,
		blockNr:      blockNr,
	}
}

func (r *RemoteReader) GetAccountReads() [][]byte {
	var output [][]byte
	for key, _ := range r.accountReads {
		output = append(output, key.Bytes())
	}
	return output
}

func (r *RemoteReader) GetStorageReads() [][]byte {
	var output [][]byte
	for key, _ := range r.storageReads {
		output = append(output, key.Bytes())
	}
	return output
}

func (r *RemoteReader) ReadAccountData(address common.Address) (*accounts.Account, error) {
	r.accountReads[address] = true
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return nil, err
	}
	r.accountReads[address] = true
	dat, err := GetAsOf(r.db, dbutils.AccountsBucket, dbutils.AccountsHistoryBucket, addrHash[:], r.blockNr)
	if err != nil || dat == nil || len(dat) == 0 {
		return nil, nil
	}
	acc := accounts.NewAccount()
	err = acc.DecodeForStorage(dat)
	if err != nil {
		return nil, err
	}
	return &acc, nil
}

func (r *RemoteReader) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	keyHash, err := common.HashData(key[:])
	if err != nil {
		return nil, err
	}

	addrHash, err := common.HashData(address[:])
	if err != nil {
		return nil, err
	}

	compositeKey := dbutils.GenerateCompositeStorageKey(addrHash, incarnation, keyHash)
	var val []byte
	err = r.db.View(context.Background(), func(tx ethdb.Tx) error {
		b := tx.Bucket(dbutils.StorageBucket)
		v, err := b.Get(compositeKey)
		val = v
		return err
	})
	if err != nil {
		return nil, err
	}
	r.storageReads[*key] = true
	return val, nil
}

func (r *RemoteReader) ReadAccountCode(address common.Address, codeHash common.Hash) ([]byte, error) {
	if bytes.Equal(codeHash[:], crypto.Keccak256(nil)) {
		return nil, nil
	}
	var val []byte
	err := r.db.View(context.Background(), func(tx ethdb.Tx) error {
		b := tx.Bucket(dbutils.StorageBucket)
		v, err := b.Get(codeHash[:])
		val = v
		return err
	})
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (r *RemoteReader) ReadAccountCodeSize(address common.Address, codeHash common.Hash) (int, error) {
	if bytes.Equal(codeHash[:], crypto.Keccak256(nil)) {
		return 0, nil
	}
	var val []byte
	err := r.db.View(context.Background(), func(tx ethdb.Tx) error {
		b := tx.Bucket(dbutils.StorageBucket)
		v, err := b.Get(codeHash[:])
		val = v
		return err
	})
	if err != nil {
		return 0, err
	}
	return len(val), nil
}

func NewRemoteContext(db ethdb.KV) *RemoteContext {
	return &RemoteContext{
		db: db,
	}
}

func (e *RemoteContext) Engine() consensus.Engine {
	return &powEngine{}
}

func (e *RemoteContext) GetHeader(hash common.Hash, number uint64) *types.Header {
	var header *types.Header
	e.db.View(context.Background(), func(tx ethdb.Tx) error {
		h, _ := remotechain.ReadHeader(tx, hash, number)
		header = h
		return nil
	})
	return header
}

// GetAsOf returns the value valid as of a given timestamp.
func GetAsOf(db ethdb.KV, bucket, hBucket, key []byte, timestamp uint64) ([]byte, error) {
	composite, _ := dbutils.CompositeKeySuffix(key, timestamp)
	var dat []byte
	err := db.View(context.Background(), func(tx ethdb.Tx) error {
		{
			hB := tx.Bucket(hBucket)
			hC := hB.Cursor()
			hK, hV, err := hC.Seek(composite)
			if err != nil {
				return err
			}

			if hK != nil && bytes.HasPrefix(hK, key) {
				dat = make([]byte, len(hV))
				copy(dat, hV)
				return nil
			}
		}
		{
			b := tx.Bucket(bucket)
			c := b.Cursor()
			k, v, err := c.Seek(key)
			if err != nil {
				return err
			}

			if k != nil && bytes.Equal(k, key) {
				dat = make([]byte, len(v))
				copy(dat, v)
				return nil
			}
		}

		return ethdb.ErrKeyNotFound
	})
	return dat, err
}
