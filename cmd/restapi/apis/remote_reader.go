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
	accountReads map[common.Address]struct{}
	storageReads map[common.Address]map[common.Hash]struct{}
	codeReads    map[common.Address]struct{}
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
		accountReads: make(map[common.Address]struct{}),
		storageReads: make(map[common.Address]map[common.Hash]struct{}),
		codeReads:    make(map[common.Address]struct{}),
		db:           db,
		blockNr:      blockNr,
	}
}

func (r *RemoteReader) GetAccountReads() [][]byte {
	output := make([][]byte, 0)
	for address := range r.accountReads {
		output = append(output, address.Bytes())
	}
	return output
}

func (r *RemoteReader) GetStorageReads() [][]byte {
	output := make([][]byte, 0)
	for address, m := range r.storageReads {
		for key := range m {
			output = append(output, append(address.Bytes(), key.Bytes()...))
		}
	}
	return output
}

func (r *RemoteReader) GetCodeReads() [][]byte {
	output := make([][]byte, 0)
	for key := range r.codeReads {
		output = append(output, key.Bytes())
	}
	return output
}

func (r *RemoteReader) ReadAccountData(address common.Address) (*accounts.Account, error) {
	r.accountReads[address] = struct{}{}
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return nil, err
	}
	enc, err := state.GetAsOf(r.db, false /* plain */, false /* storage */, addrHash[:], r.blockNr+1)
	if err != nil || enc == nil || len(enc) == 0 {
		return nil, nil
	}
	var acc accounts.Account
	if err := acc.DecodeForStorage(enc); err != nil {
		return nil, err
	}
	return &acc, nil
}

func (r *RemoteReader) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	m, ok := r.storageReads[address]
	if !ok {
		m = make(map[common.Hash]struct{})
		r.storageReads[address] = m
	}
	m[*key] = struct{}{}
	keyHash, err := common.HashData(key[:])
	if err != nil {
		return nil, err
	}

	addrHash, err := common.HashData(address[:])
	if err != nil {
		return nil, err
	}

	compositeKey := dbutils.GenerateCompositeStorageKey(addrHash, incarnation, keyHash)
	enc, err := state.GetAsOf(r.db, false /* plain */, true /* storage */, compositeKey, r.blockNr+1)
	if err != nil || enc == nil {
		return nil, nil
	}
	return enc, nil
}

func (r *RemoteReader) ReadAccountCode(address common.Address, codeHash common.Hash) ([]byte, error) {
	r.codeReads[address] = struct{}{}
	if bytes.Equal(codeHash[:], crypto.Keccak256(nil)) {
		return nil, nil
	}
	var val []byte
	err := r.db.View(context.Background(), func(tx ethdb.Tx) error {
		b := tx.Bucket(dbutils.CodeBucket)
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
	code, err := r.ReadAccountCode(address, codeHash)
	if err != nil {
		return 0, err
	}
	return len(code), nil
}

func (r *RemoteReader) ReadAccountIncarnation(address common.Address) (uint64, error) {
	return 0, nil
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
	_ = e.db.View(context.Background(), func(tx ethdb.Tx) error {
		h, _ := remotechain.ReadHeader(tx, hash, number)
		header = h
		return nil
	})
	return header
}
