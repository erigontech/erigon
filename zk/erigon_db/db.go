package erigon_db

import (
	"fmt"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	ethTypes "github.com/ledgerwatch/erigon/core/types"
)

var sha3UncleHash = common.HexToHash("0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347")

type ErigonDb struct {
	tx kv.RwTx
}

func NewErigonDb(tx kv.RwTx) *ErigonDb {
	return &ErigonDb{
		tx: tx,
	}
}

func (db ErigonDb) WriteHeader(
	blockNo *big.Int,
	blockHash common.Hash,
	stateRoot, txHash, parentHash common.Hash,
	coinbase common.Address,
	ts, gasLimit uint64, chainConfig *chain.Config,
) (*ethTypes.Header, error) {
	parentHeader, err := db.GetHeader(blockNo.Uint64() - 1)
	if err != nil {
		return nil, fmt.Errorf("failed to get parent header: %w", err)
	}

	h := &ethTypes.Header{}

	if parentHeader != nil {
		h = core.MakeEmptyHeader(parentHeader, chainConfig, ts, &gasLimit)
	} else {
		h.Number = blockNo
	}

	h.ParentHash = parentHash
	h.Root = stateRoot
	h.TxHash = txHash
	h.Coinbase = coinbase
	h.UncleHash = sha3UncleHash
	h.Extra = make([]byte, 0)
	h.Time = ts

	if chainConfig.IsShanghai(blockNo.Uint64()) {
		h.WithdrawalsHash = &ethTypes.EmptyRootHash
	}

	if !chainConfig.IsNormalcy(blockNo.Uint64()) {
		h.GasLimit = gasLimit
	}

	if err := rawdb.WriteHeaderWithhash(db.tx, blockHash, h); err != nil {
		return nil, fmt.Errorf("failed to write header: %w", err)

	}
	if err := rawdb.WriteCanonicalHash(db.tx, blockHash, blockNo.Uint64()); err != nil {
		return nil, fmt.Errorf("failed to write canonical hash: %w", err)
	}
	return h, nil
}

func (db ErigonDb) WriteBody(blockNo *big.Int, headerHash common.Hash, txs []ethTypes.Transaction) error {
	b := &ethTypes.Body{
		Transactions: txs,
	}

	// writes txs to EthTx (canonical table)
	return rawdb.WriteBody(db.tx, headerHash, blockNo.Uint64(), b)
}

func (db ErigonDb) GetBodyTransactions(fromBlockNo, toBlockNo uint64) (*[]ethTypes.Transaction, error) {
	return rawdb.GetBodyTransactions(db.tx, fromBlockNo, toBlockNo)
}

func (db ErigonDb) ReadCanonicalHash(blockNo uint64) (common.Hash, error) {
	return rawdb.ReadCanonicalHash(db.tx, blockNo)
}

func (db ErigonDb) GetHeader(blockNo uint64) (*ethTypes.Header, error) {
	hash, err := db.ReadCanonicalHash(blockNo)
	if err != nil {
		return nil, fmt.Errorf("failed to read canonical hash: %w", err)
	}
	return rawdb.ReadHeader(db.tx, hash, blockNo), nil
}
