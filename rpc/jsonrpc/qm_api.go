package jsonrpc

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/commitment/qmtree"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// QMAPI is the interface for the qm_ RPC namespace.
type QMAPI interface {
	GetProof(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (*QMProofResult, error)
	GetWitness(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*QMWitnessResult, error)
	GetTxWitness(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, txIndex hexutil.Uint) (*QMProofResult, error)
	VerifyProof(ctx context.Context, proofBytes hexutil.Bytes) (bool, error)
	VerifyWitness(ctx context.Context, witnessBytes hexutil.Bytes) (bool, error)
}

// QMAPIImpl implements the qm_ RPC namespace for qmtree proofs and witnesses.
type QMAPIImpl struct {
	db          kv.TemporalRoDB
	blockReader services.FullBlockReader
	txNumReader rawdbv3.TxNumsReader
	tracker     *qmtree.Tracker
	logger      log.Logger
}

func NewQMAPI(db kv.TemporalRoDB, blockReader services.FullBlockReader, txNumReader rawdbv3.TxNumsReader, tracker *qmtree.Tracker, logger log.Logger) *QMAPIImpl {
	return &QMAPIImpl{
		db:          db,
		blockReader: blockReader,
		txNumReader: txNumReader,
		tracker:     tracker,
		logger:      logger,
	}
}

// --- Response types ---

type QMLeafData struct {
	PreStateHash     common.Hash `json:"preStateHash"`
	StateChangeHash  common.Hash `json:"stateChangeHash"`
	TransitionHash   common.Hash `json:"transitionHash"`
	PreviousLeafHash common.Hash `json:"previousLeafHash"`
}

type QMProofResult struct {
	Address     common.Address `json:"address"`
	TxNum       hexutil.Uint64 `json:"txNum"`
	BlockNumber hexutil.Uint64 `json:"blockNumber"`
	LeafData    QMLeafData     `json:"leafData"`
	LeafHash    common.Hash    `json:"leafHash"`
	MerkleProof hexutil.Bytes  `json:"merkleProof"`
	QMTreeRoot  common.Hash    `json:"qmtreeRoot"`
	Verified    bool           `json:"verified"`
}

type QMWitnessLeaf struct {
	SerialNum hexutil.Uint64 `json:"serialNum"`
	LeafData  QMLeafData     `json:"leafData"`
	LeafHash  common.Hash    `json:"leafHash"`
}

type QMWitnessResult struct {
	BlockNumber hexutil.Uint64  `json:"blockNumber"`
	QMTreeRoot  common.Hash     `json:"qmtreeRoot"`
	FirstProof  hexutil.Bytes   `json:"firstProof"`
	LastProof   hexutil.Bytes   `json:"lastProof"`
	Leaves      []QMWitnessLeaf `json:"leaves"`
	Verified    bool            `json:"verified"`
}

// GetProof returns a qmtree inclusion proof for the given address at the
// specified block.
func (api *QMAPIImpl) GetProof(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (*QMProofResult, error) {
	if api.tracker == nil {
		return nil, fmt.Errorf("qmtree tracker not available")
	}

	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, _, _, err := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, tx, api.blockReader, nil)
	if err != nil {
		return nil, err
	}

	lastTxNum, err := api.txNumReader.Min(ctx, tx, uint64(blockNum)+1)
	if err != nil {
		return nil, fmt.Errorf("resolve block txnum: %w", err)
	}

	// Find latest txnum <= lastTxNum that touched this address via inverted index.
	it, err := tx.IndexRange(kv.AccountsHistoryIdx, address.Bytes(), -1, int(lastTxNum), false, -1)
	if err != nil {
		return nil, fmt.Errorf("index range: %w", err)
	}

	var targetTxNum uint64
	var found bool
	for it.HasNext() {
		txn, err := it.Next()
		if err != nil {
			return nil, err
		}
		if txn <= lastTxNum {
			targetTxNum = txn
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("no state change found for address %s up to block %d", address.Hex(), blockNum)
	}

	witness, err := api.tracker.GetWitness(targetTxNum)
	if err != nil {
		return nil, fmt.Errorf("get witness for txnum=%d: %w", targetTxNum, err)
	}

	hasher := &qmtree.Keccak256Hasher{}
	verified := witness.Verify(hasher) == nil

	return &QMProofResult{
		Address:     address,
		TxNum:       hexutil.Uint64(targetTxNum),
		BlockNumber: hexutil.Uint64(blockNum),
		LeafData: QMLeafData{
			PreStateHash:     witness.PreStateHash,
			StateChangeHash:  witness.StateChangeHash,
			TransitionHash:   witness.TransitionHash,
			PreviousLeafHash: witness.PreviousLeafHash,
		},
		LeafHash:    witness.LeafHash(),
		MerkleProof: witness.Proof.ToBytes(),
		QMTreeRoot:  witness.Proof.Root,
		Verified:    verified,
	}, nil
}

// GetWitness returns a qmtree range witness for all transactions in a block.
func (api *QMAPIImpl) GetWitness(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*QMWitnessResult, error) {
	if api.tracker == nil {
		return nil, fmt.Errorf("qmtree tracker not available")
	}

	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, _, _, err := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, tx, api.blockReader, nil)
	if err != nil {
		return nil, err
	}

	fromTxNum, err := api.txNumReader.Min(ctx, tx, uint64(blockNum))
	if err != nil {
		return nil, fmt.Errorf("resolve block start txnum: %w", err)
	}
	toTxNum, err := api.txNumReader.Min(ctx, tx, uint64(blockNum)+1)
	if err != nil {
		return nil, fmt.Errorf("resolve block end txnum: %w", err)
	}

	if toTxNum <= fromTxNum {
		return nil, fmt.Errorf("empty block %d (from=%d, to=%d)", blockNum, fromTxNum, toTxNum)
	}

	rw, err := api.tracker.GetRangeWitness(fromTxNum, toTxNum-1)
	if err != nil {
		return nil, fmt.Errorf("get range witness: %w", err)
	}

	hasher := &qmtree.Keccak256Hasher{}
	verified := rw.Verify(hasher) == nil

	leaves := make([]QMWitnessLeaf, len(rw.Leaves))
	for i, ld := range rw.Leaves {
		leaves[i] = QMWitnessLeaf{
			SerialNum: hexutil.Uint64(ld.SerialNum),
			LeafData: QMLeafData{
				PreStateHash:     ld.PreStateHash,
				StateChangeHash:  ld.StateChangeHash,
				TransitionHash:   ld.TransitionHash,
				PreviousLeafHash: ld.PreviousLeafHash,
			},
			LeafHash: ld.LeafHash(),
		}
	}

	return &QMWitnessResult{
		BlockNumber: hexutil.Uint64(blockNum),
		QMTreeRoot:  rw.Root(),
		FirstProof:  rw.FirstProof.ToBytes(),
		LastProof:   rw.LastProof.ToBytes(),
		Leaves:      leaves,
		Verified:    verified,
	}, nil
}

// GetTxWitness returns a qmtree witness for a specific transaction by index.
func (api *QMAPIImpl) GetTxWitness(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, txIndex hexutil.Uint) (*QMProofResult, error) {
	if api.tracker == nil {
		return nil, fmt.Errorf("qmtree tracker not available")
	}

	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, _, _, err := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, tx, api.blockReader, nil)
	if err != nil {
		return nil, err
	}

	fromTxNum, err := api.txNumReader.Min(ctx, tx, uint64(blockNum))
	if err != nil {
		return nil, err
	}

	targetTxNum := fromTxNum + uint64(txIndex)

	witness, err := api.tracker.GetWitness(targetTxNum)
	if err != nil {
		return nil, fmt.Errorf("get witness for txnum=%d: %w", targetTxNum, err)
	}

	hasher := &qmtree.Keccak256Hasher{}
	verified := witness.Verify(hasher) == nil

	return &QMProofResult{
		TxNum:       hexutil.Uint64(targetTxNum),
		BlockNumber: hexutil.Uint64(blockNum),
		LeafData: QMLeafData{
			PreStateHash:     witness.PreStateHash,
			StateChangeHash:  witness.StateChangeHash,
			TransitionHash:   witness.TransitionHash,
			PreviousLeafHash: witness.PreviousLeafHash,
		},
		LeafHash:    witness.LeafHash(),
		MerkleProof: witness.Proof.ToBytes(),
		QMTreeRoot:  witness.Proof.Root,
		Verified:    verified,
	}, nil
}

// VerifyProof verifies a serialized qmtree proof offline.
func (api *QMAPIImpl) VerifyProof(_ context.Context, proofBytes hexutil.Bytes) (bool, error) {
	proof, err := qmtree.BytesToProofPath(proofBytes)
	if err != nil {
		return false, fmt.Errorf("deserialize proof: %w", err)
	}
	hasher := &qmtree.Keccak256Hasher{}
	return proof.Check(hasher, true) == nil, nil
}

// VerifyWitness verifies a serialized qmtree witness offline.
func (api *QMAPIImpl) VerifyWitness(_ context.Context, witnessBytes hexutil.Bytes) (bool, error) {
	w, err := qmtree.BytesToWitness(witnessBytes)
	if err != nil {
		return false, fmt.Errorf("deserialize witness: %w", err)
	}
	hasher := &qmtree.Keccak256Hasher{}
	return w.Verify(hasher) == nil, nil
}
