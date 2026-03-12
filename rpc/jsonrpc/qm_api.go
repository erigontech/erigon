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
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// QMAPI is the interface for the qm_ RPC namespace.
type QMAPI interface {
	// Tx-level proofs: prove that a transaction was committed in the qmtree.
	GetProof(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (*QMProofResult, error)
	GetWitness(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*QMWitnessResult, error)
	GetTxWitness(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, txIndex hexutil.Uint) (*QMProofResult, error)

	// State proofs: prove account/storage state values AND their qmtree commitment.
	GetAccountStateProof(ctx context.Context, address common.Address, storageKeys []common.Hash, blockNrOrHash rpc.BlockNumberOrHash) (*QMStateProofResult, error)
	GetTxStateProof(ctx context.Context, address common.Address, storageKeys []common.Hash, blockNrOrHash rpc.BlockNumberOrHash, txIndex hexutil.Uint) (*QMStateProofResult, error)
	GetLatestStateProof(ctx context.Context, address common.Address, storageKeys []common.Hash) (*QMStateProofResult, error)

	// Offline verification.
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

// QMStorageSlotProof holds a single storage slot value with its qmtree witness.
type QMStorageSlotProof struct {
	Slot  common.Hash    `json:"slot"`
	Value hexutil.Bytes  `json:"value"`
}

// QMStateProofResult combines account/storage state values with the qmtree
// witness for the transaction that last changed them. A verifier can:
//  1. Check that the account state matches the stateChangeHash in the qmtree leaf.
//  2. Verify the qmtree Merkle proof against the published root.
type QMStateProofResult struct {
	Address       common.Address       `json:"address"`
	BlockNumber   hexutil.Uint64       `json:"blockNumber"`
	TxNum         hexutil.Uint64       `json:"txNum"`
	// Account state after the transaction at TxNum executed.
	Balance       *hexutil.Big         `json:"balance"`
	Nonce         hexutil.Uint64       `json:"nonce"`
	CodeHash      common.Hash          `json:"codeHash"`
	// Requested storage slots at the same point in history.
	StorageProofs []QMStorageSlotProof `json:"storageProofs"`
	// qmtree witness proving TxNum is committed.
	Witness       QMProofResult        `json:"witness"`
	Verified      bool                 `json:"verified"`
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

// readStateAtTxNum reads account data and storage slots using a history reader
// positioned just after targetTxNum (i.e., state as of after that tx executed).
func (api *QMAPIImpl) readStateAtTxNum(tx kv.TemporalTx, address common.Address, storageKeys []common.Hash, targetTxNum uint64) (
	bal *hexutil.Big, nonce hexutil.Uint64, codeHash common.Hash, slots []QMStorageSlotProof, err error,
) {
	// txNum+1: history reader returns state as of "before txNum", so we add 1
	// to get the state after targetTxNum has been applied.
	reader := state.NewHistoryReaderV3(tx, targetTxNum+1)

	addr := accounts.InternAddress(address)
	acct, err := reader.ReadAccountData(addr)
	if err != nil {
		return nil, 0, common.Hash{}, nil, fmt.Errorf("read account: %w", err)
	}
	if acct != nil {
		bal = (*hexutil.Big)(acct.Balance.ToBig())
		nonce = hexutil.Uint64(acct.Nonce)
		codeHash = acct.CodeHash.Value()
	} else {
		bal = new(hexutil.Big)
	}

	slots = make([]QMStorageSlotProof, len(storageKeys))
	for i, slot := range storageKeys {
		val, _, err := reader.ReadAccountStorage(addr, accounts.InternKey(slot))
		if err != nil {
			return nil, 0, common.Hash{}, nil, fmt.Errorf("read storage slot %s: %w", slot.Hex(), err)
		}
		var valBytes [32]byte
		val.WriteToArray32(&valBytes)
		slots[i] = QMStorageSlotProof{
			Slot:  slot,
			Value: valBytes[:],
		}
	}
	return bal, nonce, codeHash, slots, nil
}

// readLatestState reads account data and storage slots at the current chain tip.
func (api *QMAPIImpl) readLatestState(tx kv.TemporalTx, address common.Address, storageKeys []common.Hash) (
	bal *hexutil.Big, nonce hexutil.Uint64, codeHash common.Hash, slots []QMStorageSlotProof, err error,
) {
	reader := state.NewReaderV3(tx)
	addr := accounts.InternAddress(address)
	acct, err := reader.ReadAccountData(addr)
	if err != nil {
		return nil, 0, common.Hash{}, nil, fmt.Errorf("read account: %w", err)
	}
	if acct != nil {
		bal = (*hexutil.Big)(acct.Balance.ToBig())
		nonce = hexutil.Uint64(acct.Nonce)
		codeHash = acct.CodeHash.Value()
	} else {
		bal = new(hexutil.Big)
	}

	slots = make([]QMStorageSlotProof, len(storageKeys))
	for i, slot := range storageKeys {
		val, _, err := reader.ReadAccountStorage(addr, accounts.InternKey(slot))
		if err != nil {
			return nil, 0, common.Hash{}, nil, fmt.Errorf("read storage slot %s: %w", slot.Hex(), err)
		}
		var valBytes [32]byte
		val.WriteToArray32(&valBytes)
		slots[i] = QMStorageSlotProof{
			Slot:  slot,
			Value: valBytes[:],
		}
	}
	return bal, nonce, codeHash, slots, nil
}

// buildStateProofResult assembles a QMStateProofResult from a targetTxNum,
// state values, and the qmtree witness for that txnum.
func (api *QMAPIImpl) buildStateProofResult(
	address common.Address, blockNum uint64, targetTxNum uint64,
	bal *hexutil.Big, nonce hexutil.Uint64, codeHash common.Hash,
	slots []QMStorageSlotProof,
) (*QMStateProofResult, error) {
	witness, err := api.tracker.GetWitness(targetTxNum)
	if err != nil {
		return nil, fmt.Errorf("get witness for txnum=%d: %w", targetTxNum, err)
	}
	hasher := &qmtree.Keccak256Hasher{}
	verified := witness.Verify(hasher) == nil

	return &QMStateProofResult{
		Address:       address,
		BlockNumber:   hexutil.Uint64(blockNum),
		TxNum:         hexutil.Uint64(targetTxNum),
		Balance:       bal,
		Nonce:         nonce,
		CodeHash:      codeHash,
		StorageProofs: slots,
		Witness: QMProofResult{
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
		},
		Verified: verified,
	}, nil
}

// GetAccountStateProof returns the account/storage state AND a qmtree proof
// for the last transaction that touched this address up to the given block.
func (api *QMAPIImpl) GetAccountStateProof(ctx context.Context, address common.Address, storageKeys []common.Hash, blockNrOrHash rpc.BlockNumberOrHash) (*QMStateProofResult, error) {
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

	// Find the latest txnum <= lastTxNum that touched this address.
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

	bal, nonce, codeHash, slots, err := api.readStateAtTxNum(tx, address, storageKeys, targetTxNum)
	if err != nil {
		return nil, err
	}
	return api.buildStateProofResult(address, uint64(blockNum), targetTxNum, bal, nonce, codeHash, slots)
}

// GetTxStateProof returns the account/storage state after a specific
// transaction in a block, together with the qmtree proof for that transaction.
func (api *QMAPIImpl) GetTxStateProof(ctx context.Context, address common.Address, storageKeys []common.Hash, blockNrOrHash rpc.BlockNumberOrHash, txIndex hexutil.Uint) (*QMStateProofResult, error) {
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
	targetTxNum := fromTxNum + uint64(txIndex)

	bal, nonce, codeHash, slots, err := api.readStateAtTxNum(tx, address, storageKeys, targetTxNum)
	if err != nil {
		return nil, err
	}
	return api.buildStateProofResult(address, uint64(blockNum), targetTxNum, bal, nonce, codeHash, slots)
}

// GetLatestStateProof returns the current account/storage state together with
// the qmtree proof for the most recent transaction that touched this address.
func (api *QMAPIImpl) GetLatestStateProof(ctx context.Context, address common.Address, storageKeys []common.Hash) (*QMStateProofResult, error) {
	if api.tracker == nil {
		return nil, fmt.Errorf("qmtree tracker not available")
	}

	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Find the most recent txnum that touched this address across all history.
	it, err := tx.IndexRange(kv.AccountsHistoryIdx, address.Bytes(), -1, int(api.tracker.NextSN), false, -1)
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
		targetTxNum = txn
		found = true
	}
	if !found {
		return nil, fmt.Errorf("no state change found for address %s", address.Hex())
	}

	bal, nonce, codeHash, slots, err := api.readLatestState(tx, address, storageKeys)
	if err != nil {
		return nil, err
	}
	return api.buildStateProofResult(address, 0, targetTxNum, bal, nonce, codeHash, slots)
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
