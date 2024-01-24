package state

import (
	"errors"
	"github.com/ledgerwatch/erigon/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/holiman/uint256"
	"github.com/iden3/go-iden3-crypto/keccak256"
)

type ReadOnlyHermezDb interface {
	GetEffectiveGasPricePercentage(txHash libcommon.Hash) (uint8, error)
	GetStateRoot(l2BlockNo uint64) (libcommon.Hash, error)
}

func (sdb *IntraBlockState) GetTxCount() (uint64, error) {
	counter, ok := sdb.stateReader.(TxCountReader)
	if !ok {
		return 0, errors.New("state reader does not support GetTxCount")
	}
	return counter.GetTxCount()
}

func (sdb *IntraBlockState) ScalableSetTxNum() {
	saddr := libcommon.HexToAddress("0x000000000000000000000000000000005ca1ab1e")
	sl0 := libcommon.HexToHash("0x0")

	txNum := uint256.NewInt(0)
	sdb.GetState(saddr, &sl0, txNum)

	txNum.Add(txNum, uint256.NewInt(1))

	if !sdb.Exist(saddr) {
		// create account if not exists
		sdb.CreateAccount(saddr, true)
	}

	// set incremented tx num in state
	sdb.SetState(saddr, &sl0, *txNum)
}

func (sdb *IntraBlockState) ScalableSetSmtRootHash(roHermezDb ReadOnlyHermezDb) error {
	saddr := libcommon.HexToAddress("0x000000000000000000000000000000005ca1ab1e")
	sl0 := libcommon.HexToHash("0x0")

	txNum := uint256.NewInt(0)
	sdb.GetState(saddr, &sl0, txNum)

	// create mapping with keccak256(txnum,1) -> smt root
	d1 := common.LeftPadBytes(txNum.Bytes(), 32)
	d2 := common.LeftPadBytes(uint256.NewInt(1).Bytes(), 32)
	mapKey := keccak256.Hash(d1, d2)
	mkh := libcommon.BytesToHash(mapKey)

	rpcHash, err := roHermezDb.GetStateRoot(txNum.Uint64())
	if err != nil {
		return err
	}

	if txNum.Uint64() >= 1 {
		// set mapping of keccak256(txnum,1) -> smt root
		rpcHashU256 := uint256.NewInt(0).SetBytes(rpcHash.Bytes())
		sdb.SetState(saddr, &mkh, *rpcHashU256)
	}

	return nil
}
