package utils

import (
	"encoding/binary"
	"errors"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/state"

	"github.com/holiman/uint256"
	"github.com/iden3/go-iden3-crypto/keccak256"
)

const (
	GLOBAL_EXIT_ROOT_STORAGE_POS        = 0
	ADDRESS_GLOBAL_EXIT_ROOT_MANAGER_L2 = "0xa40D5f56745a118D0906a34E69aeC8C0Db1cB8fA"
)

func WriteGlobalExitRoot(stateReader state.StateReader, stateWriter state.WriterWithChangeSets, ger common.Hash, timestamp uint64) error {
	empty := common.Hash{}
	if ger == empty {
		return errors.New("empty Global Exit Root")
	}

	old := common.Hash{}.Big()
	emptyUint256, overflow := uint256.FromBig(old)
	if overflow {
		return errors.New("AddGlobalExitRoot: overflow")
	}

	exitBig := new(big.Int).SetInt64(int64(timestamp))
	headerTime, overflow := uint256.FromBig(exitBig)
	if overflow {
		return errors.New("AddGlobalExitRoot: overflow")
	}

	//get Global Exit Root position
	gerb := make([]byte, 32)
	binary.BigEndian.PutUint64(gerb, GLOBAL_EXIT_ROOT_STORAGE_POS)

	// concat global exit root and global_exit_root_storage_pos
	rootPlusStorage := append(ger[:], gerb...)
	globalExitRootPosBytes := keccak256.Hash(rootPlusStorage)
	gerp := common.BytesToHash(globalExitRootPosBytes)

	addr := common.HexToAddress(ADDRESS_GLOBAL_EXIT_ROOT_MANAGER_L2)

	// if root already has a timestamp - don't update it
	prevTs, err := stateReader.ReadAccountStorage(addr, uint64(1), &gerp)
	if err != nil {
		return err
	}
	a := new(big.Int).SetBytes(prevTs)
	if a.Cmp(big.NewInt(0)) != 0 {
		return nil
	}

	// write global exit root to state
	if err := stateWriter.WriteAccountStorage(addr, uint64(1), &gerp, emptyUint256, headerTime); err != nil {
		return err
	}

	return nil
}
