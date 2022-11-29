package verkletrie

import (
	"bytes"

	"github.com/ledgerwatch/erigon-lib/commitment"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/core/types/accounts"
)

type VerkleExternal struct {
	//tx *kv.RwTx
	vt *VerkleTreeWriter

	// Function used to load branch node and fill up the cells
	// For each cell, it sets the cell type, clears the modified flag, fills the hash,
	// and for the extension, account, and leaf type, the `l` and `k`
	branchFn func(prefix []byte) ([]byte, error)
	// Function used to fetch account with given plain key
	accountFn func(plainKey []byte, cell *commitment.Cell) error
	// Function used to fetch storage with given plain key
	storageFn func(plainKey []byte, cell *commitment.Cell) error
}

func NewExternalVerkleTree(tx *kv.RwTx, tmp string) VerkleExternal {
	return VerkleExternal{
		vt:        NewVerkleTreeWriter(*tx, tmp),
		branchFn:  nil,
		accountFn: nil,
		storageFn: nil,
	}

}

func (vt *VerkleExternal) Reset() {
	//return commitment.TrieVariant("verkle")
}

func (vt *VerkleExternal) Variant() commitment.TrieVariant {
	return commitment.TrieVariant("verkle")
}

func (vt *VerkleExternal) ReviewKeys(pk, hk [][]byte) (rootHash []byte, branchNodeUpdates map[string]commitment.BranchData, err error) {
	for i, plainKey := range pk {
		var cell commitment.Cell
		switch len(plainKey) {
		case length.Addr:
			if err := vt.accountFn(plainKey, &cell); err != nil {
				return nil, nil, err
			}
			if cell.Delete {
				if err := vt.vt.DeleteAccount(hk[i], false); err != nil {
					return nil, nil, err
				}
				continue
			}
			acc := accounts.Account{
				Initialised: true,
				Nonce:       cell.Nonce,
				Balance:     cell.Balance,
				//Root:        commitment.EmptyRootHash[:],
				CodeHash:    cell.CodeHash,
			}
			isContract := bytes.Equal(acc.CodeHash[:], commitment.EmptyCodeHash)

			err := vt.vt.UpdateAccount(hk[i], 0, isContract,acc)
			if err != nil {
				return nil, nil, err
			}
		case length.Addr + length.Hash:
			if err := vt.storageFn(plainKey, &cell); err != nil {
				return nil, nil, err
			}
			// if cell.Delete storageLen will be 0 so writing empty slice causes sotrage deletion
			if err := vt.vt.Insert(plainKey, cell.Storage[:cell.StorageLen]); err != nil {
				panic(err)
			}
		default:
			log.Warn("unexpected key length during verkle trie commitment", "len", len(plainKey))
		}
	}

	rh, err := vt.vt.CommitVerkleTreeFromScratch()
	if err != nil {
		return nil, nil, err
	}
	return rh[:], nil, nil
}

func (vt *VerkleTreeWriter) ProcessUpdates(pk, hk [][]byte, updates []commitment.Update) (rootHash []byte, branchNodeUpdates map[string]commitment.BranchData, err error) {
	//TODO implement me
	panic("implement me")
}

func (vt *VerkleExternal) ResetFns(
	branchFn func(prefix []byte) ([]byte, error),
	accountFn func(plainKey []byte, cell *commitment.Cell) error,
	storageFn func(plainKey []byte, cell *commitment.Cell) error,
	) {

	vt.branchFn =  branchFn
	vt.accountFn = accountFn
	vt.storageFn = storageFn
}


