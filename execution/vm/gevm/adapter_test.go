package gevm

import (
	"testing"

	gevmstate "github.com/Giulio2002/gevm/state"
	gevmtypes "github.com/Giulio2002/gevm/types"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	erigonstate "github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type recordingWriter struct {
	deletes []accounts.Address
	updates []accounts.Account
}

func (w *recordingWriter) UpdateAccountData(_ accounts.Address, _, account *accounts.Account) error {
	w.updates = append(w.updates, *account)
	return nil
}

func (w *recordingWriter) UpdateAccountCode(accounts.Address, uint64, accounts.CodeHash, []byte) error {
	return nil
}

func (w *recordingWriter) DeleteAccount(address accounts.Address, _ *accounts.Account) error {
	w.deletes = append(w.deletes, address)
	return nil
}

func (w *recordingWriter) WriteAccountStorage(accounts.Address, uint64, accounts.StorageKey, uint256.Int, uint256.Int) error {
	return nil
}

func (w *recordingWriter) CreateContract(accounts.Address) error {
	return nil
}

var _ erigonstate.StateWriter = (*recordingWriter)(nil)

func TestApplyStatePreservesSelfdestructResidualBalance(t *testing.T) {
	addr := gevmtypes.Address{19: 0x01}
	balance := *uint256.NewInt(99)
	acc := gevmstate.NewAccountFromInfo(gevmstate.AccountInfo{
		Balance:     *uint256.NewInt(1),
		Nonce:       7,
		Incarnation: 1,
		CodeHash:    gevmtypes.Keccak256([]byte{0x60, 0x00}),
	})
	acc.MarkSelfdestruct()
	acc.Info.Balance = balance

	writer := &recordingWriter{}
	if err := applyState(gevmstate.EvmState{addr: acc}, []gevmtypes.Address{addr}, writer, &chain.Rules{IsSpuriousDragon: true}); err != nil {
		t.Fatal(err)
	}
	if len(writer.deletes) != 1 {
		t.Fatalf("deletes: got %d, want 1", len(writer.deletes))
	}
	if len(writer.updates) != 1 {
		t.Fatalf("updates: got %d, want 1", len(writer.updates))
	}
	update := writer.updates[0]
	if update.Balance != balance {
		t.Fatalf("balance: got %v, want %v", &update.Balance, &balance)
	}
	if update.Nonce != 0 || update.Incarnation != 0 || update.CodeHash.Value() != common.Hash(gevmtypes.KeccakEmpty) {
		t.Fatalf("residual account should be balance-only, got nonce=%d incarnation=%d codeHash=%x", update.Nonce, update.Incarnation, update.CodeHash.Value())
	}
}
