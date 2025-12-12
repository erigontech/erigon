package domains

import (
	"context"
	"fmt"
	"io"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/holiman/uint256"
)

type AccountsDomain struct {
}

func (AccountsDomain) GetLatest(ctx context.Context, address accounts.Address, tx kv.TemporalTx) (*accounts.Account, kv.Step, bool, error) {
	return state.GetLatest[accounts.Address, *accounts.Account](ctx, kv.AccountsDomain, address, tx)
}

type StorageDomain struct {
}

type StorageLocation struct {
	Address accounts.Address
	Key     accounts.StorageKey
}

func (StorageLocation) Encode(w io.Writer) error {
	return fmt.Errorf("TODO")
}

func (StorageLocation) Decode(r io.Reader) error {
	return fmt.Errorf("TODO")
}

type StorageValue uint256.Int

func (StorageValue) Encode(w io.Writer) error {
	return fmt.Errorf("TODO")
}

func (StorageValue) Decode(r io.Reader) error {
	return fmt.Errorf("TODO")
}

func (StorageDomain) GetLatest(ctx context.Context, address accounts.Address, key accounts.StorageKey, tx kv.TemporalTx) (uint256.Int, kv.Step, bool, error) {
	v, s, ok, err := state.GetLatest[StorageLocation, StorageValue](ctx, kv.StorageDomain, StorageLocation{address, key}, tx)
	return uint256.Int(v), s, ok, err
}

type CodeDomain struct {
}

type CodeValue []byte

func (CodeValue) Encode(w io.Writer) error {
	return fmt.Errorf("TODO")
}

func (CodeValue) Decode(r io.Reader) error {
	return fmt.Errorf("TODO")
}

func (CodeDomain) GetLatest(ctx context.Context, hash accounts.CodeHash, tx kv.TemporalTx) ([]byte, kv.Step, bool, error) {
	return state.GetLatest[accounts.CodeHash, CodeValue](ctx, kv.StorageDomain, hash, tx)
}

type CommitmentDomain struct {
}

func (CommitmentDomain) GetLatest(ctx context.Context, hash commitment.Path, tx kv.TemporalTx) (commitment.Branch, kv.Step, bool, error) {
	return state.GetLatest[commitment.Path, commitment.Branch](ctx, kv.StorageDomain, hash, tx)
}
