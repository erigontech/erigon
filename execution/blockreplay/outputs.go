package blockreplay

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// recordingWriter is a state.StateWriter that records the post-state a block
// writes (its outputs) instead of persisting anywhere. Paired with the in-mem
// reader during a serial reference run, it captures the known-correct output
// set a parallel replay is then checked against.
type recordingWriter struct {
	out *Outputs
}

func newRecordingWriter() *recordingWriter { return &recordingWriter{out: newOutputs()} }

func (w *recordingWriter) UpdateAccountData(address accounts.Address, original, account *accounts.Account) error {
	a := address.Value()
	delete(w.out.Deleted, a)
	w.out.Accounts[a] = toAcctData(account)
	return nil
}

func (w *recordingWriter) UpdateAccountCode(address accounts.Address, incarnation uint64, codeHash accounts.CodeHash, code []byte) error {
	w.out.Code[address.Value()] = bytes.Clone(code)
	return nil
}

func (w *recordingWriter) DeleteAccount(address accounts.Address, original *accounts.Account) error {
	a := address.Value()
	delete(w.out.Accounts, a)
	w.out.Deleted[a] = true
	return nil
}

func (w *recordingWriter) WriteAccountStorage(address accounts.Address, incarnation uint64, key accounts.StorageKey, original, value uint256.Int) error {
	a := address.Value()
	m := w.out.Storage[a]
	if m == nil {
		m = map[[32]byte][32]byte{}
		w.out.Storage[a] = m
	}
	m[key.Value()] = value.Bytes32()
	return nil
}

func (w *recordingWriter) CreateContract(address accounts.Address) error { return nil }

// CollectOutputs reads the post-state a replay produced for exactly the keys in
// want (the reference output set), through the standard state reader over the
// post-execution domains. This is the "Flush → outputs" read of the ephemeral
// model: after Execute, output state is received via the domains. It mirrors
// want's shape so the two can be compared directly.
func CollectOutputs(r state.StateReader, want *Outputs) (*Outputs, error) {
	got := newOutputs()
	for a := range want.Accounts {
		acct, err := r.ReadAccountData(accounts.InternAddress(common.Address(a)))
		if err != nil {
			return nil, err
		}
		if acct == nil {
			got.Deleted[a] = true
			continue
		}
		got.Accounts[a] = toAcctData(acct)
	}
	for a := range want.Deleted {
		acct, err := r.ReadAccountData(accounts.InternAddress(common.Address(a)))
		if err != nil {
			return nil, err
		}
		if acct == nil {
			got.Deleted[a] = true
		} else {
			got.Accounts[a] = toAcctData(acct)
		}
	}
	for a, slots := range want.Storage {
		addr := accounts.InternAddress(common.Address(a))
		for k := range slots {
			v, _, err := r.ReadAccountStorage(addr, accounts.InternKey(common.Hash(k)))
			if err != nil {
				return nil, err
			}
			m := got.Storage[a]
			if m == nil {
				m = map[[32]byte][32]byte{}
				got.Storage[a] = m
			}
			m[k] = v.Bytes32()
		}
	}
	for a := range want.Code {
		code, err := r.ReadAccountCode(accounts.InternAddress(common.Address(a)))
		if err != nil {
			return nil, err
		}
		got.Code[a] = bytes.Clone(code)
	}
	return got, nil
}

// Diff returns a human-readable list of the places got departs from want
// (empty when they match). Account balance/nonce/codehash, deletions, storage
// slots, and code are all compared — everything except the trie root.
func (want *Outputs) Diff(got *Outputs) []string {
	var diffs []string
	for a, w := range want.Accounts {
		g, ok := got.Accounts[a]
		if !ok {
			diffs = append(diffs, fmt.Sprintf("account %x: missing (want present)", a))
			continue
		}
		if g.Nonce != w.Nonce || g.Balance != w.Balance || g.CodeHash != w.CodeHash || g.Incarnation != w.Incarnation {
			diffs = append(diffs, fmt.Sprintf("account %x: got {nonce=%d bal=%x codehash=%x inc=%d} want {nonce=%d bal=%x codehash=%x inc=%d}",
				a, g.Nonce, g.Balance, g.CodeHash, g.Incarnation, w.Nonce, w.Balance, w.CodeHash, w.Incarnation))
		}
	}
	for a := range want.Deleted {
		if !got.Deleted[a] {
			diffs = append(diffs, fmt.Sprintf("account %x: want deleted, got present", a))
		}
	}
	for a, ws := range want.Storage {
		gs := got.Storage[a]
		for k, wv := range ws {
			if gs[k] != wv {
				diffs = append(diffs, fmt.Sprintf("storage %x/%x: got %x want %x", a, k, gs[k], wv))
			}
		}
	}
	for a, wc := range want.Code {
		if !bytes.Equal(got.Code[a], wc) {
			diffs = append(diffs, fmt.Sprintf("code %x: length got %d want %d", a, len(got.Code[a]), len(wc)))
		}
	}
	sort.Strings(diffs)
	return diffs
}
