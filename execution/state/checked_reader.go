// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"bytes"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// assertNormalizeReads turns on the cross-check in NewNormalizeReader.
var assertNormalizeReads = dbg.EnvBool("NORMALIZE_ASSERT_READS", false)

// NewNormalizeReader returns the reader Normalize should use: the tx's own
// recorded reads first, then the versionMap, then the domain.
//
// With NORMALIZE_ASSERT_READS=true it also reads the domain for every call and
// panics on disagreement. That is diagnostic-only and changes behaviour: the
// domain reader fills the block state cache, so reading through it twice
// perturbs the cache and fails blocks that pass without it.
func NewNormalizeReader(txIndex int, reads ReadSet, versionMap *VersionMap, domain StateReader) StateReader {
	if domain == nil { // nil means "no reader"; Normalize guards on it
		return nil
	}
	readSet := NewVersionedStateReader(txIndex, reads, versionMap, domain)
	if !assertNormalizeReads {
		return readSet
	}
	return NewCheckedStateReader(readSet, domain)
}

// CheckedStateReader answers from want and asserts that got agrees, panicking
// on any disagreement. It exists to settle one question empirically: can
// Normalize take the values a worker already recorded in its read set instead
// of re-reading the domain on the apply loop? Every divergence is a case where
// it cannot, and the panic names it.
//
// Diagnostic only, and not side-effect free: see NewNormalizeReader.
type CheckedStateReader struct {
	want StateReader // read set -> versionMap -> domain
	got  StateReader // domain
}

func NewCheckedStateReader(want, got StateReader) *CheckedStateReader {
	return &CheckedStateReader{want: want, got: got}
}

func mismatch(op string, addr accounts.Address, detail string) {
	panic(fmt.Sprintf("checked reader: %s disagrees for %x: %s", op, addr.Value(), detail))
}

func (r *CheckedStateReader) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	want, err := r.want.ReadAccountData(address)
	if err != nil {
		mismatch("ReadAccountData", address, fmt.Sprintf("read-set path errored: %v", err))
	}
	got, gotErr := r.got.ReadAccountData(address)
	if gotErr != nil {
		mismatch("ReadAccountData", address, fmt.Sprintf("domain errored (%v) while read set answered", gotErr))
	}
	switch {
	case want == nil && got == nil:
	case want == nil || got == nil:
		mismatch("ReadAccountData", address, fmt.Sprintf("presence differs: readset=%v domain=%v", want != nil, got != nil))
	case want.Nonce != got.Nonce || !want.Balance.Eq(&got.Balance) ||
		want.Incarnation != got.Incarnation || want.CodeHash.Value() != got.CodeHash.Value():
		mismatch("ReadAccountData", address, fmt.Sprintf(
			"readset={n:%d bal:%s inc:%d ch:%x} domain={n:%d bal:%s inc:%d ch:%x}",
			want.Nonce, want.Balance.String(), want.Incarnation, want.CodeHash.Value(),
			got.Nonce, got.Balance.String(), got.Incarnation, got.CodeHash.Value()))
	}
	return want, nil
}

func (r *CheckedStateReader) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	want, wantOK, err := r.want.ReadAccountStorage(address, key)
	if err != nil {
		mismatch("ReadAccountStorage", address, fmt.Sprintf("read-set path errored: %v", err))
	}
	got, gotOK, gotErr := r.got.ReadAccountStorage(address, key)
	if gotErr != nil {
		mismatch("ReadAccountStorage", address, fmt.Sprintf("domain errored (%v) while read set answered", gotErr))
	}
	// Absent and present-with-zero are the same slot to the no-op filter: it
	// drops a zero write either way and keeps a non-zero one either way, so
	// only the effective value has to agree. The read set reports found=true
	// for a slot the worker read as zero; the domain reports it absent.
	wantEff, gotEff := want, got
	if !wantOK {
		wantEff = uint256.Int{}
	}
	if !gotOK {
		gotEff = uint256.Int{}
	}
	if !wantEff.Eq(&gotEff) {
		mismatch("ReadAccountStorage", address, fmt.Sprintf(
			"slot %x: readset={%s,found:%v} domain={%s,found:%v}",
			key.Value(), want.String(), wantOK, got.String(), gotOK))
	}
	return want, wantOK, nil
}

func (r *CheckedStateReader) ReadAccountCode(address accounts.Address) ([]byte, error) {
	want, err := r.want.ReadAccountCode(address)
	if err != nil {
		mismatch("ReadAccountCode", address, fmt.Sprintf("read-set path errored: %v", err))
	}
	got, gotErr := r.got.ReadAccountCode(address)
	if gotErr != nil {
		mismatch("ReadAccountCode", address, fmt.Sprintf("domain errored (%v) while read set answered", gotErr))
	}
	if !bytes.Equal(want, got) {
		mismatch("ReadAccountCode", address, fmt.Sprintf("len readset=%d domain=%d", len(want), len(got)))
	}
	return want, nil
}

func (r *CheckedStateReader) ReadAccountCodeSize(address accounts.Address) (int, error) {
	want, err := r.want.ReadAccountCodeSize(address)
	if err != nil {
		return 0, err
	}
	got, gotErr := r.got.ReadAccountCodeSize(address)
	if gotErr != nil {
		mismatch("ReadAccountCodeSize", address, fmt.Sprintf("domain errored (%v) while read set answered", gotErr))
	}
	if want != got {
		mismatch("ReadAccountCodeSize", address, fmt.Sprintf("readset=%d domain=%d", want, got))
	}
	return want, nil
}

func (r *CheckedStateReader) HasStorage(address accounts.Address) (bool, error) {
	want, err := r.want.HasStorage(address)
	if err != nil {
		return false, err
	}
	got, gotErr := r.got.HasStorage(address)
	if gotErr != nil {
		mismatch("HasStorage", address, fmt.Sprintf("domain errored (%v) while read set answered", gotErr))
	}
	if want != got {
		mismatch("HasStorage", address, fmt.Sprintf("readset=%v domain=%v", want, got))
	}
	return want, nil
}

func (r *CheckedStateReader) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
	want, err := r.want.ReadAccountIncarnation(address)
	if err != nil {
		return 0, err
	}
	got, gotErr := r.got.ReadAccountIncarnation(address)
	if gotErr != nil {
		mismatch("ReadAccountIncarnation", address, fmt.Sprintf("domain errored (%v) while read set answered", gotErr))
	}
	if want != got {
		mismatch("ReadAccountIncarnation", address, fmt.Sprintf("readset=%d domain=%d", want, got))
	}
	return want, nil
}

func (r *CheckedStateReader) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	return r.want.ReadAccountDataForDebug(address)
}

func (r *CheckedStateReader) SetTrace(trace bool, tracePrefix string) {
	r.want.SetTrace(trace, tracePrefix)
}

func (r *CheckedStateReader) Trace() bool { return r.want.Trace() }

func (r *CheckedStateReader) TracePrefix() string { return r.want.TracePrefix() }
