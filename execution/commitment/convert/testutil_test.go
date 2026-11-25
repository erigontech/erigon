package convert

import (
	"bytes"
	"testing"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	v3 "github.com/erigontech/erigon/execution/commitment/v3"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/erigontech/erigon/internal/commitmenttest/runner"
)

func newTestRecordMatcher(t *testing.T) *v3.RecordMatcher {
	m := v3.NewRecordMatcher(t.TempDir())
	t.Cleanup(m.Close)
	return m
}

func testUpdates(t testing.TB, mode commitment.Mode, ops []commitmenttest.Op) *commitment.Updates {
	t.Helper()
	updates := commitment.NewUpdates(mode, t.TempDir(), commitment.KeyToHexNibbleHash)
	t.Cleanup(updates.Close)
	for _, op := range ops {
		updates.TouchPlainKeyDirect(string(op.Key), runner.Update(op))
	}
	return updates
}

type mockContext struct {
	branches     map[string][]byte
	branchBuf    []byte
	branchCalls  [][]byte
	putCalls     int
	accountCalls int
	storageCalls int
}

func newMockContext() *mockContext {
	return &mockContext{branches: make(map[string][]byte)}
}

func (m *mockContext) Branch(key []byte) ([]byte, kv.Step, error) {
	m.branchCalls = append(m.branchCalls, bytes.Clone(key))
	data, ok := m.branches[string(key)]
	if !ok {
		return nil, 0, nil
	}
	if m.branchBuf == nil {
		m.branchBuf = make([]byte, 0, len(data))
	}
	m.branchBuf = append(m.branchBuf[:0], data...)
	return m.branchBuf, 0, nil
}

func (m *mockContext) PutBranch(key, data, _ []byte) error {
	m.putCalls++
	m.branches[string(key)] = bytes.Clone(data)
	return nil
}

func (m *mockContext) Account([]byte) (*commitment.Update, error) {
	m.accountCalls++
	return &commitment.Update{}, nil
}

func (m *mockContext) Storage([]byte) (*commitment.Update, error) {
	m.storageCalls++
	return &commitment.Update{}, nil
}
