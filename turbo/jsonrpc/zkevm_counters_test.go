package jsonrpc

import (
	"testing"
	"errors"
	"github.com/ledgerwatch/erigon/eth/tracers"
)

type MockDepthGetter struct {
	DepthBlockNum uint64
	SMTDepth      uint64
	Err           error
}

func (m *MockDepthGetter) GetClosestSmtDepth(blockNum uint64) (uint64, uint64, error) {
	return m.DepthBlockNum, m.SMTDepth, m.Err
}

func intPtr(i int) *int {
	return &i
}

func TestGetSmtDepth(t *testing.T) {
	testCases := map[string]struct {
		blockNum      uint64
		config        *tracers.TraceConfig_ZkEvm
		mockSetup     func(m *MockDepthGetter)
		expectedDepth int
		expectedErr   error
	}{
		"Config provided with SmtDepth": {
			blockNum: 100,
			config: &tracers.TraceConfig_ZkEvm{
				SmtDepth: intPtr(128),
			},
			mockSetup: func(m *MockDepthGetter) {
				// No DB call expected.
			},
			expectedDepth: 128,
			expectedErr:   nil,
		},
		"Config is nil, GetClosestSmtDepth returns depthBlockNum < blockNum": {
			blockNum: 100,
			config:   nil,
			mockSetup: func(m *MockDepthGetter) {
				m.DepthBlockNum = 90
				m.SMTDepth = 100
				m.Err = nil
			},
			expectedDepth: 110, // 100 + 100/10
			expectedErr:   nil,
		},
		"Config is nil, GetClosestSmtDepth returns depthBlockNum >= blockNum": {
			blockNum: 100,
			config:   nil,
			mockSetup: func(m *MockDepthGetter) {
				m.DepthBlockNum = 100
				m.SMTDepth = 100
				m.Err = nil
			},
			expectedDepth: 100,
			expectedErr:   nil,
		},
		"Config is nil, smtDepth after adjustment exceeds 256": {
			blockNum: 100,
			config:   nil,
			mockSetup: func(m *MockDepthGetter) {
				m.DepthBlockNum = 90
				m.SMTDepth = 250
				m.Err = nil
			},
			expectedDepth: 256, // 250 + 25 = 275 -> capped to 256
			expectedErr:   nil,
		},
		"Config is nil, smtDepth is 0": {
			blockNum: 100,
			config:   nil,
			mockSetup: func(m *MockDepthGetter) {
				m.DepthBlockNum = 90
				m.SMTDepth = 0
				m.Err = nil
			},
			expectedDepth: 256, // 0 is invalid, set to 256
			expectedErr:   nil,
		},
		"Config is nil, GetClosestSmtDepth returns error": {
			blockNum: 100,
			config:   nil,
			mockSetup: func(m *MockDepthGetter) {
				m.DepthBlockNum = 0
				m.SMTDepth = 0
				m.Err = errors.New("database error")
			},
			expectedDepth: 0,
			expectedErr:   errors.New("database error"),
		},
		"Config provided with SmtDepth exceeding 256": {
			blockNum: 100,
			config: &tracers.TraceConfig_ZkEvm{
				SmtDepth: intPtr(300),
			},
			mockSetup: func(m *MockDepthGetter) {
				// No DB call expected.
			},
			expectedDepth: 300, // As per the function logic, returned as-is.
			expectedErr:   nil,
		},
		"Config provided with SmtDepth set to 0": {
			blockNum: 100,
			config: &tracers.TraceConfig_ZkEvm{
				SmtDepth: intPtr(0),
			},
			mockSetup: func(m *MockDepthGetter) {
				// No DB call expected.
			},
			expectedDepth: 0, // As per the function logic, returned as-is.
			expectedErr:   nil,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			mock := &MockDepthGetter{}
			tc.mockSetup(mock)

			actualDepth, actualErr := getSmtDepth(mock, tc.blockNum, tc.config)

			if tc.expectedErr != nil {
				if actualErr == nil {
					t.Fatalf("expected error '%v', but got nil", tc.expectedErr)
				}
				if actualErr.Error() != tc.expectedErr.Error() {
					t.Fatalf("expected error '%v', but got '%v'", tc.expectedErr, actualErr)
				}
			} else {
				if actualErr != nil {
					t.Fatalf("expected no error, but got '%v'", actualErr)
				}
			}

			if actualDepth != tc.expectedDepth {
				t.Errorf("expected smtDepth %d, but got %d", tc.expectedDepth, actualDepth)
			}
		})
	}
}
