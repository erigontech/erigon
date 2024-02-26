package legacy_executor_verifier

//
//import (
//	"github.com/ledgerwatch/erigon-lib/common"
//	"testing"
//	"time"
//)
//
//type MockLegacyExecutor struct {
//	verifyFunc func() (bool, error)
//}
//
//func (m *MockLegacyExecutor) Verify(p *Payload, expectedStateRoot *common.Hash) (bool, error) {
//	if m.verifyFunc != nil {
//		return m.verifyFunc()
//	}
//	return false, nil
//}
//
//func TestVerifyWithAvailableExecutor(t *testing.T) {
//	mockExecutors := make([]ILegacyExecutor, 20)
//	for i := 0; i < len(mockExecutors); i++ {
//		mockExecutors[i] = &MockLegacyExecutor{
//			verifyFunc: func() (bool, error) {
//				time.Sleep(100 * time.Millisecond)
//				return true, nil
//			},
//		}
//	}
//
//	verifier := NewLegacyExecutorVerifier(mockExecutors)
//
//	done := make(chan bool)
//	for i := 0; i < len(mockExecutors)*2; i++ {
//		go func() {
//			result, err := verifier.VerifyWithAvailableExecutor(nil, nil)
//			if err != nil {
//				t.Errorf("Unexpected error: %v", err)
//			}
//			if !result {
//				t.Errorf("Expected true result, got %v", result)
//			}
//			done <- true
//		}()
//	}
//
//	for i := 0; i < len(mockExecutors)*2; i++ {
//		<-done
//	}
//}
//
//func TestVerifyWithSlowExecutor(t *testing.T) {
//	mockExecutors := make([]ILegacyExecutor, 4)
//	for i := 0; i < len(mockExecutors)-1; i++ {
//		mockExecutors[i] = &MockLegacyExecutor{
//			verifyFunc: func() (bool, error) {
//				time.Sleep(50 * time.Millisecond)
//				return true, nil
//			},
//		}
//	}
//
//	// slow executor response
//	mockExecutors[len(mockExecutors)-1] = &MockLegacyExecutor{
//		verifyFunc: func() (bool, error) {
//			time.Sleep(2 * time.Second)
//			return true, nil
//		},
//	}
//
//	verifier := NewLegacyExecutorVerifier(mockExecutors)
//
//	done := make(chan bool)
//	for i := 0; i < len(mockExecutors)*2; i++ {
//		go func(index int) {
//			result, err := verifier.VerifyWithAvailableExecutor(nil, nil)
//			if err != nil {
//				t.Errorf("Unexpected error in goroutine %d: %v", index, err)
//			}
//			if !result {
//				t.Errorf("Expected true result in goroutine %d, got %v", index, result)
//			}
//			done <- true
//		}(i)
//	}
//
//	for i := 0; i < len(mockExecutors)*2; i++ {
//		<-done
//	}
//}
