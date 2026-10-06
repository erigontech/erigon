package services

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBatchSignatureVerifierReportsEachEntryResult(t *testing.T) {
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) {
		if len(signatures) > 1 {
			return false, nil
		}
		return signatures[0][0] == 1, nil
	}

	verifier := NewBatchSignatureVerifier(t.Context(), nil)

	validRan := make(chan struct{}, 1)
	invalidRan := make(chan struct{}, 1)
	validResult := make(chan error, 1)
	invalidResult := make(chan error, 1)
	go func() {
		validResult <- verifier.VerifyAttestation(t.Context(), &AggregateVerificationData{
			Signatures: [][]byte{{1}},
			SignRoots:  [][]byte{{1}},
			Pks:        [][]byte{{1}},
			F:          func() { validRan <- struct{}{} },
		})
	}()
	go func() {
		invalidResult <- verifier.VerifyAttestation(t.Context(), &AggregateVerificationData{
			Signatures: [][]byte{{2}},
			SignRoots:  [][]byte{{2}},
			Pks:        [][]byte{{2}},
			F:          func() { invalidRan <- struct{}{} },
		})
	}()

	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for len(verifier.attVerifyAndExecute) != 2 {
		select {
		case <-deadline.C:
			t.Fatal("verification entries were not queued")
		default:
			runtime.Gosched()
		}
	}
	verifier.Start()

	require.NoError(t, <-validResult)
	require.ErrorIs(t, <-invalidResult, ErrInvalidBlsSignature)
	select {
	case <-validRan:
	case <-time.After(time.Second):
		t.Fatal("valid callback did not run")
	}
	select {
	case <-invalidRan:
		t.Fatal("invalid callback ran")
	default:
	}
}

func TestBatchSignatureVerifierReportsBeforeBlockingCallback(t *testing.T) {
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) {
		return true, nil
	}

	verifier := NewBatchSignatureVerifier(t.Context(), nil)
	verifier.Start()

	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	callbackDone := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-releaseCallback:
		default:
			close(releaseCallback)
		}
	})
	result := make(chan error, 1)
	go func() {
		result <- verifier.VerifyAttestation(t.Context(), &AggregateVerificationData{
			Signatures: [][]byte{{1}},
			SignRoots:  [][]byte{{1}},
			Pks:        [][]byte{{1}},
			F: func() {
				close(callbackStarted)
				<-releaseCallback
				close(callbackDone)
			},
		})
	}()

	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("callback did not start")
	}
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("verification waited for callback")
	}
	close(releaseCallback)
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("callback did not finish")
	}
}

func TestBatchSignatureVerifierContinuesWhileCallbackBlocked(t *testing.T) {
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) {
		return true, nil
	}

	verifier := NewBatchSignatureVerifier(t.Context(), nil)
	verifier.Start()

	firstCallbackStarted := make(chan struct{})
	releaseFirstCallback := make(chan struct{})
	firstCallbackDone := make(chan struct{})
	secondCallbackDone := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-releaseFirstCallback:
		default:
			close(releaseFirstCallback)
		}
	})

	firstResult := make(chan error, 1)
	go func() {
		firstResult <- verifier.VerifyAttestation(t.Context(), &AggregateVerificationData{
			Signatures: [][]byte{{1}},
			SignRoots:  [][]byte{{1}},
			Pks:        [][]byte{{1}},
			F: func() {
				close(firstCallbackStarted)
				<-releaseFirstCallback
				close(firstCallbackDone)
			},
		})
	}()

	select {
	case <-firstCallbackStarted:
	case <-time.After(time.Second):
		t.Fatal("first callback did not start")
	}
	select {
	case err := <-firstResult:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("first verification result was not reported")
	}

	secondResult := make(chan error, 1)
	go func() {
		secondResult <- verifier.VerifyAttestation(t.Context(), &AggregateVerificationData{
			Signatures: [][]byte{{2}},
			SignRoots:  [][]byte{{2}},
			Pks:        [][]byte{{2}},
			F:          func() { close(secondCallbackDone) },
		})
	}()

	select {
	case err := <-secondResult:
		require.NoError(t, err)
	case <-time.After(time.Second):
		close(releaseFirstCallback)
		<-firstCallbackDone
		t.Fatal("second verification waited for the first callback")
	}
	select {
	case <-secondCallbackDone:
		t.Fatal("second callback ran before the first callback finished")
	default:
	}

	close(releaseFirstCallback)
	select {
	case <-firstCallbackDone:
	case <-time.After(time.Second):
		t.Fatal("first callback did not finish")
	}
	select {
	case <-secondCallbackDone:
	case <-time.After(time.Second):
		t.Fatal("second callback did not run")
	}
}

func TestBatchSignatureVerifierImmediateVerificationRunsCallback(t *testing.T) {
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) {
		return true, nil
	}

	verifier := NewBatchSignatureVerifier(t.Context(), nil)
	callbackRan := false
	err := verifier.ImmediateVerification(&AggregateVerificationData{
		Signatures: [][]byte{{1}},
		SignRoots:  [][]byte{{1}},
		Pks:        [][]byte{{1}},
		F:          func() { callbackRan = true },
	})

	require.NoError(t, err)
	require.True(t, callbackRan)
}

func TestBatchSignatureVerifierWaitReturnsIgnoreOnContextCancel(t *testing.T) {
	verifier := NewBatchSignatureVerifier(context.Background(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := verifier.VerifyAttestation(ctx, &AggregateVerificationData{})
	require.ErrorIs(t, err, ErrIgnore)
}
