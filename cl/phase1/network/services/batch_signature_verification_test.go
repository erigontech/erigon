package services

import (
	"bytes"
	"context"
	"encoding/hex"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
	"github.com/stretchr/testify/require"
	blst "github.com/supranational/blst/bindings/go"
	"google.golang.org/grpc"
)

type blockingBanSentinel struct {
	sentinelproto.SentinelClient
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
}

func (s *blockingBanSentinel) BanPeer(context.Context, *sentinelproto.Peer, ...grpc.CallOption) (*sentinelproto.EmptyMessage, error) {
	s.startedOnce.Do(func() { close(s.started) })
	<-s.release
	return &sentinelproto.EmptyMessage{}, nil
}

func newRepeatedSignatureEntry(signature byte, signatureCount int) *AggregateVerificationData {
	signatures := make([][]byte, signatureCount)
	for i := range signatures {
		signatures[i] = []byte{signature}
	}
	return &AggregateVerificationData{
		Signatures: signatures,
		SignRoots:  make([][]byte, signatureCount),
		Pks:        make([][]byte, signatureCount),
	}
}

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

func TestBatchSignatureVerifierChecksFailedBatchEntriesConcurrently(t *testing.T) {
	saveSignatureGlobals(t)
	previousGOMAXPROCS := runtime.GOMAXPROCS(2)
	t.Cleanup(func() { runtime.GOMAXPROCS(previousGOMAXPROCS) })

	synctest.Test(t, func(t *testing.T) {
		firstStarted := make(chan struct{})
		secondDone := make(chan struct{})
		laterEntriesDone := make(chan struct{})
		blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) {
			if len(signatures) > 1 {
				return false, nil
			}
			signature := signatures[0][0]
			switch signature {
			case 1:
				close(firstStarted)
				<-laterEntriesDone
			case 2:
				<-firstStarted
				close(secondDone)
			case 3:
				<-secondDone
				close(laterEntriesDone)
			}
			return signature%2 == 1, nil
		}

		callbackOrder := make([]byte, 0, 2)
		resultOrder := make(chan error, 3)
		entries := make([]*AggregateVerificationData, 3)
		for i := range entries {
			signature := byte(i + 1)
			entries[i] = &AggregateVerificationData{
				Signatures: [][]byte{{signature}},
				SignRoots:  [][]byte{{signature}},
				Pks:        [][]byte{{signature}},
				F:          func() { callbackOrder = append(callbackOrder, signature) },
				result:     resultOrder,
			}
		}

		verifier := NewBatchSignatureVerifier(t.Context(), nil)
		callbacks, err := verifier.processSignatureVerification(entries)
		require.ErrorIs(t, err, ErrInvalidBlsSignature)
		require.NoError(t, <-resultOrder)
		require.ErrorIs(t, <-resultOrder, ErrInvalidBlsSignature)
		require.NoError(t, <-resultOrder)
		for _, callback := range callbacks {
			callback()
		}
		require.Equal(t, []byte{1, 3}, callbackOrder)
	})
}

func TestBatchSignatureVerifierCapsFallbackWeightAtBudget(t *testing.T) {
	saveSignatureGlobals(t)
	const budget = 2
	previousGOMAXPROCS := runtime.GOMAXPROCS(budget)
	t.Cleanup(func() { runtime.GOMAXPROCS(previousGOMAXPROCS) })

	synctest.Test(t, func(t *testing.T) {
		blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) {
			if signatures[0][0] == 1 && signatures[len(signatures)-1][0] == 2 {
				return false, nil
			}
			return signatures[0][0] == 1, nil
		}

		results := make(chan error, 2)
		entries := []*AggregateVerificationData{newRepeatedSignatureEntry(1, 3), newRepeatedSignatureEntry(2, 1)}
		for _, entry := range entries {
			entry.result = results
		}

		verifier := NewBatchSignatureVerifier(t.Context(), nil)
		callbacks, err := verifier.processSignatureVerification(entries)
		require.ErrorIs(t, err, ErrInvalidBlsSignature)
		require.NoError(t, <-results)
		require.ErrorIs(t, <-results, ErrInvalidBlsSignature)
		require.Len(t, callbacks, 1)
	})
}

func TestBatchSignatureVerifierSharesFallbackConcurrencyBudget(t *testing.T) {
	saveSignatureGlobals(t)
	const budget = 4
	previousGOMAXPROCS := runtime.GOMAXPROCS(budget)
	t.Cleanup(func() { runtime.GOMAXPROCS(previousGOMAXPROCS) })

	synctest.Test(t, func(t *testing.T) {
		const batchCount = 3
		releaseFallbacks := make(chan struct{})
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(releaseFallbacks) }) }
		t.Cleanup(release)
		var inFlightWeight atomic.Int32
		blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) {
			if signatures[0][0] == 1 && signatures[len(signatures)-1][0] == 2 {
				return false, nil
			}

			weight := int32(len(signatures))
			inFlight := inFlightWeight.Add(weight)
			if inFlight > budget {
				t.Errorf("fallback weight %d exceeds budget %d", inFlight, budget)
			}
			<-releaseFallbacks
			inFlightWeight.Add(-weight)
			return true, nil
		}

		verifier := NewBatchSignatureVerifier(t.Context(), nil)
		var group sync.WaitGroup
		group.Add(batchCount)
		for range batchCount {
			go func() {
				defer group.Done()
				_, _ = verifier.processSignatureVerification([]*AggregateVerificationData{newRepeatedSignatureEntry(1, 3), newRepeatedSignatureEntry(2, 1)})
			}()
		}
		synctest.Wait()

		release()
		group.Wait()
	})
}

func TestVerifyPayloadAttestationBatchMatchesSinglePath(t *testing.T) {
	saveSignatureGlobals(t)
	const count = batchSignatureVerificationThreshold
	const nonSubgroupSignatureHex = "85aa7e829cefd96a733fd7ed9cb646a95ebdafebe514ce732fe79ba85b52b396589bd2d567630fc39fd90ec5e6c134e412ec50905c7a53491aece65571eec1dea528e1e0091049cce4c4720f656a55428bdf665caf30662ec15e87269afe0061"
	nonSubgroupSignature, err := hex.DecodeString(nonSubgroupSignatureHex)
	require.NoError(t, err)
	nonSubgroupPoint := new(blst.P2Affine).Uncompress(nonSubgroupSignature)
	require.NotNil(t, nonSubgroupPoint)
	require.False(t, nonSubgroupPoint.SigValidate(false))
	realVerifyMultipleSignatures := blsVerifyMultipleSignatures
	firstBatchSize := make(chan int, 1)
	var recordFirstBatch sync.Once
	blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) {
		recordFirstBatch.Do(func() { firstBatchSize <- len(signatures) })
		return realVerifyMultipleSignatures(signatures, signRoots, pks)
	}

	root := make([]byte, 32)
	otherRoot := make([]byte, 32)
	root[0], otherRoot[0] = 1, 2
	entries := make([]*AggregateVerificationData, 0, count)
	expectedValid := make([]bool, 0, count)
	keys := make([]*bls.PrivateKey, 0, count+1)
	for range count + 1 {
		key, err := bls.GenerateKey()
		require.NoError(t, err)
		keys = append(keys, key)
	}
	for i := range count {
		publicKey := bls.CompressPublicKey(keys[i].PublicKey())
		signature := keys[i].Sign(root).Bytes()
		signingRoot := root
		switch i {
		case count - 6:
			signature = make([]byte, 96)
			signature[0] = 0xc0
		case count - 5:
			publicKey = bls.CompressPublicKey(keys[count].PublicKey())
		case count - 4:
			signingRoot = otherRoot
		case count - 3:
			signature = nonSubgroupSignature
		case count - 2:
			signature = bytes.Repeat([]byte{0xff}, 96)
			signature[0] = 0x9f
		case count - 1:
			signature = make([]byte, 96)
			signature[0] = 0xc0
			signature[len(signature)-1] = 1
		}
		valid, _ := bls.VerifyAggregate(signature, signingRoot, [][]byte{publicKey})
		expectedValid = append(expectedValid, valid)
		entries = append(entries, &AggregateVerificationData{
			Signatures: [][]byte{signature},
			SignRoots:  [][]byte{signingRoot},
			Pks:        [][]byte{publicKey},
			F:          func() {},
		})
	}

	verifier := NewBatchSignatureVerifier(t.Context(), nil)
	results := make([]chan error, count)
	for i := range entries {
		results[i] = make(chan error, 1)
		go func() {
			results[i] <- verifier.VerifyPayloadAttestation(t.Context(), entries[i])
		}()
	}
	waitForQueuedVerification(t, verifier.payloadAttestationVerify, count)
	verifier.Start()

	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for i := range entries {
		var err error
		select {
		case err = <-results[i]:
		case <-deadline.C:
			t.Fatalf("timed out waiting for payload attestation result %d", i)
		}
		if expectedValid[i] {
			require.NoError(t, err, "entry %d", i)
			continue
		}
		require.Error(t, err, "entry %d", i)
		require.NotErrorIs(t, err, ErrIgnore, "entry %d", i)
	}
	select {
	case size := <-firstBatchSize:
		require.Equal(t, count, size)
	case <-deadline.C:
		t.Fatal("timed out waiting for first payload attestation batch size")
	}
}

func TestBatchSignatureVerifierUsesPayloadAttestationFlushInterval(t *testing.T) {
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) {
		return true, nil
	}
	require.Equal(t, 20*time.Millisecond, payloadAttestationBatchCheckInterval)

	synctest.Test(t, func(t *testing.T) {
		type verificationResult struct {
			queue   string
			elapsed time.Duration
			err     error
		}

		ctx, cancel := context.WithCancel(t.Context())
		defer func() {
			cancel()
			synctest.Wait()
		}()
		verifier := NewBatchSignatureVerifier(ctx, nil)
		verifier.Start()
		startedAt := time.Now()
		results := make(chan verificationResult, 7)
		newData := func(callback func()) *AggregateVerificationData {
			return &AggregateVerificationData{
				Signatures: [][]byte{{1}},
				SignRoots:  [][]byte{{1}},
				Pks:        [][]byte{{1}},
				F:          callback,
			}
		}
		verify := func(queue string, verify func(context.Context, *AggregateVerificationData) error) {
			go func() {
				err := verify(ctx, newData(func() {}))
				results <- verificationResult{queue: queue, elapsed: time.Since(startedAt), err: err}
			}()
		}
		verifyAsync := func(queue string, verify func(*AggregateVerificationData)) {
			verify(newData(func() {
				results <- verificationResult{queue: queue, elapsed: time.Since(startedAt)}
			}))
		}

		verify("attestation", verifier.VerifyAttestation)
		verify("aggregate proof", verifier.VerifyAggregateProof)
		verify("bls to execution change", verifier.VerifyBlsToExecutionChange)
		verify("voluntary exit", verifier.VerifyVoluntaryExit)
		verify("payload attestation", verifier.VerifyPayloadAttestation)
		verifyAsync("sync contribution", verifier.AsyncVerifySyncContribution)
		verifyAsync("sync committee message", verifier.AsyncVerifySyncCommitteeMessage)
		synctest.Wait()

		time.Sleep(payloadAttestationBatchCheckInterval)
		synctest.Wait()
		require.Len(t, results, 1)
		payloadResult := <-results
		require.Equal(t, "payload attestation", payloadResult.queue)
		require.NoError(t, payloadResult.err)
		require.Equal(t, payloadAttestationBatchCheckInterval, payloadResult.elapsed)

		time.Sleep(batchCheckInterval - payloadAttestationBatchCheckInterval)
		synctest.Wait()
		require.Len(t, results, 6)
		elapsed := make(map[string]time.Duration, 7)
		for range 6 {
			result := <-results
			require.NoError(t, result.err, result.queue)
			require.NotContains(t, elapsed, result.queue)
			elapsed[result.queue] = result.elapsed
		}
		for _, queue := range []string{
			"attestation",
			"aggregate proof",
			"bls to execution change",
			"voluntary exit",
			"sync contribution",
			"sync committee message",
		} {
			require.Equal(t, batchCheckInterval, elapsed[queue], queue)
		}

		fullBatchResults := make(chan verificationResult, batchSignatureVerificationThreshold)
		fullBatchStartedAt := time.Now()
		for range batchSignatureVerificationThreshold {
			go func() {
				err := verifier.VerifyPayloadAttestation(ctx, newData(func() {}))
				fullBatchResults <- verificationResult{elapsed: time.Since(fullBatchStartedAt), err: err}
			}()
		}
		synctest.Wait()
		require.Len(t, fullBatchResults, batchSignatureVerificationThreshold)
		for range batchSignatureVerificationThreshold {
			result := <-fullBatchResults
			require.NoError(t, result.err)
			require.Zero(t, result.elapsed)
		}

		partialBatchStartedAt := time.Now()
		partialBatchResult := make(chan verificationResult, 1)
		go func() {
			err := verifier.VerifyPayloadAttestation(ctx, newData(func() {}))
			partialBatchResult <- verificationResult{elapsed: time.Since(partialBatchStartedAt), err: err}
		}()
		synctest.Wait()
		time.Sleep(payloadAttestationBatchCheckInterval)
		synctest.Wait()
		require.Len(t, partialBatchResult, 1)
		result := <-partialBatchResult
		require.NoError(t, result.err)
		require.Equal(t, payloadAttestationBatchCheckInterval, result.elapsed)
	})
}

func TestBatchSignatureVerifierContinuesAfterBlockingBan(t *testing.T) {
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) {
		if len(signatures) > 1 {
			return false, nil
		}
		return signatures[0][0] == 1, nil
	}

	banStarted := make(chan struct{})
	releaseBan := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-releaseBan:
		default:
			close(releaseBan)
		}
	})
	verifier := NewBatchSignatureVerifier(t.Context(), &blockingBanSentinel{
		started: banStarted,
		release: releaseBan,
	})

	verifier.AsyncVerifySyncCommitteeMessage(&AggregateVerificationData{
		Signatures:  [][]byte{{2}},
		SignRoots:   [][]byte{{2}},
		Pks:         [][]byte{{2}},
		F:           func() {},
		SendingPeer: &sentinelproto.Peer{Pid: "invalid-peer"},
	})
	verifier.Start()

	select {
	case <-banStarted:
	case <-time.After(time.Second):
		t.Fatal("peer ban did not start")
	}

	validResult := make(chan error, 1)
	go func() {
		validResult <- verifier.VerifyAttestation(t.Context(), &AggregateVerificationData{
			Signatures: [][]byte{{1}},
			SignRoots:  [][]byte{{1}},
			Pks:        [][]byte{{1}},
			F:          func() {},
		})
	}()
	select {
	case err := <-validResult:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("later batch waited for peer ban")
	}

	close(releaseBan)
}

func TestBatchSignatureVerifierBoundsBlockingBans(t *testing.T) {
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) {
		if len(signatures) > 1 {
			return false, nil
		}
		return signatures[0][0] == 1, nil
	}

	banStarted := make(chan struct{})
	releaseBan := make(chan struct{})
	t.Cleanup(func() { close(releaseBan) })
	verifier := NewBatchSignatureVerifier(t.Context(), &blockingBanSentinel{
		started: banStarted,
		release: releaseBan,
	})
	verifier.Start()

	queueInvalidBatch := func() {
		for range batchSignatureVerificationThreshold {
			verifier.AsyncVerifySyncCommitteeMessage(&AggregateVerificationData{
				Signatures:  [][]byte{{2}},
				SignRoots:   [][]byte{{2}},
				Pks:         [][]byte{{2}},
				F:           func() {},
				SendingPeer: &sentinelproto.Peer{Pid: "invalid-peer"},
			})
		}
	}

	queueInvalidBatch()
	select {
	case <-banStarted:
	case <-time.After(time.Second):
		t.Fatal("peer ban did not start")
	}
	baselineGoroutines := runtime.NumGoroutine()
	for range 319 {
		queueInvalidBatch()
	}

	validResult := make(chan error, 1)
	verifier.AsyncVerifySyncCommitteeMessage(&AggregateVerificationData{
		Signatures: [][]byte{{1}},
		SignRoots:  [][]byte{{1}},
		Pks:        [][]byte{{1}},
		F:          func() {},
		result:     validResult,
	})
	select {
	case err := <-validResult:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("later batch did not report its result")
	}

	require.LessOrEqual(t, runtime.NumGoroutine(), baselineGoroutines+4)
}

func TestBatchSignatureVerifierDoesNotBanWaitedEntry(t *testing.T) {
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) {
		return false, nil
	}

	banStarted := make(chan struct{})
	releaseBan := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-releaseBan:
		default:
			close(releaseBan)
		}
	})
	verifier := NewBatchSignatureVerifier(t.Context(), &blockingBanSentinel{
		started: banStarted,
		release: releaseBan,
	})
	verifier.Start()

	err := verifier.VerifyAttestation(t.Context(), &AggregateVerificationData{
		Signatures:  [][]byte{{2}},
		SignRoots:   [][]byte{{2}},
		Pks:         [][]byte{{2}},
		F:           func() {},
		SendingPeer: &sentinelproto.Peer{Pid: "invalid-peer"},
	})

	require.ErrorIs(t, err, ErrInvalidBlsSignature)
	select {
	case <-banStarted:
		t.Fatal("waited entry triggered verifier peer ban")
	case <-time.After(2 * batchCheckInterval):
	}
}

func TestBatchSignatureVerifierBansWhenWaiterCancelsAfterAdmission(t *testing.T) {
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) {
		return false, nil
	}

	banStarted := make(chan struct{})
	releaseBan := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-releaseBan:
		default:
			close(releaseBan)
		}
	})
	verifier := NewBatchSignatureVerifier(t.Context(), &blockingBanSentinel{
		started: banStarted,
		release: releaseBan,
	})

	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		result <- verifier.VerifyAttestation(ctx, &AggregateVerificationData{
			Signatures:  [][]byte{{2}},
			SignRoots:   [][]byte{{2}},
			Pks:         [][]byte{{2}},
			F:           func() {},
			SendingPeer: &sentinelproto.Peer{Pid: "invalid-peer"},
		})
	}()
	waitForQueuedVerification(t, verifier.attVerifyAndExecute, 1)
	cancel()
	require.ErrorIs(t, <-result, ErrIgnore)

	verifier.Start()
	select {
	case <-banStarted:
	case <-time.After(time.Second):
		t.Fatal("canceled waiter left invalid peer without a ban owner")
	}
	close(releaseBan)
}

func waitForQueuedVerification(t *testing.T, queue chan *AggregateVerificationData, want int) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for len(queue) != want {
		select {
		case <-deadline.C:
			t.Fatalf("got %d queued verification entries, want %d", len(queue), want)
		default:
			runtime.Gosched()
		}
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
