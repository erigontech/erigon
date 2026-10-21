// Package devvalidator implements an embedded validator client for dev mode.
// It uses the standard Beacon API (same endpoints as Lighthouse/Teku) to
// propose blocks and submit attestations. Intended for development and
// integration testing only — not for production use.
package devvalidator

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
)

// Service is the embedded dev validator. It runs as a goroutine inside
// Caplin, producing blocks and attestations for all configured validators.
type Service struct {
	client                *BeaconClient
	keys                  []*ValidatorKey
	keysByPub             map[common.Bytes48]*ValidatorKey
	cfg                   *clparams.BeaconChainConfig
	genesisValidatorsRoot common.Hash
	genesisTime           uint64
	logger                log.Logger
	cancel                context.CancelFunc
}

// NewService creates a dev validator service.
func NewService(beaconAPIURL string, seed string, validatorCount int,
	cfg *clparams.BeaconChainConfig, logger log.Logger) (*Service, error) {

	keys, err := LoadKeys(seed, validatorCount)
	if err != nil {
		return nil, fmt.Errorf("load dev validator keys: %w", err)
	}

	return &Service{
		client:    NewBeaconClient(beaconAPIURL),
		keys:      keys,
		keysByPub: PubKeyToKey(keys),
		cfg:       cfg,
		logger:    logger,
	}, nil
}

// Start begins the validator duty loop. It blocks until the context is cancelled.
func (s *Service) Start(ctx context.Context) {
	ctx, s.cancel = context.WithCancel(ctx)

	// Wait for the beacon node to be ready.
	s.waitForReady(ctx)
	if ctx.Err() != nil {
		return
	}

	// Resolve validator indices from the beacon state. The /genesis endpoint that
	// waitForReady polls answers BEFORE the beacon node has synced to head, so the
	// first /states/head/validators call can transiently return 503 "beacon node is
	// syncing" (or an empty state). Giving up here permanently killed one of the
	// per-chain dev-validators at random on a multi-chain boot, stalling that chain
	// at block 0. Retry until the node serves the state (or the context is
	// cancelled) — the failure is transient startup ordering, not a real error.
	if !s.resolveIndicesWithRetry(ctx) {
		return
	}

	s.anchorClockToHead(ctx)

	s.logger.Info("[dev-validator] started",
		"validators", len(s.keys),
		"slotsPerEpoch", s.cfg.SlotsPerEpoch,
		"secondsPerSlot", s.cfg.SecondsPerSlot,
	)

	// Main slot loop.
	s.slotLoop(ctx)
}

// Stop cancels the validator service.
func (s *Service) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
}

// waitForReady polls the beacon node until it responds, then fetches
// genesis time and validators root needed for signing.
func (s *Service) waitForReady(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		var genesis struct {
			GenesisTime           string `json:"genesis_time"`
			GenesisValidatorsRoot string `json:"genesis_validators_root"`
		}
		if err := s.client.get(ctx, "/eth/v1/beacon/genesis", &genesis); err == nil {
			gt, parseErr := strconv.ParseUint(genesis.GenesisTime, 10, 64)
			if parseErr != nil {
				s.logger.Warn("[dev-validator] invalid genesis_time", "value", genesis.GenesisTime, "err", parseErr)
				time.Sleep(time.Second)
				continue
			}
			s.genesisTime = gt
			root, err := hexutil.Decode(genesis.GenesisValidatorsRoot)
			if err == nil && len(root) == 32 {
				copy(s.genesisValidatorsRoot[:], root)
			}
			s.logger.Info("[dev-validator] beacon node ready",
				"genesisTime", s.genesisTime,
				"validatorsRoot", s.genesisValidatorsRoot.Hex(),
			)
			return
		}
		time.Sleep(time.Second)
	}
}

// anchorClockToHead re-anchors the slot clock to the chain head so a slow-booted sole producer
// resumes at head+1 rather than a wall-clock slot far past genesis (which would 404 proposer
// duties and deadlock at block 0). A boot already caught up to wall-clock is left untouched.
func (s *Service) anchorClockToHead(ctx context.Context) {
	secPerSlot := s.cfg.SecondsPerSlot
	if secPerSlot == 0 {
		return
	}
	headSlot, err := s.headSlot(ctx)
	if err != nil {
		// ⚠ A HEAD THE BEACON CANNOT NAME IS A CHAIN AT GENESIS — not a reason to give up, which is
		// what leaving the clock on wall-clock amounts to. Booting from a CUSTOM genesis STATE gives
		// Caplin a head root with no head BLOCK behind it, so /headers/head answers 404; this
		// function then returned, the clock stayed slots ahead of a chain at genesis, and proposer
		// duties 404ed on a dependent root that will never exist. That is precisely the deadlock
		// this function was written to prevent, reached through its own error path.
		//
		// So a 404 is treated as head slot 0 and the clock is anchored there. Any other error (the
		// beacon still starting, a 503, a refused connection) is transient and must NOT re-anchor —
		// a chain that really is at slot 500 would be dragged back to 1 and propose over itself.
		if !IsNotFound(err) {
			s.logger.Warn("[dev-validator] head slot unavailable; leaving the clock alone", "err", err)
			return
		}
		s.logger.Info("[dev-validator] beacon has no head block; anchoring the clock to genesis", "err", err)
		headSlot = 0
	}
	now := uint64(time.Now().Unix())
	if now < s.genesisTime {
		return
	}
	wallSlot := (now - s.genesisTime) / secPerSlot
	if wallSlot <= headSlot+1 {
		return
	}
	s.genesisTime = now - headSlot*secPerSlot
	// LOUD, because this is a compensation and not a fix. Re-anchoring shifts THIS validator's clock
	// and never shifts it back, so whatever gap it absorbs here becomes a permanent skew between the
	// validator and the beacon: every block is then produced that far after its own slot, the head
	// sits that far behind the wall clock for the life of the run, and everything downstream inherits
	// it — stage loop, state dumps, payload attributes, block production.
	//
	// It cost several rounds of debugging precisely because it announced itself at Info and looked
	// like housekeeping. On a chain that stamps its genesis when its consensus layer is READY this
	// cannot fire at boot, so if it does, the chain started late and that is the thing to fix.
	s.logger.Warn("[dev-validator] RE-ANCHORED the slot clock to the head — the chain started LATE and this validator is now permanently skewed by that much",
		"headSlot", headSlot, "wallSlotWas", wallSlot, "skewSeconds", wallSlot*secPerSlot,
		"genesisTime", s.genesisTime)
}

// headSlot returns the beacon chain head slot.
func (s *Service) headSlot(ctx context.Context) (uint64, error) {
	var resp struct {
		Header struct {
			Message struct {
				Slot string `json:"slot"`
			} `json:"message"`
		} `json:"header"`
	}
	if err := s.client.get(ctx, "/eth/v1/beacon/headers/head", &resp); err != nil {
		return 0, err
	}
	return strconv.ParseUint(resp.Header.Message.Slot, 10, 64)
}

// resolveIndicesWithRetry calls resolveIndices until it succeeds or the context
// is cancelled, polling every second. During a multi-chain boot the beacon node
// briefly reports "syncing" (503) on /states/head/validators even after /genesis
// is up, so a single attempt races startup; retrying makes the dev-validator
// start reliably instead of dying on the transient failure. Returns false only
// when the context is cancelled (shutdown).
func (s *Service) resolveIndicesWithRetry(ctx context.Context) bool {
	for attempt := 1; ; attempt++ {
		if err := s.resolveIndices(ctx); err == nil {
			return true
		} else if ctx.Err() != nil {
			return false
		} else {
			s.logger.Warn("[dev-validator] resolve indices not ready, retrying", "attempt", attempt, "err", err)
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(time.Second):
		}
	}
}

// resolveIndices maps pubkeys to on-chain validator indices.
func (s *Service) resolveIndices(ctx context.Context) error {
	type validatorEntry struct {
		Index     string `json:"index"`
		Validator struct {
			Pubkey string `json:"pubkey"`
		} `json:"validator"`
	}

	var validators []validatorEntry
	if err := s.client.get(ctx, "/eth/v1/beacon/states/head/validators", &validators); err != nil {
		return fmt.Errorf("get validators: %w", err)
	}

	resolved := 0
	for _, v := range validators {
		pubBytes, err := hexutil.Decode(v.Validator.Pubkey)
		if err != nil || len(pubBytes) != 48 {
			continue
		}
		var pub common.Bytes48
		copy(pub[:], pubBytes)
		if key, ok := s.keysByPub[pub]; ok {
			idx, parseErr := strconv.ParseUint(v.Index, 10, 64)
			if parseErr != nil {
				continue
			}
			key.ValidatorIndex = idx
			resolved++
		}
	}

	s.logger.Info("[dev-validator] resolved validator indices", "resolved", resolved, "total", len(s.keys))
	if resolved == 0 {
		return fmt.Errorf("no validators found in beacon state matching our keys")
	}
	return nil
}

// slotLoop runs once per slot, checking duties and performing them.
func (s *Service) slotLoop(ctx context.Context) {
	secPerSlot := s.cfg.SecondsPerSlot
	genesisTime := s.genesisTime
	slotDuration := time.Duration(secPerSlot) * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		genesis := time.Unix(int64(genesisTime), 0)
		if time.Now().Before(genesis) {
			time.Sleep(time.Until(genesis))
			continue
		}

		// Round to the nearest slot. The sleep to the next slot start runs on the monotonic clock but the
		// slot is read from the wall clock, so the wake can land just short of the boundary; truncating
		// would put it back in the slot just done and propose that slot again.
		currentSlot := uint64((time.Since(genesis) + slotDuration/2) / slotDuration)
		slotStart := time.Unix(int64(genesisTime+currentSlot*secPerSlot), 0)
		nextSlotStart := slotStart.Add(slotDuration)

		// ⚠ WHERE THE SLOT ACTUALLY GOES. This loop is single-threaded: if one pass takes longer than a
		// slot, the sleep below is skipped, currentSlot is recomputed from the wall clock, and every slot
		// it ran through is never proposed for. A missed slot is not a head update, so nothing re-emits
		// payload attributes and the execution layer keeps building under a slot that has already gone —
		// which is how its block comes to be asked for under attributes it never agreed to.
		//
		// Measured on live358, the execution layer was NOT what held this: post-assemble processing was
		// p50 3ms and the payload was already sealed when asked in 5,468 of 5,481 blocks, while the worst
		// hold before the first missed slot was 443ms. The remaining ~4.7s of a 5.5s pass was inside here
		// and had no timing on it at all. Now it does.
		passStart := time.Now()
		s.maybePropose(ctx, currentSlot)
		proposeMs := time.Since(passStart).Milliseconds()

		// Attest at 1/3, by which point the proposal is in — which is what the offset is for.
		attestTime := slotStart.Add(slotDuration / 3)
		if time.Now().Before(attestTime) {
			time.Sleep(time.Until(attestTime))
		}
		if ctx.Err() != nil {
			return
		}

		// Attest for all validators with duties at this slot.
		attestStart := time.Now()
		s.maybeAttest(ctx, currentSlot)
		attestMs := time.Since(attestStart).Milliseconds()

		// Submit sync committee messages for validators in the current committee.
		syncStart := time.Now()
		s.maybeSyncCommittee(ctx, currentSlot)
		syncMs := time.Since(syncStart).Milliseconds()

		if pass := time.Since(passStart); pass > slotDuration {
			s.logger.Warn("[dev-validator] slot pass ran past its slot — the next slots are dropped",
				"slot", currentSlot, "passMs", pass.Milliseconds(),
				"proposeMs", proposeMs, "attestMs", attestMs, "syncMs", syncMs,
				"slotMs", slotDuration.Milliseconds())
		}

		// Wait for next slot.
		if time.Now().Before(nextSlotStart) {
			time.Sleep(time.Until(nextSlotStart))
		}
	}
}

// maybePropose checks if any of our validators should propose at this slot
// and produces a block if so.
func (s *Service) maybePropose(ctx context.Context, slot uint64) {
	epoch := slot / s.cfg.SlotsPerEpoch

	// Get proposer duties for this epoch.
	type proposerDuty struct {
		Pubkey         string `json:"pubkey"`
		ValidatorIndex string `json:"validator_index"`
		Slot           string `json:"slot"`
	}
	var duties []proposerDuty
	path := fmt.Sprintf("/eth/v1/validator/duties/proposer/%d", epoch)
	dutiesStart := time.Now()
	if err := s.client.get(ctx, path, &duties); err != nil {
		s.logger.Warn("[dev-validator] duties skip", "slot", slot, "epoch", epoch, "spe", s.cfg.SlotsPerEpoch, "err", err)
		return
	}
	if d := time.Since(dutiesStart); d > 200*time.Millisecond {
		s.logger.Warn("[dev-validator] proposer duties fetch was slow", "slot", slot, "tookMs", d.Milliseconds())
	}

	for _, duty := range duties {
		dutySlot, parseErr := strconv.ParseUint(duty.Slot, 10, 64)
		if parseErr != nil {
			continue
		}
		if dutySlot != slot || slot == 0 {
			continue // skip genesis slot
		}

		pubBytes, err := hexutil.Decode(duty.Pubkey)
		if err != nil || len(pubBytes) != 48 {
			continue
		}
		var pub common.Bytes48
		copy(pub[:], pubBytes)
		key, ok := s.keysByPub[pub]
		if !ok {
			continue
		}

		s.logger.Debug("[dev-validator] proposing block", "slot", slot, "validator", key.ValidatorIndex)
		if err := s.proposeBlock(ctx, slot, key); err != nil {
			s.logger.Warn("[dev-validator] proposal failed", "slot", slot, "err", err)
		}
	}
}

// proposeBlock fetches a block template, signs it, and submits it.
func (s *Service) proposeBlock(ctx context.Context, slot uint64, key *ValidatorKey) error {
	epoch := slot / s.cfg.SlotsPerEpoch
	// ⚠ WHERE A PROPOSAL'S TIME GOES. maybePropose was measured at 94-97% of every slot pass that ran
	// past its slot — p50 7,248ms and max 11,113ms against a 2,000ms slot — while the execution layer's
	// share of it, the getPayload wait, was at most 3,687ms. The rest was in here with no timing on it.
	t0 := time.Now()

	// Compute RANDAO reveal.
	randaoReveal, err := signRandaoReveal(key, epoch, s.cfg, s.genesisValidatorsRoot)
	if err != nil {
		return fmt.Errorf("randao reveal: %w", err)
	}
	randaoMs := time.Since(t0).Milliseconds()

	// Get block template (unsigned BeaconBlock). Retry a few times if the EL
	// is busy with fork choice (semaphore contention in AssembleBlock).
	path := fmt.Sprintf("/eth/v3/validator/blocks/%d?randao_reveal=%s",
		slot, hexutil.Encode(randaoReveal[:]))

	tmplStart := time.Now()
	var blockResponse json.RawMessage
	var getErr error
	attempts := 0
	for attempt := 0; attempt < 5; attempt++ {
		attempts++
		getErr = s.client.get(ctx, path, &blockResponse)
		if getErr == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	templateMs := time.Since(tmplStart).Milliseconds()
	if getErr != nil {
		return fmt.Errorf("get block template: %w", getErr)
	}

	parseStart := time.Now()
	// Parse the block template. The v3 response is a DenebBeaconBlock
	// ({"block": {...}, "kzg_proofs": [...], "blobs": [...]}).
	version := s.cfg.GetCurrentStateVersion(epoch)
	var denebBlock struct {
		Block     json.RawMessage `json:"block"`
		KZGProofs json.RawMessage `json:"kzg_proofs"`
		Blobs     json.RawMessage `json:"blobs"`
	}
	if err := json.Unmarshal(blockResponse, &denebBlock); err != nil {
		return fmt.Errorf("parse deneb block wrapper: %w", err)
	}
	// The inner "block" is a BeaconBlock.
	blockJSON := denebBlock.Block
	if len(blockJSON) == 0 {
		// Pre-Deneb or non-wrapped response: use the raw response directly.
		blockJSON = blockResponse
	}
	block := cltypes.NewSignedBeaconBlock(s.cfg, version)
	if err := json.Unmarshal(blockJSON, block.Block); err != nil {
		return fmt.Errorf("parse block template: %w", err)
	}

	// Ensure execution payload sub-fields are initialized (the JSON response
	// may leave some nil which causes HashSSZ to panic).
	if block.Block.Body.ExecutionPayload != nil {
		if block.Block.Body.ExecutionPayload.Extra == nil {
			block.Block.Body.ExecutionPayload.Extra = solid.NewExtraData()
		}
		if block.Block.Body.ExecutionPayload.Transactions == nil {
			block.Block.Body.ExecutionPayload.Transactions = &solid.TransactionsSSZ{}
		}
		if version >= clparams.CapellaVersion && block.Block.Body.ExecutionPayload.Withdrawals == nil {
			block.Block.Body.ExecutionPayload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(s.cfg.MaxWithdrawalsPerPayload), 44)
		}
	}

	parseMs := time.Since(parseStart).Milliseconds()

	// Sign the block.
	signStart := time.Now()
	sig, err := signBlock(key, block.Block, slot, s.cfg, s.genesisValidatorsRoot)
	if err != nil {
		return fmt.Errorf("sign block: %w", err)
	}
	block.Signature = sig
	signMs := time.Since(signStart).Milliseconds()

	// Submit the signed block. For Deneb+, wrap in DenebSignedBeaconBlock
	// with empty blob sidecars.
	versionStr := version.String()
	var submitBody interface{} = block
	if version >= clparams.DenebVersion {
		submitBody = &cltypes.DenebSignedBeaconBlock{
			SignedBlock: block,
			KZGProofs:   solid.NewStaticListSSZ[*cltypes.KZGProof](cltypes.MaxBlobsCommittmentsPerBlock*int(s.cfg.NumberOfColumns), cltypes.BYTES_KZG_PROOF),
			Blobs:       solid.NewStaticListSSZ[*cltypes.Blob](cltypes.MaxBlobsCommittmentsPerBlock, int(cltypes.BYTES_PER_BLOB)),
		}
	}
	pubStart := time.Now()
	if err := s.client.postJSON(ctx, "/eth/v2/beacon/blocks", submitBody, versionStr); err != nil {
		return fmt.Errorf("submit block: %w", err)
	}
	publishMs := time.Since(pubStart).Milliseconds()

	if total := time.Since(t0); total > 1500*time.Millisecond {
		s.logger.Warn("[dev-validator] proposal phases", "slot", slot, "totalMs", total.Milliseconds(),
			"randaoMs", randaoMs, "templateMs", templateMs, "templateAttempts", attempts,
			"parseMs", parseMs, "signMs", signMs, "publishMs", publishMs)
	}

	s.logger.Debug("[dev-validator] proposed block", "slot", slot, "validator", key.ValidatorIndex)
	return nil
}

// maybeAttest submits attestations for validators with duties at this slot.
func (s *Service) maybeAttest(ctx context.Context, slot uint64) {
	epoch := slot / s.cfg.SlotsPerEpoch
	version := s.cfg.GetCurrentStateVersion(epoch)

	// Get attester duties for this epoch.
	type attesterDuty struct {
		Pubkey                  string `json:"pubkey"`
		ValidatorIndex          string `json:"validator_index"`
		Slot                    string `json:"slot"`
		CommitteeIndex          string `json:"committee_index"`
		CommitteeLength         string `json:"committee_length"`
		ValidatorCommitteeIndex string `json:"validator_committee_index"`
	}

	// POST attester duties with our validator indices.
	indices := make([]string, 0, len(s.keys))
	for _, k := range s.keys {
		indices = append(indices, fmt.Sprintf("%d", k.ValidatorIndex))
	}

	var duties []attesterDuty
	path := fmt.Sprintf("/eth/v1/validator/duties/attester/%d", epoch)
	// Attester duties is POST-only per the Beacon API spec (the request body
	// carries the validator index list). Use postAndDecode to send the indices
	// and parse the response in a single round-trip.
	dutiesStart := time.Now()
	if err := s.client.postAndDecode(ctx, path, indices, &duties); err != nil {
		return
	}
	dutiesMs := time.Since(dutiesStart).Milliseconds()

	// WHERE THE ATTEST ACTUALLY GOES. Every duty is served one at a time, and each one costs a GET for
	// attestation data, a POST to submit, and on Electra a second POST to aggregate. The slot loop is
	// single-threaded, so this is subtracted from the next slot's proposal.
	var attDataMs, signMs, submitMs, aggMs int64
	dutiesAtSlot := 0

	attested := 0
	for _, duty := range duties {
		dutySlot, parseErr := strconv.ParseUint(duty.Slot, 10, 64)
		if parseErr != nil {
			continue
		}
		if dutySlot != slot {
			continue
		}

		pubBytes, err := hexutil.Decode(duty.Pubkey)
		if err != nil || len(pubBytes) != 48 {
			continue
		}
		var pub common.Bytes48
		copy(pub[:], pubBytes)
		key, ok := s.keysByPub[pub]
		if !ok {
			continue
		}

		committeeIndex, parseErr := strconv.ParseUint(duty.CommitteeIndex, 10, 64)
		if parseErr != nil {
			continue
		}

		// Get attestation data for this slot + committee.
		attPath := fmt.Sprintf("/eth/v1/validator/attestation_data?slot=%d&committee_index=%d",
			slot, committeeIndex)

		dutiesAtSlot++
		attDataStart := time.Now()
		var attData solid.AttestationData
		if err := s.client.get(ctx, attPath, &attData); err != nil {
			continue
		}
		attDataMs += time.Since(attDataStart).Milliseconds()

		// Sign the attestation data.
		signStart := time.Now()
		sig, err := signAttestation(key, &attData, slot, s.cfg, s.genesisValidatorsRoot)
		signMs += time.Since(signStart).Milliseconds()
		if err != nil {
			s.logger.Warn("[dev-validator] attestation sign failed", "err", err)
			continue
		}

		committeeLength, parseErr := strconv.ParseUint(duty.CommitteeLength, 10, 64)
		if parseErr != nil {
			continue
		}
		validatorPosition, parseErr := strconv.ParseUint(duty.ValidatorCommitteeIndex, 10, 64)
		if parseErr != nil {
			continue
		}

		sub := buildAttestationSubmission(version, committeeIndex, key.ValidatorIndex, &attData, sig, committeeLength, validatorPosition)
		submitStart := time.Now()
		var submitErr error
		if sub.version != "" {
			submitErr = s.client.postJSON(ctx, sub.path, sub.body, sub.version)
		} else {
			submitErr = s.client.post(ctx, sub.path, sub.body)
		}
		submitMs += time.Since(submitStart).Milliseconds()
		if submitErr != nil {
			s.logger.Debug("[dev-validator] attestation submit failed", "slot", slot, "err", submitErr)
			continue
		}
		attested++

		if version >= clparams.ElectraVersion {
			aggStart := time.Now()
			s.submitAggregateAndProof(ctx, slot, committeeIndex, key, &attData, sig, committeeLength, validatorPosition)
			aggMs += time.Since(aggStart).Milliseconds()
		}
	}

	// Idle this is ~40ms and says nothing; under load it reaches 765ms and is the slot overrun. Only
	// the loaded case is news, so only that is reported.
	if total := dutiesMs + attDataMs + signMs + submitMs + aggMs; total > 200 {
		s.logger.Warn("[attest-phases] the attest took a share of the slot", "slot", slot,
			"totalMs", total, "dutiesAtSlot", dutiesAtSlot, "attested", attested,
			"dutiesMs", dutiesMs, "attDataMs", attDataMs, "signMs", signMs,
			"submitMs", submitMs, "aggMs", aggMs)
	}
}
