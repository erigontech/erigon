// Package devvalidator implements an embedded validator client for dev mode.
// It uses the standard Beacon API (same endpoints as Lighthouse/Teku) to
// propose blocks and submit attestations. Intended for development and
// integration testing only — not for production use.
package devvalidator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
)

const envelopeSubmissionRetryInterval = 500 * time.Millisecond

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
	lifecycleMu           sync.Mutex
	lifecycleCtx          context.Context
	cancel                context.CancelFunc
	envelopeSubmissions   sync.Map
}

// NewService creates a dev validator service.
func NewService(beaconAPIURL string, seed string, validatorCount int,
	cfg *clparams.BeaconChainConfig, logger log.Logger,
) (*Service, error) {
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
	ctx = s.beginLifecycle(ctx)
	defer s.Stop()

	// Wait for the beacon node to be ready.
	s.waitForReady(ctx)
	if ctx.Err() != nil {
		return
	}

	// Resolve validator indices from the beacon state.
	if err := s.resolveIndices(ctx); err != nil {
		s.logger.Error("[dev-validator] failed to resolve indices", "err", err)
		return
	}

	s.logger.Info(
		"[dev-validator] started",
		"validators", len(s.keys),
		"slotsPerEpoch", s.cfg.SlotsPerEpoch,
		"secondsPerSlot", s.cfg.SecondsPerSlot,
	)

	// Main slot loop.
	s.slotLoop(ctx)
}

// Stop cancels the validator service.
func (s *Service) Stop() {
	s.lifecycleMu.Lock()
	cancel := s.cancel
	s.lifecycleMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *Service) beginLifecycle(parent context.Context) context.Context {
	ctx, cancel := context.WithCancel(parent)
	s.lifecycleMu.Lock()
	previous := s.cancel
	s.lifecycleCtx = ctx
	s.cancel = cancel
	s.lifecycleMu.Unlock()
	if previous != nil {
		previous()
	}
	return ctx
}

func (s *Service) retryOwnerContext() context.Context {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.lifecycleCtx == nil {
		s.lifecycleCtx, s.cancel = context.WithCancel(context.Background())
	}
	return s.lifecycleCtx
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
			s.logger.Info(
				"[dev-validator] beacon node ready",
				"genesisTime", s.genesisTime,
				"validatorsRoot", s.genesisValidatorsRoot.Hex(),
			)
			return
		}
		time.Sleep(time.Second)
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

		now := uint64(time.Now().Unix())
		if now < genesisTime {
			time.Sleep(time.Until(time.Unix(int64(genesisTime), 0)))
			continue
		}

		currentSlot := (now - genesisTime) / secPerSlot
		slotStart := time.Unix(int64(genesisTime+currentSlot*secPerSlot), 0)
		nextSlotStart := slotStart.Add(slotDuration)

		// Wait until 1/3 into the slot for proposals, then attest.
		proposalTime := slotStart.Add(slotDuration / 3)
		if time.Now().Before(proposalTime) {
			time.Sleep(time.Until(proposalTime))
		}

		if ctx.Err() != nil {
			return
		}

		// Propose if we have a duty for this slot.
		s.maybePropose(ctx, currentSlot)

		// Attest for all validators with duties at this slot.
		s.maybeAttest(ctx, currentSlot)

		// Submit sync committee messages for validators in the current committee.
		s.maybeSyncCommittee(ctx, currentSlot)

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
	if err := s.client.get(ctx, path, &duties); err != nil {
		return // silently skip — node may not be ready
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

		s.logger.Info("[dev-validator] proposing block", "slot", slot, "validator", key.ValidatorIndex)
		if err := s.proposeBlock(ctx, slot, key); err != nil {
			s.logger.Warn("[dev-validator] proposal failed", "slot", slot, "err", err)
		}
	}
}

// proposeBlock fetches a block template, signs it, and submits it.
func (s *Service) proposeBlock(ctx context.Context, slot uint64, key *ValidatorKey) error {
	epoch := slot / s.cfg.SlotsPerEpoch

	// Compute RANDAO reveal.
	randaoReveal, err := signRandaoReveal(key, epoch, s.cfg, s.genesisValidatorsRoot)
	if err != nil {
		return fmt.Errorf("randao reveal: %w", err)
	}

	// Get block template (unsigned BeaconBlock). Retry a few times if the EL
	// is busy with fork choice (semaphore contention in AssembleBlock).
	path := fmt.Sprintf("/eth/v3/validator/blocks/%d?randao_reveal=%s",
		slot, hexutil.Encode(randaoReveal[:]))

	var response *beaconResponse
	var getErr error
	for range 5 {
		response, getErr = s.client.getResponse(ctx, path)
		if getErr == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if getErr != nil {
		return fmt.Errorf("get block template: %w", getErr)
	}

	blockResponse := response.Data

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

	if block.Block.Body == nil {
		return fmt.Errorf("block template body is missing")
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

	var signedEnvelope *cltypes.SignedExecutionPayloadEnvelope
	if version >= clparams.GloasVersion {
		if block.Block.Slot != slot {
			return fmt.Errorf("block template slot %d does not match requested slot %d", block.Block.Slot, slot)
		}
		signedEnvelope, err = s.signExecutionPayloadEnvelope(block.Block, response.ExecutionPayloadEnvelope, key)
		if err != nil {
			return err
		}
	}

	// Sign the block.
	sig, err := signBlock(key, block.Block, slot, s.cfg, s.genesisValidatorsRoot)
	if err != nil {
		return fmt.Errorf("sign block: %w", err)
	}
	block.Signature = sig

	versionStr := version.String()
	var submitBody any = block
	if version >= clparams.DenebVersion && version < clparams.GloasVersion {
		submitBody = &cltypes.DenebSignedBeaconBlock{
			SignedBlock: block,
			KZGProofs:   solid.NewStaticListSSZ[*cltypes.KZGProof](cltypes.MaxBlobsCommittmentsPerBlock*int(s.cfg.NumberOfColumns), cltypes.BYTES_KZG_PROOF),
			Blobs:       solid.NewStaticListSSZ[*cltypes.Blob](cltypes.MaxBlobsCommittmentsPerBlock, int(cltypes.BYTES_PER_BLOB)),
		}
	}
	if err := s.client.postJSON(ctx, "/eth/v2/beacon/blocks", submitBody, versionStr); err != nil {
		return fmt.Errorf("submit block: %w", err)
	}

	if signedEnvelope != nil {
		headers := http.Header{
			"Eth-Consensus-Version":  {versionStr},
			"Eth-Blob-Data-Included": {"false"},
		}
		s.submitExecutionPayloadEnvelope(ctx, slot, signedEnvelope, headers)
	}

	s.logger.Info("[dev-validator] proposed block", "slot", slot, "validator", key.ValidatorIndex)
	return nil
}

func (s *Service) submitExecutionPayloadEnvelope(
	ctx context.Context,
	slot uint64,
	envelope *cltypes.SignedExecutionPayloadEnvelope,
	headers http.Header,
) {
	root := common.Hash(envelope.Message.BeaconBlockRoot)
	if _, loaded := s.envelopeSubmissions.LoadOrStore(root, struct{}{}); loaded {
		return
	}
	deadline := s.executionPayloadEnvelopeDeadline(slot)
	initialCtx, cancel := context.WithDeadline(ctx, deadline)
	err := s.client.postJSONWithHeaders(initialCtx, "/eth/v1/beacon/execution_payload_envelopes", envelope, headers)
	cancel()
	if err == nil {
		s.expireEnvelopeSubmission(root, deadline)
		return
	}
	if !time.Now().Before(deadline) {
		s.envelopeSubmissions.Delete(root)
		return
	}
	retryOwner := s.retryOwnerContext()
	go func() {
		retryCtx, cancel := context.WithDeadline(retryOwner, deadline)
		defer cancel()
		for {
			timer := time.NewTimer(min(envelopeSubmissionRetryInterval, time.Until(deadline)))
			select {
			case <-timer.C:
			case <-retryCtx.Done():
				timer.Stop()
				s.envelopeSubmissions.Delete(root)
				return
			}
			if retryCtx.Err() != nil {
				s.envelopeSubmissions.Delete(root)
				return
			}
			if err := s.client.postJSONWithHeaders(retryCtx, "/eth/v1/beacon/execution_payload_envelopes", envelope, headers); err == nil {
				s.expireEnvelopeSubmission(root, deadline)
				return
			}
			if retryCtx.Err() != nil {
				s.logger.Warn("[dev-validator] failed to submit execution payload envelope", "slot", slot)
				s.envelopeSubmissions.Delete(root)
				return
			}
		}
	}()
}

func (s *Service) executionPayloadEnvelopeDeadline(slot uint64) time.Time {
	slotDuration := time.Duration(s.cfg.SecondsPerSlot) * time.Second
	slotStart := time.Unix(int64(s.genesisTime), 0).Add(time.Duration(slot) * slotDuration)
	return slotStart.Add(slotDuration * time.Duration(s.cfg.PayloadDueBps) / time.Duration(clparams.BpsFactor))
}

func (s *Service) expireEnvelopeSubmission(root common.Hash, deadline time.Time) {
	delay := time.Until(deadline)
	if delay <= 0 {
		s.envelopeSubmissions.Delete(root)
		return
	}
	time.AfterFunc(delay, func() { s.envelopeSubmissions.Delete(root) })
}

func (s *Service) signExecutionPayloadEnvelope(block *cltypes.BeaconBlock, data json.RawMessage, key *ValidatorKey) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	if block.Body.SignedExecutionPayloadBid == nil || block.Body.SignedExecutionPayloadBid.Message == nil {
		return nil, fmt.Errorf("block template execution payload bid is missing")
	}
	bid := block.Body.SignedExecutionPayloadBid.Message
	if bid.BuilderIndex != clparams.BuilderIndexSelfBuild {
		if len(data) != 0 {
			return nil, fmt.Errorf("external builder template includes an unsigned execution payload envelope")
		}
		return nil, nil
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("self-build template execution payload envelope is missing")
	}
	envelope := cltypes.NewExecutionPayloadEnvelope(s.cfg)
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("parse execution payload envelope: %w", err)
	}
	if envelope == nil || envelope.Payload == nil || envelope.ExecutionRequests == nil {
		return nil, fmt.Errorf("execution payload envelope is incomplete")
	}
	if withdrawals := envelope.Payload.Withdrawals; withdrawals != nil {
		for i := 0; i < withdrawals.Len(); i++ {
			if withdrawals.Get(i) == nil {
				return nil, fmt.Errorf("execution payload withdrawal %d is null", i)
			}
		}
	}
	blockRoot, err := block.HashSSZ()
	if err != nil {
		return nil, fmt.Errorf("hash block template: %w", err)
	}
	if envelope.BeaconBlockRoot != blockRoot || envelope.ParentBeaconBlockRoot != block.ParentRoot ||
		envelope.BuilderIndex != bid.BuilderIndex || envelope.Payload.BlockHash != bid.BlockHash ||
		envelope.Payload.SlotNumber != block.Slot {
		return nil, fmt.Errorf("execution payload envelope does not match block template")
	}
	requestsRoot, err := envelope.ExecutionRequests.HashSSZ()
	if err != nil {
		return nil, fmt.Errorf("hash execution requests: %w", err)
	}
	if requestsRoot != bid.ExecutionRequestsRoot {
		return nil, fmt.Errorf("execution requests do not match block template")
	}
	epoch := block.Slot / s.cfg.SlotsPerEpoch
	signature, err := signObject(key, envelope, s.cfg.DomainBeaconBuilder, epoch, s.cfg, s.genesisValidatorsRoot)
	if err != nil {
		return nil, fmt.Errorf("sign execution payload envelope: %w", err)
	}
	return &cltypes.SignedExecutionPayloadEnvelope{Message: envelope, Signature: signature}, nil
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
	if err := s.client.postAndDecode(ctx, path, indices, &duties); err != nil {
		return
	}

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

		var attData solid.AttestationData
		if err := s.client.get(ctx, attPath, &attData); err != nil {
			continue
		}

		// Sign the attestation data.
		sig, err := signAttestation(key, &attData, slot, s.cfg, s.genesisValidatorsRoot)
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
		var submitErr error
		if sub.version != "" {
			submitErr = s.client.postJSON(ctx, sub.path, sub.body, sub.version)
		} else {
			submitErr = s.client.post(ctx, sub.path, sub.body)
		}
		if submitErr != nil {
			s.logger.Debug("[dev-validator] attestation submit failed", "slot", slot, "err", submitErr)
			continue
		}
		attested++

		if version >= clparams.ElectraVersion {
			s.submitAggregateAndProof(ctx, slot, committeeIndex, key, &attData, sig, committeeLength, validatorPosition)
		}
	}

	if attested > 0 {
		s.logger.Debug("[dev-validator] attested", "slot", slot, "count", attested)
	}
}
