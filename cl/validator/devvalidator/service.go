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

	// Resolve validator indices from the beacon state.
	if err := s.resolveIndices(ctx); err != nil {
		s.logger.Error("[dev-validator] failed to resolve indices", "err", err)
		return
	}

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

	var blockResponse json.RawMessage
	var getErr error
	for attempt := 0; attempt < 5; attempt++ {
		getErr = s.client.get(ctx, path, &blockResponse)
		if getErr == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if getErr != nil {
		return fmt.Errorf("get block template: %w", getErr)
	}

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

	// Sign the block.
	sig, err := signBlock(key, block.Block, slot, s.cfg, s.genesisValidatorsRoot)
	if err != nil {
		return fmt.Errorf("sign block: %w", err)
	}
	block.Signature = sig

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
	if err := s.client.postJSON(ctx, "/eth/v2/beacon/blocks", submitBody, versionStr); err != nil {
		return fmt.Errorf("submit block: %w", err)
	}

	s.logger.Info("[dev-validator] proposed block", "slot", slot, "validator", key.ValidatorIndex)
	return nil
}

// maybeAttest submits attestations for validators with duties at this slot.
func (s *Service) maybeAttest(ctx context.Context, slot uint64) {
	epoch := slot / s.cfg.SlotsPerEpoch

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

		// Build aggregation bits — set our bit position.
		committeeLength, parseErr := strconv.ParseUint(duty.CommitteeLength, 10, 64)
		if parseErr != nil {
			continue
		}
		validatorPosition, parseErr := strconv.ParseUint(duty.ValidatorCommitteeIndex, 10, 64)
		if parseErr != nil {
			continue
		}

		aggBitsLen := (committeeLength + 7) / 8
		aggBits := make([]byte, aggBitsLen)
		aggBits[validatorPosition/8] |= 1 << (validatorPosition % 8)

		// Submit attestation.
		attestation := map[string]interface{}{
			"aggregation_bits": hexutil.Encode(aggBits),
			"data":             &attData,
			"signature":        hexutil.Encode(sig[:]),
		}
		if err := s.client.post(ctx, "/eth/v1/beacon/pool/attestations", []interface{}{attestation}); err != nil {
			s.logger.Debug("[dev-validator] attestation submit failed", "slot", slot, "err", err)
			continue
		}
		attested++
	}

	if attested > 0 {
		s.logger.Debug("[dev-validator] attested", "slot", slot, "count", attested)
	}
}
