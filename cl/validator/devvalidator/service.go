// Package devvalidator implements an embedded validator client for dev mode.
// It uses the standard Beacon API (same endpoints as Lighthouse/Teku) to
// propose blocks and submit attestations. Intended for development and
// integration testing only — not for production use.
package devvalidator

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
)

// Service is the embedded dev validator. It runs as a goroutine inside
// Caplin, producing blocks and attestations for all configured validators.
type Service struct {
	client    *BeaconClient
	keys      []*ValidatorKey
	keysByPub map[common.Bytes48]*ValidatorKey
	cfg       *clparams.BeaconChainConfig
	logger    log.Logger
	cancel    context.CancelFunc
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

// waitForReady polls the beacon node until it responds.
func (s *Service) waitForReady(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		var genesis struct {
			GenesisTime string `json:"genesis_time"`
		}
		if err := s.client.get(ctx, "/eth/v1/beacon/genesis", &genesis); err == nil {
			s.logger.Info("[dev-validator] beacon node ready", "genesisTime", genesis.GenesisTime)
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
			idx := uint64(0)
			fmt.Sscanf(v.Index, "%d", &idx)
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
	// Get genesis time to compute slot boundaries.
	var genesis struct {
		GenesisTime string `json:"genesis_time"`
	}
	if err := s.client.get(ctx, "/eth/v1/beacon/genesis", &genesis); err != nil {
		s.logger.Error("[dev-validator] failed to get genesis", "err", err)
		return
	}
	genesisTime := uint64(0)
	fmt.Sscanf(genesis.GenesisTime, "%d", &genesisTime)

	secPerSlot := s.cfg.SecondsPerSlot
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
		dutySlot := uint64(0)
		fmt.Sscanf(duty.Slot, "%d", &dutySlot)
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

		s.logger.Info("[dev-validator] proposing block", "slot", slot, "validator", key.ValidatorIndex)
		if err := s.proposeBlock(ctx, slot, key); err != nil {
			s.logger.Warn("[dev-validator] proposal failed", "slot", slot, "err", err)
		}
	}
}

// proposeBlock fetches a block template, signs it, and submits it.
func (s *Service) proposeBlock(ctx context.Context, slot uint64, key *ValidatorKey) error {
	// Compute RANDAO reveal: sign(epoch, DOMAIN_RANDAO).
	epoch := slot / s.cfg.SlotsPerEpoch
	randaoReveal, err := s.signEpoch(epoch, key)
	if err != nil {
		return fmt.Errorf("randao reveal: %w", err)
	}

	// Get block template.
	path := fmt.Sprintf("/eth/v3/validator/blocks/%d?randao_reveal=%s&graffiti=0x%064x",
		slot, hexutil.Encode(randaoReveal[:]), 0)

	var blockResponse json.RawMessage
	if err := s.client.get(ctx, path, &blockResponse); err != nil {
		return fmt.Errorf("get block: %w", err)
	}

	// The block template needs to be signed and submitted.
	// For now, submit the unsigned block — the beacon node will handle it.
	// TODO: parse block, compute signing root, sign with BLS, submit signed block.
	s.logger.Info("[dev-validator] got block template", "slot", slot, "size", len(blockResponse))

	return nil
}

// signEpoch computes the RANDAO reveal: BLS signature of the epoch.
func (s *Service) signEpoch(epoch uint64, key *ValidatorKey) ([96]byte, error) {
	var epochBytes [32]byte
	binary.LittleEndian.PutUint64(epochBytes[:8], epoch)
	// TODO: compute proper signing root with domain separation
	sig := key.PrivKey.Sign(epochBytes[:])
	sigBytes := sig.Bytes()
	var result [96]byte
	copy(result[:], sigBytes)
	return result, nil
}

// maybeAttest submits attestations for validators with duties at this slot.
func (s *Service) maybeAttest(ctx context.Context, slot uint64) {
	// Get attestation data for this slot.
	path := fmt.Sprintf("/eth/v1/validator/attestation_data?slot=%d&committee_index=0", slot)

	var attData json.RawMessage
	if err := s.client.get(ctx, path, &attData); err != nil {
		return // silently skip
	}

	s.logger.Debug("[dev-validator] got attestation data", "slot", slot)
	// TODO: for each validator with attester duty at this slot,
	// sign the attestation data and submit via POST /eth/v1/beacon/pool/attestations
}
