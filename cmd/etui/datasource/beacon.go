package datasource

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/erigontech/erigon/cmd/etui/config"
)

var ErrNoValidatorKeysConfigured = errors.New("no validator keys configured")

const (
	defaultSlotsPerEpoch   = 32
	defaultSecondsPerSlot  = 12
	proposerSearchEpochs   = 32
	validatorWalkMaxDepth  = 3
	maxErrorResponseLength = 256
)

type BeaconClient struct {
	BaseURL string
	DataDir string
	Client  *http.Client

	mu             sync.Mutex
	cachedSpec     beaconSpec
	specLoaded     bool
	cachedPerfKey  string
	cachedPerf     ValidatorPerformance
	cachedPropKey  string
	cachedPropDuty *ValidatorDuty
	cachedPropSlot uint64
}

type BeaconSyncStatus struct {
	HeadSlot     uint64
	SyncDistance uint64
	CurrentSlot  uint64
	IsSyncing    bool
	IsOptimistic bool
	ELOffline    bool
}

type ValidatorDuty struct {
	ValidatorIndex uint64
	Slot           uint64
}

type ValidatorDuties struct {
	NextAttestation *ValidatorDuty
	NextProposer    *ValidatorDuty
	CurrentSlot     uint64
	SecondsPerSlot  uint64
	Error           string
}

type ValidatorPerformance struct {
	AttestationPct float64
	Missed         int
	InclusionP50   int
	InclusionP95   int
	Error          string
}

type SlashGuardStatus string

const (
	SlashGuardOK      SlashGuardStatus = "OK"
	SlashGuardWarning SlashGuardStatus = "WARNING"
)

type ValidatorSnapshot struct {
	HasKeys              bool
	ValidatorCount       int
	ActiveValidatorCount int
	SlashGuard           SlashGuardStatus
	Duties               ValidatorDuties
	Performance          ValidatorPerformance
}

type beaconSpec struct {
	SlotsPerEpoch  uint64
	SecondsPerSlot uint64
}

type beaconEnvelope[T any] struct {
	Data T `json:"data"`
}

type beaconSyncingResponse struct {
	HeadSlot     string `json:"head_slot"`
	SyncDistance string `json:"sync_distance"`
	IsSyncing    bool   `json:"is_syncing"`
	IsOptimistic bool   `json:"is_optimistic"`
	ELOffline    bool   `json:"el_offline"`
}

type beaconValidatorResponse struct {
	Index     string `json:"index"`
	Status    string `json:"status"`
	Validator struct {
		Pubkey  string `json:"pubkey"`
		Slashed bool   `json:"slashed"`
	} `json:"validator"`
}

type dutyResponse struct {
	ValidatorIndex string `json:"validator_index"`
	Slot           string `json:"slot"`
}

type attestationRewardsData struct {
	TotalRewards []struct {
		ValidatorIndex string `json:"validator_index"`
		Target         string `json:"target"`
		InclusionDelay string `json:"inclusion_delay"`
	} `json:"total_rewards"`
}

type discoveredValidator struct {
	Index   uint64
	Status  string
	Pubkey  string
	Slashed bool
}

type apiStatusError struct {
	Method string
	Path   string
	Code   int
	Body   string
}

func (e *apiStatusError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("%s %s: unexpected status %d", e.Method, e.Path, e.Code)
	}
	return fmt.Sprintf("%s %s: unexpected status %d: %s", e.Method, e.Path, e.Code, e.Body)
}

func NewBeaconClient(datadir string) *BeaconClient {
	return &BeaconClient{
		BaseURL: config.DefaultBeaconURL,
		DataDir: datadir,
		Client:  &http.Client{Timeout: 5 * time.Second},
	}
}

func (c *BeaconClient) GetValidatorSnapshot(ctx context.Context) ValidatorSnapshot {
	pubkeys, err := c.discoverValidatorPubkeys()
	if err != nil || len(pubkeys) == 0 {
		return ValidatorSnapshot{SlashGuard: SlashGuardOK}
	}

	snapshot := ValidatorSnapshot{
		HasKeys:        true,
		ValidatorCount: len(pubkeys),
		SlashGuard:     SlashGuardOK,
	}

	spec := c.getSpec(ctx)
	syncStatus, syncErr := c.GetSyncStatus(ctx)
	if syncErr != nil {
		snapshot.SlashGuard = SlashGuardWarning
		snapshot.Duties = ValidatorDuties{
			SecondsPerSlot: spec.SecondsPerSlot,
			Error:          syncErr.Error(),
		}
		snapshot.Performance = ValidatorPerformance{Error: syncErr.Error()}
		return snapshot
	}

	validators, err := c.lookupValidators(ctx, pubkeys)
	if err != nil {
		snapshot.SlashGuard = SlashGuardWarning
		snapshot.Duties = ValidatorDuties{
			CurrentSlot:    syncStatus.CurrentSlot,
			SecondsPerSlot: spec.SecondsPerSlot,
			Error:          err.Error(),
		}
		snapshot.Performance = ValidatorPerformance{Error: err.Error()}
		return snapshot
	}

	snapshot.ValidatorCount = len(validators)
	activeValidators := filterActiveValidators(validators)
	snapshot.ActiveValidatorCount = len(activeValidators)
	if syncStatus.IsSyncing || len(activeValidators) == 0 {
		snapshot.SlashGuard = SlashGuardWarning
	}
	for _, validator := range validators {
		if validator.Slashed || strings.Contains(validator.Status, "slashed") {
			snapshot.SlashGuard = SlashGuardWarning
			break
		}
	}

	duties, err := c.getValidatorDuties(ctx, spec, syncStatus, activeValidators)
	if err != nil {
		snapshot.SlashGuard = SlashGuardWarning
		duties.Error = err.Error()
	}
	snapshot.Duties = duties

	performance, err := c.getValidatorPerformance(ctx, spec, syncStatus, activeValidators)
	if err != nil {
		snapshot.SlashGuard = SlashGuardWarning
		performance.Error = err.Error()
	}
	snapshot.Performance = performance

	return snapshot
}

func (c *BeaconClient) GetSyncStatus(ctx context.Context) (BeaconSyncStatus, error) {
	var resp beaconEnvelope[beaconSyncingResponse]
	if err := c.doJSON(ctx, http.MethodGet, "/eth/v1/node/syncing", nil, nil, &resp); err != nil {
		return BeaconSyncStatus{}, err
	}

	headSlot, err := strconv.ParseUint(resp.Data.HeadSlot, 10, 64)
	if err != nil {
		return BeaconSyncStatus{}, fmt.Errorf("parse head_slot: %w", err)
	}
	syncDistance, err := strconv.ParseUint(resp.Data.SyncDistance, 10, 64)
	if err != nil {
		return BeaconSyncStatus{}, fmt.Errorf("parse sync_distance: %w", err)
	}

	return BeaconSyncStatus{
		HeadSlot:     headSlot,
		SyncDistance: syncDistance,
		CurrentSlot:  headSlot + syncDistance,
		IsSyncing:    resp.Data.IsSyncing,
		IsOptimistic: resp.Data.IsOptimistic,
		ELOffline:    resp.Data.ELOffline,
	}, nil
}

func (c *BeaconClient) GetValidatorDuties(ctx context.Context) (ValidatorDuties, error) {
	spec, syncStatus, validators, err := c.resolveValidatorContext(ctx)
	if err != nil {
		return ValidatorDuties{}, err
	}
	return c.getValidatorDuties(ctx, spec, syncStatus, validators)
}

func (c *BeaconClient) GetValidatorPerformance(ctx context.Context) (ValidatorPerformance, error) {
	spec, syncStatus, validators, err := c.resolveValidatorContext(ctx)
	if err != nil {
		return ValidatorPerformance{}, err
	}
	return c.getValidatorPerformance(ctx, spec, syncStatus, validators)
}

func (c *BeaconClient) resolveValidatorContext(ctx context.Context) (beaconSpec, BeaconSyncStatus, []discoveredValidator, error) {
	pubkeys, err := c.discoverValidatorPubkeys()
	if err != nil {
		return beaconSpec{}, BeaconSyncStatus{}, nil, err
	}
	if len(pubkeys) == 0 {
		return beaconSpec{}, BeaconSyncStatus{}, nil, ErrNoValidatorKeysConfigured
	}

	spec := c.getSpec(ctx)
	syncStatus, err := c.GetSyncStatus(ctx)
	if err != nil {
		return spec, BeaconSyncStatus{}, nil, err
	}

	validators, err := c.lookupValidators(ctx, pubkeys)
	if err != nil {
		return spec, syncStatus, nil, err
	}

	return spec, syncStatus, filterActiveValidators(validators), nil
}

func (c *BeaconClient) getValidatorDuties(ctx context.Context, spec beaconSpec, syncStatus BeaconSyncStatus, validators []discoveredValidator) (ValidatorDuties, error) {
	duties := ValidatorDuties{
		CurrentSlot:    syncStatus.CurrentSlot,
		SecondsPerSlot: spec.SecondsPerSlot,
	}
	if len(validators) == 0 {
		return duties, nil
	}

	indices := validatorIndices(validators)
	currentEpoch := syncStatus.CurrentSlot / spec.SlotsPerEpoch

	attesterDuties, err := c.attesterDutiesForEpoch(ctx, currentEpoch, indices)
	if err != nil {
		return duties, err
	}
	duties.NextAttestation = nextDutyAtOrAfter(attesterDuties, syncStatus.CurrentSlot)
	if duties.NextAttestation == nil {
		nextEpochDuties, err := c.attesterDutiesForEpoch(ctx, currentEpoch+1, indices)
		if err != nil {
			return duties, err
		}
		duties.NextAttestation = nextDutyAtOrAfter(nextEpochDuties, syncStatus.CurrentSlot)
	}

	cacheKey := validatorCacheKey(indices)
	c.mu.Lock()
	if c.cachedPropKey == cacheKey && c.cachedPropDuty != nil && c.cachedPropSlot >= syncStatus.CurrentSlot {
		cached := *c.cachedPropDuty
		duties.NextProposer = &cached
		c.mu.Unlock()
		return duties, nil
	}
	c.mu.Unlock()

	for epoch := currentEpoch; epoch < currentEpoch+proposerSearchEpochs; epoch++ {
		proposerDuties, err := c.proposerDutiesForEpoch(ctx, epoch)
		if err != nil {
			return duties, err
		}
		nextProposer := nextMatchingDutyAtOrAfter(proposerDuties, syncStatus.CurrentSlot, indices)
		if nextProposer == nil {
			continue
		}

		duties.NextProposer = nextProposer
		c.mu.Lock()
		c.cachedPropKey = cacheKey
		c.cachedPropDuty = cloneDuty(nextProposer)
		c.cachedPropSlot = nextProposer.Slot
		c.mu.Unlock()
		break
	}

	return duties, nil
}

func (c *BeaconClient) getValidatorPerformance(ctx context.Context, spec beaconSpec, syncStatus BeaconSyncStatus, validators []discoveredValidator) (ValidatorPerformance, error) {
	if len(validators) == 0 {
		return ValidatorPerformance{}, nil
	}

	currentEpoch := syncStatus.CurrentSlot / spec.SlotsPerEpoch
	cacheKey := fmt.Sprintf("%s:%d", validatorCacheKey(validatorIndices(validators)), currentEpoch)

	c.mu.Lock()
	if c.cachedPerfKey == cacheKey {
		cached := c.cachedPerf
		c.mu.Unlock()
		return cached, nil
	}
	c.mu.Unlock()

	windowEpochs := int((24 * time.Hour) / (time.Duration(spec.SecondsPerSlot*spec.SlotsPerEpoch) * time.Second))
	if windowEpochs < 1 {
		windowEpochs = 1
	}

	var startEpoch uint64
	if currentEpoch > uint64(windowEpochs) {
		startEpoch = currentEpoch - uint64(windowEpochs)
	}

	ids := validatorIDs(validators)
	totalExpected := 0
	missed := 0
	delays := make([]int, 0, windowEpochs*len(validators))

	for epoch := startEpoch; epoch < currentEpoch; epoch++ {
		rewards, err := c.attestationRewardsForEpoch(ctx, epoch, ids)
		if err != nil {
			return ValidatorPerformance{}, err
		}

		for _, validator := range validators {
			totalExpected++
			reward, ok := rewards[validator.Index]
			if !ok || reward.Target <= 0 {
				missed++
				continue
			}
			delays = append(delays, int(max(0, reward.InclusionDelay)))
		}
	}

	attested := totalExpected - missed
	pct := 0.0
	if totalExpected > 0 {
		pct = (float64(attested) / float64(totalExpected)) * 100
	}

	sort.Ints(delays)
	performance := ValidatorPerformance{
		AttestationPct: pct,
		Missed:         missed,
		InclusionP50:   percentile(delays, 0.50),
		InclusionP95:   percentile(delays, 0.95),
	}

	c.mu.Lock()
	c.cachedPerfKey = cacheKey
	c.cachedPerf = performance
	c.mu.Unlock()

	return performance, nil
}

func (c *BeaconClient) discoverValidatorPubkeys() ([]string, error) {
	if c.DataDir == "" {
		return nil, nil
	}

	candidates := []string{
		filepath.Join(c.DataDir, "validators"),
		filepath.Join(c.DataDir, "validator_keys"),
		filepath.Join(c.DataDir, "validator-keys"),
		filepath.Join(c.DataDir, "keystore"),
		filepath.Join(c.DataDir, "keystores"),
		filepath.Join(c.DataDir, "caplin", "validators"),
		filepath.Join(c.DataDir, "caplin", "validator_keys"),
		filepath.Join(c.DataDir, "caplin", "validator-keys"),
		filepath.Join(c.DataDir, "caplin", "keystore"),
		filepath.Join(c.DataDir, "caplin", "keystores"),
	}

	pubkeys := map[string]struct{}{}
	for _, root := range candidates {
		info, err := os.Stat(root)
		if err != nil || !info.IsDir() {
			continue
		}
		if err := c.walkValidatorDir(root, pubkeys); err != nil {
			return nil, err
		}
	}

	out := make([]string, 0, len(pubkeys))
	for pubkey := range pubkeys {
		out = append(out, pubkey)
	}
	sort.Strings(out)
	return out, nil
}

func (c *BeaconClient) walkValidatorDir(root string, pubkeys map[string]struct{}) error {
	root = filepath.Clean(root)
	rootDepth := strings.Count(root, string(os.PathSeparator))

	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			depth := strings.Count(filepath.Clean(path), string(os.PathSeparator)) - rootDepth
			if depth > validatorWalkMaxDepth {
				return fs.SkipDir
			}
			return nil
		}
		if filepath.Ext(d.Name()) != ".json" {
			return nil
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		var keystore struct {
			Pubkey string          `json:"pubkey"`
			Crypto json.RawMessage `json:"crypto"`
		}
		if err := json.Unmarshal(data, &keystore); err != nil {
			return nil
		}
		if len(keystore.Crypto) == 0 {
			return nil
		}
		pubkey := normalizeValidatorPubkey(keystore.Pubkey)
		if pubkey == "" {
			return nil
		}
		pubkeys[pubkey] = struct{}{}
		return nil
	})
}

func (c *BeaconClient) lookupValidators(ctx context.Context, pubkeys []string) ([]discoveredValidator, error) {
	query := url.Values{}
	for _, pubkey := range pubkeys {
		query.Add("id", pubkey)
	}

	var resp beaconEnvelope[[]beaconValidatorResponse]
	if err := c.doJSON(ctx, http.MethodGet, "/eth/v1/beacon/states/head/validators", query, nil, &resp); err != nil {
		return nil, err
	}

	validators := make([]discoveredValidator, 0, len(resp.Data))
	for _, item := range resp.Data {
		index, err := strconv.ParseUint(item.Index, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse validator index: %w", err)
		}
		validators = append(validators, discoveredValidator{
			Index:   index,
			Status:  item.Status,
			Pubkey:  normalizeValidatorPubkey(item.Validator.Pubkey),
			Slashed: item.Validator.Slashed,
		})
	}

	sort.Slice(validators, func(i, j int) bool {
		return validators[i].Index < validators[j].Index
	})

	if len(validators) == 0 {
		return nil, errors.New("validator keys discovered locally, but no validators were found in Beacon API")
	}

	return validators, nil
}

func (c *BeaconClient) attesterDutiesForEpoch(ctx context.Context, epoch uint64, indices []uint64) ([]ValidatorDuty, error) {
	path := fmt.Sprintf("/eth/v1/validator/duties/attester/%d", epoch)
	var resp beaconEnvelope[[]dutyResponse]

	query := url.Values{}
	for _, index := range indices {
		query.Add("id", strconv.FormatUint(index, 10))
	}
	err := c.doJSON(ctx, http.MethodGet, path, query, nil, &resp)
	if err != nil {
		var statusErr *apiStatusError
		if !errors.As(err, &statusErr) {
			return nil, err
		}

		body := make([]string, 0, len(indices))
		for _, index := range indices {
			body = append(body, strconv.FormatUint(index, 10))
		}
		if postErr := c.doJSON(ctx, http.MethodPost, path, nil, body, &resp); postErr != nil {
			return nil, postErr
		}
	}

	return parseDutyResponses(resp.Data)
}

func (c *BeaconClient) proposerDutiesForEpoch(ctx context.Context, epoch uint64) ([]ValidatorDuty, error) {
	path := fmt.Sprintf("/eth/v1/validator/duties/proposer/%d", epoch)
	var resp beaconEnvelope[[]dutyResponse]
	if err := c.doJSON(ctx, http.MethodGet, path, nil, nil, &resp); err != nil {
		return nil, err
	}
	return parseDutyResponses(resp.Data)
}

func (c *BeaconClient) attestationRewardsForEpoch(ctx context.Context, epoch uint64, ids []string) (map[uint64]struct {
	Target         int64
	InclusionDelay int64
}, error) {
	path := fmt.Sprintf("/eth/v1/beacon/rewards/attestations/%d", epoch)
	var resp beaconEnvelope[attestationRewardsData]
	if err := c.doJSON(ctx, http.MethodPost, path, nil, ids, &resp); err != nil {
		return nil, err
	}

	rewards := make(map[uint64]struct {
		Target         int64
		InclusionDelay int64
	}, len(resp.Data.TotalRewards))
	for _, reward := range resp.Data.TotalRewards {
		index, err := strconv.ParseUint(reward.ValidatorIndex, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse reward validator index: %w", err)
		}
		target, err := strconv.ParseInt(reward.Target, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse attestation target reward: %w", err)
		}
		inclusionDelay, err := strconv.ParseInt(reward.InclusionDelay, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse inclusion delay: %w", err)
		}
		rewards[index] = struct {
			Target         int64
			InclusionDelay int64
		}{
			Target:         target,
			InclusionDelay: inclusionDelay,
		}
	}
	return rewards, nil
}

func (c *BeaconClient) getSpec(ctx context.Context) beaconSpec {
	c.mu.Lock()
	if c.specLoaded {
		spec := c.cachedSpec
		c.mu.Unlock()
		return spec
	}
	c.mu.Unlock()

	spec := beaconSpec{
		SlotsPerEpoch:  defaultSlotsPerEpoch,
		SecondsPerSlot: defaultSecondsPerSlot,
	}

	var resp beaconEnvelope[map[string]string]
	if err := c.doJSON(ctx, http.MethodGet, "/eth/v1/config/spec", nil, nil, &resp); err == nil {
		if slotsPerEpoch, ok := parseOptionalUint(resp.Data["SLOTS_PER_EPOCH"]); ok {
			spec.SlotsPerEpoch = slotsPerEpoch
		}
		if secondsPerSlot, ok := parseOptionalUint(resp.Data["SECONDS_PER_SLOT"]); ok {
			spec.SecondsPerSlot = secondsPerSlot
		}
	}

	c.mu.Lock()
	c.cachedSpec = spec
	c.specLoaded = true
	c.mu.Unlock()
	return spec
}

func (c *BeaconClient) doJSON(ctx context.Context, method, path string, query url.Values, body any, out any) error {
	endpoint := strings.TrimRight(c.BaseURL, "/") + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}

	var reader io.Reader
	if body != nil {
		buf := new(bytes.Buffer)
		if err := json.NewEncoder(buf).Encode(body); err != nil {
			return fmt.Errorf("encode request body: %w", err)
		}
		reader = buf
	}

	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.Client.Do(req)
	if err != nil {
		return fmt.Errorf("beacon api unavailable: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorResponseLength))
		return &apiStatusError{
			Method: method,
			Path:   path,
			Code:   resp.StatusCode,
			Body:   strings.TrimSpace(string(snippet)),
		}
	}

	if out == nil {
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode %s %s: %w", method, path, err)
	}
	return nil
}

func filterActiveValidators(validators []discoveredValidator) []discoveredValidator {
	active := make([]discoveredValidator, 0, len(validators))
	for _, validator := range validators {
		if strings.HasPrefix(validator.Status, "active") && !validator.Slashed {
			active = append(active, validator)
		}
	}
	return active
}

func validatorIDs(validators []discoveredValidator) []string {
	ids := make([]string, 0, len(validators))
	for _, validator := range validators {
		ids = append(ids, strconv.FormatUint(validator.Index, 10))
	}
	return ids
}

func validatorIndices(validators []discoveredValidator) []uint64 {
	indices := make([]uint64, 0, len(validators))
	for _, validator := range validators {
		indices = append(indices, validator.Index)
	}
	return indices
}

func validatorCacheKey(indices []uint64) string {
	parts := make([]string, 0, len(indices))
	for _, index := range indices {
		parts = append(parts, strconv.FormatUint(index, 10))
	}
	return strings.Join(parts, ",")
}

func nextDutyAtOrAfter(duties []ValidatorDuty, currentSlot uint64) *ValidatorDuty {
	var next *ValidatorDuty
	for _, duty := range duties {
		if duty.Slot < currentSlot {
			continue
		}
		if next == nil || duty.Slot < next.Slot {
			candidate := duty
			next = &candidate
		}
	}
	return next
}

func nextMatchingDutyAtOrAfter(duties []ValidatorDuty, currentSlot uint64, indices []uint64) *ValidatorDuty {
	indexSet := make(map[uint64]struct{}, len(indices))
	for _, index := range indices {
		indexSet[index] = struct{}{}
	}

	var next *ValidatorDuty
	for _, duty := range duties {
		if duty.Slot < currentSlot {
			continue
		}
		if _, ok := indexSet[duty.ValidatorIndex]; !ok {
			continue
		}
		if next == nil || duty.Slot < next.Slot {
			candidate := duty
			next = &candidate
		}
	}
	return next
}

func cloneDuty(duty *ValidatorDuty) *ValidatorDuty {
	if duty == nil {
		return nil
	}
	cloned := *duty
	return &cloned
}

func parseDutyResponses(responses []dutyResponse) ([]ValidatorDuty, error) {
	duties := make([]ValidatorDuty, 0, len(responses))
	for _, item := range responses {
		index, err := strconv.ParseUint(item.ValidatorIndex, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse duty validator index: %w", err)
		}
		slot, err := strconv.ParseUint(item.Slot, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse duty slot: %w", err)
		}
		duties = append(duties, ValidatorDuty{
			ValidatorIndex: index,
			Slot:           slot,
		})
	}
	return duties, nil
}

func parseOptionalUint(value string) (uint64, bool) {
	if value == "" {
		return 0, false
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, false
	}
	return parsed, true
}

func normalizeValidatorPubkey(pubkey string) string {
	pubkey = strings.TrimSpace(strings.ToLower(pubkey))
	pubkey = strings.TrimPrefix(pubkey, "0x")
	if len(pubkey) != 96 {
		return ""
	}
	for _, ch := range pubkey {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return ""
		}
	}
	return "0x" + pubkey
}

func percentile(values []int, p float64) int {
	if len(values) == 0 {
		return 0
	}
	if p <= 0 {
		return values[0]
	}
	if p >= 1 {
		return values[len(values)-1]
	}

	index := int(math.Ceil(float64(len(values))*p)) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}
