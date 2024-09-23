package borcfg

import (
	"math/big"
	"sort"
	"strconv"

	"github.com/ledgerwatch/erigon-lib/common"
)

// BorConfig is the consensus engine configs for Matic bor based sealing.
type BorConfig struct {
	Period                map[string]uint64 `json:"period"`                // Number of seconds between blocks to enforce
	ProducerDelay         map[string]uint64 `json:"producerDelay"`         // Number of seconds delay between two producer interval
	Sprint                map[string]uint64 `json:"sprint"`                // Epoch length to proposer
	BackupMultiplier      map[string]uint64 `json:"backupMultiplier"`      // Backup multiplier to determine the wiggle time
	ValidatorContract     string            `json:"validatorContract"`     // Validator set contract
	StateReceiverContract string            `json:"stateReceiverContract"` // State receiver contract

	OverrideStateSyncRecords map[string]int         `json:"overrideStateSyncRecords"` // override state records count
	BlockAlloc               map[string]interface{} `json:"blockAlloc"`

	JaipurBlock                *big.Int          `json:"jaipurBlock"`                // Jaipur switch block (nil = no fork, 0 = already on Jaipur)
	DelhiBlock                 *big.Int          `json:"delhiBlock"`                 // Delhi switch block (nil = no fork, 0 = already on Delhi)
	IndoreBlock                *big.Int          `json:"indoreBlock"`                // Indore switch block (nil = no fork, 0 = already on Indore)
	AgraBlock                  *big.Int          `json:"agraBlock"`                  // Agra switch block (nil = no fork, 0 = already on Agra)
	NapoliBlock                *big.Int          `json:"napoliBlock"`                // Napoli switch block (nil = no fork, 0 = already on Napoli)
	AhmedabadBlock             *big.Int          `json:"ahmedabadBlock"`             // Ahmedabad switch block (nil = no fork, 0 = already on Ahmedabad)
	StateSyncConfirmationDelay map[string]uint64 `json:"stateSyncConfirmationDelay"` // StateSync Confirmation Delay, in seconds, to calculate `to`

	sprints sprints
}

// String implements the stringer interface, returning the consensus engine details.
func (c *BorConfig) String() string {
	return "bor"
}

func (c *BorConfig) CalculateProducerDelay(number uint64) uint64 {
	return borKeyValueConfigHelper(c.ProducerDelay, number)
}

func (c *BorConfig) CalculateSprintLength(number uint64) uint64 {
	if c.sprints == nil {
		c.sprints = asSprints(c.Sprint)
	}

	for i := 0; i < len(c.sprints)-1; i++ {
		if number >= c.sprints[i].from && number < c.sprints[i+1].from {
			return c.sprints[i].size
		}
	}

	return c.sprints[len(c.sprints)-1].size
}

func (c *BorConfig) CalculateSprintNumber(number uint64) uint64 {
	if c.sprints == nil {
		c.sprints = asSprints(c.Sprint)
	}

	// unknown sprint size
	if (len(c.sprints) == 0) || (number < c.sprints[0].from) {
		return 0
	}

	// remove sprint configs that are not in effect yet
	sprints := c.sprints
	for number < sprints[len(sprints)-1].from {
		sprints = sprints[:len(sprints)-1]
	}

	var count uint64
	end := number
	for len(sprints) > 0 {
		sprint := sprints[len(sprints)-1]
		count += (end - sprint.from) / sprint.size

		sprints = sprints[:len(sprints)-1]
		end = sprint.from
	}

	if c.sprints[0].from > 0 {
		count++
	}
	return count
}

func (c *BorConfig) CalculateBackupMultiplier(number uint64) uint64 {
	return borKeyValueConfigHelper(c.BackupMultiplier, number)
}

func (c *BorConfig) CalculatePeriod(number uint64) uint64 {
	return borKeyValueConfigHelper(c.Period, number)
}

// isForked returns whether a fork scheduled at block s is active at the given head block.
func isForked(s *big.Int, head uint64) bool {
	if s == nil {
		return false
	}
	return s.Uint64() <= head
}

func (c *BorConfig) IsJaipur(number uint64) bool {
	return isForked(c.JaipurBlock, number)
}

func (c *BorConfig) IsDelhi(number uint64) bool {
	return isForked(c.DelhiBlock, number)
}

func (c *BorConfig) IsIndore(number uint64) bool {
	return isForked(c.IndoreBlock, number)
}

// IsAgra returns whether num is either equal to the Agra fork block or greater.
// The Agra hard fork is based on the Shanghai hard fork, but it doesn't include withdrawals.
// Also Agra is activated based on the block number rather than the timestamp.
// Refer to https://forum.polygon.technology/t/pip-28-agra-hardfork
func (c *BorConfig) IsAgra(num uint64) bool {
	return isForked(c.AgraBlock, num)
}

func (c *BorConfig) GetAgraBlock() *big.Int {
	return c.AgraBlock
}

// Refer to https://forum.polygon.technology/t/pip-33-napoli-upgrade
func (c *BorConfig) IsNapoli(num uint64) bool {
	return isForked(c.NapoliBlock, num)
}

func (c *BorConfig) IsAhmedabad(number uint64) bool {
	return isForked(c.AhmedabadBlock, number)
}

func (c *BorConfig) GetNapoliBlock() *big.Int {
	return c.NapoliBlock
}

func (c *BorConfig) CalculateStateSyncDelay(number uint64) uint64 {
	return borKeyValueConfigHelper(c.StateSyncConfirmationDelay, number)
}

func borKeyValueConfigHelper[T uint64 | common.Address](field map[string]T, number uint64) T {
	fieldUint := make(map[uint64]T)
	for k, v := range field {
		keyUint, err := strconv.ParseUint(k, 10, 64)
		if err != nil {
			panic(err)
		}
		fieldUint[keyUint] = v
	}

	keys := common.SortedKeys(fieldUint)

	for i := 0; i < len(keys)-1; i++ {
		if number >= keys[i] && number < keys[i+1] {
			return fieldUint[keys[i]]
		}
	}

	return fieldUint[keys[len(keys)-1]]
}

type sprint struct {
	from, size uint64
}

type sprints []sprint

func (s sprints) Len() int {
	return len(s)
}

func (s sprints) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sprints) Less(i, j int) bool {
	return s[i].from < s[j].from
}

func asSprints(configSprints map[string]uint64) sprints {
	sprints := make(sprints, len(configSprints))

	i := 0
	for key, value := range configSprints {
		sprints[i].from, _ = strconv.ParseUint(key, 10, 64)
		sprints[i].size = value
		i++
	}

	sort.Sort(sprints)

	return sprints
}
