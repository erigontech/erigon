package transition

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

const (
	SHUFFLE_ROUND_COUNT           = uint8(90)
	EPOCHS_PER_HISTORICAL_VECTOR  = uint64(1 << 16)
	MIN_SEED_LOOKAHEAD            = uint64(1)
	SLOTS_PER_EPOCH               = uint64(1 << 5)
	EPOCHS_PER_ETH1_VOTING_PERIOD = uint64(1 << 6)
)

func ComputeShuffledIndex(ind, ind_count uint64, seed [32]byte) (uint64, error) {
	if ind >= ind_count {
		return 0, fmt.Errorf("index=%d must be less than the index count=%d", ind, ind_count)
	}

	for i := uint8(0); i < SHUFFLE_ROUND_COUNT; i++ {
		// Construct first hash input.
		input := append(seed[:], i)
		hashedInput := utils.Keccak256(input)

		// Read hash value.
		hashValue := binary.LittleEndian.Uint64(hashedInput[:8])

		// Caclulate pivot and flip.
		pivot := hashValue % ind_count
		flip := (pivot + ind_count - ind) % ind_count

		// No uint64 max function in go standard library.
		position := ind
		if flip > ind {
			position = flip
		}

		// Construct the second hash input.
		positionByteArray := make([]byte, 4)
		binary.LittleEndian.PutUint32(positionByteArray, uint32(position>>8))
		input2 := append(seed[:], i)
		input2 = append(input2, positionByteArray...)

		hashedInput2 := utils.Keccak256(input2)
		// Read hash value.
		byteVal := hashedInput2[(position%256)/8]
		bitVal := (byteVal >> (position % 8)) % 2
		if bitVal == 1 {
			ind = flip
		}
	}
	return ind, nil
}

func ComputeProposerIndex(state *state.BeaconState, indices []uint64, seed [32]byte) (uint64, error) {
	if len(indices) == 0 {
		return 0, fmt.Errorf("must have >0 indices")
	}
	maxRandomByte := uint64(1<<8 - 1)
	i := uint64(0)
	total := uint64(len(indices))
	hash := sha256.New()
	buf := make([]byte, 8)
	for {
		shuffled, err := ComputeShuffledIndex(i%total, total, seed)
		if err != nil {
			return 0, err
		}
		candidateIndex := indices[shuffled]
		if candidateIndex >= uint64(len(state.Validators())) {
			return 0, fmt.Errorf("candidate index out of range: %d for validator set of length: %d", candidateIndex, len(state.Validators()))
		}
		binary.LittleEndian.PutUint64(buf, i/32)
		input := append(seed[:], buf...)
		hash.Reset()
		hash.Write(input)
		randomByte := uint64(hash.Sum(nil)[i%32])
		effectiveBalance := state.ValidatorAt(int(candidateIndex)).EffectiveBalance
		if effectiveBalance*maxRandomByte >= clparams.MainnetBeaconConfig.MaxEffectiveBalance*randomByte {
			return candidateIndex, nil
		}
		i += 1
	}
}

func GetRandaoMixes(state *state.BeaconState, epoch uint64) [32]byte {
	return state.RandaoMixes()[epoch%EPOCHS_PER_HISTORICAL_VECTOR]
}

func GetSeed(state *state.BeaconState, epoch uint64, domain [4]byte) []byte {
	mix := GetRandaoMixes(state, epoch+EPOCHS_PER_HISTORICAL_VECTOR-MIN_SEED_LOOKAHEAD-1)
	epochByteArray := make([]byte, 8)
	binary.LittleEndian.PutUint64(epochByteArray, epoch)
	input := append(domain[:], epochByteArray...)
	input = append(input, mix[:]...)
	hash := sha256.New()
	hash.Write(input)
	return hash.Sum(nil)
}

func GetActiveValidatorIndices(state *state.BeaconState, epoch uint64) []uint64 {
	indices := []uint64{}
	for i := 0; i < len(state.Validators()); i++ {
		v := state.ValidatorAt(i)
		if v.ActivationEpoch <= epoch && epoch < v.ExitEpoch {
			indices = append(indices, uint64(i))
		}
	}
	return indices
}

func GetEpochAtSlot(slot uint64) uint64 {
	return slot / SLOTS_PER_EPOCH
}

func GetDomain(state *state.BeaconState, domainType [4]byte, epoch uint64) ([]byte, error) {
	if epoch == 0 {
		epoch = GetEpochAtSlot(state.Slot())
	}
	var forkVersion [4]byte
	if epoch < state.Fork().Epoch {
		forkVersion = state.Fork().PreviousVersion
	} else {
		forkVersion = state.Fork().CurrentVersion
	}
	return fork.ComputeDomain(domainType[:], forkVersion, state.GenesisValidatorsRoot())
}

func ComputeSigningRootEpoch(epoch uint64, domain []byte) ([32]byte, error) {
	b := make([]byte, 32)
	binary.LittleEndian.PutUint64(b, epoch)
	hash := utils.Keccak256(b)
	return utils.Keccak256(hash[:], domain), nil
}

func GetBeaconProposerIndex(state *state.BeaconState) (uint64, error) {
	epoch := GetEpochAtSlot(state.Slot())

	hash := sha256.New()
	// Input for the seed hash.
	input := GetSeed(state, epoch, clparams.MainnetBeaconConfig.DomainBeaconProposer)
	slotByteArray := make([]byte, 8)
	binary.LittleEndian.PutUint64(slotByteArray, state.Slot())

	// Add slot to the end of the input.
	inputWithSlot := append(input, slotByteArray...)

	// Calculate the hash.
	hash.Write(inputWithSlot)
	seed := hash.Sum(nil)

	indices := GetActiveValidatorIndices(state, epoch)

	// Write the seed to an array.
	seedArray := [32]byte{}
	copy(seedArray[:], seed)

	return ComputeProposerIndex(state, indices, seedArray)
}

func ProcessBlockHeader(state *state.BeaconState, block *cltypes.BeaconBlock) error {
	if block.Slot != state.Slot() {
		return fmt.Errorf("state slot: %d, not equal to block slot: %d", state.Slot(), block.Slot)
	}
	if block.Slot <= state.LatestBlockHeader().Slot {
		return fmt.Errorf("slock slot: %d, not greater than latest block slot: %d", block.Slot, state.LatestBlockHeader().Slot)
	}
	propInd, err := GetBeaconProposerIndex(state)
	if err != nil {
		return fmt.Errorf("error in GetBeaconProposerIndex: %v", err)
	}
	if block.ProposerIndex != propInd {
		return fmt.Errorf("block proposer index: %d, does not match beacon proposer index: %d", block.ProposerIndex, propInd)
	}
	latestRoot, err := state.LatestBlockHeader().HashTreeRoot()
	if err != nil {
		return fmt.Errorf("unable to hash tree root of latest block header: %v", err)
	}
	if block.ParentRoot != latestRoot {
		return fmt.Errorf("block parent root: %x, does not match latest block root: %x", block.ParentRoot, latestRoot)
	}
	bodyRoot, err := block.Body.HashTreeRoot()
	if err != nil {
		return fmt.Errorf("unable to hash tree root of block body: %v", err)
	}
	state.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{
		Slot:          block.Slot,
		ProposerIndex: block.ProposerIndex,
		ParentRoot:    block.ParentRoot,
		BodyRoot:      bodyRoot,
	})

	proposer := state.ValidatorAt(int(block.ProposerIndex))
	if proposer.Slashed {
		return fmt.Errorf("proposer: %d is slashed", block.ProposerIndex)
	}
	return nil
}

func ProcessRandao(state *state.BeaconState, body *cltypes.BeaconBody) error {
	epoch := GetEpochAtSlot(state.Slot())
	propInd, err := GetBeaconProposerIndex(state)
	if err != nil {
		return fmt.Errorf("unable to get proposer index: %v", err)
	}
	proposer := state.ValidatorAt(int(propInd))
	domain, err := GetDomain(state, clparams.MainnetBeaconConfig.DomainRandao, epoch)
	if err != nil {
		return fmt.Errorf("unable to get domain: %v", err)
	}
	signingRoot, err := ComputeSigningRootEpoch(epoch, domain)
	if err != nil {
		return fmt.Errorf("unable to compute signing root: %v", err)
	}
	valid, err := bls.Verify(body.RandaoReveal[:], signingRoot[:], proposer.PublicKey[:])
	if err != nil {
		return fmt.Errorf("unable to verify public key: %x, with signing root: %x, and signature: %x, %v", proposer.PublicKey[:], signingRoot[:], body.RandaoReveal[:], err)
	}
	if !valid {
		return fmt.Errorf("invalid signature: public key: %x, signing root: %x, signature: %x", proposer.PublicKey[:], signingRoot[:], body.RandaoReveal[:])
	}
	randaoMixes := GetRandaoMixes(state, epoch)
	randaoHash := utils.Keccak256(body.RandaoReveal[:])
	mix := [32]byte{}
	for i := range mix {
		mix[i] = randaoMixes[i] ^ randaoHash[i]
	}
	state.RandaoMixes()[epoch%EPOCHS_PER_HISTORICAL_VECTOR] = mix
	return nil
}

func ProcessEth1Data(state *state.BeaconState, body *cltypes.BeaconBody) error {
	newVotes := append(state.Eth1DataVotes(), body.Eth1Data)
	state.SetEth1DataVotes(newVotes)

	ethDataHash, err := body.Eth1Data.HashTreeRoot()
	if err != nil {
		return fmt.Errorf("unable to get hash tree root of eth1data: %v", err)
	}
	// Count how many times body.Eth1Data appears in the votes by comparing their hashes.
	numVotes := 0
	for i := 0; i < len(newVotes); i++ {
		candidateHash, err := newVotes[i].HashTreeRoot()
		if err != nil {
			return fmt.Errorf("unable to get hash tree root of eth1data: %v", err)
		}
		// Check if hash bytes are equal.
		match := true
		for i := 0; i < len(candidateHash); i++ {
			if candidateHash[i] != ethDataHash[i] {
				match = false
			}
		}
		if match {
			numVotes += 1
		}
	}
	if uint64(numVotes*2) > EPOCHS_PER_ETH1_VOTING_PERIOD*SLOTS_PER_EPOCH {
		state.SetEth1Data(body.Eth1Data)
	}
	return nil
}
