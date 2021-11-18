package aura

import (
	"time"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
)

// verifyHeader checks whether a header conforms to the consensus rules.The
// caller may optionally pass in a batch of parents (ascending order) to avoid
// looking those up from the database. This is useful for concurrently verifying
// a batch of new headers.
// verifyHeader follows this https://openethereum.github.io/Trace-NewBlock
// following basic block verification from step 4
func (c *AuRa) verifyHeader(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header) error {

	if header.Number == nil {
		return errUnknownBlock
	}

	if header.Number.Uint64() < 20 {
		return errTooOldOfBlock
	}

	now := time.Now()
	unixTime := now.Unix()

	// check if block is future time
	if header.Time > uint64(unixTime) {
		return consensus.ErrFutureBlock
	}

	// checks for the vanity and the signature
	// checking blocks integrity
	if len(header.Extra) < ExtraVanity {
		return errMissingVanity
	}

	if len(header.Extra) < ExtraVanity+ExtraSeal {
		return errMissingSignature
	}

	signerBytes := len(header.Extra) - ExtraVanity - ExtraSeal

	// there can only be one signer
	if signerBytes != 0 {
		return errExtraSigners
	}

	// Ensure that the mix digest is zero as we don't have fork protection currently
	if header.MixDigest != (common.Hash{}) {
		return errInvalidMixDigest
	}

	return c.verifyCascadingFields(chain, header, parents)
}

// verifyCascadingFields verifies all the header fields that are not standalone,
// rather depend on a batch of previous headers. The caller may optionally pass
// in a batch of parents (ascending order) to avoid looking those up from the
// database. This is useful for concurrently verifying a batch of new headers.
func (c *AuRa) verifyCascadingFields(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header) error {

	stepDuration := c.cfg.StepDuration // getting the step duration value
	timeNow := time.Now().Unix()

	// checking if the step is correct

	headerStep := int64(header.Time / stepDuration) // the blocks current step

	step := timeNow / int64(stepDuration) // the current step the engine is at

	// making sure our currentStep is not a future step
	if headerStep != step {
		return errIncorrectStep
	}

	// checking if multiple blocks are being put out in the same step
	parent := chain.GetHeaderByHash(header.ParentHash)

	parentStep := int64(parent.Time / stepDuration) // the parent block step

	if parentStep > step {
		return errFutureStep
	}

	if parentStep == step {
		return errMultipleBlocksInStep
	}

	currentValidatorList := c.getValidators(header.Number.Uint64())

	// gets the signer using the header step
	indexOfBlockSigner := headerStep % int64(len(currentValidatorList))
	blockSigner := currentValidatorList[indexOfBlockSigner]

	// gets the signers using the engine step
	indexOfSupposeSigner := step % int64(len(currentValidatorList))
	supposeSigner := currentValidatorList[indexOfSupposeSigner]

	// compares signer address from the header to the validator address gotten from the step
	if blockSigner != supposeSigner {
		return errInvalidPrimary
	}

	return nil
}

func (c *AuRa) verifyUncle(chain consensus.ChainReader, uncle *types.Header) error {

	if uncle.Number == nil {
		return errUnknownBlock
	}

	if uncle.Number.Uint64() < 20 {
		return errTooOldOfBlock
	}

	now := time.Now()
	unixTime := now.Unix()

	// check if block is future time
	if uncle.Time > uint64(unixTime) {
		return consensus.ErrFutureBlock
	}

	// checks for the vanity and the signature
	// checking blocks integrity
	if len(uncle.Extra) < ExtraVanity {
		return errMissingVanity
	}

	if len(uncle.Extra) < ExtraVanity+ExtraSeal {
		return errMissingSignature
	}

	signerBytes := len(uncle.Extra) - ExtraVanity - ExtraSeal

	// there can only be one signer
	if signerBytes != 0 {
		return errExtraSigners
	}

	// Ensure that the mix digest is zero as we don't have fork protection currently
	if uncle.MixDigest != (common.Hash{}) {
		return errInvalidMixDigest
	}

	return c.verifyCascadingFields(chain, uncle, nil)

}

// verifySeal checks whether the signature contained in the header satisfies the
// consensus protocol requirements. The method accepts an optional list of parent
// verify seal is the samething as verifyHeader in AuRa
// func (c *AuRa) verifySeal(chain consensus.ChainHeaderReader, header *types.Header, snap *Snapshot) error {

// }
