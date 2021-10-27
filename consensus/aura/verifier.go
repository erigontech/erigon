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

	return nil
}

// verifyCascadingFields verifies all the header fields that are not standalone,
// rather depend on a batch of previous headers. The caller may optionally pass
// in a batch of parents (ascending order) to avoid looking those up from the
// database. This is useful for concurrently verifying a batch of new headers.
// func (c *AuRa) verifyCascadingFields(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header) error {

// }

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

	return nil

}

// verifySeal checks whether the signature contained in the header satisfies the
// consensus protocol requirements. The method accepts an optional list of parent
// verify seal is the samething as verifyHeader in AuRa
// func (c *AuRa) verifySeal(chain consensus.ChainHeaderReader, header *types.Header, snap *Snapshot) error {

// }
