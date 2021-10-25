package aura

import (
	"bytes"
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
func (c *AuRa) verifyHeader(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header) error {

	if header.Number == nil {
		return errUnknownBlock
	}

	now := time.Now()
	unixTime := now.Unix()

	// check if block is future time
	if header.Time > uint64(unixTime) {
		return consensus.ErrFutureBlock
	}

	// Nonces must be 0x00..0 or 0xff..f, zeroes enforced on checkpoints
	if !bytes.Equal(header.Nonce[:], NonceAuthVote) && !bytes.Equal(header.Nonce[:], nonceDropVote) {
		return errInvalidVote
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

}

func (c *AuRa) VerifyUncles(chain consensus.ChainReader, header *types.Header, uncles []*types.Header) error{

}

func (c *AuRa) Snapshot(chain consensus.ChainHeaderReader, number uint64, hash common.Hash, parents []*types.Header) (*Snapshot, error) {

}

// verifySeal checks whether the signature contained in the header satisfies the
// consensus protocol requirements. The method accepts an optional list of parent
// headers that aren't yet part of the local blockchain to generate the snapshots
// from.
func (c *AuRa) verifySeal(chain consensus.ChainHeaderReader, header *types.Header, snap *Snapshot) error {

}
