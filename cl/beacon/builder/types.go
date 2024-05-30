package builder

import (
	"encoding/binary"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
)

type ExecutionPayloadHeader struct {
	Version string `json:"version"`
	Data    struct {
		Message struct {
			Header             *cltypes.Eth1Header                    `json:"header"`
			BlobKzgCommitments *solid.ListSSZ[*cltypes.KZGCommitment] `json:"blob_kzg_commitments,omitempty"`
			Value              []byte                                 `json:"value"`
			PubKey             common.Bytes48                         `json:"pubkey"`
		} `json:"message"`
		Signature common.Bytes96 `json:"signature"`
	} `json:"data"`
}

func (h ExecutionPayloadHeader) BlockValue() *big.Int {
	if h.Data.Message.Value == nil {
		return nil
	}
	blockValue := binary.LittleEndian.Uint64(h.Data.Message.Value)
	return new(big.Int).SetUint64(blockValue)
}

type BlindedBlockResponse struct {
	Version string            `json:"version"`
	Data    cltypes.Eth1Block `json:"data"`
}
