package snapshot_format

import (
	"fmt"
	"io"

	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/persistence/format/chunk_encoding"
	"github.com/ledgerwatch/erigon/core/types"
)

// WriteEth1BlockForSnapshot writes an execution block to the given writer in the format expected by the snapshot.
func writeEth1BlockForSnapshot(w io.Writer, encoded []byte, block *cltypes.Eth1Block) error {
	pos := (length.Hash /*ParentHash*/ + length.Addr /*Miner*/ + length.Hash /*StateRoot*/ + length.Hash /*ReceiptsRoot*/ + types.BloomByteLength /*Bloom*/ +
		length.Hash /*PrevRandao*/ + 32 /*BlockNumber + Timestamp + GasLimit + GasUsed */ + 4 /*ExtraDataOffset*/ + length.Hash /*BaseFee*/ +
		length.Hash /*BlockHash*/ + 4 /*TransactionOffset*/)

	if block.Version() >= clparams.CapellaVersion {
		pos += 4 /*WithdrawalsOffset*/
	}
	if block.Version() >= clparams.DenebVersion {
		pos += 16 /*BlobGasUsed + ExcessBlobGas*/
	}
	// Add metadata first for Eth1Block, aka. version
	if _, err := w.Write([]byte{byte(block.Version())}); err != nil {
		return err
	}

	// Maybe reuse the buffer?
	pos += block.Extra.EncodingSizeSSZ()
	if err := chunk_encoding.WriteChunk(w, encoded[:pos], chunk_encoding.ChunkDataType); err != nil {
		return err
	}
	pos += block.Withdrawals.EncodingSizeSSZ()
	pos += block.Transactions.EncodingSizeSSZ()
	encoded = encoded[pos:]
	//pos = 0
	// write the block pointer
	if err := writeExecutionBlockPtr(w, block); err != nil {
		return err
	}
	// From now on here, just finish up
	return chunk_encoding.WriteChunk(w, encoded, chunk_encoding.ChunkDataType)
}

func readEth1BlockFromSnapshot(r io.Reader, out io.Writer, executionReader ExecutionBlockReaderByNumber, cfg *clparams.BeaconChainConfig) (clparams.StateVersion, error) {
	// Metadata section is just the current hardfork of the block.
	vArr := make([]byte, 1)
	if _, err := r.Read(vArr); err != nil {
		return 0, err
	}
	v := clparams.StateVersion(vArr[0])

	// Read the first chunk
	dT1, err := chunk_encoding.ReadChunk(r, out)
	if err != nil {
		return v, err
	}
	if dT1 != chunk_encoding.ChunkDataType {
		return v, fmt.Errorf("malformed beacon block, invalid chunk 1 type %d, expected: %d", dT1, chunk_encoding.ChunkDataType)
	}
	// Read the block pointer and retrieve chunk4 from the execution reader
	blockNumber, blockHash, err := readExecutionBlockPtr(r)
	if err != nil {
		return v, err
	}
	err = executionReader.TransactionsSSZ(out, blockNumber, blockHash)
	if err != nil {
		return v, err
	}

	if v < clparams.CapellaVersion {
		return v, nil
	}
	err = executionReader.WithdrawalsSZZ(out, blockNumber, blockHash)
	if err != nil {
		return v, err
	}

	// Read the 5h chunk
	dT2, err := chunk_encoding.ReadChunk(r, out)
	if err != nil {
		return v, err
	}
	if dT2 != chunk_encoding.ChunkDataType {
		return v, fmt.Errorf("malformed beacon block, invalid chunk 5 type %d, expected: %d", dT2, chunk_encoding.ChunkDataType)
	}

	return v, nil
}
