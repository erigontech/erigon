// Copyright 2024 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package stateless

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon/execution/types"
)

// HeaderReader is an interface to pull in headers in place of block hashes for the witness.
type HeaderReader interface {
	// GetHeader retrieves a block header from the database by hash and number.
	GetHeader(hash common.Hash, number uint64) *types.Header
}

// ValidateWitnessPreState validates that the witness pre-state root matches the parent block's state root.
func ValidateWitnessPreState(witness *Witness, headerReader HeaderReader) error {
	if witness == nil {
		return fmt.Errorf("witness is nil")
	}

	// Check if witness has any headers.
	if len(witness.Headers) == 0 {
		return fmt.Errorf("witness has no headers")
	}

	// Get the witness context header (the block this witness is for).
	contextHeader := witness.Header()
	if contextHeader == nil {
		return fmt.Errorf("witness context header is nil")
	}

	// Get the parent block header from the chain.
	parentHeader := headerReader.GetHeader(contextHeader.ParentHash, contextHeader.Number.Uint64()-1)
	if parentHeader == nil {
		return fmt.Errorf("parent block header not found: parentHash=%x, parentNumber=%d",
			contextHeader.ParentHash, contextHeader.Number.Uint64()-1)
	}

	// Get witness pre-state root (from first header which should be parent).
	witnessPreStateRoot := witness.Root()

	// Compare with actual parent block's state root.
	if witnessPreStateRoot != parentHeader.Root {
		return fmt.Errorf("witness pre-state root mismatch: witness=%x, parent=%x, blockNumber=%d",
			witnessPreStateRoot, parentHeader.Root, contextHeader.Number.Uint64())
	}

	return nil
}

// Witness encompasses the state required to apply a set of transactions and
// derive a post state/receipt root.
type Witness struct {
	context *types.Header // Header to which this witness belongs to, with rootHash and receiptHash zeroed out

	Headers []*types.Header     // Past headers in reverse order (0=parent, 1=parent's-parent, etc). First *must* be set.
	Codes   map[string]struct{} // Set of bytecodes ran or accessed
	State   map[string]struct{} // Set of MPT state trie nodes (account and storage together)

	chain HeaderReader // Chain reader to convert block hash ops to header proofs
	lock  sync.RWMutex // Lock to allow concurrent state insertions
}

// NewWitness creates an empty witness ready for population.
func NewWitness(context *types.Header, chain HeaderReader) (*Witness, error) {
	// When building witnesses, retrieve the parent header, which will *always*
	// be included to act as a trustless pre-root hash container
	var headers []*types.Header
	if chain != nil {
		parent := chain.GetHeader(context.ParentHash, context.Number.Uint64()-1)
		if parent == nil {
			return nil, errors.New("failed to retrieve parent header")
		}
		headers = append(headers, parent)
	}
	// Create the wtness with a reconstructed gutted out block
	return &Witness{
		context: context,
		Headers: headers,
		Codes:   make(map[string]struct{}),
		State:   make(map[string]struct{}),
		chain:   chain,
	}, nil
}

// AddBlockHash adds a "blockhash" to the witness with the designated offset from
// chain head. Under the hood, this method actually pulls in enough headers from
// the chain to cover the block being added.
func (w *Witness) AddBlockHash(number uint64) {
	// Keep pulling in headers until this hash is populated
	for int(w.context.Number.Uint64()-number) > len(w.Headers) {
		tail := w.Headers[len(w.Headers)-1]
		w.Headers = append(w.Headers, w.chain.GetHeader(tail.ParentHash, tail.Number.Uint64()-1))
	}
}

// AddCode adds a bytecode blob to the witness.
func (w *Witness) AddCode(code []byte) {
	if len(code) == 0 {
		return
	}
	w.Codes[string(code)] = struct{}{}
}

// AddState inserts a batch of MPT trie nodes into the witness.
func (w *Witness) AddState(nodes map[string]struct{}) {
	if len(nodes) == 0 {
		return
	}
	w.lock.Lock()
	defer w.lock.Unlock()

	maps.Copy(w.State, nodes)
}

// Copy deep-copies the witness object.  Witness.Block isn't deep-copied as it
// is never mutated by Witness
func (w *Witness) Copy() *Witness {
	w.lock.RLock()
	defer w.lock.RUnlock()
	cpy := &Witness{
		Headers: slices.Clone(w.Headers),
		Codes:   maps.Clone(w.Codes),
		State:   maps.Clone(w.State),
		chain:   w.chain,
	}
	if w.context != nil {
		cpy.context = types.CopyHeader(w.context)
	}
	return cpy
}

// Root returns the pre-state root from the first header.
//
// Note, this method will panic in case of a bad witness (but RLP decoding will
// sanitize it and fail before that).
func (w *Witness) Root() common.Hash {
	return w.Headers[0].Root
}

func (w *Witness) Header() *types.Header {
	return w.context
}

func (w *Witness) SetHeader(header *types.Header) {
	if w != nil {
		w.context = header
	}
}

// CompressionConfig holds configuration for witness compression
type CompressionConfig struct {
	Enabled          bool // Enable/disable compression
	Threshold        int  // Threshold in bytes. Only compress if witness is larger than this.
	CompressionLevel int  // Gzip compression level (1-9)
	UseDeduplication bool // Enable witness optimization
}

const compressionThreshold = 1 * 1024 * 1024

func DefaultCompressionConfig() *CompressionConfig {
	return &CompressionConfig{
		Enabled:          true,
		Threshold:        compressionThreshold,
		CompressionLevel: gzip.BestSpeed,
		UseDeduplication: true,
	}
}

var globalCompressionConfig = DefaultCompressionConfig()

// EncodeCompressed serializes a witness with optional compression.
func (w *Witness) EncodeCompressed(wr io.Writer) error {
	// First encode to RLP
	var rlpBuf bytes.Buffer
	if err := w.EncodeRLP(&rlpBuf); err != nil {
		return err
	}

	rlpData := rlpBuf.Bytes()

	// Only compress if enabled and the data is large enough to benefit from compression
	if globalCompressionConfig.Enabled && len(rlpData) > globalCompressionConfig.Threshold {
		// Compress the RLP data
		var compressedBuf bytes.Buffer
		gw, err := gzip.NewWriterLevel(&compressedBuf, globalCompressionConfig.CompressionLevel)
		if err != nil {
			return err
		}

		if _, err := gw.Write(rlpData); err != nil {
			return err
		}

		if err := gw.Close(); err != nil {
			return err
		}

		compressedData := compressedBuf.Bytes()

		// Only use compression if it actually reduces size
		if len(compressedData) < len(rlpData) {
			// Write compression marker and compressed data
			if _, err := wr.Write([]byte{0x01}); err != nil {
				return err
			}
			_, err = wr.Write(compressedData)
			return err
		}
	}

	// Write uncompressed marker and original RLP data
	if _, err := wr.Write([]byte{0x00}); err != nil {
		return err
	}
	_, err := wr.Write(rlpData)
	return err
}

// DecodeCompressed decodes a witness from compressed format.
func (w *Witness) DecodeCompressed(data []byte) error {
	if len(data) == 0 {
		return errors.New("empty data")
	}

	// Check compression marker
	compressed := data[0] == 0x01
	witnessData := data[1:]

	var rlpData []byte
	if compressed {
		// Decompress
		gr, err := gzip.NewReader(bytes.NewReader(witnessData))
		if err != nil {
			return err
		}
		defer gr.Close()

		var decompressedBuf bytes.Buffer
		if _, err := io.Copy(&decompressedBuf, gr); err != nil {
			return err
		}
		rlpData = decompressedBuf.Bytes()
	} else {
		rlpData = witnessData
	}

	// Decode the RLP data
	var ext extWitness
	if err := rlp.DecodeBytes(rlpData, &ext); err != nil {
		return err
	}

	return w.fromExtWitness(&ext)
}
