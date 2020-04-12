package stateless

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

type BlockProvider interface {
	FastFwd(uint64) error
	NextBlock() (*types.Block, error)
}

type BlockChainBlockProvider struct {
	currentBlock uint64
	bc           *core.BlockChain
}

func NewBlockProviderFromBlockChain(bc *core.BlockChain) BlockProvider {
	return &BlockChainBlockProvider{
		bc: bc,
	}
}

func (p *BlockChainBlockProvider) FastFwd(to uint64) error {
	p.currentBlock = to
	return nil
}

func (p *BlockChainBlockProvider) NextBlock() (*types.Block, error) {
	block := p.bc.GetBlockByNumber(p.currentBlock)
	p.currentBlock++
	return block, nil
}

type ExportFileBlockProvider struct {
	stream *rlp.Stream
	fh     io.Closer
}

func NewBlockProviderFromExportFile(fn string) (BlockProvider, error) {
	// Open the file handle and potentially unwrap the gzip stream
	fh, err := os.Open(fn)
	if err != nil {
		return nil, err
	}

	var reader io.Reader = fh
	if strings.HasSuffix(fn, ".gz") {
		if reader, err = gzip.NewReader(reader); err != nil {
			return nil, err
		}
	}
	stream := rlp.NewStream(reader, 0)
	return &ExportFileBlockProvider{stream, fh}, nil
}

func (p *ExportFileBlockProvider) Close() error {
	return p.fh.Close()
}

func (p *ExportFileBlockProvider) FastFwd(to uint64) error {
	var b types.Block
	for true {
		if err := p.stream.Decode(&b); err == io.EOF {
			return nil
		} else if err != nil {
			return fmt.Errorf("error fast fwd: %v", err)
		} else if b.NumberU64() >= to-1 {
			return nil
		}
	}
	return nil
}

func (p *ExportFileBlockProvider) NextBlock() (*types.Block, error) {
	var b types.Block
	if err := p.stream.Decode(&b); err == io.EOF {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("error fast fwd: %v", err)
	}
	return &b, nil
}
