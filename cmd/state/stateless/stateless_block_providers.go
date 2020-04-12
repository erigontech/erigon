package stateless

import (
	"compress/gzip"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

const (
	fileSchemeExportfile = "exportfile"
	fileSchemeDb         = "db"
)

type BlockProvider interface {
	io.Closer
	FastFwd(uint64) error
	NextBlock() (*types.Block, error)
}

func BlockProviderForURI(uri string, createDbFunc CreateDbFunc) (BlockProvider, error) {
	url, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	switch url.Scheme {
	case fileSchemeExportfile:
		fmt.Println("Source of blocks: export file @", url.Path)
		return NewBlockProviderFromExportFile(url.Path)
	case fileSchemeDb:
		fallthrough
	default:
		fmt.Println("Source of blocks: db @", url.Path)
		return NewBlockProviderFromDb(url.Path, createDbFunc)
	}
}

type BlockChainBlockProvider struct {
	currentBlock uint64
	bc           *core.BlockChain
	db           ethdb.Database
}

func NewBlockProviderFromDb(path string, createDbFunc CreateDbFunc) (BlockProvider, error) {
	ethDb, err := createDbFunc(path)
	if err != nil {
		return nil, err
	}
	chainConfig := params.MainnetChainConfig
	engine := ethash.NewFullFaker()
	chain, err := core.NewBlockChain(ethDb, nil, chainConfig, engine, vm.Config{}, nil)
	if err != nil {
		return nil, err
	}

	return &BlockChainBlockProvider{
		bc: chain,
	}, nil
}

func (p *BlockChainBlockProvider) Close() error {
	p.db.Close()
	return nil
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
	for {
		if err := p.stream.Decode(&b); err == io.EOF {
			return nil
		} else if err != nil {
			return fmt.Errorf("error fast fwd: %v", err)
		} else if b.NumberU64() >= to-1 {
			return nil
		}
	}
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
