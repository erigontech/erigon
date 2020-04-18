package stateless

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"strings"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
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
	core.ChainContext
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
		db: ethDb,
	}, nil
}

func (p *BlockChainBlockProvider) Engine() consensus.Engine {
	return p.bc.Engine()
}

func (p *BlockChainBlockProvider) GetHeader(h common.Hash, i uint64) *types.Header {
	return p.bc.GetHeader(h, i)
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
	stream    *rlp.Stream
	engine    consensus.Engine
	headersDb ethdb.Database
	batch     ethdb.DbWithPendingMutations
	fh        io.Closer
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
	engine := ethash.NewFullFaker()
	// keeping all the past block headers in memory
	headersDb := mustCreateTempDatabase()
	return &ExportFileBlockProvider{stream, engine, headersDb, nil, fh}, nil
}

func getTempFileName() string {
	tmpfile, err := ioutil.TempFile("", "headers.bolt")
	if err != nil {
		panic(fmt.Errorf("failed to create a temp file: %w", err))
	}
	tmpfile.Close()
	fmt.Printf("creating a temp headers db @ %s\n", tmpfile.Name())
	return tmpfile.Name()
}

func mustCreateTempDatabase() ethdb.Database {
	db, err := ethdb.NewBoltDatabase(getTempFileName())
	if err != nil {
		panic(fmt.Errorf("failed to create a temp db for headers: %w", err))
	}
	return db
}

func (p *ExportFileBlockProvider) Close() error {
	return p.fh.Close()
}

func (p *ExportFileBlockProvider) WriteHeader(h *types.Header) {
	if p.batch == nil {
		p.batch = p.headersDb.NewBatch()
	}

	rawdb.WriteHeader(context.TODO(), p.batch, h)

	if p.batch.BatchSize() > 1000 {
		if _, err := p.batch.Commit(); err != nil {
			panic(fmt.Errorf("error writing headers: %w", err))
		}
		p.batch = nil
	}
}

func (p *ExportFileBlockProvider) FastFwd(to uint64) error {
	var b types.Block
	for {
		if err := p.stream.Decode(&b); err == io.EOF {
			return nil
		} else if err != nil {
			return fmt.Errorf("error fast fwd: %v", err)
		} else {
			p.WriteHeader(b.Header())
			if b.NumberU64() >= to-1 {
				return nil
			}
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

	p.WriteHeader(b.Header())
	return &b, nil
}

func (p *ExportFileBlockProvider) Engine() consensus.Engine {
	return p.engine
}

func (p *ExportFileBlockProvider) GetHeader(h common.Hash, i uint64) *types.Header {
	if p.batch != nil {
		return rawdb.ReadHeader(p.batch, h, i)
	}
	return rawdb.ReadHeader(p.headersDb, h, i)
}
