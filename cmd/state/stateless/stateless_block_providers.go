package stateless

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"runtime"
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
	fileSchemeDB         = "db"
)

type BlockProvider interface {
	core.ChainContext
	io.Closer
	FastFwd(uint64) error
	NextBlock() (*types.Block, error)
}

func BlockProviderForURI(uri string, createDBFunc CreateDbFunc) (BlockProvider, error) {
	url, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	switch url.Scheme {
	case fileSchemeExportfile:
		fmt.Println("Source of blocks: export file @", url.Path)
		return NewBlockProviderFromExportFile(url.Path)
	case fileSchemeDB:
		fallthrough
	default:
		fmt.Println("Source of blocks: db @", url.Path)
		return NewBlockProviderFromDB(url.Path, createDBFunc)
	}
}

type BlockChainBlockProvider struct {
	currentBlock uint64
	bc           *core.BlockChain
	db           ethdb.Database
}

func NewBlockProviderFromDB(path string, createDBFunc CreateDbFunc) (BlockProvider, error) {
	ethDB, err := createDBFunc(path)
	if err != nil {
		return nil, err
	}
	chainConfig := params.MainnetChainConfig
	engine := ethash.NewFullFaker()
	txCacher := core.NewTxSenderCacher(runtime.NumCPU())
	chain, err := core.NewBlockChain(ethDB, nil, chainConfig, engine, vm.Config{}, nil, txCacher)
	if err != nil {
		return nil, err
	}

	return &BlockChainBlockProvider{
		bc: chain,
		db: ethDB,
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
	stream          *rlp.Stream
	engine          consensus.Engine
	headersDB       ethdb.Database
	batch           ethdb.DbWithPendingMutations
	fh              *os.File
	reader          io.Reader
	lastBlockNumber int64
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
	headersDB := ethdb.MustOpen(getTempFileName())
	return &ExportFileBlockProvider{stream, engine, headersDB, nil, fh, reader, -1}, nil
}

func getTempFileName() string {
	tmpfile, err := ioutil.TempFile("", "headers.*")
	if err != nil {
		panic(fmt.Errorf("failed to create a temp file: %w", err))
	}
	tmpfile.Close()
	fmt.Printf("creating a temp headers db @ %s\n", tmpfile.Name())
	return tmpfile.Name()
}

func (p *ExportFileBlockProvider) Close() error {
	return p.fh.Close()
}

func (p *ExportFileBlockProvider) WriteHeader(h *types.Header) {
	if p.batch == nil {
		p.batch = p.headersDB.NewBatch()
	}

	rawdb.WriteHeader(context.TODO(), p.batch, h)

	if p.batch.BatchSize() > 1000 {
		if _, err := p.batch.Commit(); err != nil {
			panic(fmt.Errorf("error writing headers: %w", err))
		}
		p.batch = nil
	}
}

func (p *ExportFileBlockProvider) resetStream() error {
	if _, err := p.fh.Seek(0, 0); err != nil {
		return err
	}
	if p.reader != p.fh {
		if err := p.reader.(*gzip.Reader).Reset(p.fh); err != nil {
			return err
		}
	}
	p.stream = rlp.NewStream(p.reader, 0)
	p.lastBlockNumber = -1
	return nil
}

func (p *ExportFileBlockProvider) FastFwd(to uint64) error {
	if to == 0 {
		fmt.Println("fastfwd: reseting stream")
		if err := p.resetStream(); err != nil {
			return err
		}
	}
	if p.lastBlockNumber == int64(to)-1 {
		fmt.Println("fastfwd: nothing to do there")
		return nil
	} else if p.lastBlockNumber > int64(to)-1 {
		fmt.Println("fastfwd: resetting stream")
		if err := p.resetStream(); err != nil {
			return err
		}
	}
	var b types.Block
	for {
		if err := p.stream.Decode(&b); err == io.EOF {
			return nil
		} else if err != nil {
			return fmt.Errorf("error fast fwd: %v", err)
		} else {
			p.WriteHeader(b.Header())
			p.lastBlockNumber = int64(b.NumberU64())
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

	p.lastBlockNumber = int64(b.NumberU64())
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
	return rawdb.ReadHeader(p.headersDB, h, i)
}
