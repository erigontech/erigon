package client

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon/zk/datastream/proto/github.com/0xPolygonHermez/zkevm-node/state/datastream"
	"github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/ledgerwatch/log/v3"
)

type StreamType uint64
type Command uint64

type EntityDefinition struct {
	Name       string
	StreamType StreamType
	Definition reflect.Type
}

const (
	versionProto         = 2 // converted to proto
	versionAddedBlockEnd = 3 // Added block end
)

type StreamClient struct {
	ctx          context.Context
	server       string // Server address to connect IP:port
	version      int
	streamType   StreamType
	conn         net.Conn
	id           string            // Client id
	Header       types.HeaderEntry // Header info received (from Header command)
	checkTimeout time.Duration     // time to wait for data before reporting an error

	// atomic
	lastWrittenTime atomic.Int64
	streaming       atomic.Bool
	progress        atomic.Uint64

	// Channels
	batchStartChan chan types.BatchStart
	batchEndChan   chan types.BatchEnd
	l2BlockChan    chan types.FullL2Block
	l2TxChan       chan types.L2TransactionProto
	gerUpdatesChan chan types.GerUpdate // NB: unused from etrog onwards (forkid 7)

	// keeps track of the latest fork from the stream to assign to l2 blocks
	currentFork uint64
}

const (
	// StreamTypeSequencer represents a Sequencer stream
	StSequencer StreamType = 1

	// Packet types
	PtPadding = 0
	PtHeader  = 1    // Just for the header page
	PtData    = 2    // Data entry
	PtResult  = 0xff // Not stored/present in file (just for client command result)
)

// Creates a new client fo datastream
// server must be in format "url:port"
func NewClient(ctx context.Context, server string, version int, checkTimeout time.Duration, latestDownloadedForkId uint16) *StreamClient {
	c := &StreamClient{
		ctx:            ctx,
		checkTimeout:   checkTimeout,
		server:         server,
		version:        version,
		streamType:     StSequencer,
		id:             "",
		batchStartChan: make(chan types.BatchStart, 100),
		batchEndChan:   make(chan types.BatchEnd, 100),
		l2BlockChan:    make(chan types.FullL2Block, 100000),
		gerUpdatesChan: make(chan types.GerUpdate, 1000),
		currentFork:    uint64(latestDownloadedForkId),
	}

	return c
}

func (c *StreamClient) IsVersion3() bool {
	return c.version >= versionAddedBlockEnd
}

func (c *StreamClient) GetBatchStartChan() chan types.BatchStart {
	return c.batchStartChan
}
func (c *StreamClient) GetBatchEndChan() chan types.BatchEnd {
	return c.batchEndChan
}
func (c *StreamClient) GetL2BlockChan() chan types.FullL2Block {
	return c.l2BlockChan
}
func (c *StreamClient) GetL2TxChan() chan types.L2TransactionProto {
	return c.l2TxChan
}
func (c *StreamClient) GetGerUpdatesChan() chan types.GerUpdate {
	return c.gerUpdatesChan
}
func (c *StreamClient) GetLastWrittenTimeAtomic() *atomic.Int64 {
	return &c.lastWrittenTime
}
func (c *StreamClient) GetStreamingAtomic() *atomic.Bool {
	return &c.streaming
}
func (c *StreamClient) GetProgressAtomic() *atomic.Uint64 {
	return &c.progress
}

// Opens a TCP connection to the server
func (c *StreamClient) Start() error {
	// Connect to server
	var err error
	c.conn, err = net.Dial("tcp", c.server)
	if err != nil {
		return fmt.Errorf("error connecting to server %s: %v", c.server, err)
	}

	c.id = c.conn.LocalAddr().String()

	return nil
}

func (c *StreamClient) Stop() {
	c.conn.Close()

	close(c.l2BlockChan)
	close(c.gerUpdatesChan)
}

// Command header: Get status
// Returns the current status of the header.
// If started, terminate the connection.
func (c *StreamClient) GetHeader() error {
	if err := c.sendHeaderCmd(); err != nil {
		return fmt.Errorf("%s send header error: %v", c.id, err)
	}

	// Read packet
	packet, err := readBuffer(c.conn, 1)
	if err != nil {
		return fmt.Errorf("%s read buffer: %v", c.id, err)
	}

	// Check packet type
	if packet[0] != PtResult {
		return fmt.Errorf("%s error expecting result packet type %d and received %d", c.id, PtResult, packet[0])
	}

	// Read server result entry for the command
	r, err := c.readResultEntry(packet)
	if err != nil {
		return fmt.Errorf("%s read result entry error: %v", c.id, err)
	}
	if err := r.GetError(); err != nil {
		return fmt.Errorf("%s got Result error code %d: %v", c.id, r.ErrorNum, err)
	}

	// Read header entry
	h, err := c.readHeaderEntry()
	if err != nil {
		return fmt.Errorf("%s read header entry error: %v", c.id, err)
	}

	c.Header = *h

	return nil
}

func (c *StreamClient) ExecutePerFile(bookmark *types.BookmarkProto, function func(file *types.FileEntry) error) error {
	// Get header from server
	if err := c.GetHeader(); err != nil {
		return fmt.Errorf("%s get header error: %v", c.id, err)
	}

	protoBookmark, err := bookmark.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal bookmark: %v", err)
	}

	if err := c.initiateDownloadBookmark(protoBookmark); err != nil {
		return err
	}
	count := uint64(0)
	logTicker := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-logTicker.C:
			fmt.Println("Entries read count: ", count)
		default:
		}
		if c.Header.TotalEntries == count {
			break
		}
		file, err := c.readFileEntry()
		if err != nil {
			return fmt.Errorf("error reading file entry: %v", err)
		}
		if err := function(file); err != nil {
			return fmt.Errorf("error executing function: %v", err)

		}
		count++
	}

	return nil
}

func (c *StreamClient) EnsureConnected() (bool, error) {
	if c.conn == nil {
		if err := c.tryReConnect(); err != nil {
			return false, fmt.Errorf("failed to reconnect the datastream client: %w", err)
		}
		log.Info("[datastream_client] Datastream client connected.")
	}

	return true, nil
}

// reads entries to the end of the stream
// at end will wait for new entries to arrive
func (c *StreamClient) ReadAllEntriesToChannel() error {
	c.streaming.Store(true)
	defer c.streaming.Store(false)

	var bookmark *types.BookmarkProto
	progress := c.progress.Load()
	if progress == 0 {
		bookmark = types.NewBookmarkProto(0, datastream.BookmarkType_BOOKMARK_TYPE_BATCH)
	} else {
		bookmark = types.NewBookmarkProto(progress, datastream.BookmarkType_BOOKMARK_TYPE_L2_BLOCK)
	}

	protoBookmark, err := bookmark.Marshal()
	if err != nil {
		return err
	}

	// send start command
	if err := c.initiateDownloadBookmark(protoBookmark); err != nil {
		return err
	}

	if err := c.readAllFullL2BlocksToChannel(); err != nil {
		err2 := fmt.Errorf("%s read full L2 blocks error: %v", c.id, err)

		if c.conn != nil {
			if err2 := c.conn.Close(); err2 != nil {
				log.Error("failed to close connection after error", "original-error", err, "new-error", err2)
			}
			c.conn = nil
		}

		// reset the channels as there could be data ahead of the bookmark we want to track here.
		// c.resetChannels()

		return err2
	}

	return nil
}

// runs the prerequisites for entries download
func (c *StreamClient) initiateDownloadBookmark(bookmark []byte) error {
	// send start command
	if err := c.sendStartBookmarkCmd(bookmark); err != nil {
		return err
	}

	if err := c.afterStartCommand(); err != nil {
		return fmt.Errorf("after start command error: %v", err)
	}

	return nil
}

func (c *StreamClient) afterStartCommand() error {
	// Read packet
	packet, err := readBuffer(c.conn, 1)
	if err != nil {
		return fmt.Errorf("read buffer error %v", err)
	}

	// Read server result entry for the command
	r, err := c.readResultEntry(packet)
	if err != nil {
		return fmt.Errorf("read result entry error: %v", err)
	}

	if err := r.GetError(); err != nil {
		return fmt.Errorf("got Result error code %d: %v", r.ErrorNum, err)
	}

	return nil
}

// reads all entries from the server and sends them to a channel
// sends the parsed FullL2Blocks with transactions to a channel
func (c *StreamClient) readAllFullL2BlocksToChannel() error {
	var err error

LOOP:
	for {
		select {
		default:
		case <-c.ctx.Done():
			log.Warn("[Datastream client] Context done - stopping")
			break LOOP
		}

		if c.checkTimeout > 0 {
			c.conn.SetReadDeadline(time.Now().Add(c.checkTimeout))
		}

		fullBlock, batchStart, batchEnd, gerUpdate, batchBookmark, blockBookmark, localErr := c.readFullBlockProto()
		if localErr != nil {
			err = localErr
			break
		}
		c.lastWrittenTime.Store(time.Now().UnixNano())

		// skip over bookmarks (but only when fullblock is nil or will miss l2 blocks)
		if batchBookmark != nil || blockBookmark != nil {
			continue
		}

		// write batch starts to channel
		if batchStart != nil {
			c.currentFork = (*batchStart).ForkId
			c.batchStartChan <- *batchStart
		}

		if gerUpdate != nil {
			c.gerUpdatesChan <- *gerUpdate
		}

		if batchEnd != nil {
			// this check was inside c.readFullBlockProto() but it is better to move it here
			c.batchEndChan <- *batchEnd
		}

		// ensure the block is assigned the currently known fork
		if fullBlock != nil {
			fullBlock.ForkId = c.currentFork
			log.Trace("writing block to channel", "blockNumber", fullBlock.L2BlockNumber, "batchNumber", fullBlock.BatchNumber)
			c.l2BlockChan <- *fullBlock
		}
	}

	return err
}

func (c *StreamClient) tryReConnect() error {
	var err error
	for i := 0; i < 50; i++ {
		if c.conn != nil {
			if err := c.conn.Close(); err != nil {
				return err
			}
			c.conn = nil
		}
		if err = c.Start(); err != nil {
			time.Sleep(5 * time.Second)
			continue
		}
		return nil
	}

	return err
}

func (c *StreamClient) readFullBlockProto() (
	l2Block *types.FullL2Block,
	batchStart *types.BatchStart,
	batchEnd *types.BatchEnd,
	gerUpdate *types.GerUpdate,
	batchBookmark *types.BookmarkProto,
	blockBookmark *types.BookmarkProto,
	err error,
) {
	file, err := c.readFileEntry()
	if err != nil {
		err = fmt.Errorf("read file entry error: %v", err)
		return
	}

	switch file.EntryType {
	case types.BookmarkEntryType:
		var bookmark *types.BookmarkProto
		if bookmark, err = types.UnmarshalBookmark(file.Data); err != nil {
			return
		}
		if bookmark.BookmarkType() == datastream.BookmarkType_BOOKMARK_TYPE_BATCH {
			batchBookmark = bookmark
			return
		} else {
			blockBookmark = bookmark
			return
		}
	case types.EntryTypeGerUpdate:
		if gerUpdate, err = types.DecodeGerUpdateProto(file.Data); err != nil {
			return
		}
		log.Trace("ger update", "ger", gerUpdate)
		return
	case types.EntryTypeBatchStart:
		if batchStart, err = types.UnmarshalBatchStart(file.Data); err != nil {
			return
		}
		return
	case types.EntryTypeBatchEnd:
		if batchEnd, err = types.UnmarshalBatchEnd(file.Data); err != nil {
			return
		}
		return
	case types.EntryTypeL2Block:
		if l2Block, err = types.UnmarshalL2Block(file.Data); err != nil {
			return
		}

		txs := []types.L2TransactionProto{}

		var innerFile *types.FileEntry
		var l2Tx *types.L2TransactionProto
	LOOP:
		for {
			if innerFile, err = c.readFileEntry(); err != nil {
				return
			}

			if innerFile.IsL2Tx() {
				if l2Tx, err = types.UnmarshalTx(innerFile.Data); err != nil {
					return
				}
				txs = append(txs, *l2Tx)
			} else if innerFile.IsL2BlockEnd() {
				var l2BlockEnd *types.L2BlockEndProto
				if l2BlockEnd, err = types.UnmarshalL2BlockEnd(innerFile.Data); err != nil {
					return
				}
				if l2BlockEnd.GetBlockNumber() != l2Block.L2BlockNumber {
					err = fmt.Errorf("block end number (%d) not equal to block number (%d)", l2BlockEnd.GetBlockNumber(), l2Block.L2BlockNumber)
					return
				}
				break LOOP
			} else if innerFile.IsBookmark() {
				var bookmark *types.BookmarkProto
				if bookmark, err = types.UnmarshalBookmark(innerFile.Data); err != nil || bookmark == nil {
					return
				}
				if bookmark.BookmarkType() == datastream.BookmarkType_BOOKMARK_TYPE_L2_BLOCK {
					break LOOP
				} else {
					err = fmt.Errorf("unexpected bookmark type inside block: %v", bookmark.Type())
					return
				}
			} else if innerFile.IsBatchEnd() {
				if batchEnd, err = types.UnmarshalBatchEnd(file.Data); err != nil {
					return
				}
				break LOOP
			} else {
				err = fmt.Errorf("unexpected entry type inside a block: %d", innerFile.EntryType)
				return
			}
		}

		l2Block.L2Txs = txs
		return
	case types.EntryTypeL2Tx:
		err = fmt.Errorf("unexpected l2Tx out of block")
		return
	default:
		err = fmt.Errorf("unexpected entry type: %d", file.EntryType)
		return
	}
}

// reads file bytes from socket and tries to parse them
// returns the parsed FileEntry
func (c *StreamClient) readFileEntry() (file *types.FileEntry, err error) {
	// Read packet type
	packet, err := readBuffer(c.conn, 1)
	if err != nil {
		return file, fmt.Errorf("failed to read packet type: %v", err)
	}

	// Check packet type
	if packet[0] == PtResult {
		// Read server result entry for the command
		r, err := c.readResultEntry(packet)
		if err != nil {
			return file, err
		}
		if err := r.GetError(); err != nil {
			return file, fmt.Errorf("got Result error code %d: %v", r.ErrorNum, err)
		}
		return file, nil
	} else if packet[0] != PtData {
		return file, fmt.Errorf("error expecting data packet type %d and received %d", PtData, packet[0])
	}

	// Read the rest of fixed size fields
	buffer, err := readBuffer(c.conn, types.FileEntryMinSize-1)
	if err != nil {
		return file, fmt.Errorf("error reading file bytes: %v", err)
	}
	buffer = append(packet, buffer...)

	// Read variable field (data)
	length := binary.BigEndian.Uint32(buffer[1:5])
	if length < types.FileEntryMinSize {
		return file, errors.New("error reading data entry: wrong data length")
	}

	// Read rest of the file data
	bufferAux, err := readBuffer(c.conn, length-types.FileEntryMinSize)
	if err != nil {
		return file, fmt.Errorf("error reading file data bytes: %v", err)
	}
	buffer = append(buffer, bufferAux...)

	// Decode binary data to data entry struct
	if file, err = types.DecodeFileEntry(buffer); err != nil {
		return file, fmt.Errorf("decode file entry error: %v", err)
	}

	return
}

// reads header bytes from socket and tries to parse them
// returns the parsed HeaderEntry
func (c *StreamClient) readHeaderEntry() (h *types.HeaderEntry, err error) {

	// Read header stream bytes
	binaryHeader, err := readBuffer(c.conn, types.HeaderSizePreEtrog)
	if err != nil {
		return h, fmt.Errorf("failed to read header bytes %v", err)
	}

	headLength := binary.BigEndian.Uint32(binaryHeader[1:5])
	if headLength == types.HeaderSize {
		// Read the rest of fixed size fields
		buffer, err := readBuffer(c.conn, types.HeaderSize-types.HeaderSizePreEtrog)
		if err != nil {
			return h, fmt.Errorf("failed to read header bytes %v", err)
		}
		binaryHeader = append(binaryHeader, buffer...)
	}

	// Decode bytes stream to header entry struct
	if h, err = types.DecodeHeaderEntry(binaryHeader); err != nil {
		return h, fmt.Errorf("error decoding binary header: %v", err)
	}

	return
}

// reads result bytes and tries to parse them
// returns the parsed ResultEntry
func (c *StreamClient) readResultEntry(packet []byte) (re *types.ResultEntry, err error) {
	if len(packet) != 1 {
		return re, fmt.Errorf("expected packet size of 1, got: %d", len(packet))
	}

	// Read the rest of fixed size fields
	buffer, err := readBuffer(c.conn, types.ResultEntryMinSize-1)
	if err != nil {
		return re, fmt.Errorf("failed to read main result bytes %v", err)
	}
	buffer = append(packet, buffer...)

	// Read variable field (errStr)
	length := binary.BigEndian.Uint32(buffer[1:5])
	if length < types.ResultEntryMinSize {
		return re, fmt.Errorf("%s Error reading result entry", c.id)
	}

	// read the rest of the result
	bufferAux, err := readBuffer(c.conn, length-types.ResultEntryMinSize)
	if err != nil {
		return re, fmt.Errorf("failed to read result errStr bytes %v", err)
	}
	buffer = append(buffer, bufferAux...)

	// Decode binary entry result
	if re, err = types.DecodeResultEntry(buffer); err != nil {
		return re, fmt.Errorf("decode result entry error: %v", err)
	}

	return re, nil
}
