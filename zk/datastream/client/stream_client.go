package client

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon/zk/datastream/types"
)

type StreamType uint64
type Command uint64

type EntityDefinition struct {
	Name       string
	StreamType StreamType
	Definition reflect.Type
}

type StreamClient struct {
	server     string // Server address to connect IP:port
	streamType StreamType
	conn       net.Conn
	id         string            // Client id
	Header     types.HeaderEntry // Header info received (from Header command)

	entriesDefinition map[types.EntryType]EntityDefinition

	// atomic
	LastWrittenTime atomic.Int64
	Streaming       atomic.Bool

	// Channels
	L2BlockChan    chan types.FullL2Block
	GerUpdatesChan chan types.GerUpdate
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
func NewClient(server string) *StreamClient {
	// Create the client data stream
	c := &StreamClient{
		server:     server,
		streamType: StSequencer,
		id:         "",
		entriesDefinition: map[types.EntryType]EntityDefinition{
			types.EntryTypeStartL2Block: {
				Name:       "StartL2Block",
				StreamType: StSequencer,
				Definition: reflect.TypeOf(types.StartL2Block{}),
			},
			types.EntryTypeL2Tx: {
				Name:       "L2Transaction",
				StreamType: StSequencer,
				Definition: reflect.TypeOf(types.L2Transaction{}),
			},
			types.EntryTypeEndL2Block: {
				Name:       "EndL2Block",
				StreamType: StSequencer,
				Definition: reflect.TypeOf(types.EndL2Block{}),
			},
			types.EntryTypeGerUpdate: {
				Name:       "GerUpdate",
				StreamType: StSequencer,
				Definition: reflect.TypeOf(types.GerUpdate{}),
			},
		},
		L2BlockChan:    make(chan types.FullL2Block, 100000),
		GerUpdatesChan: make(chan types.GerUpdate, 1000),
	}

	return c
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

	close(c.L2BlockChan)
	close(c.GerUpdatesChan)
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
		return fmt.Errorf("%s read buffer error: %v", c.id, err)
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

// sends start command, reads entries until limit reached and sends end command
func (c *StreamClient) ReadEntries(bookmark *types.Bookmark, l2BlocksAmount int) (*[]types.FullL2Block, *[]types.GerUpdate, map[uint64][]byte, uint64, error) {
	// Get header from server
	if err := c.GetHeader(); err != nil {
		return nil, nil, nil, 0, fmt.Errorf("%s get header error: %v", c.id, err)
	}

	// send start command
	if err := c.initiateDownloadBookmark(bookmark.Encode()); err != nil {
		return nil, nil, nil, 0, ErrBadBookmark
	}

	fullL2Blocks, gerUpates, bookmarks, entriesRead, err := c.readFullL2Blocks(l2BlocksAmount)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	return fullL2Blocks, gerUpates, bookmarks, entriesRead, nil
}

// reads entries to the end of the stream
// at end will wait for new entries to arrive
func (c *StreamClient) ReadAllEntriesToChannel(bookmark *types.Bookmark) error {
	// send start command
	if err := c.initiateDownloadBookmark(bookmark.Encode()); err != nil {
		return ErrBadBookmark
	}

	if err := c.readAllFullL2BlocksToChannel(); err != nil {
		return fmt.Errorf("%s read full L2 blocks error: %v", c.id, err)
	}

	return nil
}

// runs the prerequisites for entries download
func (c *StreamClient) initiateDownloadBookmark(bookmark []byte) error {
	// send start command
	if err := c.sendStartBookmarkCmd(bookmark); err != nil {
		return fmt.Errorf("send start command error: %v", err)
	}

	if err := c.afterStartCommand(); err != nil {
		return fmt.Errorf("after start command error: %v", err)
	}

	return nil
}

// runs the prerequisites for entries download
func (c *StreamClient) initiateDownload(fromEntry uint64) error {
	// send start command
	if err := c.sendStartCmd(fromEntry); err != nil {
		return fmt.Errorf("send start command error: %v", err)
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
	for {
		fullBlock, gerUpdates, _, _, _, err := c.readFullBlock()
		if err != nil {
			return fmt.Errorf("failed to read full block: %v", err)
		}

		if gerUpdates != nil {
			for _, gerUpdate := range *gerUpdates {
				c.GerUpdatesChan <- gerUpdate
			}
		}
		c.LastWrittenTime.Store(time.Now().UnixNano())
		c.Streaming.Store(true)
		c.L2BlockChan <- *fullBlock
	}

	c.Streaming.Store(false)
	return nil
}

// reads a set amount of l2blocks from the server and returns them
// returns the parsed FullL2Blocks with transactions and the amount of entries read
func (c *StreamClient) readFullL2Blocks(l2BlocksAmount int) (*[]types.FullL2Block, *[]types.GerUpdate, map[uint64][]byte, uint64, error) {
	fullL2Blocks := []types.FullL2Block{}
	totalGerUpdates := []types.GerUpdate{}
	entriesRead := uint64(0)
	bookmarks := map[uint64][]byte{}
	fromEntry := uint64(0)

	for {
		if len(fullL2Blocks) >= l2BlocksAmount || entriesRead+fromEntry >= c.Header.TotalEntries {
			break
		}
		fullBlock, gerUpdates, bookmark, fe, er, err := c.readFullBlock()
		if err != nil {
			return nil, nil, nil, 0, fmt.Errorf("failed to read full block: %v", err)
		}

		if fromEntry == 0 {
			fromEntry = fe
		}

		if gerUpdates != nil {
			totalGerUpdates = append(totalGerUpdates, *gerUpdates...)
		}
		entriesRead += er
		fullL2Blocks = append(fullL2Blocks, *fullBlock)
		bookmarks[fullBlock.L2BlockNumber] = bookmark
	}

	return &fullL2Blocks, &totalGerUpdates, bookmarks, entriesRead, nil
}

// reads a full block from the server
// returns the parsed FullL2Block and the amount of entries read
func (c *StreamClient) readFullBlock() (*types.FullL2Block, *[]types.GerUpdate, []byte, uint64, uint64, error) {
	entriesRead := uint64(0)

	// TODO: maybe parse it and return it if needed
	file, err := c.readFileEntry()
	if err != nil {
		return nil, nil, []byte{}, 0, 0, fmt.Errorf("read file entry error: %v", err)
	}
	entriesRead++
	fromEntry := file.EntryNum

	// read whatever might be between current position and block start
	gerUpdates := []types.GerUpdate{}
	var bookmark []byte
	for {
		if file.IsBlockStart() {
			break
		}

		if file.IsBookmark() {
			bookmark = file.Data
		} else if file.IsGerUpdate() {
			gerUpdate, err := types.DecodeGerUpdate(file.Data)
			if err != nil {
				return nil, nil, []byte{}, 0, 0, fmt.Errorf("parse gerUpdate error: %v", err)
			}
			gerUpdates = append(gerUpdates, *gerUpdate)
		} else {
			return nil, nil, []byte{}, 0, 0, fmt.Errorf("expected GerUpdate or Bookmark type, got type: %d", file.EntryType)
		}

		file, err = c.readFileEntry()
		if err != nil {
			return nil, nil, []byte{}, 0, 0, fmt.Errorf("read file entry error: %v", err)
		}
		entriesRead++
	}

	// should start with a StartL2Block entry, followed by
	// txs entries and ending with a block endL2BlockEntry
	var startL2Block *types.StartL2Block
	l2Txs := []types.L2Transaction{}
	var endL2Block *types.EndL2Block
	if file.IsBlockStart() {
		startL2Block, err = types.DecodeStartL2Block(file.Data)
		if err != nil {
			return nil, nil, []byte{}, 0, 0, fmt.Errorf("read start of block error: %v", err)
		}

		for {
			file, err := c.readFileEntry()
			if err != nil {
				return nil, nil, []byte{}, 0, 0, fmt.Errorf("read file entry error: %v", err)
			}

			entriesRead++

			if file.IsTx() {
				l2Tx, err := types.DecodeL2Transaction(file.Data)
				if err != nil {
					return nil, nil, []byte{}, 0, 0, fmt.Errorf("parse l2Transaction error: %v", err)
				}
				l2Txs = append(l2Txs, *l2Tx)
			} else if file.IsBlockEnd() {
				endL2Block, err = types.DecodeEndL2Block(file.Data)
				if err != nil {
					return nil, nil, []byte{}, 0, 0, fmt.Errorf("parse endL2Block error: %v", err)
				}
				if startL2Block.L2BlockNumber != endL2Block.L2BlockNumber {
					return nil, nil, []byte{}, 0, 0, fmt.Errorf("start block block number different than endBlock block number. StartBlock: %d, EndBlock: %d", startL2Block.L2BlockNumber, endL2Block.L2BlockNumber)
				}
				break
			} else {
				return nil, nil, []byte{}, 0, 0, fmt.Errorf("expected EndL2Block or L2Transaction type, got type: %d", file.EntryType)
			}
		}
	} else {
		return nil, nil, []byte{}, 0, 0, fmt.Errorf("expected StartL2Block, but got type: %d", file.EntryType)
	}

	fullL2Block := types.ParseFullL2Block(startL2Block, endL2Block, &l2Txs)

	return fullL2Block, &gerUpdates, bookmark, fromEntry, entriesRead, nil
}

// reads file bytes from socket and tries to parse them
// returns the parsed FileEntry
func (c *StreamClient) readFileEntry() (*types.FileEntry, error) {
	// Read packet type
	packet, err := readBuffer(c.conn, 1)
	if err != nil {
		return &types.FileEntry{}, fmt.Errorf("failed to read packet type: %v", err)
	}

	// Check packet type
	if packet[0] == PtResult {
		// Read server result entry for the command
		r, err := c.readResultEntry(packet)
		if err != nil {
			return &types.FileEntry{}, err
		}
		if err := r.GetError(); err != nil {
			return &types.FileEntry{}, fmt.Errorf("got Result error code %d: %v", r.ErrorNum, err)
		}
		return &types.FileEntry{}, nil
	} else if packet[0] != PtData {
		return &types.FileEntry{}, fmt.Errorf("error expecting data packet type %d and received %d", PtData, packet[0])
	}

	// Read the rest of fixed size fields
	buffer, err := readBuffer(c.conn, types.FileEntryMinSize-1)
	if err != nil {
		return &types.FileEntry{}, fmt.Errorf("error reading file bytes: %v", err)
	}
	buffer = append(packet, buffer...)

	// Read variable field (data)
	length := binary.BigEndian.Uint32(buffer[1:5])
	if length < types.FileEntryMinSize {
		return &types.FileEntry{}, errors.New("error reading data entry: wrong data length")
	}

	// Read rest of the file data
	bufferAux, err := readBuffer(c.conn, length-types.FileEntryMinSize)
	if err != nil {
		return &types.FileEntry{}, fmt.Errorf("error reading file data bytes: %v", err)
	}
	buffer = append(buffer, bufferAux...)

	// Decode binary data to data entry struct
	file, err := types.DecodeFileEntry(buffer)
	if err != nil {
		return &types.FileEntry{}, fmt.Errorf("decode file entry error: %v", err)
	}

	return file, nil
}

// reads header bytes from socket and tries to parse them
// returns the parsed HeaderEntry
func (c *StreamClient) readHeaderEntry() (*types.HeaderEntry, error) {
	// Read header stream bytes
	binaryHeader, err := readBuffer(c.conn, types.HeaderSize)
	if err != nil {
		return &types.HeaderEntry{}, fmt.Errorf("failed to read header bytes %v", err)
	}

	// Decode bytes stream to header entry struct
	h, err := types.DecodeHeaderEntry(binaryHeader)
	if err != nil {
		return &types.HeaderEntry{}, fmt.Errorf("error decoding binary header: %v", err)
	}

	return h, nil
}

// reads result bytes and tries to parse them
// returns the parsed ResultEntry
func (c *StreamClient) readResultEntry(packet []byte) (*types.ResultEntry, error) {
	if len(packet) != 1 {
		return &types.ResultEntry{}, fmt.Errorf("expected packet size of 1, got: %d", len(packet))
	}

	// Read the rest of fixed size fields
	buffer, err := readBuffer(c.conn, types.ResultEntryMinSize-1)
	if err != nil {
		return &types.ResultEntry{}, fmt.Errorf("failed to read main result bytes %v", err)
	}
	buffer = append(packet, buffer...)

	// Read variable field (errStr)
	length := binary.BigEndian.Uint32(buffer[1:5])
	if length < types.ResultEntryMinSize {
		return &types.ResultEntry{}, fmt.Errorf("%s Error reading result entry", c.id)
	}

	// read the rest of the result
	bufferAux, err := readBuffer(c.conn, length-types.ResultEntryMinSize)
	if err != nil {
		return &types.ResultEntry{}, fmt.Errorf("failed to read result errStr bytes %v", err)
	}
	buffer = append(buffer, bufferAux...)

	// Decode binary entry result
	re, err := types.DecodeResultEntry(buffer)
	if err != nil {
		return &types.ResultEntry{}, fmt.Errorf("decode result entry error: %v", err)
	}

	return re, nil
}
