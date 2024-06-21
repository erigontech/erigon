package client

import "fmt"

const (
	// Commands
	CmdUnknown       Command = 0
	CmdStart         Command = 1
	CmdStop          Command = 2
	CmdHeader        Command = 3
	CmdStartBookmark Command = 4 // CmdStartBookmark for the start from bookmark TCP client command
	CmdEntry         Command = 5 // CmdEntry for the get entry TCP client command
	CmdBookmark      Command = 6 // CmdBookmark for the get bookmark TCP client command
)

// sendHeaderCmd sends the header command to the server.
func (c *StreamClient) sendHeaderCmd() error {
	err := c.sendCommand(CmdHeader)
	if err != nil {
		return fmt.Errorf("%s %v", c.id, err)
	}

	return nil
}

// sendStartBookmarkCmd sends a start command to the server, indicating
// that the client wishes to start streaming from the given bookmark
func (c *StreamClient) sendStartBookmarkCmd(bookmark []byte) error {
	err := c.sendCommand(CmdStartBookmark)
	if err != nil {
		return err
	}

	// Send starting/from entry number
	if err := writeFullUint32ToConn(c.conn, uint32(len(bookmark))); err != nil {
		return err
	}
	if err := writeBytesToConn(c.conn, bookmark); err != nil {
		return err
	}

	return nil
}

// sendStartCmd sends a start command to the server, indicating
// that the client wishes to start streaming from the given entry number.
func (c *StreamClient) sendStartCmd(from uint64) error {
	err := c.sendCommand(CmdStart)
	if err != nil {
		return err
	}

	// Send starting/from entry number
	if err := writeFullUint64ToConn(c.conn, from); err != nil {
		return err
	}

	return nil
}

// sendHeaderCmd sends the header command to the server.
func (c *StreamClient) sendStopCmd() error {
	err := c.sendCommand(CmdStop)
	if err != nil {
		return fmt.Errorf("%s %v", c.id, err)
	}

	return nil
}

func (c *StreamClient) sendCommand(cmd Command) error {
	// Send command
	if err := writeFullUint64ToConn(c.conn, uint64(cmd)); err != nil {
		return fmt.Errorf("%s %v", c.id, err)
	}

	// Send stream type
	if err := writeFullUint64ToConn(c.conn, uint64(c.streamType)); err != nil {
		return fmt.Errorf("%s %v", c.id, err)
	}

	return nil
}
