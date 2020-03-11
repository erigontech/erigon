package remote

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/ledgerwatch/turbo-geth/ethdb/codecpool"
	"github.com/stretchr/testify/assert"
)

func TestReconnect(t *testing.T) {
	assert := assert.New(t)
	// Prepare input buffer with one command CmdVersion
	var inBuf bytes.Buffer
	encoder := codecpool.Encoder(&inBuf)
	defer codecpool.Return(encoder)
	// output buffer to receive the result of the command
	var outBuf bytes.Buffer
	decoder := codecpool.Decoder(&outBuf)
	defer codecpool.Return(decoder)

	dialCallCounter := 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pingCh := make(chan time.Time, ClientMaxConnections)
	opts := DefaultOpts
	opts.DialFunc = func(ctx context.Context) (in io.Reader, out io.Writer, closer io.Closer, err error) {
		dialCallCounter++
		if dialCallCounter%2 == 0 {
			return &inBuf, &outBuf, nil, net.UnknownNetworkError("Oops")
		}
		return &inBuf, &outBuf, nil, nil
	}

	db := &DB{
		opts:           opts,
		connectionPool: make(chan *conn, ClientMaxConnections),
		doDial:         make(chan struct{}, ClientMaxConnections),
		doPing:         pingCh,
	}

	// no open connections by default
	assert.Equal(0, dialCallCounter)
	assert.Equal(0, len(db.connectionPool))

	// open 1 connection and wait for it
	db.doDial <- struct{}{}
	db.autoReconnect(ctx)
	<-db.connectionPool
	assert.Equal(1, dialCallCounter)
	assert.Equal(0, len(db.connectionPool))

	// open 2nd connection - dialFunc will return err on 2nd call, but db must reconnect automatically
	db.doDial <- struct{}{}
	db.autoReconnect(ctx) // dial err
	db.autoReconnect(ctx) // dial ok
	<-db.connectionPool
	assert.Equal(3, dialCallCounter)
	assert.Equal(0, len(db.connectionPool))

	// open conn and call ping on it
	db.doDial <- struct{}{}
	assert.Nil(encoder.Encode(ResponseOk))
	assert.Nil(encoder.Encode(Version))
	db.autoReconnect(ctx) // dial err
	db.autoReconnect(ctx) // dial ok
	assert.Equal(5, dialCallCounter)
	assert.Equal(1, len(db.connectionPool))
	pingCh <- time.Now()
	db.autoReconnect(ctx)
	var cmd Command
	assert.Nil(decoder.Decode(&cmd))
	assert.Equal(CmdVersion, cmd)

	// TODO: cover case when ping receive io.EOF
}
