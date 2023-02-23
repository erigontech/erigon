package app

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
)

var supportCommand = cli.Command{
	Action:    MigrateFlags(connectDiagnostics),
	Name:      "support",
	Usage:     "Connect Erigon instance to a diagnostics system for support",
	ArgsUsage: "--diagnostics.url <URL for the diagnostics system> --metrics.url <http://erigon_host:metrics_port>",
	Flags: []cli.Flag{
		&utils.MetricsURLsFlag,
		&utils.DiagnosticsURLFlag,
	},
	Category: "SUPPORT COMMANDS",
	Description: `
The support command connects a running Erigon instances to a diagnostics system specified
by the URL.`,
}

// Conn is client/server symmetric connection.
// It implements the io.Reader/io.Writer/io.Closer to read/write or close the connection to the other side.
// It also has a Send/Recv function to use channels to communicate with the other side.
type Conn struct {
	r  io.Reader
	wc io.WriteCloser

	cancel context.CancelFunc

	wLock sync.Mutex
	rLock sync.Mutex
}

func newConn(ctx context.Context, r io.Reader, wc io.WriteCloser) (*Conn, context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	return &Conn{
		r:      r,
		wc:     wc,
		cancel: cancel,
	}, ctx
}

// Write writes data to the connection
func (c *Conn) Write(data []byte) (int, error) {
	c.wLock.Lock()
	defer c.wLock.Unlock()
	return c.wc.Write(data)
}

// Read reads data from the connection
func (c *Conn) Read(data []byte) (int, error) {
	c.rLock.Lock()
	defer c.rLock.Unlock()
	return c.r.Read(data)
}

// Close closes the connection
func (c *Conn) Close() error {
	c.cancel()
	return c.wc.Close()
}

func connectDiagnostics(cliCtx *cli.Context) error {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-sigs
		cancel()
	}()

	pollInterval := 500 * time.Millisecond
	pollEvery := time.NewTicker(pollInterval)
	defer pollEvery.Stop()
	client := &http.Client{}
	defer client.CloseIdleConnections()
	metricsURLs := cliCtx.StringSlice(utils.MetricsURLsFlag.Name)
	metricsURL := metricsURLs[0] // TODO: Generalise

	diagnosticsUrl := cliCtx.String(utils.DiagnosticsURLFlag.Name)

	reader, writer := io.Pipe()
	httpClient := &http.Client{ /*Transport: &http2.Transport{}*/ }

	// Create a request object to send to the server
	req, err := http.NewRequest(http.MethodPost, diagnosticsUrl, reader)
	if err != nil {
		return err
	}

	// Apply custom headers

	// Apply given context to the sent request
	req = req.WithContext(ctx)

	// Perform the request
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	// Create a connection
	ctx1, cancel1 := context.WithCancel(req.Context())
	defer cancel1()
	defer resp.Body.Close()
	defer writer.Close()

	// Apply the connection context on the request context
	resp.Request = req.WithContext(ctx1)
	var metricsBuf bytes.Buffer

outerLoop:
	for {
		var buf [4096]byte
		var readLen int
		for readLen < len(buf) && (readLen == 0 || buf[readLen-1] == '\n') {
			len, err := resp.Body.Read(buf[readLen:])
			if err != nil {
				log.Error("Connection read", "err", err)
				break outerLoop
			}
			readLen += len
		}
		if buf[readLen-1] != '\n' {
			log.Error("Request too long, circuit breaker")
			break outerLoop
		}
		metricsResponse, err := client.Get(metricsURL + string(buf[:readLen-1]))
		if err != nil {
			log.Error("Problem requesting metrics", "url", metricsURL, "query", string(buf[:readLen-1]), "err", err)
			break outerLoop
		}
		// Buffer the metrics response, and relay it back to the diagnostics system, prepending with the size
		metricsBuf.Reset()
		if _, err := io.Copy(&metricsBuf, metricsResponse.Body); err != nil {
			metricsResponse.Body.Close()
			log.Error("Problem extracting metrics", "url", metricsURL, "query", string(buf[:readLen-1]), "err", err)
			break outerLoop
		}
		metricsResponse.Body.Close()
		var sizeBuf [4]byte
		binary.BigEndian.PutUint32(sizeBuf[:], uint32(metricsBuf.Len()))
		if _, err := writer.Write(sizeBuf[:]); err != nil {
			log.Error("Problem relaying metrics prefix len", "url", metricsURL, "query", string(buf[:readLen-1]), "err", err)
			break outerLoop
		}
		if _, err := writer.Write(metricsBuf.Bytes()); err != nil {
			log.Error("Problem relaying", "url", metricsURL, "query", string(buf[:readLen-1]), "err", err)
			break outerLoop
		}
	}
	return nil
}
