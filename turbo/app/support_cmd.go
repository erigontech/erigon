package app

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
	"golang.org/x/net/http2"
)

var (
	diagnosticsURLFlag = cli.StringFlag{
		Name:  "diagnostics.url",
		Usage: "URL of the diagnostics system provided by the support team, include unique session PIN",
	}
	metricsURLsFlag = cli.StringSliceFlag{
		Name:  "metrics.urls",
		Usage: "Comma separated list of URLs to the metrics endpoints thats are being diagnosed",
	}
	insecureFlag = cli.BoolFlag{
		Name:  "insecure",
		Usage: "Allows communication with diagnostics system using self-signed TLS certificates",
	}
)

var supportCommand = cli.Command{
	Action:    MigrateFlags(connectDiagnostics),
	Name:      "support",
	Usage:     "Connect Erigon instance to a diagnostics system for support",
	ArgsUsage: "--diagnostics.url <URL for the diagnostics system> --metrics.urls <http://erigon_host:metrics_port>",
	Flags: []cli.Flag{
		&metricsURLsFlag,
		&diagnosticsURLFlag,
		&insecureFlag,
	},
	//Category: "SUPPORT COMMANDS",
	Description: `
The support command connects a running Erigon instances to a diagnostics system specified
by the URL.`,
}

const Version = 1

func connectDiagnostics(cliCtx *cli.Context) error {
	return ConnectDiagnostics(cliCtx, log.Root())
}

func ConnectDiagnostics(cliCtx *cli.Context, logger log.Logger) error {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	metricsURLs := cliCtx.StringSlice(metricsURLsFlag.Name)
	metricsURL := metricsURLs[0] // TODO: Generalise

	diagnosticsUrl := cliCtx.String(diagnosticsURLFlag.Name)

	// Create TLS configuration with the certificate of the server
	insecure := cliCtx.Bool(insecureFlag.Name)
	tlsConfig := &tls.Config{
		InsecureSkipVerify: insecure, //nolint:gosec
	}

	// Perform the requests in a loop (reconnect)
	for {
		if err := tunnel(ctx, cancel, sigs, tlsConfig, diagnosticsUrl, metricsURL, logger); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			// Quit immediately if the context was cancelled (by Ctrl-C or TERM signal)
			return nil
		default:
		}
		logger.Info("Reconnecting in 1 second...")
		timer := time.NewTimer(1 * time.Second)
		<-timer.C
	}
}

var successLine = []byte("SUCCESS")

// tunnel operates the tunnel from diagnostics system to the metrics URL for one http/2 request
// needs to be called repeatedly to implement re-connect logic
func tunnel(ctx context.Context, cancel context.CancelFunc, sigs chan os.Signal, tlsConfig *tls.Config, diagnosticsUrl string, metricsURL string, logger log.Logger) error {
	diagnosticsClient := &http.Client{Transport: &http2.Transport{TLSClientConfig: tlsConfig}}
	defer diagnosticsClient.CloseIdleConnections()
	metricsClient := &http.Client{}
	defer metricsClient.CloseIdleConnections()
	// Create a request object to send to the server
	reader, writer := io.Pipe()
	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	go func() {
		select {
		case <-sigs:
			cancel()
		case <-ctx1.Done():
		}
		reader.Close()
		writer.Close()
	}()
	req, err := http.NewRequestWithContext(ctx1, http.MethodPost, diagnosticsUrl, reader)
	if err != nil {
		return err
	}

	// Create a connection
	// Apply given context to the sent request
	resp, err := diagnosticsClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	defer writer.Close()

	// Apply the connection context on the request context
	var metricsBuf bytes.Buffer
	r := bufio.NewReaderSize(resp.Body, 4096)
	line, isPrefix, err := r.ReadLine()
	if err != nil {
		return fmt.Errorf("reading first line: %v", err)
	}
	if isPrefix {
		return fmt.Errorf("request too long")
	}
	if !bytes.Equal(line, successLine) {
		return fmt.Errorf("connecting to diagnostics system, first line [%s]", line)
	}
	var versionBytes [8]byte
	binary.BigEndian.PutUint64(versionBytes[:], Version)
	if _, err = writer.Write(versionBytes[:]); err != nil {
		return fmt.Errorf("sending version: %v", err)
	}

	logger.Info("Connected")

	for line, isPrefix, err = r.ReadLine(); err == nil && !isPrefix; line, isPrefix, err = r.ReadLine() {
		metricsBuf.Reset()
		metricsResponse, err := metricsClient.Get(metricsURL + string(line))
		if err != nil {
			fmt.Fprintf(&metricsBuf, "ERROR: Requesting metrics url [%s], query [%s], err: %v", metricsURL, line, err)
		} else {
			// Buffer the metrics response, and relay it back to the diagnostics system, prepending with the size
			if _, err := io.Copy(&metricsBuf, metricsResponse.Body); err != nil {
				metricsBuf.Reset()
				fmt.Fprintf(&metricsBuf, "ERROR: Extracting metrics url [%s], query [%s], err: %v", metricsURL, line, err)
			}
			metricsResponse.Body.Close()
		}
		var sizeBuf [4]byte
		binary.BigEndian.PutUint32(sizeBuf[:], uint32(metricsBuf.Len()))
		if _, err = writer.Write(sizeBuf[:]); err != nil {
			logger.Error("Problem relaying metrics prefix len", "url", metricsURL, "query", line, "err", err)
			break
		}
		if _, err = writer.Write(metricsBuf.Bytes()); err != nil {
			logger.Error("Problem relaying", "url", metricsURL, "query", line, "err", err)
			break
		}
	}
	if err != nil {
		select {
		case <-ctx.Done():
		default:
			logger.Error("Breaking connection", "err", err)
		}
	}
	if isPrefix {
		logger.Error("Request too long, circuit breaker")
	}
	return nil
}
