package app

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"sync"
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
	ArgsUsage: "--diagnostics.url <URL for the diagnostics system> --metrics.url <http://erigon_host:metrics_port>",
	Flags: []cli.Flag{
		&metricsURLsFlag,
		&diagnosticsURLFlag,
		&insecureFlag,
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
	metricsURLs := cliCtx.StringSlice(metricsURLsFlag.Name)
	metricsURL := metricsURLs[0] // TODO: Generalise

	diagnosticsUrl := cliCtx.String(diagnosticsURLFlag.Name)

	// Create a pool with the server certificate since it is not signed
	// by a known CA
	certPool := x509.NewCertPool()
	srvCert, err := ioutil.ReadFile("diagnostics.crt")
	if err != nil {
		return fmt.Errorf("reading server certificate: %v", err)
	}
	caCert, err := ioutil.ReadFile("CA-cert.pem")
	if err != nil {
		return fmt.Errorf("reading server certificate: %v", err)
	}
	certPool.AppendCertsFromPEM(srvCert)
	certPool.AppendCertsFromPEM(caCert)

	// Create TLS configuration with the certificate of the server
	insecure := cliCtx.Bool(insecureFlag.Name)
	tlsConfig := &tls.Config{
		RootCAs:            certPool,
		InsecureSkipVerify: insecure, //nolint:gosec
	}

	reader, writer := io.Pipe()
	httpClient := &http.Client{Transport: &http2.Transport{TLSClientConfig: tlsConfig}}

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
	r := bufio.NewReader(resp.Body)
	firstLine, err := r.ReadBytes('\n')
	if err != nil {
		return err
	}
	if string(firstLine) != "SUCCESS\n" {
		return fmt.Errorf("connecting to diagnostics system: %s", firstLine)
	}

outerLoop:
	for {
		var buf [4096]byte
		var readLen int
		for readLen < len(buf) && (readLen == 0 || buf[readLen-1] != '\n') {
			len, err := r.Read(buf[readLen:])
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
		fmt.Printf("Got request: %s\n", buf[:readLen-1])
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
		fmt.Printf("Got response:\n%s\n", metricsBuf.Bytes())
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
