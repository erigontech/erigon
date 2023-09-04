package app

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
	"golang.org/x/net/http2"
)

var (
	diagnosticsURLFlag = cli.StringFlag{
		Name:  "diagnostics.url",
		Usage: "URL of the diagnostics system provided by the support team, include unique session PIN",
	}

	debugURLsFlag = cli.StringSliceFlag{
		Name:  "debug.urls",
		Usage: "Comma separated list of URLs to the debug endpoints thats are being diagnosed",
	}

	insecureFlag = cli.BoolFlag{
		Name:  "insecure",
		Usage: "Allows communication with diagnostics system using self-signed TLS certificates",
	}

	sessionsFlag = cli.StringSliceFlag{
		Name:  "diagnostics.sessions",
		Usage: "Comma separated list of support session ids to connect to",
	}
)

var supportCommand = cli.Command{
	Action:    MigrateFlags(connectDiagnostics),
	Name:      "support",
	Usage:     "Connect Erigon instance to a diagnostics system for support",
	ArgsUsage: "--diagnostics.url <URL for the diagnostics system> --ids <diagnostic session ids allowed to connect> --metrics.urls <http://erigon_host:metrics_port>",
	Flags: []cli.Flag{
		&debugURLsFlag,
		&diagnosticsURLFlag,
		&sessionsFlag,
		&insecureFlag,
	},
	//Category: "SUPPORT COMMANDS",
	Description: `The support command connects a running Erigon instances to a diagnostics system specified by the URL.`,
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

	debugURLs := cliCtx.StringSlice(debugURLsFlag.Name)

	diagnosticsUrl := cliCtx.String(diagnosticsURLFlag.Name)

	// Create TLS configuration with the certificate of the server
	insecure := cliCtx.Bool(insecureFlag.Name)
	tlsConfig := &tls.Config{
		InsecureSkipVerify: insecure, //nolint:gosec
	}

	sessionIds := cliCtx.StringSlice(sessionsFlag.Name)

	// Perform the requests in a loop (reconnect)
	for {
		if err := tunnel(ctx, cancel, sigs, tlsConfig, diagnosticsUrl, sessionIds, debugURLs, logger); err != nil {
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

type conn struct {
	io.ReadCloser
	*io.PipeWriter
}

func (c *conn) Close() error {
	c.ReadCloser.Close()
	c.PipeWriter.Close()
	return nil
}

func (c *conn) SetWriteDeadline(time time.Time) error {
	return nil
}

// tunnel operates the tunnel from diagnostics system to the metrics URL for one http/2 request
// needs to be called repeatedly to implement re-connect logic
func tunnel(ctx context.Context, cancel context.CancelFunc, sigs chan os.Signal, tlsConfig *tls.Config, diagnosticsUrl string, sessionIds []string, debugURLs []string, logger log.Logger) error {
	diagnosticsClient := &http.Client{Transport: &http2.Transport{TLSClientConfig: tlsConfig}}
	defer diagnosticsClient.CloseIdleConnections()
	metricsClient := &http.Client{}
	defer metricsClient.CloseIdleConnections()

	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()

	// Create a request object to send to the server
	reader, writer := io.Pipe()

	go func() {
		select {
		case <-sigs:
			cancel()
		case <-ctx1.Done():
		}
		reader.Close()
		writer.Close()
	}()

	type enode struct {
		Enode        string               `json:"enode,omitempty"`
		Enr          string               `json:"enr,omitempty"`
		Ports        *types.NodeInfoPorts `json:"ports,omitempty"`
		ListenerAddr string               `json:"listener_addr,omitempty"`
	}

	type info struct {
		Id        string          `json:"id,omitempty"`
		Name      string          `json:"name,omitempty"`
		Protocols json.RawMessage `json:"protocols,omitempty"`
		Enodes    []enode         `json:"enodes,omitempty"`
	}

	type node struct {
		debugURL string
		info     *info
	}

	nodes := map[string]*node{}

	for _, debugURL := range debugURLs {
		debugResponse, err := metricsClient.Get(debugURL + "/" + "nodeinfo")

		if err != nil {
			return err
		}

		if debugResponse.StatusCode != http.StatusOK {
			return fmt.Errorf("Debug request to %s failed: %s", debugResponse.Status)
		}

		var reply remote.NodesInfoReply

		if err = json.NewDecoder(debugResponse.Body).Decode(&reply); err != nil {
			return err
		}

		for _, ni := range reply.NodesInfo {
			if n, ok := nodes[ni.Id]; ok {
				n.info.Enodes = append(n.info.Enodes, enode{
					Enode:        ni.Enode,
					Enr:          ni.Enr,
					Ports:        ni.Ports,
					ListenerAddr: ni.ListenerAddr,
				})
			} else {
				nodes[ni.Id] = &node{debugURL, &info{
					Id:        ni.Id,
					Name:      ni.Name,
					Protocols: ni.Protocols,
					Enodes: []enode{enode{
						Enode:        ni.Enode,
						Enr:          ni.Enr,
						Ports:        ni.Ports,
						ListenerAddr: ni.ListenerAddr,
					}}}}
			}
		}
	}

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

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Support request to %s failed: %s", diagnosticsUrl, resp.Status)
	}

	type connectionInfo struct {
		Version  uint64   `json:"version"`
		Sessions []string `json:"sessions"`
		Nodes    []*info  `json:"nodes"`
	}

	err = json.NewEncoder(writer).Encode(&connectionInfo{
		Version:  Version,
		Sessions: sessionIds,
		Nodes: func() (replies []*info) {
			for _, node := range nodes {
				replies = append(replies, node.info)
			}

			return replies
		}(),
	})

	if err != nil {
		return err
	}

	logger.Info("Connected")

	codec := rpc.NewCodec(&conn{
		ReadCloser: resp.Body,
		PipeWriter: writer,
	})
	defer codec.Close()

	for requests, _, err := codec.ReadBatch(); err == nil; requests, _, err = codec.ReadBatch() {

		fmt.Println(requests[0])
		/*metricsBuf.Reset()
		debugResponse, err := metricsClient.Get(debugURL + string(line))

		if err != nil {
			fmt.Fprintf(&metricsBuf, "ERROR: Requesting metrics url [%s], query [%s], err: %v", debugURL, line, err)
		} else {
			// Buffer the metrics response, and relay it back to the diagnostics system, prepending with the size
			if _, err := io.Copy(&metricsBuf, debugResponse.Body); err != nil {
				metricsBuf.Reset()
				fmt.Fprintf(&metricsBuf, "ERROR: Extracting metrics url [%s], query [%s], err: %v", debugURL, line, err)
			}
			debugResponse.Body.Close()
		}

		var sizeBuf [4]byte
		binary.BigEndian.PutUint32(sizeBuf[:], uint32(metricsBuf.Len()))
		if _, err = writer.Write(sizeBuf[:]); err != nil {
			logger.Error("Problem relaying metrics prefix len", "url", debugURL, "query", line, "err", err)
			break
		}
		if _, err = writer.Write(metricsBuf.Bytes()); err != nil {
			logger.Error("Problem relaying", "url", debugURL, "query", line, "err", err)
			break
		}*/
	}

	if err != nil {
		select {
		case <-ctx.Done():
		default:
			logger.Error("Breaking connection", "err", err)
		}
	}

	return nil
}
