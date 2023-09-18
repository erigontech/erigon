package app

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
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

	diagnosticsUrl := cliCtx.String(diagnosticsURLFlag.Name) + "/bridge"

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
		debugResponse, err := metricsClient.Get(debugURL + "/debug/nodeinfo")

		if err != nil {
			return err
		}

		if debugResponse.StatusCode != http.StatusOK {
			return fmt.Errorf("debug request to %s failed: %s", debugURL, debugResponse.Status)
		}

		var reply remote.NodesInfoReply

		err = json.NewDecoder(debugResponse.Body).Decode(&reply)

		debugResponse.Body.Close()

		if err != nil {
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
					Enodes: []enode{{
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
		return fmt.Errorf("support request to %s failed: %s", diagnosticsUrl, resp.Status)
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

	for {
		requests, _, err := codec.ReadBatch()

		select {
		case <-ctx.Done():
			return nil
		default:
			if err != nil {
				logger.Info("Breaking connection", "err", err)
				return nil
			}
		}

		var requestId string

		if err = json.Unmarshal(requests[0].ID, &requestId); err != nil {
			logger.Error("Invalid request id", "err", err)
			continue
		}

		nodeRequest := struct {
			NodeId      string     `json:"nodeId"`
			QueryParams url.Values `json:"queryParams"`
		}{}

		if err = json.Unmarshal(requests[0].Params, &nodeRequest); err != nil {
			logger.Error("Invalid node request", "err", err, "id", requestId)
			continue
		}

		type responseError struct {
			Code    int64            `json:"code"`
			Message string           `json:"message"`
			Data    *json.RawMessage `json:"data,omitempty"`
		}

		type nodeResponse struct {
			Id     string          `json:"id"`
			Result json.RawMessage `json:"result,omitempty"`
			Error  *responseError  `json:"error,omitempty"`
			Last   bool            `json:"last,omitempty"`
		}

		if node, ok := nodes[nodeRequest.NodeId]; ok {
			err := func() error {
				var queryString string

				if len(nodeRequest.QueryParams) > 0 {
					queryString = "?" + nodeRequest.QueryParams.Encode()
				}

				debugURL := node.debugURL + "/debug/" + requests[0].Method + queryString

				debugResponse, err := metricsClient.Get(debugURL)

				if err != nil {
					return json.NewEncoder(writer).Encode(&nodeResponse{
						Id: requestId,
						Error: &responseError{
							Code:    http.StatusFailedDependency,
							Message: fmt.Sprintf("Request for metrics method [%s] failed: %v", debugURL, err),
						},
						Last: true,
					})
				}

				defer debugResponse.Body.Close()

				if resp.StatusCode != http.StatusOK {
					body, _ := io.ReadAll(debugResponse.Body)
					return json.NewEncoder(writer).Encode(&nodeResponse{
						Id: requestId,
						Error: &responseError{
							Code:    int64(resp.StatusCode),
							Message: fmt.Sprintf("Request for metrics method [%s] failed: %s", debugURL, string(body)),
						},
						Last: true,
					})
				}

				buffer := &bytes.Buffer{}

				switch debugResponse.Header.Get("Content-Type") {
				case "application/json":
					if _, err := io.Copy(buffer, debugResponse.Body); err != nil {
						return json.NewEncoder(writer).Encode(&nodeResponse{
							Id: requestId,
							Error: &responseError{
								Code:    http.StatusInternalServerError,
								Message: fmt.Sprintf("Request for metrics method [%s] failed: %v", debugURL, err),
							},
							Last: true,
						})
					}
				case "application/octet-stream":
					if _, err := io.Copy(buffer, debugResponse.Body); err != nil {
						return json.NewEncoder(writer).Encode(&nodeResponse{
							Id: requestId,
							Error: &responseError{
								Code:    int64(http.StatusInternalServerError),
								Message: fmt.Sprintf("Can't copy metrics response for [%s]: %s", debugURL, err),
							},
							Last: true,
						})
					}

					offset, _ := strconv.ParseInt(debugResponse.Header.Get("X-Offset"), 10, 64)
					size, _ := strconv.ParseInt(debugResponse.Header.Get("X-Size"), 10, 64)

					data, err := json.Marshal(struct {
						Offset int64  `json:"offset"`
						Size   int64  `json:"size"`
						Data   []byte `json:"chunk"`
					}{
						Offset: offset,
						Size:   size,
						Data:   buffer.Bytes(),
					})

					buffer = bytes.NewBuffer(data)

					if err != nil {
						return json.NewEncoder(writer).Encode(&nodeResponse{
							Id: requestId,
							Error: &responseError{
								Code:    int64(http.StatusInternalServerError),
								Message: fmt.Sprintf("Can't copy metrics response for [%s]: %s", debugURL, err),
							},
							Last: true,
						})
					}

				default:
					return json.NewEncoder(writer).Encode(&nodeResponse{
						Id: requestId,
						Error: &responseError{
							Code:    int64(http.StatusInternalServerError),
							Message: fmt.Sprintf("Unhandled content type: %s, from: %s", debugResponse.Header.Get("Content-Type"), debugURL),
						},
						Last: true,
					})
				}

				return json.NewEncoder(writer).Encode(&nodeResponse{
					Id:     requestId,
					Result: json.RawMessage(buffer.Bytes()),
					Last:   true,
				})
			}()

			if err != nil {
				return err
			}
		}
	}
}
