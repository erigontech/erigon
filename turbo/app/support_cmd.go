// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/urfave/cli/v2"

	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	types "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/debug"
)

const (
	wsReadBuffer       = 1024
	wsWriteBuffer      = 1024
	wsPingInterval     = 60 * time.Second
	wsPingWriteTimeout = 5 * time.Second
	wsMessageSizeLimit = 32 * 1024 * 1024
)

var wsBufferPool = new(sync.Pool)

var (
	diagnosticsURLFlag = cli.StringFlag{
		Name:     "diagnostics.addr",
		Usage:    "Address of the diagnostics system provided by the support team, include unique session PIN",
		Required: false,
		Value:    "localhost:6062",
	}

	debugURLsFlag = cli.StringSliceFlag{
		Name:     "debug.addrs",
		Usage:    "Comma separated list of URLs to the debug endpoints thats are being diagnosed",
		Required: false,
		Value:    cli.NewStringSlice("localhost:6063"),
	}

	sessionsFlag = cli.StringSliceFlag{
		Name:  "diagnostics.sessions",
		Usage: "Comma separated list of session PINs to connect to",
	}
)

var supportCommand = cli.Command{
	Action:    MigrateFlags(connectDiagnostics),
	Name:      "support",
	Usage:     "Connect Erigon instance to a diagnostics system for support",
	ArgsUsage: "--diagnostics.addr <URL for the diagnostics system> --ids <diagnostic session ids allowed to connect> --metrics.urls <http://erigon_host:metrics_port>",
	Before: func(cliCtx *cli.Context) error {
		_, _, _, err := debug.Setup(cliCtx, true /* rootLogger */)
		if err != nil {
			return err
		}
		return nil
	},
	Flags: append([]cli.Flag{
		&debugURLsFlag,
		&diagnosticsURLFlag,
		&sessionsFlag,
	}, debug.Flags...),
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

	debugURLs := []string{}

	for _, debugURL := range cliCtx.StringSlice(debugURLsFlag.Name) {
		debugURLs = append(debugURLs, "http://"+debugURL)
	}

	diagnosticsUrl := cliCtx.String(diagnosticsURLFlag.Name) + "/bridge"

	sessionIds := cliCtx.StringSlice(sessionsFlag.Name)

	// Perform the requests in a loop (reconnect)
	for {
		if err := tunnel(ctx, cancel, sigs, diagnosticsUrl, sessionIds, debugURLs, logger); err != nil {
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
func tunnel(ctx context.Context, cancel context.CancelFunc, sigs chan os.Signal, diagnosticsUrl string, sessionIds []string, debugURLs []string, logger log.Logger) error {
	metricsClient := &http.Client{}
	defer metricsClient.CloseIdleConnections()

	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()

	go func() {
		select {
		case <-sigs:
			cancel()
		case <-ctx1.Done():
		}
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
		debugResponse, err := metricsClient.Get(debugURL + "/debug/diag/nodeinfo")

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

	dialer := websocket.Dialer{
		ReadBufferSize:  wsReadBuffer,
		WriteBufferSize: wsWriteBuffer,
		WriteBufferPool: wsBufferPool,
	}

	conn, resp, err := dialer.DialContext(ctx1, "wss://"+diagnosticsUrl, nil)

	if err != nil {
		conn, resp, err = dialer.DialContext(ctx1, "ws://"+diagnosticsUrl, nil)

		if err != nil {
			return err
		}
	}

	if resp.StatusCode != http.StatusSwitchingProtocols {
		return fmt.Errorf("support request to %s failed: %s", diagnosticsUrl, resp.Status)
	}

	type connectionInfo struct {
		Version  uint64   `json:"version"`
		Sessions []string `json:"sessions"`
		Nodes    []*info  `json:"nodes"`
	}

	codec := rpc.NewWebsocketCodec(conn, "wss://"+diagnosticsUrl, nil) //TODO: revise why is it so
	defer codec.Close()

	err = codec.WriteJSON(ctx1, &connectionInfo{
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

				debugURL := node.debugURL + "/debug/diag/" + requests[0].Method + queryString
				debugResponse, err := metricsClient.Get(debugURL)

				if err != nil {
					return codec.WriteJSON(ctx1, &nodeResponse{
						Id: requestId,
						Error: &responseError{
							Code:    http.StatusFailedDependency,
							Message: fmt.Sprintf("Request for metrics method [%s] failed: %v", debugURL, err),
						},
						Last: true,
					})
				}

				defer debugResponse.Body.Close()

				//Websocket ok message
				if resp.StatusCode != http.StatusSwitchingProtocols {
					body, _ := io.ReadAll(debugResponse.Body)
					return codec.WriteJSON(ctx1, &nodeResponse{
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
						return codec.WriteJSON(ctx1, &nodeResponse{
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
						return codec.WriteJSON(ctx1, &nodeResponse{
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
						return codec.WriteJSON(ctx1, &nodeResponse{
							Id: requestId,
							Error: &responseError{
								Code:    int64(http.StatusInternalServerError),
								Message: fmt.Sprintf("Can't copy metrics response for [%s]: %s", debugURL, err),
							},
							Last: true,
						})
					}

				case "aplication/profile":
					if _, err := io.Copy(buffer, debugResponse.Body); err != nil {
						return codec.WriteJSON(ctx1, &nodeResponse{
							Id: requestId,
							Error: &responseError{
								Code:    http.StatusInternalServerError,
								Message: fmt.Sprintf("Request for metrics method [%s] failed: %v", debugURL, err),
							},
							Last: true,
						})
					}

					data, err := json.Marshal(struct {
						Data []byte `json:"chunk"`
					}{
						Data: buffer.Bytes(),
					})

					buffer = bytes.NewBuffer(data)

					if err != nil {
						return codec.WriteJSON(ctx1, &nodeResponse{
							Id: requestId,
							Error: &responseError{
								Code:    int64(http.StatusInternalServerError),
								Message: fmt.Sprintf("Can't copy metrics response for [%s]: %s", debugURL, err),
							},
							Last: true,
						})
					}

				default:
					return codec.WriteJSON(ctx1, &nodeResponse{
						Id: requestId,
						Error: &responseError{
							Code:    int64(http.StatusInternalServerError),
							Message: fmt.Sprintf("Unhandled content type: %s, from: %s", debugResponse.Header.Get("Content-Type"), debugURL),
						},
						Last: true,
					})
				}

				return codec.WriteJSON(ctx1, &nodeResponse{
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
