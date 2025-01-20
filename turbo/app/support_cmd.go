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
		Value:    "localhost:8080",
	}

	debugURLsFlag = cli.StringSliceFlag{
		Name:     "debug.addrs",
		Usage:    "Comma separated list of URLs to the debug endpoints thats are being diagnosed",
		Required: false,
		Value:    cli.NewStringSlice("localhost:6062"),
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

// tunnel operates the tunnel from diagnostics system to the metrics URL for one http/2 request
// needs to be called repeatedly to implement re-connect logic
func tunnel(ctx context.Context, cancel context.CancelFunc, sigs chan os.Signal, diagnosticsUrl string, sessionIds []string, debugURLs []string, logger log.Logger) error {
	go func() {
		select {
		case <-sigs:
			fmt.Println("Got interrupt, shutting down...")
			cancel()
		case <-ctx.Done():
			fmt.Println("Context done")
		}
	}()

	codec, err := createCodec(ctx, diagnosticsUrl)
	if err != nil {
		return err
	} else {
		defer codec.Close()
	}

	metricsClient := &http.Client{}
	defer metricsClient.CloseIdleConnections()

	nodes, _ := getNodes(metricsClient, debugURLs)

	err = sendNodesInfoToDiagnostics(ctx, codec, sessionIds, nodes)
	if err != nil {
		return err
	}

	for _, debugURL := range debugURLs {
		go connectToSocket(ctx, cancel, sigs, &codec, debugURL+"/debug/diag/ws")
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

		for _, request := range requests {
			var requestId string

			if err = json.Unmarshal(request.ID, &requestId); err != nil {
				logger.Error("Invalid request id", "err", err)
				continue
			}

			nodeRequest := nodeRequest{}

			if err = json.Unmarshal(request.Params, &nodeRequest); err != nil {
				logger.Error("Invalid node request", "err", err, "id", requestId)
				continue
			}

			if node, ok := nodes[nodeRequest.NodeId]; ok {
				err := func() error {
					var queryString string
					if len(nodeRequest.QueryParams) > 0 {
						queryString = "?" + nodeRequest.QueryParams.Encode()
					}

					debugURL := node.debugURL + "/debug/diag/" + request.Method + queryString
					debugResponse, err := metricsClient.Get(debugURL)
					if err != nil {
						return sendErrorResponse(ctx, codec, requestId, http.StatusFailedDependency, fmt.Sprintf("Request for metrics method [%s] failed: %v", debugURL, err))
					}
					defer debugResponse.Body.Close()

					if debugResponse.StatusCode != http.StatusOK {
						body, _ := io.ReadAll(debugResponse.Body)
						return sendErrorResponse(ctx, codec, requestId, int64(debugResponse.StatusCode), fmt.Sprintf("Request for metrics method [%s] failed: %s", debugURL, string(body)))
					}

					buffer := &bytes.Buffer{}
					if err := copyResponseBody(buffer, debugResponse); err != nil {
						return sendErrorResponse(ctx, codec, requestId, http.StatusInternalServerError, fmt.Sprintf("Request for metrics method [%s] failed: %v", debugURL, err))
					}

					return codec.WriteJSON(ctx, &nodeResponse{
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
}

func getNodes(metricsClient *http.Client, debugURLs []string) (map[string]*tunnelNode, error) {
	nodes := map[string]*tunnelNode{}

	for _, debugURL := range debugURLs {
		reply, err := queryNode(metricsClient, debugURL)
		if err != nil {
			return nil, err
		}

		for _, ni := range reply.NodesInfo {
			if n, ok := nodes[ni.Id]; ok {
				n.info.Enodes = append(n.info.Enodes, tunnelEnode{
					Enode:        ni.Enode,
					Enr:          ni.Enr,
					Ports:        ni.Ports,
					ListenerAddr: ni.ListenerAddr,
				})
			} else {
				nodes[ni.Id] = &tunnelNode{debugURL, &tunnelInfo{
					Id:        ni.Id,
					Name:      ni.Name,
					Protocols: ni.Protocols,
					Enodes: []tunnelEnode{{
						Enode:        ni.Enode,
						Enr:          ni.Enr,
						Ports:        ni.Ports,
						ListenerAddr: ni.ListenerAddr,
					}}}}
			}
		}
	}

	return nodes, nil
}

func createCodec(ctx context.Context, diagnosticsUrl string) (rpc.ServerCodec, error) {
	conn, err := establishConnection(ctx, diagnosticsUrl)
	if err != nil {
		return nil, err
	}

	codec := rpc.NewWebsocketCodec(conn, "wss://"+diagnosticsUrl, nil) //TODO: revise why is it so

	return codec, nil
}

func establishConnection(ctx context.Context, diagnosticsUrl string) (*websocket.Conn, error) {
	dialer := websocket.Dialer{
		ReadBufferSize:  wsReadBuffer,
		WriteBufferSize: wsWriteBuffer,
		WriteBufferPool: wsBufferPool,
	}

	conn, resp, err := dialer.DialContext(ctx, "wss://"+diagnosticsUrl, nil)

	if err != nil {
		conn, resp, err = dialer.DialContext(ctx, "ws://"+diagnosticsUrl, nil)

		if err != nil {
			return nil, err
		}
	}

	if resp.StatusCode != http.StatusSwitchingProtocols {
		return nil, fmt.Errorf("support request to %s failed: %s", diagnosticsUrl, resp.Status)
	}

	return conn, nil
}

func sendNodesInfoToDiagnostics(ctx context.Context, codec rpc.ServerCodec, sessionIds []string, nodes map[string]*tunnelNode) error {
	type connectionInfo struct {
		Version  uint64        `json:"version"`
		Sessions []string      `json:"sessions"`
		Nodes    []*tunnelInfo `json:"nodes"`
	}

	err := codec.WriteJSON(ctx, &connectionInfo{
		Version:  Version,
		Sessions: sessionIds,
		Nodes: func() (replies []*tunnelInfo) {
			for _, node := range nodes {
				replies = append(replies, node.info)
			}

			return replies
		}(),
	})

	return err
}

func queryNode(metricsClient *http.Client, debugURL string) (*remote.NodesInfoReply, error) {
	debugResponse, err := metricsClient.Get(debugURL + "/debug/diag/nodeinfo")

	if err != nil {
		return nil, err
	}

	if debugResponse.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("debug request to %s failed: %s", debugURL, debugResponse.Status)
	}

	var reply remote.NodesInfoReply

	err = json.NewDecoder(debugResponse.Body).Decode(&reply)

	debugResponse.Body.Close()

	if err != nil {
		return nil, err
	}

	return &reply, nil
}

func sendErrorResponse(ctx context.Context, codec rpc.ServerCodec, requestId string, code int64, message string) error {
	return codec.WriteJSON(ctx, &nodeResponse{
		Id: requestId,
		Error: &responseError{
			Code:    code,
			Message: message,
		},
		Last: true,
	})
}

func copyResponseBody(buffer *bytes.Buffer, debugResponse *http.Response) error {
	switch debugResponse.Header.Get("Content-Type") {
	case "application/json":
		_, err := io.Copy(buffer, debugResponse.Body)
		return err
	case "application/octet-stream":
		if _, err := io.Copy(buffer, debugResponse.Body); err != nil {
			return err
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
		if err != nil {
			return err
		}
		buffer.Reset()
		buffer.Write(data)
	case "application/profile":
		if _, err := io.Copy(buffer, debugResponse.Body); err != nil {
			return err
		}
		data, err := json.Marshal(struct {
			Data []byte `json:"chunk"`
		}{
			Data: buffer.Bytes(),
		})
		if err != nil {
			return err
		}
		buffer.Reset()
		buffer.Write(data)
	default:
		return fmt.Errorf("Unhandled content type: %s, from: %s", debugResponse.Header.Get("Content-Type"), debugResponse.Request.URL)
	}
	return nil
}

type tunnelEnode struct {
	Enode        string               `json:"enode,omitempty"`
	Enr          string               `json:"enr,omitempty"`
	Ports        *types.NodeInfoPorts `json:"ports,omitempty"`
	ListenerAddr string               `json:"listener_addr,omitempty"`
}

type tunnelInfo struct {
	Id        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Protocols json.RawMessage `json:"protocols,omitempty"`
	Enodes    []tunnelEnode   `json:"enodes,omitempty"`
}

type tunnelNode struct {
	debugURL string
	info     *tunnelInfo
}

type nodeRequest struct {
	NodeId      string     `json:"nodeId"`
	QueryParams url.Values `json:"queryParams"`
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
