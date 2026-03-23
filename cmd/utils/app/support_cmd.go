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
	"strings"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/debug"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/rpc"
)

// wsMessageSizeLimit caps incoming message size to 32 MB — large enough for
// pprof profiles, binary snapshot chunks, and metric payloads sent over the
// diagnostics tunnel. coder/websocket's default is only 32 KB.
const wsMessageSizeLimit = 32 * 1024 * 1024

var (
	diagnosticsURLFlag = cli.StringFlag{
		Name:     "diagnostics.addr",
		Usage:    "Address of the diagnostics system provided by the support team, include unique session PIN",
		Required: false,
		Value:    "localhost:8080",
	}

	debugURLsFlag = cli.StringSliceFlag{
		Name:     "debug.addrs",
		Usage:    "Comma separated list of URLs to the debug endpoints that are being diagnosed",
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
		_, err := debug.SetupSimple(cliCtx, true /* rootLogger */)
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

type nodeRequest struct {
	NodeId      string     `json:"nodeId"`
	QueryParams url.Values `json:"queryParams"`
}

type nodeResponse struct {
	Id     string          `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *responseError  `json:"error,omitempty"`
	Last   bool            `json:"last"`
}

type tunnelInfo struct {
	Id        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Protocols json.RawMessage `json:"protocols,omitempty"`
	Enodes    []tunnelEnode   `json:"enodes,omitempty"`
}

type tunnelEnode struct {
	Enode        string                    `json:"enode,omitempty"`
	Enr          string                    `json:"enr,omitempty"`
	Ports        *typesproto.NodeInfoPorts `json:"ports,omitempty"`
	ListenerAddr string                    `json:"listener_addr,omitempty"`
}

type requestAction struct {
	requestId   string
	method      string
	queryParams url.Values
}

type responseError struct {
	Code    int64            `json:"code"`
	Message string           `json:"message"`
	Data    *json.RawMessage `json:"data,omitempty"`
}

const Version = 1

func connectDiagnostics(cliCtx *cli.Context) error {
	return ConnectDiagnostics(cliCtx, log.Root())
}

// Setting up the connection to the diagnostics system
// by creating a tunnel between the diagnostics system and the debug endpoints
// (Erigon node)  <-------->  (  Support cmd  )  <--->  (diagnostics system)
// (debug.addrs)  (wss,http)  (current program)  (wss)  ( diagnostics.addr )
func ConnectDiagnostics(cliCtx *cli.Context, logger log.Logger) error {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	debugURLSlice := cliCtx.StringSlice(debugURLsFlag.Name)
	debugURLs := make([]string, 0, len(debugURLSlice))

	for _, debugURL := range debugURLSlice {
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

// Setting up connections to erigon nodes (if more then one is specified in (debug.addrs)
// and send nodes info to diagnostics to notify about connection and nodes details like: enode, enr, ports, listener_addr
//
// Listen for incoming requests from diagnostics system and send them to the incommitg request channel
func tunnel(ctx context.Context, cancel context.CancelFunc, sigs chan os.Signal, diagnosticsUrl string, sessionIds []string, debugURLs []string, logger log.Logger) error {
	go func() {
		select {
		case <-sigs:
			logger.Info("Got interrupt, shutting down...")
			cancel() // Cancel the outer context
		case <-ctx.Done():
			return
		}
	}()

	codec, err := createCodec(ctx, diagnosticsUrl)
	if err != nil {
		return err
	}
	defer codec.Close()

	go func() {
		<-ctx.Done()
		codec.Close()
	}()

	metricsClient := &http.Client{}
	defer metricsClient.CloseIdleConnections()

	connections, err := createConnections(ctx, codec, metricsClient, debugURLs)
	if err != nil {
		return err
	}

	if len(connections) == 0 {
		return nil
	}

	err = sendNodesInfoToDiagnostics(ctx, codec, sessionIds, connections)
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

		for _, request := range requests {
			go handleRequest(ctx, request, connections, codec, logger)
		}
	}
}

func createCodec(ctx context.Context, diagnosticsUrl string) (rpc.ServerCodec, error) {
	conn, err := establishConnection(ctx, diagnosticsUrl)
	if err != nil {
		return nil, err
	}

	codec := rpc.NewWebsocketCodec(conn, "wss://"+diagnosticsUrl, nil, diagnosticsUrl) //TODO: revise why is it so

	return codec, nil
}

// Attempts to establish a WebSocket connection to the diagnostics URL.
// Trying to establish secure wss:// connection first, if it fails, fallback to ws://.
// Returns the WebSocket connection if successful, otherwise an error.
func establishConnection(ctx context.Context, diagnosticsUrl string) (*websocket.Conn, error) {
	var lastErr error
	for _, scheme := range []string{"wss://", "ws://"} {
		conn, resp, err := websocket.Dial(ctx, scheme+diagnosticsUrl, nil)
		if err != nil {
			if resp != nil {
				resp.Body.Close()
			}
			lastErr = err
			continue
		}
		conn.SetReadLimit(wsMessageSizeLimit)
		return conn, nil
	}
	return nil, fmt.Errorf("support connection to %s failed: %w", diagnosticsUrl, lastErr)
}

// Creates connections to the nodes specified by the flag debug.addrs.
// Each node connection is established via its diagnostics WebSocket endpoint.
func createConnections(ctx context.Context, codec rpc.ServerCodec, metricsClient *http.Client, debugURLs []string) ([]*nodeConnection, error) {
	connections := make([]*nodeConnection, 0, len(debugURLs))
	for _, debugURL := range debugURLs {
		connection, err := newNodeConnection(ctx, debugURL, codec, metricsClient)
		if err != nil {
			return nil, err
		}
		connections = append(connections, connection)
	}
	return connections, nil
}

func sendNodesInfoToDiagnostics(ctx context.Context, codec rpc.ServerCodec, sessionIds []string, connections []*nodeConnection) error {
	for _, sessionId := range sessionIds {
		for _, connection := range connections {
			nodeInfo, err := connection.readNodeInfo()
			if err != nil {
				return err
			}
			message := map[string]any{
				"jsonrpc": "2.0",
				"method":  "diag_connect",
				"params": map[string]any{
					"version":   Version,
					"session_id": sessionId,
					"node_info":  nodeInfo,
				},
			}
			if err := codec.WriteJSON(ctx, message); err != nil {
				return err
			}
		}
	}
	return nil
}

func handleRequest(ctx context.Context, request *rpc.JsonRpcMessage, connections []*nodeConnection, codec rpc.ServerCodec, logger log.Logger) {
	if !request.IsNotification() && !request.IsCall() {
		return
	}

	var nodeReq nodeRequest
	if err := json.Unmarshal(request.Params, &nodeReq); err != nil {
		logger.Warn("Failed to parse node request", "err", err)
		return
	}

	for _, connection := range connections {
		if connection.nodeId == nodeReq.NodeId {
			response := connection.handleRequest(ctx, request.ID, request.Method, nodeReq.QueryParams)
			if err := codec.WriteJSON(ctx, response); err != nil {
				logger.Warn("Failed to write response", "err", err)
			}
			return
		}
	}
}

type nodeConnection struct {
	ctx           context.Context
	debugURL      string
	nodeId        string
	connection    *websocket.Conn
	codec         rpc.ServerCodec
	metricsClient *http.Client
	requestId     string
}

func newNodeConnection(ctx context.Context, debugURL string, codec rpc.ServerCodec, metricsClient *http.Client) (*nodeConnection, error) {
	connection := &nodeConnection{ctx: ctx, debugURL: debugURL, codec: codec, metricsClient: metricsClient}
	if err := connection.connect(); err != nil {
		return nil, err
	}
	return connection, nil
}

func (nc *nodeConnection) connect() error {
	nodeInfo, err := nc.readNodeInfo()
	if err != nil {
		return err
	}
	nc.nodeId = nodeInfo.Id
	return nil
}

func (nc *nodeConnection) readNodeInfo() (*tunnelInfo, error) {
	result, err := nc.performRequest(nc.debugURL + "/debug/diag/nodeinfo")
	if err != nil {
		return nil, err
	}
	var nodeInfo tunnelInfo
	if err := json.Unmarshal(result, &nodeInfo); err != nil {
		return nil, err
	}
	return &nodeInfo, nil
}

func (nc *nodeConnection) handleRequest(ctx context.Context, requestId json.RawMessage, method string, queryParams url.Values) *nodeResponse {
	result, err := nc.performRequest(nc.debugURL + "/debug/diag/" + method + "?" + queryParams.Encode())
	if err != nil {
		return &nodeResponse{Id: string(requestId), Error: &responseError{Code: -1, Message: err.Error()}, Last: true}
	}
	return &nodeResponse{Id: string(requestId), Result: result, Last: true}
}

func (nc *nodeConnection) performRequest(requestURL string) (json.RawMessage, error) {
	resp, err := nc.metricsClient.Get(requestURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("request failed: %s", resp.Status)
	}
	return body, nil
}

func (nc *nodeConnection) connectSocket(requestId string) error {
	if nc.connection != nil {
		return nil
	}

	socketURL := strings.Replace(nc.debugURL, "http://", "ws://", 1) + "/debug/diag/ws"
	conn, resp, err := websocket.Dial(nc.ctx, socketURL, nil)
	if err != nil {
		if resp != nil {
			resp.Body.Close()
		}
		return err
	}
	conn.SetReadLimit(wsMessageSizeLimit)

	nc.connection = conn
	nc.requestId = requestId

	go nc.startListening()
	return nil
}

func (nc *nodeConnection) closeWebSocket() error {
	if nc.connection == nil {
		return nil
	}

	err := nc.connection.Close(websocket.StatusNormalClosure, "closing connection")
	if err != nil {
		fmt.Printf("Failed to close connection: %v\n", err)
		return err
	}

	nc.connection = nil
	return nil
}

func (nc *nodeConnection) startListening() {
	for {
		select {
		case <-nc.ctx.Done():
			return
		default:
		}

		if nc.connection == nil {
			return
		}

		_, message, err := nc.connection.Read(nc.ctx)
		if err != nil {
			if websocket.CloseStatus(err) != -1 {
				fmt.Println("Connection closed by peer:", err)
			} else {
				fmt.Println("Error reading message:", err)
			}
			return
		}

		var jsonMessage map[string]any
		if err := json.Unmarshal(message, &jsonMessage); err != nil {
			fmt.Printf("Error unmarshalling message: %v\n", err)
			continue
		}

		jsonMessage["requestId"] = nc.requestId
		if err := nc.codec.WriteJSON(nc.ctx, jsonMessage); err != nil {
			fmt.Printf("Error writing message to codec: %v\n", err)
			return
		}
	}
}
