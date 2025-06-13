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
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon-lib/fastjson"
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
		_, _, _, _, err := debug.Setup(cliCtx, true /* rootLogger */)
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
	Enode        string               `json:"enode,omitempty"`
	Enr          string               `json:"enr,omitempty"`
	Ports        *types.NodeInfoPorts `json:"ports,omitempty"`
	ListenerAddr string               `json:"listener_addr,omitempty"`
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
			fmt.Println("Received request", request)
			var requestId string

			if err = fastjson.Unmarshal(request.ID, &requestId); err != nil {
				logger.Error("Invalid request id", "err", err)
				continue
			}

			nodeRequest := nodeRequest{}

			if err = fastjson.Unmarshal(request.Params, &nodeRequest); err != nil {
				logger.Error("Invalid node request", "err", err, "id", requestId)
				continue
			}

			if conn, ok := connections[nodeRequest.NodeId]; ok {
				fmt.Println("Sending request to", conn.debugURL)
				conn.requestChannel <- requestAction{
					requestId:   requestId,
					method:      request.Method,
					queryParams: nodeRequest.QueryParams,
				}
			}
		}
	}
}

// Establishes a WebSocket connection with diagnostics system (flag: diagnostics.addr) and creates a codec for communication.
// This function connects to the diagnostics server using a WebSocket and initializes an RPC codec for bidirectional communication.
func createCodec(ctx context.Context, diagnosticsUrl string) (rpc.ServerCodec, error) {
	conn, err := establishConnection(ctx, diagnosticsUrl)
	if err != nil {
		return nil, err
	}

	codec := rpc.NewWebsocketCodec(conn, "wss://"+diagnosticsUrl, nil) //TODO: revise why is it so

	return codec, nil
}

// Establishes a WebSocket connection with the diagnostics system.
// Trying to establish secure wss:// connection first, if it fails, fallback to ws://.
// Returns the WebSocket connection if successful, otherwise an error.
func establishConnection(ctx context.Context, diagnosticsUrl string) (*websocket.Conn, error) {
	dialer := websocket.Dialer{
		ReadBufferSize:  wsReadBuffer,
		WriteBufferSize: wsWriteBuffer,
		WriteBufferPool: wsBufferPool,
	}

	var conn *websocket.Conn
	var resp *http.Response
	var err error

	// Attempt to establish a secure WebSocket connection (wss://)
	conn, resp, err = dialer.DialContext(ctx, "wss://"+diagnosticsUrl, nil)
	if err != nil {
		conn, resp, err = dialer.DialContext(ctx, "ws://"+diagnosticsUrl, nil)
	}

	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()

	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusSwitchingProtocols {
		return nil, fmt.Errorf("support request to %s failed: %s", diagnosticsUrl, resp.Status)
	}

	return conn, nil
}

// Creates connections to the nodes specified by the flag debug.addrs.
// Returns a map of node connections, where the key is the node ID.
// As soon as connection created for a node, it starts processing requests and responses.
func createConnections(ctx context.Context, codec rpc.ServerCodec, metricsClient *http.Client, debugURLs []string) (map[string]*nodeConnection, error) {
	nodes := map[string]*nodeConnection{}

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
				node := &nodeConnection{
					ctx:             ctx,
					debugURL:        debugURL,
					info:            &tunnelInfo{Id: ni.Id, Name: ni.Name, Protocols: ni.Protocols},
					requestChannel:  make(chan requestAction, 100),
					responseChannel: make(chan nodeResponse, 100),
					codec:           codec,
				}
				go node.processRequests(metricsClient)
				go node.processResponses()
				nodes[ni.Id] = node
			}
		}
	}

	return nodes, nil
}

// Attempt to query nodes specified by flag debug.addrs and return the response.
// If the request fails, an error is returned, as we expect all nodes to be reachable.
// TODO: maybe it make sense to think about allowing some nodes to be unreachable
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

// Send nodes info to diagnostics to notify about connection and nodes details like: enode, enr, ports, listener_addr
// This info will tell diagnostics about the nodes that are connected to the diagnostics system
func sendNodesInfoToDiagnostics(ctx context.Context, codec rpc.ServerCodec, sessionIds []string, nodes map[string]*nodeConnection) error {
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

type nodeConnection struct {
	ctx             context.Context
	debugURL        string
	info            *tunnelInfo
	connection      *websocket.Conn
	requestId       string
	requestChannel  chan requestAction
	responseChannel chan nodeResponse
	codec           rpc.ServerCodec
}

// Connects to the WebSocket endpoint of the node and starts listening for incoming messages.
// (erigon nodes) ----> (support cmd)
func (nc *nodeConnection) connectSocket(requestId string) error {
	//already connected
	if nc.connection != nil {
		return nil
	}

	socketURL := strings.Replace(nc.debugURL, "http://", "ws://", 1) + "/debug/diag/ws"
	conn, resp, err := websocket.DefaultDialer.Dial(socketURL, nil)
	defer resp.Body.Close()
	if err != nil {
		return err
	}

	nc.connection = conn
	nc.requestId = requestId

	go nc.startListening()

	return nil
}

func (nc *nodeConnection) closeWebSocket() error {
	if nc.connection == nil {
		return nil
	}

	err := nc.connection.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "closing connection"))
	if err != nil {
		fmt.Printf("Failed to send close message: %v\n", err)
	}

	err = nc.connection.Close()
	if err != nil {
		fmt.Printf("Failed to close connection: %v\n", err)
		return err
	}

	nc.connection = nil
	fmt.Println("WebSocket connection closed successfully")
	return nil
}

// Starts listening for incoming messages from the WebSocket connection.
// (erigon nodes) ----> (support cmd)
func (nc *nodeConnection) startListening() {
	for {
		select {
		case <-nc.ctx.Done():
			return
		default:
		}
		if nc.connection == nil {
			fmt.Println("connection closed, exiting read loop")
			return
		}

		_, message, err := nc.connection.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) ||
				websocket.IsUnexpectedCloseError(err) {
				fmt.Println("Connection closed by peer:", err)
				return
			}

			fmt.Println("Error reading message:", err)
			return
		}

		nc.responseChannel <- nodeResponse{
			Id:     nc.requestId,
			Result: message,
			Last:   false,
		}
	}
}

type subscrigeResponse struct {
	MessageType string `json:"type"`
	Message     string `json:"message"`
}

// Processes the incoming requests from the diagnostics system.
// Handle subscribe/unsubscribe and all other messages.
func (nc *nodeConnection) processRequests(metricsClient *http.Client) {
	for action := range nc.requestChannel {
		select {
		case <-nc.ctx.Done():
			return
		default:
		}

		switch {
		case isSubscribe(action.method):
			err := nc.connectSocket(action.requestId)
			if err != nil {
				nc.responseChannel <- errorResponseMessage(action.requestId, http.StatusFailedDependency, fmt.Sprintf("Subscription failed: %v", err))
				continue
			}

			fmt.Println("Subscribed to", nc.debugURL)

			response := subscrigeResponse{
				MessageType: "subscribe",
				Message:     "subscribed",
			}

			bytes := &bytes.Buffer{}

			if err := json.NewEncoder(bytes).Encode(response); err != nil {
				nc.responseChannel <- errorResponseMessage(action.requestId, http.StatusInternalServerError, "Failed to encode response: "+err.Error())
				continue
			}

			nc.responseChannel <- nodeResponse{
				Id:     action.requestId,
				Result: json.RawMessage(bytes.Bytes()),
				Last:   false,
			}

		case isUnsubscribe(action.method):
			err := nc.closeWebSocket()
			if err != nil {
				nc.responseChannel <- errorResponseMessage(action.requestId, http.StatusFailedDependency, fmt.Sprintf("Unsubscription failed: %v", err))
				continue
			}

			fmt.Println("Unsubscribed from", nc.debugURL)
			response := subscrigeResponse{
				MessageType: "subscribe",
				Message:     "unsubscribed",
			}

			bytes := &bytes.Buffer{}

			if err := json.NewEncoder(bytes).Encode(response); err != nil {
				nc.responseChannel <- errorResponseMessage(action.requestId, http.StatusInternalServerError, "Failed to encode response: "+err.Error())
				continue
			}

			nc.responseChannel <- nodeResponse{
				Id:     action.requestId,
				Result: json.RawMessage(bytes.Bytes()),
				Last:   true,
			}

		default:
			debugURL := nc.debugURL + "/debug/diag/" + action.method + "?" + action.queryParams.Encode()
			debugResponse, err := metricsClient.Get(debugURL)
			if err != nil {
				nc.responseChannel <- errorResponseMessage(action.requestId, http.StatusFailedDependency, "Request failed: "+err.Error())
				debugResponse.Body.Close()
				continue
			}

			if debugResponse.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(debugResponse.Body)
				nc.responseChannel <- errorResponseMessage(action.requestId, int64(debugResponse.StatusCode), "Request failed: "+string(body))
				debugResponse.Body.Close()
				continue
			}

			buffer := &bytes.Buffer{}
			if err := copyResponseBody(buffer, debugResponse); err != nil {
				nc.responseChannel <- errorResponseMessage(action.requestId, http.StatusInternalServerError, "Request failed: "+err.Error())
				debugResponse.Body.Close()
				continue
			}

			debugResponse.Body.Close()
			nc.responseChannel <- nodeResponse{
				Id:     action.requestId,
				Result: json.RawMessage(buffer.Bytes()),
				Last:   true,
			}
		}
	}
}

// Sends responses to the diagnostics system.
// (support cmd) ----> (diagnostics system)
func (nc *nodeConnection) processResponses() {
	for response := range nc.responseChannel {
		select {
		case <-nc.ctx.Done():
			return
		default:
		}

		if err := nc.codec.WriteJSON(nc.ctx, response); err != nil {
			fmt.Println("Failed to send response", err)
		}
	}
}

// detect is the method is a txpool subscription
// TODO: implementation of othere subscribtions (e.g. downloader)
// TODO: change subscribe from plain string to something more structured
func isSubscribe(method string) bool {
	return method == "subscribe/txpool"
}

func isUnsubscribe(method string) bool {
	return method == "unsubscribe/txpool"
}

func errorResponseMessage(requestId string, code int64, message string) nodeResponse {
	return nodeResponse{
		Id: requestId,
		Error: &responseError{
			Code:    code,
			Message: message,
		},
		Last: true,
	}
}

// Processes and copies the HTTP response body to a buffer based on content type.
// Supported Content Types: application/json, application/octet-stream, application/profile
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
		data, err := fastjson.Marshal(struct {
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
		data, err := fastjson.Marshal(struct {
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
		return fmt.Errorf("unhandled content type: %s, from: %s", debugResponse.Header.Get("Content-Type"), debugResponse.Request.URL)
	}
	return nil
}
