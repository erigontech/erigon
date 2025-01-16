package diagnostics

import (
	"context"
	"net/http"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/gorilla/websocket"
)

type DiagMessages struct {
	Type    string      `json:"type"`
	Message interface{} `json:"message"`
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// SetupNotifier initializes and starts a WebSocket diagnostics server.
// It listens for incoming WebSocket connections and provides a mechanism
// for notifying clients of diagnostic messages.
//
// Parameters:
//   - rootCtx: The root context to control the server lifecycle.
//   - socketAddr: The address (e.g., "127.0.0.1:6059") where the WebSocket server will listen.
func (d *DiagnosticClient) SetupNotifier(rootCtx context.Context, socketAddr string) {
	diagMux := http.NewServeMux()
	diagMux.HandleFunc("/debug/diag/ws", d.HandleConnections)

	diagServer := &http.Server{
		Addr:    socketAddr,
		Handler: diagMux,
	}

	go func() {
		log.Debug("[Diagnostics] Starting diagnostics websocket server", "address", diagServer.Addr)
		if err := diagServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Debug("[Diagnostics] Error running diagnostics websocket server", "err", err)
		}
	}()

	// Graceful shutdown on context cancellation
	go func() {
		<-rootCtx.Done()
		log.Debug("[Diagnostics] Shutting down diagnostics websocket server")
		if err := diagServer.Shutdown(context.Background()); err != nil {
			log.Debug("[Diagnostics] Error shutting down websocket server", "err", err)
		}
	}()
}

// Notify sends a structured diagnostic message to the connected WebSocket client.
// If no client is currently connected, the message is discarded silently.
//
// Parameters:
//   - msg: A DiagMessages struct containing the type and content of the message.
func (d *DiagnosticClient) Notify(msg DiagMessages) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// If there is no connection, don't bother sending the message
	if d.conn == nil {
		return
	}

	if err := d.conn.WriteJSON(msg); err != nil {
		log.Debug("[Diagnostics] Error writing message to WebSocket client", "err", err)
	}
}

// HandleConnections handles incoming WebSocket connection requests.
// It upgrades an HTTP connection to a WebSocket, listens for incoming messages,
// and handles supported message types.
//
// This function supports the following message types:
//   - TextMessage: Logs the message content.
func (d *DiagnosticClient) HandleConnections(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Debug("[Diagnostics] Error upgrading to WebSocket", "err", err)
		return
	}
	defer conn.Close()

	log.Debug("[Diagnostics] WebSocket client connected")
	d.setConnection(conn)

	for {
		mt, message, err := conn.ReadMessage()
		if err != nil {
			log.Debug("[Diagnostics] WebSocket client disconnected", "err", err)
			d.clearConnection()
			return
		}

		switch mt {
		case websocket.TextMessage:
			log.Debug("[Diagnostics] Received message", "message", string(message))
		case websocket.BinaryMessage:
			log.Debug("[Diagnostics] Binary messages not supported")
			if writeErr := conn.WriteMessage(websocket.TextMessage, []byte("server doesn't support binary messages")); writeErr != nil {
				log.Debug("[Diagnostics] Error responding to binary message", "err", writeErr)
			}
			return
		default:
			log.Debug("[Diagnostics] Unsupported message type", "type", mt)
		}
	}
}

func (d *DiagnosticClient) setConnection(conn *websocket.Conn) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.conn = conn
}

func (d *DiagnosticClient) clearConnection() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.conn = nil
}
